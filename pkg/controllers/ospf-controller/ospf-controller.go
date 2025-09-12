/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ospfctrl

import (
	"context"
	"encoding/json"
	"fmt"

	"os"
	"time"

	dockerUtil "example.com/go-util/pkg/util/docker"

	dockerSDK "github.com/docker/docker/client"
	"golang.org/x/time/rate"

	pkgreconcile "k8s.io/sample-controller/pkg/reconcile"
	pkgutils "k8s.io/sample-controller/pkg/utils"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	clientset "k8s.io/sample-controller/pkg/generated/clientset/versioned"
	samplescheme "k8s.io/sample-controller/pkg/generated/clientset/versioned/scheme"
	v1alpha1Informer "k8s.io/sample-controller/pkg/generated/informers/externalversions/networking/v1alpha1"
	v1alpha1Lister "k8s.io/sample-controller/pkg/generated/listers/networking/v1alpha1"
)

const controllerAgentName = "ospf-controller"

const (
	LabelResourceId     = "networkplan.networking.dn42.io/resource-id"
	LabelConfigHash     = "networkplan.networking.dn42.io/config-hash"
	LabelIsControlledBy = "networkplan.networking.dn42.io/is-controlled-by"
)

const (
	AnnotationObservedGeneration = "networkplan.networking.dn42.io/parent-observed-generation"
)

const (
	// FieldManager distinguishes this controller from other things writing to API objects
	FieldManager = controllerAgentName
)

// Controller is the controller implementation for WireGuardInterface resources
type Controller struct {
	vtyshPath string
	// ns is the namespace where the controller is working on
	ns           string
	nodeName     string
	dockerClient *dockerSDK.Client
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	sampleclientset clientset.Interface

	nlLister v1alpha1Lister.OSPFProtocolLister
	nlSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.TypedRateLimitingInterface[cache.ObjectName]
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder   record.EventRecorder
	reconciler *pkgreconcile.FRROSPFv2Reconciler
}

type ControllerConfig struct {
	Kubeclientset   kubernetes.Interface
	Sampleclientset clientset.Interface
	NetlinkInformer v1alpha1Informer.OSPFProtocolInformer
	NodeName        string
	Namespace       string
	VtyshPath       string
}

// NewController returns a new WireGuardInterface controller
func NewController(
	ctx context.Context,
	config ControllerConfig,
) *Controller {
	logger := klog.FromContext(ctx)

	// Create event broadcaster
	// Add WireGuardInterface types to the default Kubernetes Scheme so Events can be
	// logged for WireGuardInterface types.
	utilruntime.Must(samplescheme.AddToScheme(scheme.Scheme))
	logger.V(4).Info("Creating event broadcaster")

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: config.Kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName, Host: config.NodeName})
	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](5*time.Millisecond, 1000*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)},
	)

	dockerClient, err := dockerUtil.NewDefaultDockerClient()
	if err != nil {
		logger.Error(err, "Error creating docker client")
		return nil
	}

	if config.VtyshPath == "" {
		logger.Error(fmt.Errorf("vtysh path is not set"), "Error creating vtysh client")
		return nil
	}

	reconciler, err := pkgreconcile.NewFRROSPFv2Reconciler(config.VtyshPath)
	if err != nil {
		logger.Error(err, "Error creating reconciler", "vtyshPath", config.VtyshPath)
		return nil
	}

	controller := &Controller{
		dockerClient:    dockerClient,
		kubeclientset:   config.Kubeclientset,
		sampleclientset: config.Sampleclientset,
		nlLister:        config.NetlinkInformer.Lister(),
		nlSynced:        config.NetlinkInformer.Informer().HasSynced,
		workqueue:       workqueue.NewTypedRateLimitingQueue(ratelimiter),
		recorder:        recorder,
		nodeName:        config.NodeName,
		ns:              config.Namespace,
		vtyshPath:       config.VtyshPath,
		reconciler:      reconciler,
	}

	logger.Info("Setting up event handlers")

	// Set up event handler for when WireGuardNetworkPlan resources change
	config.NetlinkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			nlObj, _ := obj.(*networkingv1alpha1.OSPFProtocol)
			revLog := pkgutils.RevChangeLog{
				Generation:         fmt.Sprintf("%d", nlObj.GetGeneration()),
				ResourceVersion:    nlObj.GetResourceVersion(),
				ObservedGeneration: fmt.Sprintf("%d", nlObj.Status.ObservedGeneration),
			}
			revLogJSON, _ := json.Marshal(revLog)
			logger.Info("AddFunc for NetlinkInterface resource is called", "objectReference", klog.KObj(nlObj), "Revision log", string(revLogJSON))
			controller.enqueueOSPFProtocol(nlObj)
		},
		UpdateFunc: func(old, new interface{}) {
			oldNl := old.(*networkingv1alpha1.OSPFProtocol)
			newNl := new.(*networkingv1alpha1.OSPFProtocol)

			revisionChanged := newNl.GetResourceVersion() != oldNl.GetResourceVersion()
			if revisionChanged {
				logger.Info("Revision changed", "old", oldNl.ResourceVersion, "new", newNl.ResourceVersion, "objectReference", klog.KObj(newNl))
			}

			changelog := pkgutils.RevChangeLog{
				Generation:         fmt.Sprintf("%d -> %d", oldNl.GetGeneration(), newNl.GetGeneration()),
				ResourceVersion:    fmt.Sprintf("%s -> %s", oldNl.GetResourceVersion(), newNl.GetResourceVersion()),
				ObservedGeneration: fmt.Sprintf("%d -> %d", oldNl.Status.ObservedGeneration, newNl.GetGeneration()),
			}
			changelogJSON, _ := json.Marshal(changelog)
			logger.Info("UpdateFunc for NetlinkInterface resource is called", "objectReference", klog.KObj(newNl), "Revision change log", string(changelogJSON))

			if !revisionChanged {
				logger.Info("Updating OSPFProtocol due to force resync, and resourceVersion is not changed", "objectReference", klog.KObj(newNl))
				if err := controller.updateOSPFProtocolStatus(context.Background(), newNl); err != nil {
					logger.Error(err, "Failed to update NetlinkInterface status", "objectReference", newNl.Name, "object is enqueued, and will retry later")
					// if failed to update status, simply give up rather than retry, because there's still next force-resync
				}
				return
			}

			logger.Info("Updating OSPFProtocol due to resourceVersion is changed", "objectReference", klog.KObj(newNl))
			controller.enqueueOSPFProtocol(new)
		},
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	hostname, err := os.Hostname()
	if err != nil {
		logger.Error(err, "Error getting hostname")
		return nil
	}

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting controller", "hostname", hostname, "nodeName", c.nodeName)

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(),
		c.nlSynced,
	); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch two workers to process WireGuardInterface resources
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	logger.Info("Started workers")
	<-ctx.Done()
	logger.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	objRef, shutdown := c.workqueue.Get()
	logger := klog.FromContext(ctx)

	if shutdown {
		return false
	}

	// We call Done at the end of this func so the workqueue knows we have
	// finished processing this item. We also must remember to call Forget
	// if we do not want this work item being re-queued. For example, we do
	// not call Forget if a transient error occurs, instead the item is
	// put back on the workqueue and attempted again after a back-off
	// period.
	defer c.workqueue.Done(objRef)

	// Run the syncHandler, passing it the structured reference to the object to be synced.
	err := c.syncHandler(ctx, objRef)
	if err == nil {
		// If no error occurs then we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(objRef)
		logger.Info("Successfully synced", "objectName", objRef)
		return true
	}
	// there was a failure so be sure to report it.  This method allows for
	// pluggable error handling which can be used for things like
	// cluster-monitoring.
	utilruntime.HandleErrorWithContext(ctx, err, "Error syncing; requeuing for later retry", "objectReference", objRef)
	// since we failed, we should requeue the item to work on later.  This
	// method will add a backoff to avoid hotlooping on particular items
	// (they're probably still not going to work right away) and overall
	// controller protection (everything I've done is broken, this controller
	// needs to calm down or it can starve other useful work) cases.
	c.workqueue.AddRateLimited(objRef)
	return true
}

// enqueueWG takes a WireGuardInterface resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than WireGuardInterface.
func (c *Controller) enqueueOSPFProtocol(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.AddRateLimited(objectRef)
	}
}

func (c *Controller) syncHandler(ctx context.Context, objectRef cache.ObjectName) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "objectRef", objectRef)

	logger.V(4).Info("Processing netlinkinterface object update/creation", "object", objectRef.Name)

	nlObj, err := c.nlLister.OSPFProtocols(c.ns).Get(objectRef.Name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "NetlinkInterface referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	nodeName := nlObj.Spec.Node
	if c.nodeName != nodeName {
		logger.V(4).Info("This node is not responsible for this WireGuardInterface", "objectReference", objectRef)
		return nil
	}

	deletionTime := nlObj.GetDeletionTimestamp()
	if deletionTime != nil {
		// Clean up underlying resources, then
		// clear all finalizers from the object

		err := c.reconciler.CleanUpResource(ctx, &nlObj.Spec)
		if err != nil {
			return fmt.Errorf("failed to clean up underlying resources: %s", err.Error())
		}

		nlObjCopy := nlObj.DeepCopy()
		nlObjCopy.SetFinalizers([]string{})
		_, err = c.sampleclientset.NetworkingV1alpha1().OSPFProtocols(c.ns).Update(context.Background(), nlObjCopy, metav1.UpdateOptions{})
		if err != nil {
			if !k8serrors.IsNotFound(err) {
				return fmt.Errorf("failed to clear finalizers from OSPFProtocol, will retry: %s", err.Error())
			}
		}

		return nil
	}

	needReconcile := nlObj.GetGeneration() != nlObj.Status.ObservedGeneration
	if needReconcile {
		logger.Info("Need to reconcile", "objectReference", klog.KObj(nlObj), "observedGeneration", nlObj.Status.ObservedGeneration, "generation", nlObj.GetGeneration())

		hasUpdates, err := c.reconciler.DetectChanges(ctx, &nlObj.Spec, nil)
		if err != nil {
			return fmt.Errorf("failed to detect changes: %s", err.Error())
		}

		if hasUpdates {
			maxLoops := 10
			for hasUpdates && maxLoops > 0 {
				err = c.reconciler.ApplyReconcile(ctx, &nlObj.Spec)
				if err != nil {
					return fmt.Errorf("failed to apply reconcile: %s", err.Error())
				}

				c.reconciler.ResetState()
				hasUpdates, err = c.reconciler.DetectChanges(ctx, &nlObj.Spec, nil)
				if err != nil {
					return fmt.Errorf("failed to detect changes: %s", err.Error())
				}

				maxLoops--
			}

			if maxLoops == 0 && hasUpdates {
				return fmt.Errorf("failed to apply reconcile: %s", "out of max loops")
			}
		}

		logger.Info("Updating OSPFProtocol status", "objectReference", klog.KObj(nlObj))
		// Update the status with current NetlinkInterface information
		err = c.updateOSPFProtocolStatus(ctx, nlObj)
		if err != nil {
			return fmt.Errorf("failed to update OSPFProtocol status: %s", err.Error())
		}
	}

	return nil
}

// updateWireGuardNetworkPlanStatus updates the status of a WireGuardNetworkPlan with current information
func (c *Controller) updateOSPFProtocolStatus(ctx context.Context, nlObj *networkingv1alpha1.OSPFProtocol) error {
	logger := klog.FromContext(ctx)

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	nlObjCopy := nlObj.DeepCopy()

	status := new(networkingv1alpha1.OSPFProtocolStatus)
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %s", err.Error())
	}

	status.Hostname = hostname
	status.Nodename = c.nodeName

	// todo: check if there are any other fields to be added to the status

	hasUpdates, err := c.reconciler.DetectChanges(ctx, &nlObj.Spec, status)
	if err != nil {
		return fmt.Errorf("failed to detect changes: %s", err.Error())
	}

	if !hasUpdates {
		status.ObservedGeneration = nlObj.GetGeneration()
	}
	// otherwise, if hasUpdates is true, the status.ObservedGeneration will be the zero value, which
	// is definitely unmatch with metadata.Generation, so that will trigger the reconcile on the next force-resync.

	// Update the status
	nlObjCopy.Status = *status

	// Use UpdateStatus to update only the Status block of the OSPFProtocol resource
	_, err = c.sampleclientset.NetworkingV1alpha1().OSPFProtocols(c.ns).UpdateStatus(ctx, nlObjCopy, metav1.UpdateOptions{FieldManager: FieldManager})
	if err != nil {
		return fmt.Errorf("failed to update status: %s", err.Error())
	}

	logger.V(4).Info("Updated NetlinkInterface status", "objectReference", klog.KObj(nlObjCopy))
	return nil
}
