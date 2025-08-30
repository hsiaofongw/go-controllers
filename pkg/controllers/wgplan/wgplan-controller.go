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

package wgplan

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	dockerUtil "example.com/go-util/pkg/util/docker"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	dockerSDK "github.com/docker/docker/client"
	"golang.org/x/time/rate"

	pkgutils "k8s.io/sample-controller/pkg/utils"

	"github.com/cbergoon/merkletree"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	secretsinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	secretlisters "k8s.io/client-go/listers/core/v1"
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

const controllerAgentName = "sample-controller"

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
	dockerClient *dockerSDK.Client
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	sampleclientset clientset.Interface

	secretsLister secretlisters.SecretLister
	wgLister      v1alpha1Lister.WireGuardInterfaceLister
	wgPlanLister  v1alpha1Lister.WireGuardNetworkPlanLister

	wgSynced      cache.InformerSynced
	secretsSynced cache.InformerSynced
	wgPlanSynced  cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.TypedRateLimitingInterface[cache.ObjectName]
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

type ControllerConfig struct {
	Kubeclientset   kubernetes.Interface
	Sampleclientset clientset.Interface
	WgInformer      v1alpha1Informer.WireGuardInterfaceInformer
	WgPlanInformer  v1alpha1Informer.WireGuardNetworkPlanInformer
	SecretsInformer secretsinformers.SecretInformer
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
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})
	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](5*time.Millisecond, 1000*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)},
	)

	dockerClient, err := dockerUtil.NewDefaultDockerClient()
	if err != nil {
		logger.Error(err, "Error creating docker client")
		return nil
	}

	controller := &Controller{
		dockerClient:    dockerClient,
		kubeclientset:   config.Kubeclientset,
		sampleclientset: config.Sampleclientset,
		wgLister:        config.WgInformer.Lister(),
		wgPlanLister:    config.WgPlanInformer.Lister(),
		secretsLister:   config.SecretsInformer.Lister(),
		wgSynced:        config.WgInformer.Informer().HasSynced,
		wgPlanSynced:    config.WgPlanInformer.Informer().HasSynced,
		secretsSynced:   config.SecretsInformer.Informer().HasSynced,
		workqueue:       workqueue.NewTypedRateLimitingQueue(ratelimiter),
		recorder:        recorder,
	}

	logger.Info("Setting up event handlers")

	// Set up event handler for when WireGuardNetworkPlan resources change
	config.WgPlanInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			objWgPlan, _ := obj.(*networkingv1alpha1.WireGuardNetworkPlan)
			revLog := pkgutils.RevChangeLog{
				Generation:         fmt.Sprintf("%d", objWgPlan.GetGeneration()),
				ResourceVersion:    objWgPlan.GetResourceVersion(),
				ObservedGeneration: fmt.Sprintf("%d", objWgPlan.Status.ObservedGeneration),
			}
			revLogJSON, _ := json.Marshal(revLog)
			logger.Info("AddFunc for WireGuardNetworkPlan resource is called", "objectReference", klog.KObj(objWgPlan), "Revision log", string(revLogJSON))
			controller.enqueueWG(objWgPlan)
		},
		UpdateFunc: func(old, new interface{}) {
			oldWG := old.(*networkingv1alpha1.WireGuardNetworkPlan)
			newWG := new.(*networkingv1alpha1.WireGuardNetworkPlan)

			revisionChanged := newWG.GetResourceVersion() != oldWG.GetResourceVersion()
			if revisionChanged {
				logger.Info("Revision changed", "old", oldWG.ResourceVersion, "new", newWG.ResourceVersion, "objectReference", klog.KObj(newWG))
			}

			changelog := pkgutils.RevChangeLog{
				Generation:         fmt.Sprintf("%d -> %d", oldWG.GetGeneration(), newWG.GetGeneration()),
				ResourceVersion:    fmt.Sprintf("%s -> %s", oldWG.GetResourceVersion(), newWG.GetResourceVersion()),
				ObservedGeneration: fmt.Sprintf("%d -> %d", oldWG.Status.ObservedGeneration, newWG.GetGeneration()),
			}
			changelogJSON, _ := json.Marshal(changelog)
			logger.Info("UpdateFunc for WireGuardNetworkPlan resource is called", "objectReference", klog.KObj(newWG), "Revision change log", string(changelogJSON))

			if !revisionChanged {
				logger.Info("Updating WireGuardNetworkPlan due to force resync, and resourceVersion is not changed", "objectReference", klog.KObj(newWG))
				if err := controller.updateWireGuardNetworkPlanStatus(context.Background(), newWG); err != nil {
					logger.Error(err, "Failed to update WireGuardInterface status", "objectReference", newWG.Name, "object is enqueued, and will retry later")
					// if failed to update status, simply give up rather than retry, because there's still next force-resync
				}
				return
			}

			logger.Info("Updating WireGuardNetworkPlan due to resourceVersion is changed", "objectReference", klog.KObj(newWG))
			controller.enqueueWG(new)
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
	logger.Info("Starting controller", "hostname", hostname)

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(),
		c.wgSynced,
		c.secretsSynced,
		c.wgPlanSynced,
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
func (c *Controller) enqueueWG(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.AddRateLimited(objectRef)
	}
}

func (c *Controller) syncHandler(ctx context.Context, objectRef cache.ObjectName) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "objectRef", objectRef)

	logger.V(4).Info("Processing wgnetworkplan object update/creation", "object", objectRef.Name)

	wgPlanObj, err := c.wgPlanLister.Get(objectRef.Name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "WireGuardNetworkPlan referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	selectorOfThis := labels.SelectorFromSet(labels.Set{
		LabelIsControlledBy: wgPlanObj.Name,
	})

	deletionTime := wgPlanObj.GetDeletionTimestamp()
	if deletionTime != nil {
		// Clean up underlying resources, then
		// clear all finalizers from the object

		wgIntfObjs, err := c.wgLister.List(selectorOfThis)
		if err != nil {
			logger.Error(err, "Failed to list WireGuardInterface resources for cleanup", "objectReference", klog.KObj(wgPlanObj))
		}

		for _, wgIntfObj := range wgIntfObjs {
			err := c.sampleclientset.NetworkingV1alpha1().WireGuardInterfaces().Delete(context.Background(), wgIntfObj.Name, metav1.DeleteOptions{})
			if err != nil {
				logger.Error(err, "Failed to delete WireGuardInterface resource for cleanup", "objectReference", klog.KObj(wgIntfObj))
			}
		}

		wgPlanObjCopy := wgPlanObj.DeepCopy()
		wgPlanObjCopy.SetFinalizers([]string{})
		_, err = c.sampleclientset.NetworkingV1alpha1().WireGuardNetworkPlans().Update(context.Background(), wgPlanObjCopy, metav1.UpdateOptions{})
		if err != nil {
			if !k8serrors.IsNotFound(err) {
				return fmt.Errorf("failed to clear finalizers from WireGuardNetworkPlan, will retry: %s", err.Error())
			}
		}

		return nil
	}

	needReconcile := wgPlanObj.GetGeneration() != wgPlanObj.Status.ObservedGeneration
	if needReconcile {
		logger.Info("Need to reconcile", "objectReference", klog.KObj(wgPlanObj), "observedGeneration", wgPlanObj.Status.ObservedGeneration, "generation", wgPlanObj.GetGeneration())

		// 1. work out an actual plan from the spec
		actualPlan, err := c.NewWGActualPlanFromObj(wgPlanObj)
		if err != nil {
			return fmt.Errorf("failed to work out an actual plan from the spec: %s", err.Error())
		}

		// 2. query the lister to get depedent WireGuardInterface resources that are controlled by this
		wgIntfObjs, err := c.wgLister.List(selectorOfThis)
		if err != nil {
			if !k8serrors.IsNotFound(err) {
				return fmt.Errorf("failed to list WireGuardInterface resources: %s", err.Error())
			}
			wgIntfObjs = make([]*networkingv1alpha1.WireGuardInterface, 0)
		}

		// 3. comparing two sets of resources, and generate a resourceSet (that tells us how to reconcile)
		resourceSet, err := c.comparingResourceSets(actualPlan.Interfaces, wgIntfObjs)
		if err != nil {
			return fmt.Errorf("failed to compare resource sets: %s", err.Error())
		}

		// 4. create or update the WireGuardInterface resources that are needed
		for _, item := range resourceSet.ShouldBeAdded {
			wgActualIntfObj := item.(*WGActualPlanInterface)
			wgIntfObj := wgActualIntfObj.ToWireGuardInterfaceObject(wgPlanObj.GetUID(), wgPlanObj.Name, wgPlanObj.GetGeneration())
			_, err := c.sampleclientset.NetworkingV1alpha1().WireGuardInterfaces().Create(ctx, wgIntfObj, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("failed to create WireGuardInterface resource: %s", err.Error())
			}
		}

		for _, item := range resourceSet.ShouldBeRemoved {
			wgIntfObj := item.(*networkingv1alpha1.WireGuardInterface)
			err := c.sampleclientset.NetworkingV1alpha1().WireGuardInterfaces().Delete(ctx, wgIntfObj.Name, metav1.DeleteOptions{})
			if err != nil {
				return fmt.Errorf("failed to delete WireGuardInterface resource: %s", err.Error())
			}
		}

		for _, item := range resourceSet.ShouldBeUpdated {
			wgActualIntfObj := item.(*WGActualPlanInterface)
			wgIntfObj := wgActualIntfObj.ToWireGuardInterfaceObject(wgPlanObj.GetUID(), wgPlanObj.Name, wgPlanObj.GetGeneration())
			_, err := c.sampleclientset.NetworkingV1alpha1().WireGuardInterfaces().Update(ctx, wgIntfObj, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to update WireGuardInterface resource: %s", err.Error())
			}
		}
	}

	logger.Info("Updating WireGuardNetworkPlan status", "objectReference", klog.KObj(wgPlanObj))
	// Update the status with current WireGuard interface information
	err = c.updateWireGuardNetworkPlanStatus(ctx, wgPlanObj)
	if err != nil {
		return fmt.Errorf("failed to update WireGuardNetworkPlan status: %s", err.Error())
	}

	return nil
}

// updateWireGuardNetworkPlanStatus updates the status of a WireGuardNetworkPlan with current information
func (c *Controller) updateWireGuardNetworkPlanStatus(ctx context.Context, wgPlanObj *networkingv1alpha1.WireGuardNetworkPlan) error {
	logger := klog.FromContext(ctx)

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	wgPlanObjCopy := wgPlanObj.DeepCopy()

	wgIntfObjs, err := c.wgLister.List(labels.SelectorFromSet(labels.Set{
		LabelIsControlledBy: wgPlanObj.Name,
	}))
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return fmt.Errorf("failed to list WireGuardInterface resources: %s", err.Error())
		}
	}

	status, err := c.getCurrentWireGuardNetworkPlanStatus(ctx, wgIntfObjs)
	if err != nil {
		logger.Error(err, "Failed to get current WireGuardNetworkPlan status", "objectReference", klog.KObj(wgPlanObj))
		// Don't fail the entire sync if status update fails
		return nil
	}

	// Update the status
	wgPlanObjCopy.Status = *status

	// Use UpdateStatus to update only the Status block of the WireGuardNetworkPlan resource
	_, err = c.sampleclientset.NetworkingV1alpha1().WireGuardNetworkPlans().UpdateStatus(ctx, wgPlanObjCopy, metav1.UpdateOptions{FieldManager: FieldManager})
	if err != nil {
		return fmt.Errorf("failed to update status: %s", err.Error())
	}

	logger.V(4).Info("Updated WireGuardNetworkPlan status", "objectReference", klog.KObj(wgPlanObj))
	return nil
}

func (c *Controller) getCurrentWireGuardNetworkPlanStatus(ctx context.Context, wgIntfObjs []*networkingv1alpha1.WireGuardInterface) (*networkingv1alpha1.WireGuardNetworkPlanStatus, error) {
	logger := klog.FromContext(ctx)

	status := new(networkingv1alpha1.WireGuardNetworkPlanStatus)

	generations := make([]int64, 0)

	for _, wgIntfObj := range wgIntfObjs {

		annotations := wgIntfObj.GetAnnotations()
		if annotations != nil {
			if genstr, ok := annotations[AnnotationObservedGeneration]; ok {
				if gen, err := strconv.ParseInt(genstr, 10, 64); err == nil {
					generations = append(generations, gen)
				}
			}
		}

		intfStatus := networkingv1alpha1.WireGuardNetworkPlanInterfaceStatus{
			WGObjectRef: wgIntfObj.Name,
			NodeName:    wgIntfObj.Status.Nodename,
			HostName:    wgIntfObj.Status.Hostname,
		}

		wgStatus := wgIntfObj.Status.WireGuard
		if wgStatus != nil {
			intfStatus.ListenPort = wgStatus.ListenPort
			intfStatus.PublicKey = wgStatus.PublicKey
			intfStatus.PrivateKey = wgStatus.PrivateKey
			if len(wgStatus.Peers) > 0 {
				peer1 := wgStatus.Peers[0]
				if peer1.LastHandshakeTime != nil {
					intfStatus.LastHandshake = peer1.LastHandshakeTime
				}
			}
		}

		nlStatus := wgIntfObj.Status.Netlink
		if nlStatus != nil {
			mtu := nlStatus.MTU
			intfStatus.MTU = &mtu

			if len(nlStatus.Addrs) > 0 {
				for _, addr := range nlStatus.Addrs {
					if addr.Peer != nil {
						peerAddr := fmt.Sprintf("%s/%d", *addr.Peer, addr.Prefixlen)
						intfStatus.Addresses = append(intfStatus.Addresses, fmt.Sprintf("%s -> %s", addr.Local, peerAddr))
					} else {
						intfStatus.Addresses = append(intfStatus.Addresses, fmt.Sprintf("%s/%d", addr.Local, addr.Prefixlen))
					}
				}
			}
		}

		status.Interfaces = append(status.Interfaces, intfStatus)
	}

	if len(generations) > 0 {
		minGen := generations[0]
		for _, gen := range generations {
			if gen < minGen {
				minGen = gen
			}
		}
		status.ObservedGeneration = minGen
		logger.Info("Set ObservedGeneration to", "generation", status.ObservedGeneration)
	}

	return status, nil
}

type WGActualPlanInterface struct {
	Node          string
	InterfaceName string

	// Addresses are the addresses that would be actually assigned to the WireGuard interface created on the node/container.
	Addresses []networkingv1alpha1.WireGuardInterfaceAddressSpec

	// MTU is the MTU of the actual WireGuard interface created on the node/container, default to 1420, not 0.
	MTU int

	// MoveToContainer is a flag to indicate whether the node should be moved to a container once just created.
	MoveToContainer bool

	// Container is where to host the WireGuardInterface when provided, otherwise it's in the host netns.
	Container *networkingv1alpha1.WireGuardInterfaceContainerSpec

	// Name of underlying WireGuardInterface resource.
	WGIntfName string

	// if no listen port, means that the node is behind a NAT.
	ListenPort *int

	// same as the listen port, if the node is behind a NAT, the hostname can be left empty.
	Hostname string

	// WireGuard public key, in base64 format.
	PublicKey string

	// WireGuard private key, in base64 format.
	PrivateKey string

	// The ResourceId will be assigned to the dependent WireGuardInterface resources that
	// are owned by the WireGuardNetworkPlan resources created by this controller.
	// We use this are the key to track what WireGuardInterface resources are needed to be created or deleted.
	ResourceId string

	// Same like the ResourceId, we use this field to track what WireGuardInterface resources are needed
	// for a update, if the ConfigHash of the WireGuardInterface resource doesn't match that of this one,
	// then it is the moment to reconcile the spec of the WireGuardInterface resource to re-converge it to here.
	ConfigHash string

	// The WireGuardPlan controller will only create one-to-one WireGuard interfaces,
	// There is no chance that multiple peers are in the same WireGuard interface.
	Peer networkingv1alpha1.WireGuardPeerSpec
}

func (wgaIntf *WGActualPlanInterface) GetResourceId(fromNode, toNode string, linkIdx int) string {
	return fmt.Sprintf("%s-%s-%d", fromNode, toNode, linkIdx)
}

func (wgaIntf *WGActualPlanInterface) GetWGIntfName(fromNode, toNode string, linkIdx int) string {
	return fmt.Sprintf("wg-%s-%s-%d", fromNode, toNode, linkIdx)
}

func (wgaIntf *WGActualPlanInterface) ComputeConfigHash() error {
	criticalFields := make([]merkletree.Content, 0)

	criticalFields = append(criticalFields, &pkgutils.CriticalField{
		FieldPath: []string{"Node"},
		Value:     wgaIntf.Node,
	})
	criticalFields = append(criticalFields, &pkgutils.CriticalField{
		FieldPath: []string{"InterfaceName"},
		Value:     wgaIntf.InterfaceName,
	})
	for addrIdx, addr := range wgaIntf.Addresses {
		addrJSON, err := json.Marshal(addr)
		if err != nil {
			return err
		}
		criticalFields = append(criticalFields, &pkgutils.CriticalField{
			FieldPath: []string{"Addresses", fmt.Sprintf("%d", addrIdx)},
			Value:     string(addrJSON),
		})
	}
	criticalFields = append(criticalFields, &pkgutils.CriticalField{
		FieldPath: []string{"MTU"},
		Value:     fmt.Sprintf("%d", wgaIntf.MTU),
	})
	criticalFields = append(criticalFields, &pkgutils.CriticalField{
		FieldPath: []string{"MoveToContainer"},
		Value:     fmt.Sprintf("%t", wgaIntf.MoveToContainer),
	})
	if wgaIntf.Container != nil {
		containerJSON, err := json.Marshal(wgaIntf.Container)
		if err != nil {
			return err
		}
		criticalFields = append(criticalFields, &pkgutils.CriticalField{
			FieldPath: []string{"Container"},
			Value:     string(containerJSON),
		})
	}
	criticalFields = append(criticalFields, &pkgutils.CriticalField{
		FieldPath: []string{"WGIntfName"},
		Value:     wgaIntf.WGIntfName,
	})
	if wgaIntf.ListenPort != nil {
		criticalFields = append(criticalFields, &pkgutils.CriticalField{
			FieldPath: []string{"ListenPort"},
			Value:     fmt.Sprintf("%d", *wgaIntf.ListenPort),
		})
	}
	criticalFields = append(criticalFields, &pkgutils.CriticalField{
		FieldPath: []string{"Hostname"},
		Value:     wgaIntf.Hostname,
	})
	criticalFields = append(criticalFields, &pkgutils.CriticalField{
		FieldPath: []string{"PublicKey"},
		Value:     wgaIntf.PublicKey,
	})
	criticalFields = append(criticalFields, &pkgutils.CriticalField{
		FieldPath: []string{"PrivateKey"},
		Value:     wgaIntf.PrivateKey,
	})
	peerJSON, err := json.Marshal(wgaIntf.Peer)
	if err != nil {
		return err
	}
	criticalFields = append(criticalFields, &pkgutils.CriticalField{
		FieldPath: []string{"Peer"},
		Value:     string(peerJSON),
	})

	//Create a new Merkle Tree from the list of Content
	mkTree, err := merkletree.NewTree(criticalFields)
	if err != nil {
		return err
	}

	mkTreeRoot := mkTree.MerkleRoot()
	sha1Hash := sha1.Sum(mkTreeRoot[:])

	wgaIntf.ConfigHash = fmt.Sprintf("%x", sha1Hash)
	return nil
}

func (wgaIntf *WGActualPlanInterface) ToWireGuardInterfaceObject(wgPlanUID types.UID, wgPlanName string, wgPlanGeneration int64) *networkingv1alpha1.WireGuardInterface {
	if wgaIntf.ResourceId == "" {
		// to remind the developer that the resourceId must be generated before calling this function.
		panic("ResourceId is empty")
	}

	if wgaIntf.ConfigHash == "" {
		// to remind the developer that the configHash must be generated before calling this function.
		panic("ConfigHash is empty")
	}

	if wgaIntf.WGIntfName == "" {
		// to remind the developer that the wgIntfName must be generated before calling this function.
		panic("WGIntfName is empty")
	}

	return &networkingv1alpha1.WireGuardInterface{
		ObjectMeta: metav1.ObjectMeta{
			Name: wgaIntf.WGIntfName,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "networking.dn42.io/v1alpha1",
					Kind:       "WireGuardNetworkPlan",
					Name:       wgPlanName,
					UID:        wgPlanUID,
				},
			},
			Labels: map[string]string{
				LabelResourceId:     wgaIntf.ResourceId, // by comparing the resourceId, we can know which resources need to be created or deleted.
				LabelConfigHash:     wgaIntf.ConfigHash, // by comparing the configHash against that in the computed value, we can decide whether this resource is needed to be updated.
				LabelIsControlledBy: wgPlanName,         // use this label to facilitate the selection of dependent resources.
			},
			Annotations: map[string]string{
				AnnotationObservedGeneration: fmt.Sprintf("%d", wgPlanGeneration),
			},
			Finalizers: []string{
				"networkplan.networking.dn42.io/finalizer",
			},
		},
		Spec: networkingv1alpha1.WireGuardInterfaceSpec{
			Node:            wgaIntf.Node,
			MoveToContainer: wgaIntf.MoveToContainer,
			Container:       wgaIntf.Container,
			InterfaceName:   wgaIntf.InterfaceName,
			PrivateKey:      wgaIntf.PrivateKey,
			Addresses:       wgaIntf.Addresses,
			ListenPort:      wgaIntf.ListenPort,
			MTU:             &wgaIntf.MTU,
			Peers:           []networkingv1alpha1.WireGuardPeerSpec{wgaIntf.Peer},
		},
	}
}

type WGActualPlan struct {
	Interfaces []*WGActualPlanInterface `json:"interfaces"`
}

func (c *Controller) NewWGActualPlanFromObj(wgPlanObj *networkingv1alpha1.WireGuardNetworkPlan) (*WGActualPlan, error) {

	plan := new(WGActualPlan)

	type nodeEntry struct {
		nodeSpec   networkingv1alpha1.WireGuardNetworkPlanNodeSpec
		portRange  *networkingv1alpha1.WireGuardNetworkPlanPortRangeSpec
		privateKey string
		publicKey  string
	}

	nodeEntries := make(map[string]*nodeEntry)
	for _, node := range wgPlanObj.Spec.Nodes {
		ent := nodeEntry{
			nodeSpec:  node,
			portRange: &wgPlanObj.Spec.DefaultPortRange,
		}
		if node.Underlay != nil && node.Underlay.PortRange != nil {
			ent.portRange = node.Underlay.PortRange
		}

		if node.PrivateKey != nil && *node.PrivateKey != "" {
			ent.privateKey = *node.PrivateKey
		}

		if ent.privateKey == "" {
			pk, err := c.getPrivKey(node.PrivateKey, node.PrivateKeyRef)
			if err != nil {
				return nil, fmt.Errorf("failed to get private key of node %s: %s", node.NodeName, err.Error())
			}
			if pk == "" {
				return nil, fmt.Errorf("private key of node %s is empty", node.NodeName)
			}

			ent.privateKey = pk
		}

		keyObj, err := wgtypes.ParseKey(ent.privateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key %s: %s", ent.privateKey, err.Error())
		}
		ent.publicKey = keyObj.PublicKey().String()

		nodeEntries[node.NodeName] = &ent
	}

	linkIdxMap := make(map[string]map[string]int)
	for _, link := range wgPlanObj.Spec.Links {
		toNodes := make([]networkingv1alpha1.WireGuardNetworkPlanLinkPeerSpec, 0)
		toNodes = append(toNodes, link.ToNodes...)
		sort.Slice(toNodes, func(i, j int) bool {
			return toNodes[i].NodeName < toNodes[j].NodeName
		})
		if _, ok := linkIdxMap[link.FromNode]; !ok {
			linkIdxMap[link.FromNode] = make(map[string]int)
		}
		for linkIdx, toNode := range toNodes {
			linkIdxMap[link.FromNode][toNode.NodeName] = linkIdx
		}
	}

	for p, m := range linkIdxMap {
		for q := range m {
			hasRev := false
			if s, ok := linkIdxMap[q]; ok {
				if _, ok := s[p]; ok {
					hasRev = true
				}
			}
			if !hasRev {
				return nil, fmt.Errorf("link %s-%s is not bidirectional", p, q)
			}
		}
	}

	planIntfObjs := make([]*WGActualPlanInterface, 0)

	for _, link := range wgPlanObj.Spec.Links {
		fromNode, found := nodeEntries[link.FromNode]
		if !found {
			return nil, fmt.Errorf("node %s not found in nodeEntries", link.FromNode)
		}

		toNodes := make([]networkingv1alpha1.WireGuardNetworkPlanLinkPeerSpec, 0)
		toNodes = append(toNodes, link.ToNodes...)

		for _, toNode := range toNodes {
			toNodeEnt, found := nodeEntries[toNode.NodeName]
			if !found {
				return nil, fmt.Errorf("node %s not found in nodeEntries", toNode.NodeName)
			}

			linkIdx := linkIdxMap[link.FromNode][toNode.NodeName]

			planIntfObj := new(WGActualPlanInterface)
			planIntfObj.Node = link.FromNode
			planIntfObj.InterfaceName = planIntfObj.GetWGIntfName(link.FromNode, toNode.NodeName, linkIdx)
			planIntfObj.WGIntfName = planIntfObj.InterfaceName

			listenPort := fromNode.portRange.Start + linkIdx
			planIntfObj.ListenPort = &listenPort
			if fromNode.nodeSpec.Underlay != nil {
				underlay := fromNode.nodeSpec.Underlay
				if underlay.Hostname != "" {
					planIntfObj.Hostname = underlay.Hostname
				}
			}

			planIntfObj.PrivateKey = fromNode.privateKey
			planIntfObj.PublicKey = fromNode.publicKey

			planIntfObj.MTU = 1420
			if fromNode.nodeSpec.MTU != nil {
				planIntfObj.MTU = *fromNode.nodeSpec.MTU
			}

			if len(fromNode.nodeSpec.Addresses) > 0 {
				planIntfObj.Addresses = fromNode.nodeSpec.Addresses
			}

			if toNode.OverrideAddresses {
				planIntfObj.Addresses = toNode.Addresses
			} else {
				if planIntfObj.Addresses == nil {
					planIntfObj.Addresses = make([]networkingv1alpha1.WireGuardInterfaceAddressSpec, 0)
				}
				planIntfObj.Addresses = append(planIntfObj.Addresses, toNode.Addresses...)
			}
			planIntfObj.MoveToContainer = fromNode.nodeSpec.MoveToContainer
			planIntfObj.Container = fromNode.nodeSpec.Container
			peer := networkingv1alpha1.WireGuardPeerSpec{
				PublicKey:  toNodeEnt.publicKey,
				AllowedIPs: []string{"0.0.0.0/0", "::/0"}, // ACL-based firewall is not the duty of a VPN tunnel.
			}
			if toNodeEnt.nodeSpec.Underlay != nil {
				underlay := toNodeEnt.nodeSpec.Underlay
				if underlay.Hostname != "" {
					portRange := toNodeEnt.portRange
					revLinkIdx := linkIdxMap[toNode.NodeName][link.FromNode]
					port := portRange.Start + revLinkIdx
					if port > portRange.End {
						return nil, fmt.Errorf("port %d is out of range %d-%d", port, portRange.Start, portRange.End)
					}

					endpoint := fmt.Sprintf("%s:%d", underlay.Hostname, port)
					peer.Endpoint = &endpoint
				}
			}

			if fromNode.nodeSpec.Underlay == nil {
				// this node is behind a NAT, so set the PersistentKeepalive to 25, which is reasonable for most cases.
				pkl := 25
				peer.PersistentKeepalive = &pkl
			}

			planIntfObj.Peer = peer

			planIntfObj.ResourceId = planIntfObj.GetResourceId(link.FromNode, toNodeEnt.nodeSpec.NodeName, linkIdx)
			if err := planIntfObj.ComputeConfigHash(); err != nil {
				return nil, fmt.Errorf("failed to compute config hash for interface %s: %s", planIntfObj.InterfaceName, err.Error())
			}
			planIntfObjs = append(planIntfObjs, planIntfObj)
		}
	}
	plan.Interfaces = planIntfObjs

	return plan, nil
}

// A ResourceSet is generated by comparing two sets of resources: the lhs set and the rhs set.
// 1. The 'ShouldBeAdded' set contains the ResourceIds of the resources that is in the lhs set but not the rhs set.
// 2. The 'ShouldBeRemoved' set contains the ResourceIds of the resources that is in the rhs set but not the lhs set.
// 3. The 'ShouldBeUpdated' set contains those that are in both sets (lhs and rhs) but differs in the ConfigHash.
type ResourceSet struct {
	// the type of the value of the 'ShouldBeAdded' map is equal to that of the value type of the lhs map.
	ShouldBeAdded map[string]interface{} `json:"shouldBeAdded"`
	// the type of the value of the 'ShouldBeRemoved' map is equal to that of the value type of the rhs map.
	ShouldBeRemoved map[string]interface{} `json:"shouldBeRemoved"`
	// the type of the value of the 'ShouldBeUpdated' map is equal to that of the value type of the lhs map.
	ShouldBeUpdated map[string]interface{} `json:"shouldBeUpdated"`
}

func (c *Controller) comparingResourceSets(lhs []*WGActualPlanInterface, rhs []*networkingv1alpha1.WireGuardInterface) (*ResourceSet, error) {
	lhsMap := make(map[string]interface{})
	rhsMap := make(map[string]interface{})

	for _, item := range lhs {
		lhsMap[item.ResourceId] = item
	}

	for _, item := range rhs {
		rhsMap[item.Labels[LabelResourceId]] = item
	}

	// 1. addedSet are those that are in lhs but not the rhs, the type of the value is same as that of the lhs.
	// 2. removedSet are those that are in the rhs but not the lhs, the type of the value is same as that of the rhs.
	// 3. commonSet are those that are in both lhs and rhs, the type of the value is undefined (and should not be used).
	added, removed, _, common := getDifferenceSets(lhsMap, rhsMap)

	result := new(ResourceSet)
	result.ShouldBeAdded = added
	result.ShouldBeRemoved = removed

	result.ShouldBeUpdated = make(map[string]interface{})

	for k := range common {
		lhsItemAny := lhsMap[k]
		lhsItem := lhsItemAny.(*WGActualPlanInterface)

		rhsItemAny := rhsMap[k]
		rhsItem := rhsItemAny.(*networkingv1alpha1.WireGuardInterface)

		if lhsItem.ConfigHash != rhsItem.Labels[LabelConfigHash] {
			result.ShouldBeUpdated[k] = lhsItem
		}
	}

	return result, nil
}

func getDifferenceSets(lhs, rhs map[string]interface{}) (added, removed, union, common map[string]interface{}) {

	unionSet := make(map[string]interface{})

	addedSet := make(map[string]interface{})
	for k := range lhs {
		unionSet[k] = lhs[k]
		if _, ok := rhs[k]; !ok {
			addedSet[k] = lhs[k]
		}
	}

	removedSet := make(map[string]interface{})
	for k := range rhs {
		unionSet[k] = rhs[k]
		if _, ok := lhs[k]; !ok {
			removedSet[k] = rhs[k]
		}
	}

	commonSet := make(map[string]interface{})
	for k := range unionSet {
		if _, ok := lhs[k]; ok {
			if _, ok := rhs[k]; ok {
				commonSet[k] = unionSet[k]
			}
		}
	}

	return addedSet, removedSet, unionSet, commonSet
}

func (c *Controller) getPrivKey(privKey *string, privKeySecRef *networkingv1alpha1.PrivateKeySecretRef) (string, error) {
	if privKey != nil && *privKey != "" {
		return *privKey, nil
	}

	if privKeySecRef == nil {
		return "", fmt.Errorf("privateKeyRef is nil and spec.privateKey is not provided")
	}

	if privKeySecRef.Name == "" {
		return "", fmt.Errorf("privateKeyRef.name is empty")
	}
	if privKeySecRef.Key == "" {
		return "", fmt.Errorf("privateKeyRef.key is empty")
	}

	secNs := "default"
	if privKeySecRef.Namespace != nil && *privKeySecRef.Namespace != "" {
		secNs = *privKeySecRef.Namespace
	}

	secObj, err := c.secretsLister.Secrets(secNs).Get(privKeySecRef.Name)
	if err != nil {
		return "", fmt.Errorf("failed to get secret %s/%s: %s", secNs, privKeySecRef.Name, err.Error())
	}

	return base64.StdEncoding.EncodeToString(secObj.Data[privKeySecRef.Key]), nil
}
