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

package birdbgp

import (
	"context"
	"fmt"
	"time"

	pkgnetapplybird "github.com/internetworklab/netapply/pkg/bird"
	pkgutils "github.com/internetworklab/netapply/pkg/utils"
	"golang.org/x/time/rate"
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

const controllerAgentName = "birdbgp-controller"

const (
	// FieldManager distinguishes this controller from other things writing to API objects
	FieldManager = controllerAgentName
)

// Controller is the controller implementation for BirdBGPProtocol resources
type Controller struct {
	nodename string
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface

	// myClientset is a clientset for our own API group
	myClientset clientset.Interface

	birdBGPLister v1alpha1Lister.BirdBGPProtocolLister
	birdBGPSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.TypedRateLimitingInterface[cache.ObjectName]
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder

	ns string

	statusInterval time.Duration

	birdClient     *pkgnetapplybird.BirdClient
	birdConfigDir  string
	birdSocketPath string
}

type ControllerConfig struct {
	Nodename        string
	Kubeclientset   kubernetes.Interface
	Sampleclientset clientset.Interface
	BirdBGPInformer v1alpha1Informer.BirdBGPProtocolInformer
	Namespace       string
	StatusInterval  time.Duration
	BirdSocketPath  string
	BirdConfigDir   string
}

func provisionerFromRes(res *networkingv1alpha1.BirdBGPProtocol) (*pkgnetapplybird.BGPProtocol, error) {
	return &pkgnetapplybird.BGPProtocol{
		Name:         res.Spec.Name,
		Template:     res.Spec.Template,
		Interface:    res.Spec.Interface,
		LocalAddress: res.Spec.LocalAddress,
		PeerAddress:  res.Spec.PeerAddress,
		LocalASN:     res.Spec.LocalASN,
		PeerASN:      res.Spec.PeerASN,
		PeerExternal: res.Spec.PeerExternal,
		PeerInternal: res.Spec.PeerInternal,
	}, nil
}

// NewController returns a new BirdBGPProtocol controller
func NewController(
	ctx context.Context,
	config ControllerConfig,
) *Controller {
	logger := klog.FromContext(ctx)

	// Create event broadcaster
	// Add BirdBGPProtocol types to the default Kubernetes Scheme so Events can be
	// logged for BirdBGPProtocol types.
	utilruntime.Must(samplescheme.AddToScheme(scheme.Scheme))
	logger.V(4).Info("Creating event broadcaster")

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: config.Kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName, Host: config.Nodename})
	ratelimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[cache.ObjectName](5*time.Millisecond, 1000*time.Second),
		&workqueue.TypedBucketRateLimiter[cache.ObjectName]{Limiter: rate.NewLimiter(rate.Limit(50), 300)},
	)

	controller := &Controller{
		nodename:       config.Nodename,
		kubeclientset:  config.Kubeclientset,
		myClientset:    config.Sampleclientset,
		birdBGPLister:  config.BirdBGPInformer.Lister(),
		birdBGPSynced:  config.BirdBGPInformer.Informer().HasSynced,
		workqueue:      workqueue.NewTypedRateLimitingQueue(ratelimiter),
		recorder:       recorder,
		ns:             config.Namespace,
		statusInterval: config.StatusInterval,
		birdClient:     pkgnetapplybird.NewBirdClientFromSocket(config.BirdSocketPath),
		birdConfigDir:  config.BirdConfigDir,
		birdSocketPath: config.BirdSocketPath,
	}

	logger.Info("Setting up event handlers")

	handleAddOrUpdate := func(obj interface{}) {
		newRes, _ := obj.(*networkingv1alpha1.BirdBGPProtocol)
		if newRes.Spec.Node != controller.nodename {
			// each controller only responsible for a single node
			return
		}

		logger.Info("Updating BirdBGPProtocol due to creation, resourceVersion or generation changed", "objectReference", klog.KObj(newRes))
		controller.enqueueBirdBGPRes(newRes)
	}

	// Set up an event handler for when BirdBGPProtocol resources change
	config.BirdBGPInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: handleAddOrUpdate,
		UpdateFunc: func(old, new interface{}) {
			oldRes, ok := old.(*networkingv1alpha1.BirdBGPProtocol)
			if !ok {
				// simply ignore non-relevant events
				return
			}
			newRes, ok := new.(*networkingv1alpha1.BirdBGPProtocol)
			if !ok {
				// simply ignore non-relevant events
				return
			}
			if newRes.Spec.Node != controller.nodename {
				// each controller only responsible for a single node
				return
			}

			if (newRes.GetResourceVersion() == oldRes.GetResourceVersion()) || (newRes.GetGeneration() == oldRes.GetGeneration()) {
				// status-only op
				ctx = pkgutils.SetBirdBGPConfigDirInCtx(ctx, controller.birdConfigDir)
				ctx = pkgutils.SetBirdControlSocketInCtx(ctx, controller.birdSocketPath)
				if err := controller.updateBirdBGPResStatus(ctx, newRes, nil); err != nil {
					logger.Error(err, "Failed to update BirdBGPProtocol status", "objectReference", newRes.Name)
					// if failed to update status, simply give up rather than retry, because there's still next force-resync
				}
				return
			}
			handleAddOrUpdate(newRes)
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

	ctx = pkgutils.SetBirdBGPConfigDirInCtx(ctx, c.birdConfigDir)
	ctx = pkgutils.SetBirdControlSocketInCtx(ctx, c.birdSocketPath)

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync", "nodename", c.nodename)
	if ok := cache.WaitForCacheSync(ctx.Done(), c.birdBGPSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}
	logger.Info("Informer caches synced")

	logger.Info("Starting workers", "count", workers)
	// Launch two workers to process BirdBGPProtocol resources
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	logger.Info("Started workers")

	logger.Info("Connecting to bird control socket")
	if err := c.birdClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to bird control socket: %s", err.Error())
	}
	logger.Info("Connected to bird control socket")

	defer c.birdClient.Close()

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

// enqueueBirdBGPRes takes a BirdBGPProtocol resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than BirdBGPProtocol.
func (c *Controller) enqueueBirdBGPRes(obj interface{}) {
	if objectRef, err := cache.ObjectToName(obj); err != nil {
		utilruntime.HandleError(err)
		return
	} else {
		c.workqueue.AddRateLimited(objectRef)
	}
}

func (c *Controller) syncHandler(ctx context.Context, objectRef cache.ObjectName) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "objectRef", objectRef)

	logger.V(4).Info("Processing object creation", "object", objectRef.Name)

	newRes, err := c.birdBGPLister.BirdBGPProtocols(c.ns).Get(objectRef.Name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "BirdBGPProtocol referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	provisioner, err := provisionerFromRes(newRes)
	if err != nil {
		return fmt.Errorf("failed to create bird bgp provisioner from resource: %s", err.Error())
	}

	if newRes.GetDeletionTimestamp() != nil {
		// handle deletion and cleanup

		err := provisioner.Delete(ctx)
		if err != nil {
			return fmt.Errorf("failed to delete interface: %s", err.Error())
		}

		newRes := newRes.DeepCopy()
		newRes.SetFinalizers([]string{})
		_, err = c.myClientset.NetworkingV1alpha1().BirdBGPProtocols(c.ns).Update(context.Background(), newRes, metav1.UpdateOptions{})
		if err != nil {
			if !k8serrors.IsNotFound(err) {
				return fmt.Errorf("failed to clear finalizers from BirdBGPProtocol, will retry: %s", err.Error())
			}
		}

		return nil
	}

	if newRes.Spec.Name == "" {
		// Event recorder is useful because it enables the user to see things that happened in a central place.
		c.recorder.Eventf(newRes, corev1.EventTypeWarning, "NameEmpty", "Name is empty")
		return fmt.Errorf("resource name is empty")
	}

	isExist, err := provisioner.CheckExist(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if resource exists: %s", err.Error())
	}
	if isExist {
		changeset, err := provisioner.DetectChanges(ctx)
		if err != nil {
			return fmt.Errorf("failed to detect changes: %s", err.Error())
		}

		if changeset != nil && changeset.HasUpdates() {
			logger.Info("Need to reconcile", "objectReference", klog.KObj(newRes))
			if err := changeset.Apply(ctx); err != nil {
				return fmt.Errorf("failed to apply changes: %s", err.Error())
			}
		}
	} else {
		if err := provisioner.Create(ctx); err != nil {
			return fmt.Errorf("failed to create resource: %s", err.Error())
		}
	}

	// Update the status with current BirdBGPProtocol information
	err = c.updateBirdBGPResStatus(ctx, newRes, provisioner)
	if err != nil {
		return fmt.Errorf("failed to update BirdBGPProtocol status: %s", err.Error())
	}

	return nil
}

// updateBirdBGPResStatus updates the status of a BirdBGPProtocol with current information
func (c *Controller) updateBirdBGPResStatus(ctx context.Context, res *networkingv1alpha1.BirdBGPProtocol, provisioner *pkgnetapplybird.BGPProtocol) error {
	logger := klog.FromContext(ctx)

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	res = res.DeepCopy()

	if provisioner == nil {
		// in some calling path, the provisioner could be nil
		v, err := provisionerFromRes(res)
		if err != nil {
			return fmt.Errorf("failed to create birdbgp provisioner from resource: %s", err.Error())
		}
		provisioner = v
	}

	latestResStatus, err := provisioner.ToStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to convert birdbgp config to status: %s", err.Error())
	}

	var prevResStatus *pkgnetapplybird.BirdBGPProtocolStatus
	if res.Status != nil {
		prevResStatus = res.Status.Resource
	}

	if latestResStatus.IsEqual(prevResStatus) || (res.Status != nil && time.Since(time.Unix(res.Status.GeneratedAt, 0)) < c.statusInterval) {
		// no changes, or changes too quickly, just return
		logger.V(4).Info("No changes (or changes too quickly), skipping status update", "resource name", res.Spec.Name)
		return nil
	}

	bgpResStatus, ok := latestResStatus.(*pkgnetapplybird.BirdBGPProtocolStatus)
	if !ok {
		return fmt.Errorf("failed to convert abstract birdbgp resource status to concrete birdbgp resource status")
	}

	// Update the status
	res.Status = &networkingv1alpha1.BirdBGPProtocolStatus{
		Nodename:    c.nodename,
		Resource:    bgpResStatus,
		GeneratedAt: time.Now().Unix(),
	}

	// Use UpdateStatus to update only the Status block of the BirdBGPProtocol resource
	_, err = c.myClientset.NetworkingV1alpha1().BirdBGPProtocols(c.ns).UpdateStatus(ctx, res, metav1.UpdateOptions{FieldManager: FieldManager})
	if err != nil {
		return fmt.Errorf("failed to update status: %s", err.Error())
	}

	logger.V(4).Info("Updated BirdBGPProtocol status", "resource name", res.Spec.Name)
	return nil
}
