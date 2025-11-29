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

package wgng

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	"golang.org/x/time/rate"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	pkgnetapplywg "github.com/internetworklab/netapply/pkg/interface/wireguard"

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	clientset "k8s.io/sample-controller/pkg/generated/clientset/versioned"
	samplescheme "k8s.io/sample-controller/pkg/generated/clientset/versioned/scheme"
	v1alpha1Informer "k8s.io/sample-controller/pkg/generated/informers/externalversions/networking/v1alpha1"
	v1alpha1Lister "k8s.io/sample-controller/pkg/generated/listers/networking/v1alpha1"
)

const controllerAgentName = "wgng-controller"

const (
	// FieldManager distinguishes this controller from other things writing to API objects
	FieldManager = controllerAgentName
)

// Controller is the controller implementation for WireGuardInterface resources
type Controller struct {
	nodename string
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface

	// myClientset is a clientset for our own API group
	myClientset clientset.Interface

	secretsLister secretlisters.SecretLister
	wgLister      v1alpha1Lister.WireGuardInterfaceNGLister

	wgSynced      cache.InformerSynced
	secretsSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.TypedRateLimitingInterface[cache.ObjectName]
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder

	dryRun bool

	ns string

	statusInterval time.Duration
}

type ControllerConfig struct {
	Nodename        string
	Kubeclientset   kubernetes.Interface
	Sampleclientset clientset.Interface
	WgInformer      v1alpha1Informer.WireGuardInterfaceNGInformer
	SecretsInformer secretsinformers.SecretInformer
	DryRun          bool
	Namespace       string
	StatusInterval  time.Duration
}

func doGetSecretValue(lister secretlisters.SecretLister, ns *string, secName, key string) ([]byte, error) {
	usedNs := "default"
	if ns != nil && *ns != "" {
		usedNs = *ns
	}

	secObj, err := lister.Secrets(usedNs).Get(secName)
	if err != nil {
		return nil, err
	}

	return secObj.Data[key], nil
}

// should return base64 encoded standard wg key
func getWGKey(secRef *networkingv1alpha1.PrivateStuffRef, secLister secretlisters.SecretLister) (*wgtypes.Key, error) {
	if secRef != nil {
		if secRef.String != nil && *secRef.String != "" {
			keyObj, err := wgtypes.ParseKey(*secRef.String)
			if err != nil {
				return nil, fmt.Errorf("failed to parse private key: %s", err.Error())
			}
			return &keyObj, nil
		}
		if secRef.SecretRef != nil {
			secData, err := doGetSecretValue(secLister, secRef.SecretRef.Namespace, secRef.SecretRef.Name, secRef.SecretRef.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to get secret value: %s", err.Error())
			}
			secDataStr := base64.StdEncoding.EncodeToString(secData)
			keyObj, err := wgtypes.ParseKey(secDataStr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse secret value: %s", err.Error())
			}
			return &keyObj, nil
		}
	}

	return nil, nil
}

func doConvertPeerSpec(peerSpec *networkingv1alpha1.WireGuardPeerNGSpec, lister secretlisters.SecretLister) (*pkgnetapplywg.WireGuardPeerConfig, error) {
	pkObj, err := wgtypes.ParseKey(peerSpec.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %s", err.Error())
	}
	cfg := &pkgnetapplywg.WireGuardPeerConfig{}
	cfg.PublicKey = pkObj.String()
	if peerSpec.PresharedKeySecretRef != nil {
		keyObj, err := getWGKey(peerSpec.PresharedKeySecretRef, lister)
		if err != nil {
			return nil, fmt.Errorf("preshared key is specified but failed to get preshared key secret: %s", err.Error())
		}
		cfg.PresharedKey = keyObj.String()
	}
	cfg.AllowedIPs = peerSpec.AllowedIPs
	cfg.Endpoint = peerSpec.Endpoint
	cfg.PersistentKeepalive = peerSpec.PersistentKeepalive

	return cfg, nil
}

func resProvisionerFromRes(res *networkingv1alpha1.WireGuardInterfaceNG, secLister secretlisters.SecretLister) (*pkgnetapplywg.WireGuardConfig, error) {
	cfg := &pkgnetapplywg.WireGuardConfig{
		Name:       res.Spec.InterfaceName,
		MTU:        res.Spec.MTU,
		ListenPort: res.Spec.ListenPort,
		VRF:        res.Spec.VRF,
		Container:  res.Spec.Container,
		Addresses:  res.Spec.Addresses,
	}

	if res.Spec.PrivateKey != nil {
		keyObj, err := getWGKey(res.Spec.PrivateKey, secLister)
		if err != nil {
			return nil, fmt.Errorf("failed to get secret object: %s", err.Error())
		}
		cfg.PrivateKey = keyObj.String()
	}
	for _, peer := range res.Spec.Peers {
		peerConf, err := doConvertPeerSpec(&peer, secLister)
		if err != nil {
			return nil, fmt.Errorf("failed to convert peer spec to wg peer conf: %s", err.Error())
		}
		cfg.Peers = append(cfg.Peers, *peerConf)
	}

	return cfg, nil
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
	if config.DryRun {
		logger.Info("Dry run mode is enabled, the controller won't make any actual changes to the node")
	}

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
		wgLister:       config.WgInformer.Lister(),
		secretsLister:  config.SecretsInformer.Lister(),
		wgSynced:       config.WgInformer.Informer().HasSynced,
		secretsSynced:  config.SecretsInformer.Informer().HasSynced,
		workqueue:      workqueue.NewTypedRateLimitingQueue(ratelimiter),
		recorder:       recorder,
		dryRun:         config.DryRun,
		ns:             config.Namespace,
		statusInterval: config.StatusInterval,
	}

	logger.Info("Setting up event handlers")

	handleAddOrUpdate := func(obj interface{}) {
		newWg, _ := obj.(*networkingv1alpha1.WireGuardInterfaceNG)
		if newWg.Spec.Node != controller.nodename {
			// each controller only responsible for a single node
			return
		}

		logger.Info("Updating WireGuardInterfaceNG due to creation, resourceVersion or generation changed", "objectReference", klog.KObj(newWg))
		controller.enqueueWG(newWg)
	}

	// Set up an event handler for when WireGuardInterface resources change
	config.WgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: handleAddOrUpdate,
		UpdateFunc: func(old, new interface{}) {
			oldWg, ok := old.(*networkingv1alpha1.WireGuardInterfaceNG)
			if !ok {
				// simply ignore non-relevant events
				return
			}
			newWg, ok := new.(*networkingv1alpha1.WireGuardInterfaceNG)
			if !ok {
				// simply ignore non-relevant events
				return
			}
			if newWg.Spec.Node != controller.nodename {
				// each controller only responsible for a single node
				return
			}

			if (newWg.GetResourceVersion() == oldWg.GetResourceVersion()) || (newWg.GetGeneration() == oldWg.GetGeneration()) {
				// status-only op
				if err := controller.updateWireGuardInterfaceStatus(context.Background(), newWg, nil); err != nil {
					logger.Error(err, "Failed to update WireGuardInterface status", "objectReference", newWg.Name)
					// if failed to update status, simply give up rather than retry, because there's still next force-resync
				}
				return
			}
			handleAddOrUpdate(newWg)
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

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync", "nodename", c.nodename)

	if ok := cache.WaitForCacheSync(ctx.Done(),
		c.wgSynced,
		c.secretsSynced,
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

	logger.V(4).Info("Processing object creation", "object", objectRef.Name)

	wgObj, err := c.wgLister.WireGuardInterfaceNGs(c.ns).Get(objectRef.Name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "WireGuardInterface referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	wgProvisioner, err := resProvisionerFromRes(wgObj, c.secretsLister)
	if err != nil {
		return fmt.Errorf("failed to create wg provisioner from resource: %s", err.Error())
	}

	if wgObj.GetDeletionTimestamp() != nil {
		// handle deletion and cleanup

		err := wgProvisioner.Delete(ctx)
		if err != nil {
			return fmt.Errorf("failed to delete interface: %s", err.Error())
		}

		wgObjCopy := wgObj.DeepCopy()
		wgObjCopy.SetFinalizers([]string{})
		_, err = c.myClientset.NetworkingV1alpha1().WireGuardInterfaceNGs(c.ns).Update(context.Background(), wgObjCopy, metav1.UpdateOptions{})
		if err != nil {
			if !k8serrors.IsNotFound(err) {
				return fmt.Errorf("failed to clear finalizers from WireGuardInterface, will retry: %s", err.Error())
			}
		}

		return nil
	}

	if c.dryRun {
		logger.Info("Dry run mode is enabled, skipping underlying resources manipulation")
		return nil
	}

	if wgObj.Spec.InterfaceName == "" {
		// Event recorder is useful because it enables the user to see things that happened in a central place.
		c.recorder.Eventf(wgObj, corev1.EventTypeWarning, "InterfaceNameEmpty", "Interface name is empty")
		return fmt.Errorf("interface name is empty")
	}

	isExist, err := wgProvisioner.CheckExist(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if interface exists: %s", err.Error())
	}
	if isExist {
		changeset, err := wgProvisioner.DetectChanges(ctx)
		if err != nil {
			return fmt.Errorf("failed to detect changes: %s", err.Error())
		}

		if changeset != nil && changeset.HasUpdates() {
			logger.Info("Need to reconcile", "objectReference", klog.KObj(wgObj))
			if err := changeset.Apply(ctx); err != nil {
				return fmt.Errorf("failed to apply changes: %s", err.Error())
			}
		}
	} else {
		if err := wgProvisioner.Create(ctx); err != nil {
			return fmt.Errorf("failed to create interface: %s", err.Error())
		}
	}

	// Update the status with current WireGuard interface information
	err = c.updateWireGuardInterfaceStatus(ctx, wgObj, wgProvisioner)
	if err != nil {
		return fmt.Errorf("failed to update WireGuardInterface status: %s", err.Error())
	}

	return nil
}

// updateWireGuardInterfaceStatus updates the status of a WireGuard interface with current information
func (c *Controller) updateWireGuardInterfaceStatus(ctx context.Context, wgObj *networkingv1alpha1.WireGuardInterfaceNG, provisioner *pkgnetapplywg.WireGuardConfig) error {
	logger := klog.FromContext(ctx)

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	wgObj = wgObj.DeepCopy()

	if provisioner == nil {
		// in some calling path, the provisioner could be nil
		v, err := resProvisionerFromRes(wgObj, c.secretsLister)
		if err != nil {
			return fmt.Errorf("failed to create wg provisioner from resource: %s", err.Error())
		}
		provisioner = v
	}

	latestResStatus, err := provisioner.ToStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to convert wireguard config to status: %s", err.Error())
	}

	var prevResStatus *pkgnetapplywg.WireGuardInterfaceStatus
	if wgObj.Status != nil {
		prevResStatus = wgObj.Status.Resource
	}
	if latestResStatus.IsEqual(prevResStatus) || (wgObj.Status != nil && time.Since(time.Unix(wgObj.Status.GeneratedAt, 0)) < c.statusInterval) {
		// no changes, or changes too quickly, just return
		logger.V(4).Info("No changes (or changes too quickly), skipping status update", "resource name", wgObj.Spec.InterfaceName)
		return nil
	}

	wgResStatus, ok := latestResStatus.(*pkgnetapplywg.WireGuardInterfaceStatus)
	if !ok {
		return fmt.Errorf("failed to convert abstract interface status to concrete wireguard resource status")
	}

	// Update the status
	wgObj.Status = &networkingv1alpha1.WireGuardInterfaceNGStatus{
		Nodename:    c.nodename,
		Resource:    wgResStatus,
		GeneratedAt: time.Now().Unix(),
	}

	// Use UpdateStatus to update only the Status block of the WireGuardInterface resource
	_, err = c.myClientset.NetworkingV1alpha1().WireGuardInterfaceNGs(c.ns).UpdateStatus(ctx, wgObj, metav1.UpdateOptions{FieldManager: FieldManager})
	if err != nil {
		return fmt.Errorf("failed to update status: %s", err.Error())
	}

	logger.V(4).Info("Updated WireGuard interface status", "interfaceName", wgObj.Spec.InterfaceName)
	return nil
}
