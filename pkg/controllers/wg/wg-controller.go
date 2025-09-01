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

package wg

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	dockerUtil "example.com/go-util/pkg/util/docker"
	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	dockerSDK "github.com/docker/docker/client"
	"github.com/vishvananda/netlink"
	"golang.org/x/time/rate"

	pkgutils "k8s.io/sample-controller/pkg/utils"

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

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	clientset "k8s.io/sample-controller/pkg/generated/clientset/versioned"
	samplescheme "k8s.io/sample-controller/pkg/generated/clientset/versioned/scheme"
	v1alpha1Informer "k8s.io/sample-controller/pkg/generated/informers/externalversions/networking/v1alpha1"
	v1alpha1Lister "k8s.io/sample-controller/pkg/generated/listers/networking/v1alpha1"
	pkgreconcile "k8s.io/sample-controller/pkg/reconcile"
)

const controllerAgentName = "sample-controller"

const (
	// FieldManager distinguishes this controller from other things writing to API objects
	FieldManager = controllerAgentName
)

// Controller is the controller implementation for WireGuardInterface resources
type Controller struct {
	nodename     string
	dockerClient *dockerSDK.Client
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	sampleclientset clientset.Interface

	secretsLister secretlisters.SecretLister
	wgLister      v1alpha1Lister.WireGuardInterfaceLister

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
}

type ControllerConfig struct {
	Nodename        string
	Kubeclientset   kubernetes.Interface
	Sampleclientset clientset.Interface
	WgInformer      v1alpha1Informer.WireGuardInterfaceInformer
	WgPlanInformer  v1alpha1Informer.WireGuardNetworkPlanInformer
	SecretsInformer secretsinformers.SecretInformer
	DryRun          bool
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
		nodename:        config.Nodename,
		dockerClient:    dockerClient,
		kubeclientset:   config.Kubeclientset,
		sampleclientset: config.Sampleclientset,
		wgLister:        config.WgInformer.Lister(),
		secretsLister:   config.SecretsInformer.Lister(),
		wgSynced:        config.WgInformer.Informer().HasSynced,
		secretsSynced:   config.SecretsInformer.Informer().HasSynced,
		workqueue:       workqueue.NewTypedRateLimitingQueue(ratelimiter),
		recorder:        recorder,
		dryRun:          config.DryRun,
	}

	logger.Info("Setting up event handlers")

	// Set up an event handler for when WireGuardInterface resources change
	config.WgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			objWg, ok := obj.(*networkingv1alpha1.WireGuardInterface)
			if !ok {
				return
			}

			if objWg.Spec.Node != controller.nodename {
				return
			}

			revLog := pkgutils.RevChangeLog{
				Generation:         fmt.Sprintf("%d", objWg.GetGeneration()),
				ResourceVersion:    objWg.GetResourceVersion(),
				ObservedGeneration: fmt.Sprintf("%d", objWg.Status.ObservedGeneration),
			}

			revLogJSON, _ := json.Marshal(revLog)
			logger.Info("AddFunc for WireGuardInterface resource is called", "objectReference", klog.KObj(objWg), "Revision log", string(revLogJSON))

			controller.enqueueWG(objWg)
		},
		UpdateFunc: func(old, new interface{}) {
			oldWG, ok := old.(*networkingv1alpha1.WireGuardInterface)
			if !ok {
				return
			}
			newWG, ok := new.(*networkingv1alpha1.WireGuardInterface)
			if !ok {
				return
			}

			if newWG.Spec.Node != controller.nodename {
				// For un-managed WireGuardInterface, `spec.node` is not supported to be edited.
				// For managed WireGuardInterface, the higher level controller will delete the
				// object ties to the old node and create a new object ties to the new node.
				// So, only the newWG's `spec.node` needs to be concerned.
				return
			}

			logger.Info("UpdateFunc for WireGuardInterface resource is called", "objectReference", klog.KObj(newWG))

			revisionChanged := newWG.ResourceVersion != oldWG.ResourceVersion
			if revisionChanged {
				logger.Info("Revision changed", "old", oldWG.ResourceVersion, "new", newWG.ResourceVersion, "objectReference", klog.KObj(newWG))
			}

			changelog := pkgutils.RevChangeLog{
				Generation:         fmt.Sprintf("%d -> %d", oldWG.GetGeneration(), newWG.GetGeneration()),
				ResourceVersion:    fmt.Sprintf("%s -> %s", oldWG.GetResourceVersion(), newWG.GetResourceVersion()),
				ObservedGeneration: fmt.Sprintf("%d -> %d", oldWG.Status.ObservedGeneration, newWG.GetGeneration()),
			}
			changelogJSON, _ := json.Marshal(changelog)
			logger.Info("Revision change log", string(changelogJSON))

			if !revisionChanged {
				logger.Info("Updating WireGuardInterface due to force resync", "objectReference", klog.KObj(newWG))
				if err := controller.updateWireGuardInterfaceStatus(context.Background(), newWG); err != nil {
					logger.Error(err, "Failed to update WireGuardInterface status", "objectReference", newWG.Name, "object is enqueued, and will retry later")
					// if failed to update status, simply give up rather than retry, because there's still next force-resync
				}
				return
			}

			logger.Info("Updating WireGuardInterface due to resourceVersion is changed", "objectReference", klog.KObj(newWG))
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
	logger.Info("Starting controller", "nodename", c.nodename, "hostname", hostname)

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

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

	logger.V(4).Info("Processing wgi object creation", "object", objectRef.Name)

	wgObj, err := c.wgLister.Get(objectRef.Name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "WireGuardInterface referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	nodeName := wgObj.Spec.Node
	host, err := c.getThisHostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %s", err.Error())
	}

	if host != nodeName {
		logger.V(4).Info("This node is not responsible for this WireGuardInterface", "objectReference", objectRef)
		return nil
	}

	pid, err := c.getInterfacePid(&wgObj.Spec)
	if err != nil {
		return fmt.Errorf("failed to get interface pid: %s", err.Error())
	}

	deletionTime := wgObj.GetDeletionTimestamp()
	if deletionTime != nil {
		// Clean up underlying resources, then
		// clear all finalizers from the object
		err := pkgutils.WithNetlinkHandle(pid, func(handle *netlink.Handle) error {
			link, err := handle.LinkByName(wgObj.Spec.InterfaceName)
			if err != nil {
				if _, ok := err.(netlink.LinkNotFoundError); !ok {
					return fmt.Errorf("failed to get link by name: %s", err.Error())
				}

				return nil
			}

			return handle.LinkDel(link)
		})
		if err != nil {
			return fmt.Errorf("failed to delete interface: %s", err.Error())
		}

		wgObjCopy := wgObj.DeepCopy()
		wgObjCopy.SetFinalizers([]string{})
		_, err = c.sampleclientset.NetworkingV1alpha1().WireGuardInterfaces().Update(context.Background(), wgObjCopy, metav1.UpdateOptions{})
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

	needReconcile := wgObj.GetGeneration() != wgObj.Status.ObservedGeneration
	if needReconcile {
		logger.Info("Need to reconcile", "objectReference", klog.KObj(wgObj), "observedGeneration", wgObj.Status.ObservedGeneration, "generation", wgObj.GetGeneration())

		reconciler, err := pkgreconcile.NewWGReconciler(wgObj.Spec.InterfaceName, pid)
		if err != nil {
			return fmt.Errorf("failed to create reconciler: %s", err.Error())
		}

		hasUpdates, err := reconciler.DetectChanges(ctx, &wgObj.Spec, &wgObj.Status)
		if err != nil {
			return fmt.Errorf("failed to detect changes: %s", err.Error())
		}
		maxLoops := 10
		desiredSpec, err := c.toWGDesiredConfig(&wgObj.Spec)
		if err != nil {
			return fmt.Errorf("failed to convert wireguard config: %s", err.Error())
		}
		for hasUpdates && maxLoops > 0 {

			err = reconciler.ApplyReconcile(ctx, &wgObj.Spec)
			if err != nil {
				break
			}

			reconciler.ResetState()
			hasUpdates, err = reconciler.DetectChanges(ctx, desiredSpec, nil)
			if err != nil {
				break
			}

			maxLoops--
		}

		if err != nil {
			return fmt.Errorf("failed to apply reconcile: %s", err.Error())
		}
		if maxLoops == 0 {
			return fmt.Errorf("failed to apply reconcile: %s", "out of max loops")
		}

		logger.Info("Updating WireGuardInterface status", "objectReference", klog.KObj(wgObj))
		// Update the status with current WireGuard interface information
		err = c.updateWireGuardInterfaceStatus(ctx, wgObj)
		if err != nil {
			return fmt.Errorf("failed to update WireGuardInterface status: %s", err.Error())
		}

	}

	return nil
}

// updateWireGuardInterfaceStatus updates the status of a WireGuard interface with current information
func (c *Controller) updateWireGuardInterfaceStatus(ctx context.Context, wgObj *networkingv1alpha1.WireGuardInterface) error {
	logger := klog.FromContext(ctx)

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	wgObjCopy := wgObj.DeepCopy()

	pid, err := c.getInterfacePid(&wgObj.Spec)
	if err != nil {
		return fmt.Errorf("failed to get interface pid: %s", err.Error())
	}

	// Get current WireGuard interface status
	status, err := c.getCurrentWireGuardStatus(wgObj.Spec.InterfaceName, pid)
	if err != nil {
		logger.Error(err, "Failed to get current WireGuard status", "interfaceName", wgObj.Spec.InterfaceName)
		// Don't fail the entire sync if status update fails
		return nil
	}

	status.ObservedGeneration = wgObj.GetGeneration()

	// Update the status
	wgObjCopy.Status = *status

	// Use UpdateStatus to update only the Status block of the WireGuardInterface resource
	_, err = c.sampleclientset.NetworkingV1alpha1().WireGuardInterfaces().UpdateStatus(ctx, wgObjCopy, metav1.UpdateOptions{FieldManager: FieldManager})

	if err != nil {
		return fmt.Errorf("failed to update status: %s", err.Error())
	}

	logger.V(4).Info("Updated WireGuard interface status", "interfaceName", wgObj.Spec.InterfaceName)
	return nil
}

// getCurrentWireGuardStatus retrieves the current status of a WireGuard interface
func (c *Controller) getCurrentWireGuardStatus(interfaceName string, containerPid *int) (*networkingv1alpha1.WireGuardInterfaceStatus, error) {

	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %s", err.Error())
	}

	status := &networkingv1alpha1.WireGuardInterfaceStatus{
		MTU:      nil,
		Hostname: hostname,
		Nodename: c.nodename,
	}

	netlinkHook := func(handle *netlink.Handle, wgLink netlink.Link) error {
		mtu := wgLink.Attrs().MTU
		status.MTU = &mtu

		addrObjs, err := handle.AddrList(wgLink, netlink.FAMILY_ALL)
		if err != nil {
			return fmt.Errorf("failed to get addresses: %s", err.Error())
		}

		status.Netlink = networkingv1alpha1.NewFromNetlinkLinkAttrs(wgLink.Attrs(), addrObjs)

		return nil
	}

	wgHook := func(wgCtrlCli *wgctrl.Client) error {
		device, err := wgCtrlCli.Device(interfaceName)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("device %s does not exist: %s", interfaceName, err.Error())
			}
			return fmt.Errorf("failed to get device: %s", err.Error())
		}
		status.WireGuard = networkingv1alpha1.NewWireGuardStatusWrapper(device)
		return nil
	}

	if err := pkgutils.WithNetnsWGCli(containerPid, wgHook); err != nil {
		return nil, fmt.Errorf("failed to configure/reconcile wg: %s", err.Error())
	}

	err = pkgutils.WithNetlinkHandle(containerPid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(interfaceName)
		if err != nil {
			return fmt.Errorf("failed to get link by name: %s", err.Error())
		}

		return netlinkHook(handle, link)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to configure/reconcile ip: %s", err.Error())
	}

	return status, nil
}

func (c *Controller) getSecretValue(ns *string, secName, key string) ([]byte, error) {
	usedNs := "default"
	if ns != nil && *ns != "" {
		usedNs = *ns
	}

	secObj, err := c.secretsLister.Secrets(usedNs).Get(secName)
	if err != nil {
		return nil, err
	}

	return secObj.Data[key], nil
}

func (c *Controller) getDockerContainerPid(containerName string) (int, error) {
	if containerName == "" {
		return -1, fmt.Errorf("moveToContainer is true but no container name is provided")
	}

	p, err := dockerUtil.GetPidOfContainer(context.Background(), c.dockerClient, containerName)
	if err != nil {
		return -1, err
	}

	return p, nil
}

func (c *Controller) getThisHostname() (string, error) {

	if c.nodename != "" {
		return c.nodename, nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("failed to get hostname: %s", err.Error())
	}

	return hostname, nil
}

// if the interface should be placed in current namespace, return nil
// use this function to determine where to look for the interface: is it in the current namespace or in a container?
// for example, if it returns a nil, look for the interface in the current namespace
// otherwise, look for the interface in the container specified by the pid
func (c *Controller) getInterfacePid(wgObjSpec *networkingv1alpha1.WireGuardInterfaceSpec) (*int, error) {
	if !wgObjSpec.MoveToContainer {
		return nil, nil
	}

	if wgObjSpec.Container == nil {
		return nil, fmt.Errorf("moveToContainer is true but no container is specified")
	}

	contObj := wgObjSpec.Container
	if contObj.Docker != nil && contObj.Docker.Name != "" {
		pid, err := c.getDockerContainerPid(contObj.Docker.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get pid of container %s: %s", contObj.Docker.Name, err.Error())
		}
		return &pid, nil
	}

	if contObj.NetNS != nil && contObj.NetNS.PID != nil {
		return contObj.NetNS.PID, nil
	}

	return nil, fmt.Errorf("no container is specified")
}

func (c *Controller) toWireGuardConf(wgi *networkingv1alpha1.WireGuardInterfaceSpec) (*wgtypes.Config, error) {
	privateKey := ""
	if wgi.PrivateKey != "" {
		privateKey = wgi.PrivateKey
	} else if wgi.PrivateKeySecretRef != nil {
		sec, err := c.getSecretValue(wgi.PrivateKeySecretRef.Namespace, wgi.PrivateKeySecretRef.Name, wgi.PrivateKeySecretRef.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to get private key secret: %s", err.Error())
		}
		privateKey = base64.StdEncoding.EncodeToString(sec)
	} else {
		return nil, fmt.Errorf("private key is required")
	}

	privKeyObj, err := wgtypes.ParseKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain the private key, either not provided or invalid: %s", err.Error())
	}

	wgConf := new(wgtypes.Config)
	if wgi.ListenPort != nil && *wgi.ListenPort != 0 {
		wgConf.ListenPort = wgi.ListenPort
	}

	wgConf.PrivateKey = &privKeyObj

	peerConfigs := make([]wgtypes.PeerConfig, 0)
	for _, peer := range wgi.Peers {
		peerConf, err := c.toZX2c4WGPeerConf(&peer)
		if err != nil {
			return nil, fmt.Errorf("failed to convert peer spec to wg peer conf: %s", err.Error())
		}
		peerConf.ReplaceAllowedIPs = true
		peerConfigs = append(peerConfigs, *peerConf)
	}

	wgConf.Peers = peerConfigs
	wgConf.ReplacePeers = true
	return wgConf, nil
}

func (c *Controller) toZX2c4WGPeerConf(peerSpec *networkingv1alpha1.WireGuardPeerSpec) (*wgtypes.PeerConfig, error) {
	wgPeerConf := new(wgtypes.PeerConfig)
	if peerSpec.PublicKey == "" {
		return nil, fmt.Errorf("public key is required")
	}

	pubkeyObj, err := wgtypes.ParseKey(peerSpec.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("invalid peer public key: %s", err.Error())
	}

	wgPeerConf.PublicKey = pubkeyObj

	if peerSpec.PresharedKeySecretRef != nil {

		psk, err := c.getSecretValue(peerSpec.PresharedKeySecretRef.Namespace, peerSpec.PresharedKeySecretRef.Name, peerSpec.PresharedKeySecretRef.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to get preshared key secret: %s", err.Error())
		}
		pskObj, err := wgtypes.ParseKey(base64.StdEncoding.EncodeToString(psk))
		if err != nil {
			return nil, fmt.Errorf("preshared provided but invalid: %s (note it is optional)", err.Error())
		}
		wgPeerConf.PresharedKey = &pskObj
	}

	if peerSpec.PersistentKeepalive != nil {
		intv := time.Duration(*peerSpec.PersistentKeepalive) * time.Second
		wgPeerConf.PersistentKeepaliveInterval = &intv
	}

	if peerSpec.Endpoint != nil && *peerSpec.Endpoint != "" {
		peerUDPAddr, err := net.ResolveUDPAddr("udp", *peerSpec.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve peer endpoint %s: %s", *peerSpec.Endpoint, err.Error())
		}
		wgPeerConf.Endpoint = peerUDPAddr
	}

	if len(peerSpec.AllowedIPs) > 0 {
		for _, iprange := range peerSpec.AllowedIPs {
			_, ipNet, err := net.ParseCIDR(iprange)
			if err != nil {
				return nil, fmt.Errorf("invalid allowed ip cidr: %s: %s", iprange, err.Error())
			}
			wgPeerConf.AllowedIPs = append(wgPeerConf.AllowedIPs, *ipNet)
		}
	}

	return wgPeerConf, nil
}

func (c *Controller) toWGDesiredConfig(wgObjSpec *networkingv1alpha1.WireGuardInterfaceSpec) (*pkgreconcile.WGDesiredState, error) {
	desiredState := new(pkgreconcile.WGDesiredState)
	desiredState.MTU = wgObjSpec.MTU
	defaultMTU := 1420
	if desiredState.MTU == nil {
		desiredState.MTU = &defaultMTU
	}
	if *desiredState.MTU == 0 {
		desiredState.MTU = &defaultMTU
	}

	netlinkIPAddrs := make([]netlink.Addr, 0)

	for _, addrSpec := range wgObjSpec.Addresses {
		addrObj, err := c.makeNetlinkAddrObject(&addrSpec)
		if err != nil {
			return nil, fmt.Errorf("failed to make netlink addr object: %s", err.Error())
		}
		netlinkIPAddrs = append(netlinkIPAddrs, *addrObj)
	}

	wgConf, err := c.toWireGuardConf(wgObjSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to convert wireguard config: %s", err.Error())
	}
	desiredState.WGOuterConfig = wgConf
	desiredState.MoveToContainer = wgObjSpec.MoveToContainer
	desiredState.Peers = wgConf.Peers

	desiredState.IPAddrs = netlinkIPAddrs
	return desiredState, nil
}

func (c *Controller) makeNetlinkAddrObject(addrSpec *networkingv1alpha1.WireGuardInterfaceAddressSpec) (*netlink.Addr, error) {
	family := addrSpec.Family
	local := addrSpec.Local
	peer := addrSpec.Peer
	prefixlen := addrSpec.Prefixlen
	if prefixlen == 0 {
		return nil, fmt.Errorf("invalid prefix length: %d", prefixlen)
	}

	bits := 32
	if family == networkingv1alpha1.InetFamilyInet6 {
		bits = 128
	}

	addrObj := new(netlink.Addr)
	addrObj.IPNet = new(net.IPNet)
	addrObj.IP = net.ParseIP(local)
	addrObj.Peer = new(net.IPNet)
	addrObj.Peer.IP = net.ParseIP(peer)
	addrObj.Peer.Mask = net.CIDRMask(prefixlen, bits)

	return addrObj, nil
}
