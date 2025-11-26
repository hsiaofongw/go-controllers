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
	"fmt"
	"net"
	"os"
	"time"

	dockerUtil "example.com/go-util/pkg/util/docker"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	dockerSDK "github.com/docker/docker/client"
	"github.com/vishvananda/netlink"
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

	pkgnetapplycommon "github.com/internetworklab/netapply/pkg/interface/common"
	pkgnetapplywg "github.com/internetworklab/netapply/pkg/interface/wireguard"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	clientset "k8s.io/sample-controller/pkg/generated/clientset/versioned"
	samplescheme "k8s.io/sample-controller/pkg/generated/clientset/versioned/scheme"
	v1alpha1Informer "k8s.io/sample-controller/pkg/generated/informers/externalversions/networking/v1alpha1"
	v1alpha1Lister "k8s.io/sample-controller/pkg/generated/listers/networking/v1alpha1"
	pkgreconcile "k8s.io/sample-controller/pkg/reconcile"
)

const controllerAgentName = "wg-controller"

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
}

type ControllerConfig struct {
	Nodename        string
	Kubeclientset   kubernetes.Interface
	Sampleclientset clientset.Interface
	WgInformer      v1alpha1Informer.WireGuardInterfaceNGInformer
	SecretsInformer secretsinformers.SecretInformer
	DryRun          bool
	Namespace       string
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
	}
	if res.Spec.Container != nil {
		containerInfo := pkgnetapplycommon.ContainerInfo(*res.Spec.Container)
		cfg.Container = &containerInfo
	}
	for _, addr := range res.Spec.Addresses {
		cfg.Addresses = append(cfg.Addresses, pkgnetapplycommon.AddressConfig(addr))
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
		ns:              config.Namespace,
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
		_, err = c.sampleclientset.NetworkingV1alpha1().WireGuardInterfaceNGs(c.ns).Update(context.Background(), wgObjCopy, metav1.UpdateOptions{})
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

	status, err := provisioner.ToStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to convert wireguard config to status: %s", err.Error())
	}

	prevStatus := wgObj.Status.Resource
	if status.IsEqual(prevStatus) {
		// well, no changes, just return
		return nil
	}

	wgResStatus, ok := status.(*pkgnetapplywg.WireGuardInterfaceStatus)
	if !ok {
		return fmt.Errorf("failed to convert abstract interface status to concrete wireguard resource status")
	}

	// Update the status
	wgObj.Status = networkingv1alpha1.WireGuardInterfaceNGStatus{
		Nodename: c.nodename,
		Resource: wgResStatus,
	}

	// Use UpdateStatus to update only the Status block of the WireGuardInterface resource
	_, err = c.sampleclientset.NetworkingV1alpha1().WireGuardInterfaceNGs(c.ns).UpdateStatus(ctx, wgObj, metav1.UpdateOptions{FieldManager: FieldManager})

	if err != nil {
		return fmt.Errorf("failed to update status: %s", err.Error())
	}

	logger.V(4).Info("Updated WireGuard interface status", "interfaceName", wgObj.Spec.InterfaceName)
	return nil
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

func (c *Controller) getSecretValue(ns *string, secName, key string) ([]byte, error) {
	return doGetSecretValue(c.secretsLister, ns, secName, key)
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
