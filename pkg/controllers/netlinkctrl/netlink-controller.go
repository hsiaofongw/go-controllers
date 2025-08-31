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

package netlinkctrl

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	dockerUtil "example.com/go-util/pkg/util/docker"

	dockerSDK "github.com/docker/docker/client"
	"github.com/vishvananda/netlink"
	"golang.org/x/time/rate"

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
	nodeName     string
	dockerClient *dockerSDK.Client
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	sampleclientset clientset.Interface

	nlLister v1alpha1Lister.NetlinkInterfaceLister
	nlSynced cache.InformerSynced

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
	NetlinkInformer v1alpha1Informer.NetlinkInterfaceInformer
	NodeName        string
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
		nlLister:        config.NetlinkInformer.Lister(),
		nlSynced:        config.NetlinkInformer.Informer().HasSynced,
		workqueue:       workqueue.NewTypedRateLimitingQueue(ratelimiter),
		recorder:        recorder,
		nodeName:        config.NodeName,
	}

	logger.Info("Setting up event handlers")

	// Set up event handler for when WireGuardNetworkPlan resources change
	config.NetlinkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			nlObj, _ := obj.(*networkingv1alpha1.NetlinkInterface)
			revLog := pkgutils.RevChangeLog{
				Generation:         fmt.Sprintf("%d", nlObj.GetGeneration()),
				ResourceVersion:    nlObj.GetResourceVersion(),
				ObservedGeneration: fmt.Sprintf("%d", nlObj.Status.ObservedGeneration),
			}
			revLogJSON, _ := json.Marshal(revLog)
			logger.Info("AddFunc for NetlinkInterface resource is called", "objectReference", klog.KObj(nlObj), "Revision log", string(revLogJSON))
			controller.enqueueNl(nlObj)
		},
		UpdateFunc: func(old, new interface{}) {
			oldNl := old.(*networkingv1alpha1.NetlinkInterface)
			newNl := new.(*networkingv1alpha1.NetlinkInterface)

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
				logger.Info("Updating NetlinkInterface due to force resync, and resourceVersion is not changed", "objectReference", klog.KObj(newNl))
				if err := controller.updateNetlinkInterfaceStatus(context.Background(), newNl); err != nil {
					logger.Error(err, "Failed to update NetlinkInterface status", "objectReference", newNl.Name, "object is enqueued, and will retry later")
					// if failed to update status, simply give up rather than retry, because there's still next force-resync
				}
				return
			}

			logger.Info("Updating NetlinkInterface due to resourceVersion is changed", "objectReference", klog.KObj(newNl))
			controller.enqueueNl(new)
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
func (c *Controller) enqueueNl(obj interface{}) {
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

	nlObj, err := c.nlLister.Get(objectRef.Name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "NetlinkInterface referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	pid, err := c.getInterfacePid(nlObj.Spec.Container)
	if err != nil {
		return fmt.Errorf("failed to get interface pid: %s", err.Error())
	}

	deletionTime := nlObj.GetDeletionTimestamp()
	if deletionTime != nil {
		// Clean up underlying resources, then
		// clear all finalizers from the object

		err := pkgutils.WithNetlinkHandle(pid, func(handle *netlink.Handle) error {
			link, err := handle.LinkByName(nlObj.Spec.InterfaceName)
			if err != nil {
				if _, ok := err.(netlink.LinkNotFoundError); !ok {
					return fmt.Errorf("failed to get link %s: %s", nlObj.Spec.InterfaceName, err.Error())
				}

				return nil
			}

			return handle.LinkDel(link)
		})

		if err != nil {
			return fmt.Errorf("failed to delete link %s: %s", nlObj.Spec.InterfaceName, err.Error())
		}

		nlObjCopy := nlObj.DeepCopy()
		nlObjCopy.SetFinalizers([]string{})
		_, err = c.sampleclientset.NetworkingV1alpha1().NetlinkInterfaces().Update(context.Background(), nlObjCopy, metav1.UpdateOptions{})
		if err != nil {
			if !k8serrors.IsNotFound(err) {
				return fmt.Errorf("failed to clear finalizers from NetlinkInterface, will retry: %s", err.Error())
			}
		}

		return nil
	}

	reconcilerFactoryMap := make(map[string]ReconcilerFactory)
	reconcilerFactoryMap[string(networkingv1alpha1.NetlinkInterfaceTypeDummy)] = func() (Reconciler, error) {
		return NewDummyReconciler(nlObj.Spec.InterfaceName, pid)
	}
	reconcilerFactoryMap[string(networkingv1alpha1.NetlinkInterfaceTypeBridge)] = func() (Reconciler, error) {
		return NewBridgeReconciler(nlObj.Spec.InterfaceName, pid)
	}

	needReconcile := nlObj.GetGeneration() != nlObj.Status.ObservedGeneration
	if needReconcile {
		logger.Info("Need to reconcile", "objectReference", klog.KObj(nlObj), "observedGeneration", nlObj.Status.ObservedGeneration, "generation", nlObj.GetGeneration())

		reconcilerFactory, ok := reconcilerFactoryMap[string(nlObj.Spec.Type)]
		if !ok {
			return fmt.Errorf("unsupported NetlinkInterface type: %s", nlObj.Spec.Type)
		}

		reconciler, err := reconcilerFactory()
		if err != nil {
			return fmt.Errorf("failed to create reconciler: %s", err.Error())
		}

		var hasUpdates bool
		hasUpdates, err = reconciler.DetectChanges(ctx, nlObj.Spec)
		if err != nil {
			return fmt.Errorf("failed to detect changes: %s", err.Error())
		}
		maxLoops := 10
		for hasUpdates && maxLoops > 0 {
			err = reconciler.ApplyReconcile(ctx, nlObj.Spec)
			if err != nil {
				return fmt.Errorf("failed to apply reconcile: %s", err.Error())
			}

			reconciler.ResetState()
			hasUpdates, err = reconciler.DetectChanges(ctx, nlObj.Spec)
			if err != nil {
				break
			}
			if !hasUpdates {
				// It's converged here, no more reconciliation is needed
				break
			}

			maxLoops--
		}
		if err != nil {
			return fmt.Errorf("failed to reconcile NetlinkInterface: %s of type %s: %s", nlObj.Spec.InterfaceName, nlObj.Spec.Type, err.Error())
		}

		logger.Info("Updating NetlinkInterface status", "objectReference", klog.KObj(nlObj))
		// Update the status with current NetlinkInterface information
		err = c.updateNetlinkInterfaceStatus(ctx, nlObj)
		if err != nil {
			return fmt.Errorf("failed to update NetlinkInterface status: %s", err.Error())
		}
	}

	return nil
}

// updateWireGuardNetworkPlanStatus updates the status of a WireGuardNetworkPlan with current information
func (c *Controller) updateNetlinkInterfaceStatus(ctx context.Context, nlObj *networkingv1alpha1.NetlinkInterface) error {
	logger := klog.FromContext(ctx)

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	nlObjCopy := nlObj.DeepCopy()

	status, err := c.getCurrentNetlinkInterfaceStatus(ctx, nlObj)
	if err != nil {
		logger.Error(err, "Failed to get current NetlinkInterface status", "objectReference", klog.KObj(nlObj))
		// Don't fail the entire sync if status update fails
		return nil
	}

	// Update the status
	nlObjCopy.Status = *status

	// Use UpdateStatus to update only the Status block of the NetlinkInterface resource
	_, err = c.sampleclientset.NetworkingV1alpha1().NetlinkInterfaces().UpdateStatus(ctx, nlObjCopy, metav1.UpdateOptions{FieldManager: FieldManager})
	if err != nil {
		return fmt.Errorf("failed to update status: %s", err.Error())
	}

	logger.V(4).Info("Updated NetlinkInterface status", "objectReference", klog.KObj(nlObjCopy))
	return nil
}

func (c *Controller) getCurrentNetlinkInterfaceStatus(ctx context.Context, nlObj *networkingv1alpha1.NetlinkInterface) (*networkingv1alpha1.NetlinkInterfaceStatus, error) {
	logger := klog.FromContext(ctx)

	logger.Info("Getting current NetlinkInterface status", "objectReference", klog.KObj(nlObj))

	status := new(networkingv1alpha1.NetlinkInterfaceStatus)
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %s", err.Error())
	}

	status.Hostname = hostname
	status.Nodename = c.nodeName

	pid, err := c.getInterfacePid(nlObj.Spec.Container)
	if err != nil {
		return nil, fmt.Errorf("failed to get interface pid: %s", err.Error())
	}

	err = pkgutils.WithNetlinkHandle(pid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(nlObj.Spec.InterfaceName)
		if err != nil {
			return fmt.Errorf("failed to get link %s: %s", nlObj.Spec.InterfaceName, err.Error())
		}

		attrs := link.Attrs()
		mtu := attrs.MTU
		status.MTU = &mtu

		addrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			return fmt.Errorf("failed to get addresses of link %s: %s", nlObj.Spec.InterfaceName, err.Error())
		}

		status.Netlink = networkingv1alpha1.NewFromNetlinkLinkAttrs(attrs, addrs)

		status.ObservedGeneration = nlObj.GetGeneration()

		status.OperState = attrs.OperState.String()

		status.Flags = pkgutils.FlagsToStrings(attrs.Flags)

		addrStrs := make([]string, 0)
		for _, addr := range addrs {
			addrStrs = append(addrStrs, pkgutils.AddrToString(addr))
		}
		status.Addresses = addrStrs

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get current NetlinkInterface status: %s", err.Error())
	}

	return status, nil
}

// if the interface should be placed in current namespace, return nil
// use this function to determine where to look for the interface: is it in the current namespace or in a container?
// for example, if it returns a nil, look for the interface in the current namespace
// otherwise, look for the interface in the container specified by the pid
func (c *Controller) getInterfacePid(nlContainerSpec *networkingv1alpha1.NetlinkInterfaceContainerSpec) (*int, error) {
	if nlContainerSpec == nil {
		return nil, nil
	}

	if nlContainerSpec.Docker != nil && nlContainerSpec.Docker.Name != "" {
		pid, err := c.getDockerContainerPid(nlContainerSpec.Docker.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get pid of container %s: %s", nlContainerSpec.Docker.Name, err.Error())
		}
		return &pid, nil
	}

	if nlContainerSpec.NetNS != nil && nlContainerSpec.NetNS.PID != nil {
		return nlContainerSpec.NetNS.PID, nil
	}

	return nil, nil
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

type NetlinkAddrDifferenceSet struct {
	// the 'Added' addresses are those present in the spec but not in the current netlink interface's addresses list
	// Once should always ignore the key and treat it as the opaque.
	Added map[string]*networkingv1alpha1.NetlinkInterfaceAddressSpec

	// the 'Removed' addresses are those present in the current netlink interface but not in the spec.
	// Once should always ignore the key and treat it as the opaque.
	Removed map[string]*netlink.Addr

	// And there is no 'Updated' set since we never try to 'update' the address entry,
	// whenever there is a mismatch we just remove it and append the new one.
}

func getAddrSpecKey(addrSpec *networkingv1alpha1.NetlinkInterfaceAddressSpec) string {
	if addrSpec.PeerCIDR != nil {
		return fmt.Sprintf("%s -> %s", addrSpec.IPCIDR, *addrSpec.PeerCIDR)
	}
	return addrSpec.IPCIDR
}

func getNlAddrKey(addr *netlink.Addr) string {
	if addr == nil {
		return ""
	}
	return pkgutils.AddrToString(*addr)
}

func getReconciliationPlan(addrSpecs []networkingv1alpha1.NetlinkInterfaceAddressSpec, nlAddrs []netlink.Addr) (*NetlinkAddrDifferenceSet, error) {
	lhsSet := make(map[string]*networkingv1alpha1.NetlinkInterfaceAddressSpec)
	for _, addrSpec := range addrSpecs {
		lhsSet[getAddrSpecKey(&addrSpec)] = &addrSpec
	}

	rhsSet := make(map[string]*netlink.Addr)
	for _, addr := range nlAddrs {
		rhsSet[getNlAddrKey(&addr)] = &addr
	}

	addedSet := make(map[string]*networkingv1alpha1.NetlinkInterfaceAddressSpec)
	for k, v := range lhsSet {
		if _, ok := rhsSet[k]; !ok {
			addedSet[k] = v
		}
	}

	removedSet := make(map[string]*netlink.Addr)
	for k, v := range rhsSet {
		if _, ok := lhsSet[k]; !ok {
			removedSet[k] = v
		}
	}

	result := new(NetlinkAddrDifferenceSet)
	result.Added = addedSet
	result.Removed = removedSet
	return result, nil
}

func applyReconciliationPlan(handle *netlink.Handle, link netlink.Link, diffSet *NetlinkAddrDifferenceSet) error {
	for _, staleAddrPtr := range diffSet.Removed {
		if err := handle.AddrDel(link, staleAddrPtr); err != nil {
			return fmt.Errorf("failed to remove address %s: %s", staleAddrPtr.String(), err.Error())
		}
	}
	for _, newAddrSpecPtr := range diffSet.Added {
		addrObj, err := toNetlinkAddr(newAddrSpecPtr)
		if err != nil {
			return fmt.Errorf("failed to convert address spec %s to netlink address: %s", newAddrSpecPtr.IPCIDR, err.Error())
		}
		if err := handle.AddrAdd(link, addrObj); err != nil {
			return fmt.Errorf("failed to add address %s: %s", addrObj.String(), err.Error())
		}
	}
	return nil
}

func toNetlinkAddr(addrSpec *networkingv1alpha1.NetlinkInterfaceAddressSpec) (*netlink.Addr, error) {
	if addrSpec.PeerCIDR == nil {
		addrObj, err := netlink.ParseAddr(addrSpec.IPCIDR)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ipcidr %s: %s", addrSpec.IPCIDR, err.Error())
		}
		return addrObj, nil
	}

	_, peeripnet, err := net.ParseCIDR(*addrSpec.PeerCIDR)
	if err != nil {
		return nil, fmt.Errorf("failed to parse peerCidr %s: %s", *addrSpec.PeerCIDR, err.Error())
	}

	localIp := net.ParseIP(addrSpec.IPCIDR)
	if localIp == nil {
		return nil, fmt.Errorf("failed to parse ipcidr %s", addrSpec.IPCIDR)
	}

	addrObj := new(netlink.Addr)
	addrObj.IPNet = new(net.IPNet)
	addrObj.IP = localIp
	addrObj.Peer = peeripnet
	return addrObj, nil
}

// Returns: (updated, error)
func reconcileMTU(handle *netlink.Handle, link netlink.Link, mtu *int, dryRun bool) (bool, error) {
	hasUpdated := false

	if mtu != nil {
		if *mtu != link.Attrs().MTU {
			hasUpdated = true
			if dryRun {
				return true, nil
			}

			if err := handle.LinkSetMTU(link, *mtu); err != nil {
				return false, fmt.Errorf("failed to set mtu: %s", err.Error())
			}
		}
	}
	return hasUpdated, nil
}

// Returns: (updated, error)
func reconcileAddrs(handle *netlink.Handle, link netlink.Link, addrs []networkingv1alpha1.NetlinkInterfaceAddressSpec, dryRun bool) (bool, *NetlinkAddrDifferenceSet, error) {
	hasUpdated := false
	var diffSet *NetlinkAddrDifferenceSet

	if addrs != nil {
		nlAddrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			return false, diffSet, fmt.Errorf("failed to get addresses: %s", err.Error())
		}

		diffSet, err = getReconciliationPlan(addrs, nlAddrs)
		if err != nil {
			return false, nil, fmt.Errorf("failed to calculate the difference between the spec and the current netlink interface's addresses: %s", err.Error())
		}

		hasUpdated = len(diffSet.Added) > 0 || len(diffSet.Removed) > 0

		if dryRun {
			return hasUpdated, diffSet, nil
		}

		if err := applyReconciliationPlan(handle, link, diffSet); err != nil {
			return true, nil, fmt.Errorf("failed to apply the reconciliation plan: %s", err.Error())
		}
	}

	return hasUpdated, diffSet, nil
}

// Returns: (updated, error)
func reconcileAdminState(ctx context.Context, handle *netlink.Handle, link netlink.Link, up bool, dryRun bool) (bool, error) {
	logger := klog.FromContext(ctx)

	if up {
		if link.Attrs().Flags&net.FlagUp == 0 {
			// The admin state in spec is 'Up', but the link's admin state is 'Down'

			if dryRun {
				return true, nil
			}

			logger.Info("Setting up link", link.Attrs().Name)
			if err := handle.LinkSetUp(link); err != nil {
				return false, fmt.Errorf("failed to set up link %s: %s", link.Attrs().Name, err.Error())
			}
			return true, nil
		}
	} else {
		if link.Attrs().Flags&net.FlagUp == net.FlagUp {
			// The admin state in spec is 'Down', but the link's admin state is 'Up'
			if dryRun {
				return true, nil
			}

			logger.Info("Setting down link", link.Attrs().Name)
			if err := handle.LinkSetDown(link); err != nil {
				return false, fmt.Errorf("failed to set down link %s: %s", link.Attrs().Name, err.Error())
			}
			return true, nil
		}
	}

	// The admin state in spec is the same as the link's admin state
	return false, nil
}

type EnslavedLinksDifferenceSet struct {
	Added   map[string]netlink.Link
	Removed map[string]netlink.Link
}

// Returns: (updated, error)
func reconcileEnslavedLinks(handle *netlink.Handle, master netlink.Link, slaves []string, dryRun bool) (bool, *EnslavedLinksDifferenceSet, error) {
	diffSet := new(EnslavedLinksDifferenceSet)

	allNLLinks, err := handle.LinkList()
	if err != nil {
		return false, nil, fmt.Errorf("failed to get all netlink links: %s", err.Error())
	}

	enslavedNLLinks := make(map[string]netlink.Link)
	for _, lk := range allNLLinks {
		if lk.Attrs().MasterIndex == master.Attrs().Index {
			enslavedNLLinks[lk.Attrs().Name] = lk
		}
	}

	specEnslaveSet := make(map[string]bool)

	// the 'addedEnslavedLinks' set are those interfaces that
	// specified as the slaves of the bridge but not really enslaved
	addedEnslavedLinks := make(map[string]netlink.Link)
	for _, ifname := range slaves {
		specEnslaveSet[ifname] = true
		if _, ok := enslavedNLLinks[ifname]; !ok {
			newslave, err := handle.LinkByName(ifname)
			if err != nil {
				return false, nil, fmt.Errorf("failed to get link %s: %s", ifname, err.Error())
			}
			addedEnslavedLinks[ifname] = newslave
		}
	}

	// the 'removedEnslavedLinks' set are those that are already enslaved,
	// but not present in the spec
	removedEnslavedLinks := make(map[string]netlink.Link)
	for ifname, lk := range enslavedNLLinks {
		if _, ok := specEnslaveSet[ifname]; !ok {
			removedEnslavedLinks[ifname] = lk
		}
	}

	updated := len(addedEnslavedLinks) > 0 || len(removedEnslavedLinks) > 0
	diffSet.Added = addedEnslavedLinks
	diffSet.Removed = removedEnslavedLinks

	if dryRun {
		return updated, diffSet, nil
	}

	// Now, worked out these two sets, we are going to
	// un-enslave all the interfaces in the 'removedEnslavedLinks' set,
	// and enslave all the interfaces in the 'addedEnslavedLinks' set
	for _, lk := range addedEnslavedLinks {
		if err := handle.LinkSetMaster(lk, master); err != nil {
			return true, nil, fmt.Errorf("failed to enslave link %s to bridge %s: %s", lk.Attrs().Name, master.Attrs().Name, err.Error())
		}
	}

	for _, lk := range removedEnslavedLinks {
		if err := handle.LinkSetNoMaster(lk); err != nil {
			return true, nil, fmt.Errorf("failed to un-enslave link %s from bridge %s: %s", lk.Attrs().Name, master.Attrs().Name, err.Error())
		}
	}

	return updated, diffSet, nil
}

// Reconciler interface, its primary purpose is to decouple the
// detection of changes from the act of applying changes
type Reconciler interface {
	// DetectChanges is used to detect changes from the spec,
	// It should be guaranteed to not actually alter the underlying system state.
	// Returns: (hasUpdates, error)
	DetectChanges(ctx context.Context, desiredState interface{}) (bool, error)

	// ApplyReconcile is used to apply the changes to the underlying system state,
	// It acts based on the result of DetectChanges, which are store in the internal state of the reconciler.
	ApplyReconcile(ctx context.Context, desiredState interface{}) error

	// ResetState is used to reset the state of the reconciler,
	// it resets the internal state of the reconciler to the initial state.
	ResetState()
}

// A DummyReconciler implements the Reconciler interface
type DummyReconciler struct {
	interfaceName          string
	pid                    *int
	shouldCreateInterface  bool
	shouldRemoveAddrs      map[string]*netlink.Addr
	shouldAddAddrs         map[string]*networkingv1alpha1.NetlinkInterfaceAddressSpec
	shouldUpdateMTU        *int
	shouldUpdateAdminState *bool
}

func NewDummyReconciler(interfaceName string, pid *int) (*DummyReconciler, error) {
	dummyReconciler := new(DummyReconciler)
	dummyReconciler.interfaceName = interfaceName
	dummyReconciler.pid = pid
	return dummyReconciler, nil
}

func (r *DummyReconciler) gatherAllUpdates() bool {
	return r.shouldCreateInterface ||
		len(r.shouldRemoveAddrs) > 0 ||
		len(r.shouldAddAddrs) > 0 ||
		r.shouldUpdateMTU != nil ||
		r.shouldUpdateAdminState != nil
}

// Returns: (hasUpdates, error)
func (r *DummyReconciler) DetectChanges(ctx context.Context, desiredState interface{}) (bool, error) {
	dummySpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return false, fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
	}

	err := pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		_, err := handle.LinkByName(r.interfaceName)
		if err != nil {
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
			}

			r.shouldCreateInterface = true
		}
		return nil
	})

	if err != nil {
		return r.gatherAllUpdates(), err
	}

	err = pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(r.interfaceName)
		if err != nil {
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
			}

			return nil
		}

		updated, err := reconcileMTU(handle, link, dummySpec.MTU, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			mtu := *dummySpec.MTU
			r.shouldUpdateMTU = &mtu
		}

		updated, diffSet, err := reconcileAddrs(handle, link, dummySpec.Addresses, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			r.shouldRemoveAddrs = diffSet.Removed
			r.shouldAddAddrs = diffSet.Added
		}

		updated, err = reconcileAdminState(ctx, handle, link, dummySpec.Up, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			desiredAdminState := dummySpec.Up
			r.shouldUpdateAdminState = &desiredAdminState
		}

		return nil
	})

	return r.gatherAllUpdates(), err
}

// Returns: (converged, error)
func (r *DummyReconciler) ApplyReconcile(ctx context.Context, desiredState interface{}) error {
	dummySpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
	}

	return pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		if r.shouldCreateInterface {
			link := new(netlink.Dummy)
			if err := handle.LinkSetName(link, r.interfaceName); err != nil {
				return fmt.Errorf("failed to set name of link %s: %s", r.interfaceName, err.Error())
			}

			if err := handle.LinkAdd(link); err != nil {
				return fmt.Errorf("failed to add link %s: %s", r.interfaceName, err.Error())
			}

			// one should run `DetectChanges` again after `ApplyReconcile` until it's converged.
			return nil
		}

		link, _ := handle.LinkByName(r.interfaceName)
		if _, err := reconcileMTU(handle, link, r.shouldUpdateMTU, false); err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if _, _, err := reconcileAddrs(handle, link, dummySpec.Addresses, false); err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if _, err := reconcileAdminState(ctx, handle, link, dummySpec.Up, false); err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		return nil
	})
}

func (r *DummyReconciler) ResetState() {
	r.shouldCreateInterface = false
	r.shouldRemoveAddrs = nil
	r.shouldAddAddrs = nil
	r.shouldUpdateMTU = nil
	r.shouldUpdateAdminState = nil
}

type BridgeReconciler struct {
	interfaceName             string
	pid                       *int
	shouldUpdateMTU           *int
	shouldUpdateAdminState    *bool
	shouldAddAddrs            map[string]*networkingv1alpha1.NetlinkInterfaceAddressSpec
	shouldRemoveAddrs         map[string]*netlink.Addr
	shouldAddEnslavedLinks    map[string]netlink.Link
	shouldRemoveEnslavedLinks map[string]netlink.Link
	shouldCreateInterface     bool
}

func NewBridgeReconciler(interfaceName string, pid *int) (*BridgeReconciler, error) {
	bridgeReconciler := new(BridgeReconciler)
	bridgeReconciler.interfaceName = interfaceName
	bridgeReconciler.pid = pid
	return bridgeReconciler, nil
}

func (r *BridgeReconciler) gatherAllUpdates() bool {
	return r.shouldUpdateMTU != nil ||
		r.shouldUpdateAdminState != nil ||
		len(r.shouldAddAddrs) > 0 ||
		len(r.shouldRemoveAddrs) > 0 ||
		len(r.shouldAddEnslavedLinks) > 0 ||
		len(r.shouldRemoveEnslavedLinks) > 0
}

func (r *BridgeReconciler) DetectChanges(ctx context.Context, desiredState interface{}) (bool, error) {
	bridgeSpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return false, fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
	}

	err := pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		_, err := handle.LinkByName(r.interfaceName)
		if err != nil {
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
			}

			r.shouldCreateInterface = true
		}
		return nil
	})

	if err != nil {
		return r.gatherAllUpdates(), err
	}

	err = pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(r.interfaceName)
		if err != nil {
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
			}

			return nil
		}

		updated, err := reconcileMTU(handle, link, bridgeSpec.MTU, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			mtu := *bridgeSpec.MTU
			r.shouldUpdateMTU = &mtu
		}

		updated, diffSet, err := reconcileAddrs(handle, link, bridgeSpec.Addresses, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			r.shouldRemoveAddrs = diffSet.Removed
			r.shouldAddAddrs = diffSet.Added
		}

		updated, err = reconcileAdminState(ctx, handle, link, bridgeSpec.Up, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			desiredAdminState := bridgeSpec.Up
			r.shouldUpdateAdminState = &desiredAdminState
		}

		if bridgeSpec.Bridge != nil {
			updated, diffSet, err := reconcileEnslavedLinks(handle, link, bridgeSpec.Bridge.Slaves, true)
			if err != nil {
				return fmt.Errorf("failed to reconcile enslaved links of link %s: %s", r.interfaceName, err.Error())
			}

			if updated {
				r.shouldAddEnslavedLinks = diffSet.Added
				r.shouldRemoveEnslavedLinks = diffSet.Removed
			}

		}

		return nil
	})

	return r.gatherAllUpdates(), err
}

func (r *BridgeReconciler) ApplyReconcile(ctx context.Context, desiredState interface{}) error {
	bridgeSpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
	}

	return pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		if r.shouldCreateInterface {
			link := new(netlink.Dummy)
			if err := handle.LinkSetName(link, r.interfaceName); err != nil {
				return fmt.Errorf("failed to set name of link %s: %s", r.interfaceName, err.Error())
			}

			if err := handle.LinkAdd(link); err != nil {
				return fmt.Errorf("failed to add link %s: %s", r.interfaceName, err.Error())
			}

			// one should run `DetectChanges` again after `ApplyReconcile` until it's converged.
			return nil
		}

		link, _ := handle.LinkByName(r.interfaceName)
		if _, err := reconcileMTU(handle, link, r.shouldUpdateMTU, false); err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if _, err := reconcileMTU(handle, link, r.shouldUpdateMTU, false); err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if _, _, err := reconcileAddrs(handle, link, bridgeSpec.Addresses, false); err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if _, err := reconcileAdminState(ctx, handle, link, bridgeSpec.Up, false); err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		if _, _, err := reconcileEnslavedLinks(handle, link, bridgeSpec.Bridge.Slaves, false); err != nil {
			return fmt.Errorf("failed to reconcile enslaved links of link %s: %s", r.interfaceName, err.Error())
		}

		return nil
	})
}

func (r *BridgeReconciler) ResetState() {
	r.shouldUpdateMTU = nil
	r.shouldUpdateAdminState = nil
	r.shouldAddAddrs = nil
	r.shouldRemoveAddrs = nil
	r.shouldAddEnslavedLinks = nil
	r.shouldRemoveEnslavedLinks = nil
	r.shouldCreateInterface = false
}

type ReconcilerFactory = func() (Reconciler, error)
