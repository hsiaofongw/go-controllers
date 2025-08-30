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
	nlLister      v1alpha1Lister.NetlinkInterfaceLister

	wgSynced      cache.InformerSynced
	secretsSynced cache.InformerSynced
	wgPlanSynced  cache.InformerSynced
	nlSynced      cache.InformerSynced

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
	NetlinkInformer v1alpha1Informer.NetlinkInterfaceInformer
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
		nlLister:        config.NetlinkInformer.Lister(),
		wgSynced:        config.WgInformer.Informer().HasSynced,
		wgPlanSynced:    config.WgPlanInformer.Informer().HasSynced,
		secretsSynced:   config.SecretsInformer.Informer().HasSynced,
		nlSynced:        config.NetlinkInformer.Informer().HasSynced,
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
			controller.enqueueNl(objWgPlan)
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
		c.wgSynced,
		c.secretsSynced,
		c.wgPlanSynced,
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

	deletionTime := nlObj.GetDeletionTimestamp()
	if deletionTime != nil {
		// Clean up underlying resources, then
		// clear all finalizers from the object

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

	needReconcile := nlObj.GetGeneration() != nlObj.Status.ObservedGeneration
	if needReconcile {
		logger.Info("Need to reconcile", "objectReference", klog.KObj(nlObj), "observedGeneration", nlObj.Status.ObservedGeneration, "generation", nlObj.GetGeneration())

		switch nlObj.Spec.Type {
		case networkingv1alpha1.NetlinkInterfaceTypeDummy:
			logger.Info("Reconciling Dummy NetlinkInterface", "objectReference", klog.KObj(nlObj))
			err = c.reconcileDummyNetlinkInterface(ctx, nlObj)
			if err != nil {
				return fmt.Errorf("failed to reconcile Dummy NetlinkInterface: %s", err.Error())
			}
		// todo: add more types support here
		default:
			panic("unsupported NetlinkInterface type: " + nlObj.Spec.Type)
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

func (c *Controller) reconcileDummyNetlinkInterface(ctx context.Context, nlObj *networkingv1alpha1.NetlinkInterface) error {
	logger := klog.FromContext(ctx)

	logger.Info("Reconciling Dummy NetlinkInterface", "objectReference", klog.KObj(nlObj))

	pid, err := c.getInterfacePid(nlObj.Spec.Container)
	if err != nil {
		return fmt.Errorf("failed to get interface pid: %s", err.Error())
	}

	err = pkgutils.WithNetlinkHandle(pid, func(handle *netlink.Handle) error {
		_, err := handle.LinkByName(nlObj.Spec.InterfaceName)
		if err != nil {
			if _, ok := err.(netlink.LinkNotFoundError); ok {
				return fmt.Errorf("link %s not found", nlObj.Spec.InterfaceName)
			}

			link := new(netlink.Dummy)
			link.Attrs().Name = nlObj.Spec.InterfaceName
			if err := handle.LinkAdd(link); err != nil {
				return fmt.Errorf("failed to add link %s: %s", nlObj.Spec.InterfaceName, err.Error())
			}

			return nil
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to reconcile Dummy NetlinkInterface: %s", err.Error())
	}

	// Reconciliation of dummy interface is rather easy,
	// only have to check the MTU, addresses and administrative state (Up/Down).
	return pkgutils.WithNetlinkHandle(pid, func(handle *netlink.Handle) error {
		link, _ := handle.LinkByName(nlObj.Spec.InterfaceName)
		if nlObj.Spec.MTU != nil {
			if *nlObj.Spec.MTU != link.Attrs().MTU {
				if err := handle.LinkSetMTU(link, *nlObj.Spec.MTU); err != nil {
					return fmt.Errorf("failed to set mtu of link %s: %s", nlObj.Spec.InterfaceName, err.Error())
				}
			}
		}

		if nlObj.Spec.Addresses != nil {
			nlAddrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
			if err != nil {
				return fmt.Errorf("failed to get addresses of link %s: %s", nlObj.Spec.InterfaceName, err.Error())
			}

			diffSet, err := getReconciliationPlan(nlObj.Spec.Addresses, nlAddrs)
			if err != nil {
				return fmt.Errorf("failed to calculate the difference between the spec and the current netlink interface's addresses: %s", err.Error())
			}

			if err := applyReconciliationPlan(handle, link, diffSet); err != nil {
				return fmt.Errorf("failed to apply the reconciliation plan for link %s: %s", nlObj.Spec.InterfaceName, err.Error())
			}
		}

		if nlObj.Spec.Up {
			if link.Attrs().Flags&net.FlagUp == 0 {
				logger.Info("Setting up link %s", "objectReference", klog.KObj(nlObj))
				if err := handle.LinkSetUp(link); err != nil {
					return fmt.Errorf("failed to set up link %s: %s", nlObj.Spec.InterfaceName, err.Error())
				}
			}
		} else {
			if link.Attrs().Flags&net.FlagUp == net.FlagUp {
				logger.Info("Setting down link %s", "objectReference", klog.KObj(nlObj))
				if err := handle.LinkSetDown(link); err != nil {
					return fmt.Errorf("failed to set down link %s: %s", nlObj.Spec.InterfaceName, err.Error())
				}
			}
		}

		return nil
	})
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

	status := new(networkingv1alpha1.NetlinkInterfaceStatus)

	logger.Info("Getting current NetlinkInterface status", "objectReference", klog.KObj(nlObj))

	// todo: implement this
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
	if addr.Peer != nil {
		return fmt.Sprintf("%s -> %s", addr.String(), addr.Peer)
	}
	return addr.String()
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

	localIp, _, err := net.ParseCIDR(addrSpec.IPCIDR)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ipcidr %s: %s", addrSpec.IPCIDR, err.Error())
	}

	addrObj := new(netlink.Addr)
	addrObj.IPNet = new(net.IPNet)
	addrObj.IP = localIp
	addrObj.Peer = peeripnet
	return addrObj, nil
}
