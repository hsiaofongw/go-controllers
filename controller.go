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

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	dockerUtil "example.com/go-util/pkg/util/docker"
	"golang.zx2c4.com/wireguard/wgctrl"

	dockerSDK "github.com/docker/docker/client"
	"github.com/vishvananda/netlink"
	"golang.org/x/time/rate"

	"github.com/vishvananda/netns"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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
	wginformers "k8s.io/sample-controller/pkg/generated/informers/externalversions/networking/v1alpha1"
	wglister "k8s.io/sample-controller/pkg/generated/listers/networking/v1alpha1"
)

const controllerAgentName = "sample-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Foo is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a Foo fails
	// to sync due to a Deployment of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "Resource %q already exists and is not managed by Foo"
	// MessageResourceSynced is the message used for an Event fired when a Foo
	// is synced successfully
	MessageResourceSynced = "Foo synced successfully"
	// FieldManager distinguishes this controller from other things writing to API objects
	FieldManager = controllerAgentName
)

// Controller is the controller implementation for Foo resources
type Controller struct {
	dockerClient *dockerSDK.Client
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	sampleclientset clientset.Interface

	secretsLister secretlisters.SecretLister
	wgLister      wglister.WireGuardInterfaceLister
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
}

// NewController returns a new sample controller
func NewController(
	ctx context.Context,
	kubeclientset kubernetes.Interface,
	sampleclientset clientset.Interface,
	wgInformer wginformers.WireGuardInterfaceInformer,
	secretsInformer secretsinformers.SecretInformer,
) *Controller {
	logger := klog.FromContext(ctx)

	// Create event broadcaster
	// Add sample-controller types to the default Kubernetes Scheme so Events can be
	// logged for sample-controller types.
	utilruntime.Must(samplescheme.AddToScheme(scheme.Scheme))
	logger.V(4).Info("Creating event broadcaster")

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
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
		kubeclientset:   kubeclientset,
		sampleclientset: sampleclientset,
		wgLister:        wgInformer.Lister(),
		secretsLister:   secretsInformer.Lister(),
		wgSynced:        wgInformer.Informer().HasSynced,
		secretsSynced:   secretsInformer.Informer().HasSynced,
		workqueue:       workqueue.NewTypedRateLimitingQueue(ratelimiter),
		recorder:        recorder,
	}

	logger.Info("Setting up event handlers")

	// Set up an event handler for when Foo resources change
	wgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueWG,
		UpdateFunc: func(old, new interface{}) {
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

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting Foo controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(),
		c.wgSynced,
		c.secretsSynced,
	); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch two workers to process Foo resources
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

	var object metav1.Object

	logger.V(4).Info("Processing wgi object creation", "object", objectRef.Name)

	wgObj, err := c.wgLister.Get(object.GetName())
	if err != nil {
		if errors.IsNotFound(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "WireGuardInterface referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	nodeName := wgObj.Spec.Node
	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %s", err.Error())
	}

	if host != nodeName {
		logger.V(4).Info("This node is not responsible for this WireGuardInterface", "objectReference", objectRef)
		return nil
	}

	deletionTime := wgObj.GetDeletionTimestamp()
	if deletionTime != nil {
		// Clean up underlying resources, then
		// clear all finalizers from the object
		err := c.tryDeleteInterfaceIfExists(wgObj.Spec.InterfaceName, nil)
		if err != nil {
			return fmt.Errorf("failed to delete interface: %s", err.Error())
		}

		wgObjCopy := wgObj.DeepCopy()
		wgObjCopy.SetFinalizers([]string{})
		_, err = c.sampleclientset.NetworkingV1alpha1().WireGuardInterfaces().Update(context.Background(), wgObjCopy, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to clear finalizers from WireGuardInterface, will retry: %s", err.Error())
		}

		return nil
	}

	var containerPid *int = nil
	if wgObj.Spec.MoveToContainer && wgObj.Spec.Container != nil && wgObj.Spec.Container.Docker != nil {
		p, err := c.getDockerContainerPid(wgObj.Spec.Container.Docker.Name)
		if err != nil {
			return fmt.Errorf("failed to get container pid: %s", err.Error())
		}
		containerPid = &p
	}

	privkeyNS := "default"
	if wgObj.Spec.PrivateKeySecretRef.Namespace != nil {
		privkeyNS = *wgObj.Spec.PrivateKeySecretRef.Namespace
	}

	privKey, err := c.getSecretValue(privkeyNS, wgObj.Spec.PrivateKeySecretRef.Name, wgObj.Spec.PrivateKeySecretRef.Key)
	if err != nil {
		return fmt.Errorf("failed to get private key: %s", err.Error())
	}

	privkeyStr := string(privKey)

	ipconfigurator := func(handle *netlink.Handle, wgLink netlink.Link) error {
		// todo
		return nil
	}

	wgconfigurator := func(wgCtrlCli *wgctrl.Client, reconcile bool) error {
		wgConf, err := wgObj.Spec.ToZX2c4WGConf(&privkeyStr)
		if err != nil {
			return fmt.Errorf("failed to convert WireGuardInterface to config: %s", err.Error())
		}

		for peerIdx, peer := range wgObj.Spec.Peers {
			wgPeerConf, err := peer.ToZX2c4WGPeerConf(nil)
			if err != nil {
				return fmt.Errorf("failed to convert wgi peer spec to zx2c4 wg peer conf: %s, peerIdx: %d", err.Error(), peerIdx)
			}

			wgConf.Peers = append(wgConf.Peers, *wgPeerConf)
		}

		if reconcile {
			wgConf.ReplacePeers = true
			if wgConf.Peers != nil {
				for peeridx := range wgConf.Peers {
					wgConf.Peers[peeridx].ReplaceAllowedIPs = true
				}
			}
		}

		return wgCtrlCli.ConfigureDevice(wgObj.Spec.InterfaceName, *wgConf)
	}

	err = c.getCurrentWGInterface(wgObj.Spec.InterfaceName, containerPid, ipconfigurator, func(wgCtrlCli *wgctrl.Client) error {
		return wgconfigurator(wgCtrlCli, true)
	})

	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to get current WireGuard interface: %s", err.Error())
		}

		err = c.createNewWGInterface(wgObj, containerPid, ipconfigurator, func(wgCtrlCli *wgctrl.Client) error {
			return wgconfigurator(wgCtrlCli, false)
		})
		if err != nil {
			return fmt.Errorf("failed to create new WireGuard interface: %s", err.Error())
		}
	}

	return nil
}

func (c *Controller) getSecretValue(ns, secName, key string) ([]byte, error) {
	secObj, err := c.secretsLister.Secrets(ns).Get(secName)
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

// return a non-nil error only if the error is non-recoverable.
// if the interface doesn't exist at the moment, it does nothing and silently returns nil.
func (c *Controller) tryDeleteInterfaceIfExists(interfaceName string, pid *int) error {
	var nlHandle *netlink.Handle
	var err error

	logger := klog.FromContext(context.Background())

	nlHandle, err = netlink.NewHandle()
	if err != nil {
		return fmt.Errorf("failed to get netlink handle: %s", err.Error())
	}

	defer nlHandle.Close()

	if pid == nil {
		lk, err := netlink.LinkByName(interfaceName)
		if err != nil || lk == nil {
			return nil
		}

		err = nlHandle.LinkDel(lk)
		if err != nil {
			return fmt.Errorf("failed to delete link: %s", err.Error())
		}
		return nil
	}

	nsHandle, err := netns.GetFromPid(*pid)
	if err != nil {
		return fmt.Errorf("failed to get ns handle of PID %d: %s", *pid, err.Error())
	}
	defer func() {
		if err := nsHandle.Close(); err != nil {
			logger.Error(err, "Error closing ns handle", "pid", *pid)
		}
	}()

	nsNlHandle, err := netlink.NewHandleAt(nsHandle)
	if err != nil {
		return fmt.Errorf("failed to get netlink at ns PID %d: %s", *pid, err.Error())
	}

	defer nsNlHandle.Close()

	intf, err := nsNlHandle.LinkByName(interfaceName)
	if err != nil {
		return nil
	}

	err = nsNlHandle.LinkDel(intf)
	if err != nil {
		return fmt.Errorf("failed to delete link inside ns: %s", err.Error())
	}
	return nil
}

// find then configure the existing WireGuard interface
func (c *Controller) getCurrentWGInterface(interfaceName string, pid *int, ipconfigurator func(handle *netlink.Handle, wgLink netlink.Link) error, wgconfigurator func(wgCtrlCli *wgctrl.Client) error) error {
	handle, err := netlink.NewHandle()
	if err != nil {
		return fmt.Errorf("failed to get netlink handle: %s", err.Error())
	}
	defer handle.Close()

	wgCtrlCli, err := wgctrl.New()
	if err != nil {
		return fmt.Errorf("failed to get wgctrl client: %s", err.Error())
	}
	defer wgCtrlCli.Close()

	if pid == nil {
		link, err := handle.LinkByName(interfaceName)
		if err != nil {
			nfErr, ok := err.(netlink.LinkNotFoundError)
			if ok {
				return errors.NewNotFound(corev1.Resource("wireguardinterface"), interfaceName)
			}
			return nfErr
		}

		if err := wgconfigurator(wgCtrlCli); err != nil {
			return fmt.Errorf("failed to configure wgctrl client: %s", err.Error())
		}
		if err := ipconfigurator(handle, link); err != nil {
			return fmt.Errorf("failed to configure ip: %s", err.Error())
		}
		return nil
	}

	nsHandle, err := netns.GetFromPid(*pid)
	if err != nil {
		return fmt.Errorf("failed to get ns handle of PID %d: %s", *pid, err.Error())
	}
	defer nsHandle.Close()

	nsNlHandle, err := netlink.NewHandleAt(nsHandle)
	if err != nil {
		return fmt.Errorf("failed to get netlink at ns PID %d: %s", *pid, err.Error())
	}
	defer nsNlHandle.Close()

	link, err := nsNlHandle.LinkByName(interfaceName)
	if err != nil {
		nfErr, ok := err.(netlink.LinkNotFoundError)
		if ok {
			return errors.NewNotFound(corev1.Resource("wireguardinterface"), interfaceName)
		}
		return nfErr
	}

	if err := wgconfigurator(wgCtrlCli); err != nil {
		return fmt.Errorf("failed to configure wgctrl client: %s", err.Error())
	}

	if err := ipconfigurator(nsNlHandle, link); err != nil {
		return fmt.Errorf("failed to configure ip: %s", err.Error())
	}

	return nil
}

// create then configure the new WireGuard interface
func (c *Controller) createNewWGInterface(
	wgObj *networkingv1alpha1.WireGuardInterface,
	pid *int,
	ipconfigurator func(handle *netlink.Handle, wgLink netlink.Link) error,
	wgconfigurator func(wgCtrlCli *wgctrl.Client) error,
) error {
	wgLink := new(netlink.Wireguard)
	wgLink.Attrs().Name = wgObj.Spec.InterfaceName

	handle, err := netlink.NewHandle()
	if err != nil {
		return fmt.Errorf("failed to get netlink handle: %s", err.Error())
	}
	defer handle.Close()

	if err := handle.LinkAdd(wgLink); err != nil {
		return fmt.Errorf("failed to add link: %s", err.Error())
	}

	wgCtrlCli, err := wgctrl.New()
	if err != nil {
		return fmt.Errorf("failed to get wgctrl client: %s", err.Error())
	}
	defer wgCtrlCli.Close()

	if err := wgconfigurator(wgCtrlCli); err != nil {
		return fmt.Errorf("failed to configure wgctrl client: %s", err.Error())
	}

	if err := handle.LinkAdd(wgLink); err != nil {
		return fmt.Errorf("failed to add link: %s", err.Error())
	}

	if pid == nil {
		if err := handle.LinkSetUp(wgLink); err != nil {
			return fmt.Errorf("failed to set link %s up: %s", wgObj.Spec.InterfaceName, err.Error())
		}

		if err := ipconfigurator(handle, wgLink); err != nil {
			return fmt.Errorf("failed to configure link: %s", err.Error())
		}

		return nil
	}

	if err := handle.LinkSetNsPid(wgLink, *pid); err != nil {
		return fmt.Errorf("failed to move link to ns: %s", err.Error())
	}

	nsHandle, err := netns.GetFromPid(*pid)
	if err != nil {
		return fmt.Errorf("failed to get ns handle of PID %d: %s", *pid, err.Error())
	}
	defer nsHandle.Close()

	nsNlHandle, err := netlink.NewHandleAt(nsHandle)
	if err != nil {
		return fmt.Errorf("failed to get netlink at ns PID %d: %s", *pid, err.Error())
	}
	defer nsNlHandle.Close()

	nsWgLink, err := nsNlHandle.LinkByName(wgObj.Spec.InterfaceName)
	if err != nil {
		return fmt.Errorf("failed to get link inside ns: %s", err.Error())
	}

	if err := nsNlHandle.LinkSetUp(nsWgLink); err != nil {
		return fmt.Errorf("failed to set link %s up: %s", wgObj.Spec.InterfaceName, err.Error())
	}

	if ipconfigurator == nil {
		return fmt.Errorf("configure function is nil")
	}

	if err := ipconfigurator(nsNlHandle, wgLink); err != nil {
		return fmt.Errorf("failed to configure link: %s", err.Error())
	}

	return nil
}
