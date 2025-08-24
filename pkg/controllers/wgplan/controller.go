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
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	dockerUtil "example.com/go-util/pkg/util/docker"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	dockerSDK "github.com/docker/docker/client"
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

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	clientset "k8s.io/sample-controller/pkg/generated/clientset/versioned"
	samplescheme "k8s.io/sample-controller/pkg/generated/clientset/versioned/scheme"
	v1alpha1Informer "k8s.io/sample-controller/pkg/generated/informers/externalversions/networking/v1alpha1"
	v1alpha1Lister "k8s.io/sample-controller/pkg/generated/listers/networking/v1alpha1"
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
	Nodename        string
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
		nodename:        config.Nodename,
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
			objMeta, _ := obj.(metav1.Object)
			logger.Info("Updating WireGuardNetworkPlan due to creation", "objectReference", klog.KObj(objMeta))
			controller.enqueueWG(obj)
		},
		UpdateFunc: func(old, new interface{}) {
			oldWG := old.(*networkingv1alpha1.WireGuardNetworkPlan)
			newWG := new.(*networkingv1alpha1.WireGuardNetworkPlan)

			revisionChanged := newWG.ResourceVersion != oldWG.ResourceVersion
			if revisionChanged {
				logger.Info("Revision changed", "old", oldWG.ResourceVersion, "new", newWG.ResourceVersion, "objectReference", klog.KObj(newWG))
			}

			generationChanged := newWG.GetGeneration() != oldWG.GetGeneration()
			if generationChanged {
				logger.Info("Generation changed", "old", oldWG.GetGeneration(), "new", newWG.GetGeneration(), "objectReference", klog.KObj(newWG))
			}

			generationLagged := oldWG.Status.ObservedGeneration != newWG.GetGeneration()
			if generationLagged {
				// this is probably due to the controller went offline while the api-server is sending updates.
				logger.Info("Generation lagged", "observedGeneration", oldWG.Status.ObservedGeneration, "new", newWG.GetGeneration(), "objectReference", klog.KObj(newWG))
			}

			if revisionChanged || generationChanged || generationLagged {
				logger.Info("Updating WireGuardNetworkPlan due to both resourceVersion and generation are changed", "objectReference", klog.KObj(newWG))
				controller.enqueueWG(new)
			} else if newWG.GetDeletionTimestamp() != nil {
				logger.Info("Updating WireGuardNetworkPlan due to deletion", "objectReference", klog.KObj(newWG))
				controller.enqueueWG(new)
			} else {
				logger.Info("Updating WireGuardNetworkPlan due to force resync", "objectReference", klog.KObj(newWG))
				if err := controller.updateWireGuardNetworkPlanStatus(context.Background(), newWG); err != nil {
					logger.Error(err, "Failed to update WireGuardInterface status", "objectReference", newWG.Name, "object is enqueued, and will retry later")
					// if failed to update status, enqueue the object for a later retry, otherwise we'll have to wait for the next resync.
					controller.enqueueWG(new)
				}
			}
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

	logger.V(4).Info("Processing wgi object creation", "object", objectRef.Name)

	wgPlanObj, err := c.wgPlanLister.Get(objectRef.Name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			utilruntime.HandleErrorWithContext(ctx, err, "WireGuardNetworkPlan referenced by item in work queue no longer exists", "objectReference", objectRef)
			return nil
		}

		return err
	}

	deletionTime := wgPlanObj.GetDeletionTimestamp()
	if deletionTime != nil {
		// Clean up underlying resources, then
		// clear all finalizers from the object

		// todo: clean up underlying resources (those WireGuardInterface resources that has ownerReference pointing to this WireGuardNetworkPlan)

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

	// todo: reconcile logic goes here

	// 1. work out an actual plan from the spec
	actualPlan, err := c.NewWGActualPlanFromObj(wgPlanObj)

	// 2. query the lister to get depedent WireGuardInterface resources that are controlled by this
	// 3. create or update any WireGuardInterface resources that are needed

	// Update the status with current WireGuard interface information
	err = c.updateWireGuardNetworkPlanStatus(ctx, wgPlanObj)
	if err != nil {
		return fmt.Errorf("failed to update WireGuard interface status: %s", err.Error())
	}

	return nil
}

// updateWireGuardNetworkPlanStatus updates the status of a WireGuardNetworkPlan with current information
func (c *Controller) updateWireGuardNetworkPlanStatus(ctx context.Context, wgObj *networkingv1alpha1.WireGuardNetworkPlan) error {
	// logger := klog.FromContext(ctx)

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// wgPlanObjCopy := wgPlanObj.DeepCopy()

	// todo:
	// 1. get all WireGuardInterface resources
	// 2. collect the statuses from the underlying WireGuardInterface resources
	// 3. update the status of the WireGuardNetworkPlan resource

	// logger.V(4).Info("Updated WireGuardNetworkPlan status", "objectReference", klog.KObj(wgObj))
	return nil
}

type WGActualPlanInterface struct {
	Node          string
	InterfaceName string

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
}

func (wgaIntf *WGActualPlanInterface) GetResourceId(fromNode, toNode string, linkIdx int) string {
	return fmt.Sprintf("%s-%s-%d", fromNode, toNode, linkIdx)
}

func (wgaIntf *WGActualPlanInterface) GetWGIntfName(fromNode, toNode string, linkIdx int) string {
	return fmt.Sprintf("wg-%s-%s-%d", fromNode, toNode, linkIdx)
}

func (wgaIntf *WGActualPlanInterface) ComputeConfigHash() error {
	type configHashPayload struct {
		WGIntfName    string `json:"wgIntfName"`
		InterfaceName string `json:"interfaceName"`
		ListenPort    *int   `json:"listenPort"`
		Hostname      string `json:"hostname"`
		PublicKey     string `json:"publicKey"`
		PrivateKey    string `json:"privateKey"`
	}
	payload := configHashPayload{
		WGIntfName:    wgaIntf.WGIntfName,
		InterfaceName: wgaIntf.InterfaceName,
		ListenPort:    wgaIntf.ListenPort,
		Hostname:      wgaIntf.Hostname,
		PublicKey:     wgaIntf.PublicKey,
		PrivateKey:    wgaIntf.PrivateKey,
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	wgaIntf.ConfigHash = fmt.Sprintf("%x", sha256.Sum256(jsonPayload))
	return nil
}

func (wgaIntf *WGActualPlanInterface) ToWireGuardInterfaceObject(wgPlanName string) *networkingv1alpha1.WireGuardInterface {
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
				},
			},
			Labels: map[string]string{
				"networkplan.networking.dn42.io/resourceId":       wgaIntf.ResourceId, // by comparing the resourceId, we can know which resources need to be created or deleted.
				"networkplan.networking.dn42.io/configHash":       wgaIntf.ConfigHash, // by comparing the configHash against that in the computed value, we can decide whether this resource is needed to be updated.
				"networkplan.networking.dn42.io/is-controlled-by": wgPlanName,         // use this label to facilitate the selection of dependent resources.
			},
			Finalizers: []string{
				"networkplan.networking.dn42.io/finalizer",
			},
		},
		Spec: networkingv1alpha1.WireGuardInterfaceSpec{
			// todo: these are todos
		},
	}
}

type WGActualPlan struct {
	Interfaces []*WGActualPlanInterface
}

func (c *Controller) NewWGActualPlanFromObj(wgPlanObj *networkingv1alpha1.WireGuardNetworkPlan) (*WGActualPlan, error) {
	if wgPlanObj.Spec.DB == nil {
		return nil, fmt.Errorf("spec.db is nil")
	}

	plan := new(WGActualPlan)

	type nodeEntry struct {
		nodeName   string
		hostName   string
		portRange  *networkingv1alpha1.WireGuardNetworkPlanPortRangeSpec
		privateKey string
		publicKey  string
	}

	nodeEntries := make(map[string]*nodeEntry)
	for _, node := range wgPlanObj.Spec.DB.Nodes {
		ent := nodeEntry{
			nodeName:   node.NodeName,
			hostName:   "",
			portRange:  &wgPlanObj.Spec.DB.DefaultPortRange,
			privateKey: "",
			publicKey:  "",
		}
		if node.Underlay != nil {
			if node.Underlay.Hostname != "" {
				ent.hostName = node.Underlay.Hostname
			}
			if node.Underlay.PortRange != nil {
				ent.portRange = node.Underlay.PortRange
			}
		}
		if node.PrivateKeyRef.Name == "" {
			return nil, fmt.Errorf("privateKeyRef.name of node %s is empty", node.NodeName)
		}
		if node.PrivateKeyRef.Key == "" {
			return nil, fmt.Errorf("privateKeyRef.key of node %s is empty", node.NodeName)
		}
		secNs := "default"
		if node.PrivateKeyRef.Namespace != nil && *node.PrivateKeyRef.Namespace != "" {
			secNs = *node.PrivateKeyRef.Namespace
		}
		secObj, err := c.secretsLister.Secrets(secNs).Get(node.PrivateKeyRef.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get secret %s/%s: %s", secNs, node.PrivateKeyRef.Name, err.Error())
		}

		ent.privateKey = base64.StdEncoding.EncodeToString(secObj.Data[node.PrivateKeyRef.Key])
		keyObj, err := wgtypes.ParseKey(ent.privateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key %s: %s", ent.privateKey, err.Error())
		}
		ent.publicKey = keyObj.PublicKey().String()

		nodeEntries[node.NodeName] = &ent
	}

	if wgPlanObj.Spec.Adjacency == nil {
		return nil, fmt.Errorf("spec.adjacency is nil")
	}

	planIntfObjs := make([]*WGActualPlanInterface, 0)
	plan.Interfaces = planIntfObjs
	for _, link := range wgPlanObj.Spec.Adjacency.Links {
		fromNode, found := nodeEntries[link.FromNode]
		if !found {
			return nil, fmt.Errorf("node %s not found in nodeEntries", link.FromNode)
		}

		toNodes := make([]string, 0)
		toNodes = append(toNodes, link.ToNodes...)
		sort.Strings(toNodes)

		for linkIdx, toNode := range toNodes {
			toNode, found := nodeEntries[toNode]
			if !found {
				return nil, fmt.Errorf("node %s not found in nodeEntries", toNode)
			}

			planIntfObj := new(WGActualPlanInterface)
			planIntfObj.Node = link.FromNode
			planIntfObj.InterfaceName = planIntfObj.GetWGIntfName(link.FromNode, toNode.nodeName, linkIdx)
			planIntfObj.WGIntfName = planIntfObj.InterfaceName
			if fromNode.hostName != "" {
				// not behind a NAT
				baseListenPort := fromNode.portRange.Start
				planIntfObj.ListenPort = &baseListenPort
				planIntfObj.Hostname = fromNode.hostName
			}
			planIntfObj.PrivateKey = fromNode.privateKey
			planIntfObj.PublicKey = fromNode.publicKey
			planIntfObj.ResourceId = planIntfObj.GetResourceId(link.FromNode, toNode.nodeName, linkIdx)
			if err := planIntfObj.ComputeConfigHash(); err != nil {
				return nil, fmt.Errorf("failed to compute config hash for interface %s: %s", planIntfObj.InterfaceName, err.Error())
			}
			planIntfObjs = append(planIntfObjs, planIntfObj)
		}
	}

	return plan, nil
}
