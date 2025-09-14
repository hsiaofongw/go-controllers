package reconcile

import (
	"context"
	"fmt"

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgutilsfrr "k8s.io/sample-controller/pkg/utils/frr"
)

type FRROSPFv2ReconcilerIfaceDiff struct {
	AddedIfaces   map[string]*networkingv1alpha1.OSPFProtocolInterfaceSpec
	RemovedIfaces map[string]*pkgutilsfrr.FRROSPFIface
}

type FRROSPFv2ReconcilerRoutersDiff struct {
	// key is the vrf name, the name for the default vrf is always 'default'
	// value is spec of the router to be added
	AddedRouterList map[string]*networkingv1alpha1.OSPFProtocolRouterSpec

	// key is the vrf name, the name for the default vrf is always 'default'
	RemovedRouterList map[string]interface{}
}

type FRROSPFv2Reconciler struct {
	manager *pkgutilsfrr.FRROSPFManager

	// key is the vrf name, the name for the default vrf is always 'default'
	IfaceDiffs map[string]FRROSPFv2ReconcilerIfaceDiff

	RoutersDiffs *FRROSPFv2ReconcilerRoutersDiff
}

func NewFRROSPFv2Reconciler(frrCfgMgr *pkgutilsfrr.FRROSPFManager) (*FRROSPFv2Reconciler, error) {
	reconciler := &FRROSPFv2Reconciler{
		manager: frrCfgMgr,
	}
	return reconciler, nil
}

func (r *FRROSPFv2Reconciler) gatherAllUpdates() bool {
	return r.RoutersDiffs != nil ||
		r.IfaceDiffs != nil
}

func (r *FRROSPFv2Reconciler) CleanUpResource(ctx context.Context, spec *networkingv1alpha1.OSPFProtocolSpec) error {
	if spec == nil {
		return fmt.Errorf("spec is nil")
	}

	for _, routerSpec := range spec.Routers {
		var currentIntfList map[string]*pkgutilsfrr.FRROSPFIface
		var err error
		if routerSpec.VRF != nil && *routerSpec.VRF != pkgutilsfrr.FRRVRFUnspecified {
			currentIntfList, err = r.manager.GetVRFInterfaceList(*routerSpec.VRF)
		} else {
			currentIntfList, err = r.manager.GetInterfaceList()
		}
		if err != nil {
			return fmt.Errorf("failed to get interface list: %v", err)
		}

		vrfName := pkgutilsfrr.FRRVRFDefault
		if routerSpec.VRF != nil && *routerSpec.VRF != pkgutilsfrr.FRRVRFUnspecified {
			vrfName = *routerSpec.VRF
		}

		for intfName, intfObj := range currentIntfList {
			if intfObj.RouterID != nil && *intfObj.RouterID == routerSpec.RouterID {
				if err := r.manager.DeleteInterface(intfName, intfObj, vrfName); err != nil {
					return fmt.Errorf("failed to delete interface %s: %v", intfName, err)
				}
			}
		}

		if err := r.manager.DeleteOSPFv2Router(routerSpec.VRF); err != nil {
			return fmt.Errorf("failed to delete OSPFv2 router: %v", err)
		}
	}

	return nil
}

func (r *FRROSPFv2Reconciler) detectOSPFv2VRFRouterChanges(ctx context.Context, routerSpecs []networkingv1alpha1.OSPFProtocolRouterSpec) (*FRROSPFv2ReconcilerRoutersDiff, error) {
	currentVRFBriefList, err := r.manager.GetOSPFVRFBriefList()
	if err != nil {
		return nil, fmt.Errorf("failed to get OSPF VRF brief list: %v", err)
	}

	specRouterList := make(map[string]*networkingv1alpha1.OSPFProtocolRouterSpec)
	for _, routerSpec := range routerSpecs {
		vrfName := pkgutilsfrr.FRRVRFDefault
		if routerSpec.VRF != nil && *routerSpec.VRF != pkgutilsfrr.FRRVRFUnspecified {
			vrfName = *routerSpec.VRF
		}
		specRouterList[vrfName] = &routerSpec
	}

	// in current but not in spec
	removedVRFs := make(map[string]interface{})
	for vrfName := range currentVRFBriefList.VRFs {
		if _, ok := specRouterList[vrfName]; !ok {
			removedVRFs[vrfName] = true
		}
	}

	// in spec but not in current
	addedVRFs := make(map[string]*networkingv1alpha1.OSPFProtocolRouterSpec)
	for vrfName := range specRouterList {
		if _, ok := currentVRFBriefList.VRFs[vrfName]; !ok {
			addedVRFs[vrfName] = specRouterList[vrfName]
		}
	}

	if len(removedVRFs)+len(addedVRFs) > 0 {
		return &FRROSPFv2ReconcilerRoutersDiff{
			RemovedRouterList: removedVRFs,
			AddedRouterList:   addedVRFs,
		}, nil
	}

	return nil, nil
}

// ifaceSpecs is a map of vrf name -> iface name -> iface spec, the name for the default vrf is always 'default'
func (r *FRROSPFv2Reconciler) detectOSPFv2VRFInterfaceChanges(ctx context.Context, ifaceSpecs map[string]map[string]*networkingv1alpha1.OSPFProtocolInterfaceSpec) (map[string]FRROSPFv2ReconcilerIfaceDiff, error) {
	// key is the vrf name, the name for the default vrf is always 'default', the value is the diff of the interface in the vrf
	vrfIfaceDiffs := make(map[string]FRROSPFv2ReconcilerIfaceDiff)

	vrfList, err := r.manager.GetOSPFVRFBriefList()
	if err != nil {
		return nil, fmt.Errorf("failed to get VRF brief list: %v", err)
	}

	allVRFNames := make(map[string]interface{})
	for vrfName := range vrfList.VRFs {
		allVRFNames[vrfName] = true
	}
	for vrfName := range ifaceSpecs {
		allVRFNames[vrfName] = true
	}

	for vrfName := range allVRFNames {
		currentIntfList, err := r.manager.GetVRFInterfaceList(vrfName)
		if err != nil {
			return nil, fmt.Errorf("failed to get VRF interface list: %v", err)
		}

		var removedIfaces map[string]*pkgutilsfrr.FRROSPFIface

		specIfaceList, ok := ifaceSpecs[vrfName]
		if !ok || len(specIfaceList) == 0 {
			removedIfaces = currentIntfList
		} else if len(specIfaceList) > 0 {
			removedIfaces = make(map[string]*pkgutilsfrr.FRROSPFIface)
			for intfName, intfObj := range currentIntfList {
				if _, ok := specIfaceList[intfName]; !ok {
					removedIfaces[intfName] = intfObj
				}
			}
		}

		ifacesDiff := FRROSPFv2ReconcilerIfaceDiff{
			RemovedIfaces: removedIfaces,
		}

		var addedIfaces map[string]*networkingv1alpha1.OSPFProtocolInterfaceSpec
		if len(currentIntfList) == 0 {
			addedIfaces = specIfaceList
		} else {
			addedIfaces = make(map[string]*networkingv1alpha1.OSPFProtocolInterfaceSpec)
			for intfName, intfSpec := range specIfaceList {
				if _, ok := currentIntfList[intfName]; !ok {
					addedIfaces[intfName] = intfSpec
				}
			}
		}

		ifacesDiff.AddedIfaces = addedIfaces
		vrfIfaceDiffs[vrfName] = ifacesDiff
	}

	return vrfIfaceDiffs, nil
}

func indexVRFIfaceMaps(intfSpecs []networkingv1alpha1.OSPFProtocolInterfaceSpec) (map[string]map[string]*networkingv1alpha1.OSPFProtocolInterfaceSpec, error) {
	vrfIfacesMap := make(map[string]map[string]*networkingv1alpha1.OSPFProtocolInterfaceSpec)
	for _, intfSpec := range intfSpecs {
		vrfName := pkgutilsfrr.FRRVRFDefault
		if intfSpec.VRF != nil && *intfSpec.VRF != pkgutilsfrr.FRRVRFUnspecified {
			vrfName = pkgutilsfrr.FRRVRFDefault
		}

		if _, ok := vrfIfacesMap[vrfName]; !ok {
			vrfIfacesMap[vrfName] = make(map[string]*networkingv1alpha1.OSPFProtocolInterfaceSpec)
		}

		vrfIfacesMap[vrfName][intfSpec.InterfaceName] = &intfSpec
	}

	return vrfIfacesMap, nil
}

func (r *FRROSPFv2Reconciler) DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error) {
	spec, ok := desiredState.(networkingv1alpha1.OSPFProtocolSpec)
	if !ok {
		return false, fmt.Errorf("desiredState is not a OSPFProtocolSpec")
	}

	routersDiff, err := r.detectOSPFv2VRFRouterChanges(ctx, spec.Routers)
	if err != nil {
		return r.gatherAllUpdates(), fmt.Errorf("failed to detect OSPF VRF router changes: %v", err)
	}

	r.RoutersDiffs = routersDiff

	vrfIfacesMap, err := indexVRFIfaceMaps(spec.Interfaces)
	if err != nil {
		return r.gatherAllUpdates(), fmt.Errorf("failed to index VRF interface maps: %v", err)
	}

	ifaceDiffs, err := r.detectOSPFv2VRFInterfaceChanges(ctx, vrfIfacesMap)
	if err != nil {
		return r.gatherAllUpdates(), fmt.Errorf("failed to detect OSPF VRF interface changes: %v", err)
	}

	r.IfaceDiffs = ifaceDiffs

	return r.gatherAllUpdates(), nil
}

func (r *FRROSPFv2Reconciler) applyOSPFv2VRFRouterChanges(ctx context.Context, routersDiff *FRROSPFv2ReconcilerRoutersDiff) error {
	if routersDiff.RemovedRouterList != nil {
		for vrfName := range routersDiff.RemovedRouterList {
			if err := r.manager.DeleteOSPFv2Router(&vrfName); err != nil {
				return fmt.Errorf("failed to delete OSPF VRF router %s: %v", vrfName, err)
			}
		}
	}

	if routersDiff.AddedRouterList != nil {
		for vrfName, routerSpec := range routersDiff.AddedRouterList {
			if err := r.manager.EnableOSPFv2Router(routerSpec.RouterID, &vrfName); err != nil {
				return fmt.Errorf("failed to add OSPF VRF router %s: %v", vrfName, err)
			}
		}
	}

	return nil
}

func (r *FRROSPFv2Reconciler) applyOSPFv2VRFInterfaceChanges(ctx context.Context, ifaceDiff map[string]FRROSPFv2ReconcilerIfaceDiff) error {

	for vrfName, ifaceDiff := range ifaceDiff {
		if ifaceDiff.RemovedIfaces != nil {
			for intfName := range ifaceDiff.RemovedIfaces {
				intfobj := ifaceDiff.RemovedIfaces[intfName]
				if err := r.manager.DeleteInterface(intfName, intfobj, vrfName); err != nil {
					return fmt.Errorf("failed to delete interface %s: %v", intfName, err)
				}
			}
		}

		if ifaceDiff.AddedIfaces != nil {
			for intfName, intfSpec := range ifaceDiff.AddedIfaces {
				if err := r.manager.AddInterface(intfName, intfSpec, vrfName); err != nil {
					return fmt.Errorf("failed to add interface %s: %v", intfName, err)
				}
			}
		}
	}

	return nil
}

func (r *FRROSPFv2Reconciler) ApplyReconcile(ctx context.Context, desiredState interface{}) error {
	if r.RoutersDiffs != nil {
		if err := r.applyOSPFv2VRFRouterChanges(ctx, r.RoutersDiffs); err != nil {
			return fmt.Errorf("failed to apply OSPF VRF router changes: %v", err)
		}
	}

	if r.IfaceDiffs != nil {
		if err := r.applyOSPFv2VRFInterfaceChanges(ctx, r.IfaceDiffs); err != nil {
			return fmt.Errorf("failed to apply OSPF VRF interface changes: %v", err)
		}
	}

	return nil
}

func (r *FRROSPFv2Reconciler) ResetState() {
	r.RoutersDiffs = nil
	r.IfaceDiffs = nil
}
