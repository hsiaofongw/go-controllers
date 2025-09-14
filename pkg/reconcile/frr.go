package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
)

const defaultVRFName string = "default"

type FRRVtyshAgent struct {
	vtyshPath string
}

func NewFRRVtyshAgent(vtyshPath string) (*FRRVtyshAgent, error) {
	if vtyshPath == "" {
		return nil, fmt.Errorf("vtyshPath is required")
	}
	return &FRRVtyshAgent{vtyshPath: vtyshPath}, nil
}

// Equivilent to `vtysh -c "command"`
func (a *FRRVtyshAgent) ExecuteCommand(command string) ([]byte, error) {
	stdout, err := exec.Command(a.vtyshPath, "-c", command).Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %v", err)
	}

	return stdout, nil
}

// Equivilent to `vtysh -c "command1" -c "command2" -c "command3" ...`
func (a *FRRVtyshAgent) ExecuteCommands(commands []string) ([]byte, error) {
	cliArgs := make([]string, 0)
	for _, c := range commands {
		trimedC := strings.TrimSpace(c)
		if len(trimedC) > 0 {
			cliArgs = append(cliArgs, "-c", trimedC)
		}
	}

	stdout, err := exec.Command(a.vtyshPath, cliArgs...).Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute commands: %v", err)
	}

	return stdout, nil
}

func (a *FRRVtyshAgent) ExecuteCommandIO(stdin io.Reader) ([]byte, error) {
	cmd := exec.Command(a.vtyshPath)
	cmd.Stdin = stdin
	stdout, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute commands: %v", err)
	}

	return stdout, nil
}

func (a *FRRVtyshAgent) ExecuteMultilineCommand(cmds []string) ([]byte, error) {
	var buf bytes.Buffer
	for _, cmd := range cmds {
		cmd = strings.TrimSpace(cmd)
		if len(cmd) == 0 {
			continue
		}
		fmt.Fprintf(&buf, "%s\n", cmd)
	}
	return a.ExecuteCommandIO(&buf)
}

type FRROSPFManager struct {
	vtyshAgent *FRRVtyshAgent
}

type FRROSPFv2ReconcilerIfaceDiff struct {
	AddedIfaces   map[string]*networkingv1alpha1.OSPFProtocolInterfaceSpec
	RemovedIfaces map[string]*FRROSPFIface
}

type FRROSPFv2ReconcilerRoutersDiff struct {
	// key is the vrf name, the name for the default vrf is always 'default'
	// value is spec of the router to be added
	AddedRouterList map[string]*networkingv1alpha1.OSPFProtocolRouterSpec

	// key is the vrf name, the name for the default vrf is always 'default'
	RemovedRouterList map[string]interface{}
}

type FRROSPFv2Reconciler struct {
	manager *FRROSPFManager

	// key is the vrf name, the name for the default vrf is always 'default'
	IfaceDiffs map[string]FRROSPFv2ReconcilerIfaceDiff

	RoutersDiffs *FRROSPFv2ReconcilerRoutersDiff
}

func NewFRROSPFManager(vtyshPath string) (*FRROSPFManager, error) {
	vtyshAgent, err := NewFRRVtyshAgent(vtyshPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create vtysh agent: %v", err)
	}
	manager := &FRROSPFManager{
		vtyshAgent: vtyshAgent,
	}
	return manager, nil
}

func NewFRROSPFv2Reconciler(vtyshPath string) (*FRROSPFv2Reconciler, error) {
	manager, err := NewFRROSPFManager(vtyshPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create FRR OSPF manager: %v", err)
	}
	reconciler := &FRROSPFv2Reconciler{
		manager: manager,
	}
	return reconciler, nil
}

func (r *FRROSPFv2Reconciler) gatherAllUpdates() bool {
	return r.RoutersDiffs != nil ||
		r.IfaceDiffs != nil
}

type FRROSPFIfaceNWType string

const (
	FRRIFACE_NETWORK_TYPE_BROADCAST           = "BROADCAST"
	FRRIFACE_NETWORK_TYPE_POINT_TO_POINT      = "POINTTOPOINT"
	FRRIFACE_NETWORK_TYPE_POINT_TO_MULTIPOINT = "POINTTOMULTIPOINT"
)

type FRROSPFIface struct {
	Area              *string             `json:"area,omitempty"`
	TimerPassiveIface *bool               `json:"timerPassiveIface,omitempty"`
	NetworkType       *FRROSPFIfaceNWType `json:"networkType,omitempty"`
	RouterID          *string             `json:"routerId,omitempty"`
}

type FRROSPFIfaceList struct {
	VRFName    *string                 `json:"vrfName,omitempty"`
	VRFID      *string                 `json:"vrfId,omitempty"`
	Interfaces map[string]FRROSPFIface `json:"interfaces,omitempty"`
}

type FRROSPFVRFIfaceList map[string]FRROSPFIfaceList

// Equivilent to `show ip ospf interface json`
func (m *FRROSPFManager) GetInterfaceList() (map[string]*FRROSPFIface, error) {
	output, err := m.vtyshAgent.ExecuteCommand("show ip ospf interface json")
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %v", err)
	}

	if len(output) == 0 {
		return nil, nil
	}

	intflistobj := new(FRROSPFIfaceList)
	err = json.Unmarshal(output, intflistobj)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal output: %v", err)
	}

	res := make(map[string]*FRROSPFIface)
	if intflistobj.Interfaces != nil {
		for intfName, intfObj := range intflistobj.Interfaces {
			res[intfName] = &intfObj
		}
	}

	return res, nil
}

// Equivilent to `show ip ospf vrf <vrf> interface json`
func (m *FRROSPFManager) GetVRFInterfaceList(vrf string) (map[string]*FRROSPFIface, error) {
	if vrf == "" {
		return nil, fmt.Errorf("vrf is required")
	}

	output, err := m.vtyshAgent.ExecuteCommand(fmt.Sprintf("show ip ospf vrf %s interface json", vrf))
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %v", err)
	}

	if len(output) == 0 {
		return nil, nil
	}

	vrfIfaceListobj := new(FRROSPFVRFIfaceList)
	if err := json.Unmarshal(output, vrfIfaceListobj); err != nil {
		return nil, fmt.Errorf("failed to unmarshal output: %v", err)
	}

	if vrfIfaceListobj == nil {
		return nil, nil
	}

	res := make(map[string]*FRROSPFIface)
	for _, ifaceList := range *vrfIfaceListobj {
		for intfName, intfObj := range ifaceList.Interfaces {
			res[intfName] = &intfObj
		}
	}

	return res, nil
}

type FRROSPFAreaDetail struct {
	Backbone *bool `json:"backbone,omitempty"`
}

type FRROSPFVRFDetail struct {
	VRFName  *string                      `json:"vrfName,omitempty"`
	VRFID    *string                      `json:"vrfId,omitempty"`
	RouterID *string                      `json:"routerId,omitempty"`
	Areas    map[string]FRROSPFAreaDetail `json:"areas,omitempty"`
}

type FRROSPFVRFList map[string]FRROSPFVRFDetail

func (m *FRROSPFManager) GetOSPFVRFList() (*FRROSPFVRFList, error) {
	output, err := m.vtyshAgent.ExecuteCommand("show ip ospf vrf all json")
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %v", err)
	}

	if len(output) == 0 {
		return nil, nil
	}

	vrfListobj := new(FRROSPFVRFList)
	if err := json.Unmarshal(output, vrfListobj); err != nil {
		return nil, fmt.Errorf("failed to unmarshal output: %v", err)
	}

	return vrfListobj, nil
}

type FRROSPFVRFBrief struct {
	VRFId    *int    `json:"vrfId,omitempty"`
	RouterID *string `json:"routerId,omitempty"`
}

type FRROSPFVRFBriefList struct {
	// key is the vrf name, the name for the default vrf is 'default'
	VRFs map[string]FRROSPFVRFBrief `json:"vrfs,omitempty"`

	TotalVRFs *int `json:"totalVrfs,omitempty"`
}

func (m *FRROSPFManager) GetOSPFVRFBriefList() (*FRROSPFVRFBriefList, error) {
	res := new(FRROSPFVRFBriefList)
	output, err := m.vtyshAgent.ExecuteCommand("show ip ospf vrf all brief json")

	emptyResult := new(FRROSPFVRFBriefList)
	emptyResult.VRFs = make(map[string]FRROSPFVRFBrief)
	emptyResult.TotalVRFs = new(int)
	*emptyResult.TotalVRFs = 0

	if err != nil {
		return emptyResult, fmt.Errorf("failed to execute command: %v", err)
	}

	if len(output) == 0 {
		return emptyResult, nil
	}

	if err := json.Unmarshal(output, res); err != nil {
		return emptyResult, fmt.Errorf("failed to unmarshal output: %v", err)
	}

	return res, nil
}

func (r *FRROSPFv2Reconciler) CleanUpResource(ctx context.Context, spec *networkingv1alpha1.OSPFProtocolSpec) error {
	if spec == nil {
		return fmt.Errorf("spec is nil")
	}

	for _, routerSpec := range spec.Routers {
		var currentIntfList map[string]*FRROSPFIface
		var err error
		if routerSpec.VRF != nil && *routerSpec.VRF != "" {
			currentIntfList, err = r.manager.GetVRFInterfaceList(*routerSpec.VRF)
		} else {
			currentIntfList, err = r.manager.GetInterfaceList()
		}
		if err != nil {
			return fmt.Errorf("failed to get interface list: %v", err)
		}

		vrfName := defaultVRFName
		if routerSpec.VRF != nil && *routerSpec.VRF != "" {
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
		vrfName := "default"
		if routerSpec.VRF != nil && *routerSpec.VRF != "" {
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

		var removedIfaces map[string]*FRROSPFIface

		specIfaceList, ok := ifaceSpecs[vrfName]
		if !ok || len(specIfaceList) == 0 {
			removedIfaces = currentIntfList
		} else if len(specIfaceList) > 0 {
			removedIfaces = make(map[string]*FRROSPFIface)
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
		vrfName := defaultVRFName
		if intfSpec.VRF != nil && *intfSpec.VRF != "" {
			vrfName = defaultVRFName
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
	// todo
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

func (m *FRROSPFManager) EnableOSPFv2Router(routerId string, vrf *string) error {
	cmds := make([]string, 0)
	cmds = append(cmds, "configure")
	if vrf != nil && *vrf != "" {
		cmds = append(cmds, fmt.Sprintf("router ospf vrf %s", *vrf))
	} else {
		cmds = append(cmds, "router ospf")
	}
	cmds = append(cmds, fmt.Sprintf("ospf router-id %s", routerId))
	cmds = append(cmds, "exit")
	cmds = append(cmds, "exit")

	_, err := m.vtyshAgent.ExecuteMultilineCommand(cmds)
	if err != nil {
		return fmt.Errorf("failed to enable OSPFv2 router: %v", err)
	}
	return nil
}

func (m *FRROSPFManager) DeleteOSPFv2Router(vrf *string) error {
	cmds := make([]string, 0)
	cmds = append(cmds, "configure")
	if vrf != nil && *vrf != "" {
		cmds = append(cmds, fmt.Sprintf("no router ospf vrf %s", *vrf))
	} else {
		cmds = append(cmds, "no router ospf")
	}

	cmds = append(cmds, "exit")

	_, err := m.vtyshAgent.ExecuteMultilineCommand(cmds)
	if err != nil {
		return fmt.Errorf("failed to delete OSPFv2 router: %v", err)
	}

	return nil
}

func (m *FRROSPFManager) DeleteInterface(intfName string, intfobj *FRROSPFIface, vrfName string) error {

	cmds := make([]string, 0)
	cmds = append(cmds, "configure")
	if vrfName == "" {
		cmds = append(cmds, fmt.Sprintf("interface %s", intfName))
	} else {
		cmds = append(cmds, fmt.Sprintf("interface %s vrf %s", intfName, vrfName))
	}

	// actually, just 'no ip ospf area' (omitting the area id) also works
	cmds = append(cmds, fmt.Sprintf("no ip ospf area %s", *intfobj.Area))
	if intfobj.TimerPassiveIface != nil && *intfobj.TimerPassiveIface {
		cmds = append(cmds, "no ip ospf passive")
	} else {
		cmds = append(cmds, "no ip ospf network")
	}
	cmds = append(cmds, "exit")
	cmds = append(cmds, "exit")

	_, err := m.vtyshAgent.ExecuteMultilineCommand(cmds)
	if err != nil {
		return fmt.Errorf("failed to delete interface %s: %v", intfName, err)
	}
	return nil
}

func (m *FRROSPFManager) AddInterface(intfName string, intfSpec *networkingv1alpha1.OSPFProtocolInterfaceSpec, vrfName string) error {

	cmds := make([]string, 0)
	cmds = append(cmds, "configure")
	cmds = append(cmds, fmt.Sprintf("interface %s", intfName))
	cmds = append(cmds, fmt.Sprintf("ip ospf area %s", intfSpec.Area))

	if intfSpec.Passive != nil && *intfSpec.Passive {
		cmds = append(cmds, "ip ospf passive")
	} else {
		cmds = append(cmds, fmt.Sprintf("ip ospf network %s", intfSpec.NetworkType))
	}

	cmds = append(cmds, "exit")
	cmds = append(cmds, "exit")

	_, err := m.vtyshAgent.ExecuteMultilineCommand(cmds)
	if err != nil {
		return fmt.Errorf("failed to add interface %s: %v", intfName, err)
	}

	return nil
}

func (r *FRROSPFv2Reconciler) ResetState() {
	r.RoutersDiffs = nil
	r.IfaceDiffs = nil
}
