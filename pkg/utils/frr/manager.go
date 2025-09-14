package frr

import (
	"encoding/json"
	"fmt"

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
)

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
	if vrf == FRRVRFUnspecified {
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

func (m *FRROSPFManager) GetOSPFVRFBriefList() (*FRROSPFVRFBriefList, error) {
	res := new(FRROSPFVRFBriefList)
	emptyResult := new(FRROSPFVRFBriefList)
	emptyResult.VRFs = make(map[string]FRROSPFVRFBrief)
	emptyResult.TotalVRFs = new(int)
	*emptyResult.TotalVRFs = 0

	output, err := m.vtyshAgent.ExecuteCommand("show ip ospf vrfs json")
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

func (m *FRROSPFManager) EnableOSPFv2Router(routerId string, vrf *string) error {
	cmds := make([]string, 0)
	cmds = append(cmds, "configure")
	if vrf != nil && *vrf != FRRVRFUnspecified {
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
	if vrf != nil && *vrf != FRRVRFUnspecified {
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
	if vrfName == FRRVRFUnspecified {
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
	if vrfName != FRRVRFUnspecified {
		cmds = append(cmds, fmt.Sprintf("interface %s vrf %s", intfName, vrfName))
	} else {
		cmds = append(cmds, fmt.Sprintf("interface %s", intfName))
	}
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
