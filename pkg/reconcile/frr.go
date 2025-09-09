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

type FRRVtyshAgent struct {
	vtyshPath string
}

func NewFRRVtyshAgent(vtyshPath string) (*FRRVtyshAgent, error) {
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

type FRROSPFv2Reconciler struct {
	vtyshAgent             *FRRVtyshAgent
	needEnableOSPFv2Router bool
	addedIntfList          map[string]interface{}
	removedIntfList        map[string]*intf
}

func NewFRROSPFv2Reconciler(vtyshPath string) (*FRROSPFv2Reconciler, error) {
	vtyshAgent, err := NewFRRVtyshAgent(vtyshPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create vtysh agent: %v", err)
	}
	reconciler := &FRROSPFv2Reconciler{
		vtyshAgent: vtyshAgent,
	}
	return reconciler, nil
}

func (r *FRROSPFv2Reconciler) gatherAllUpdates() bool {
	return r.needEnableOSPFv2Router ||
		r.addedIntfList != nil ||
		r.removedIntfList != nil
}

type intf struct {
	Area string `json:"area,omitempty"`
}

type intflist struct {
	Interfaces map[string]intf `json:"interfaces,omitempty"`
}

func (r *FRROSPFv2Reconciler) getInterfaceList() (map[string]*intf, error) {
	output, err := r.vtyshAgent.ExecuteCommand("show ip ospf interface json")
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %v", err)
	}

	if len(output) == 0 {
		return nil, nil
	}

	intflistobj := new(intflist)
	err = json.Unmarshal(output, intflistobj)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal output: %v", err)
	}

	res := make(map[string]*intf)
	if intflistobj.Interfaces != nil {
		for intfName, intfObj := range intflistobj.Interfaces {
			res[intfName] = &intfObj
		}
	}

	return res, nil
}

func (r *FRROSPFv2Reconciler) DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error) {
	spec, ok := desiredState.(networkingv1alpha1.OSPFProtocolSpec)
	if !ok {
		return false, fmt.Errorf("desiredState is not a OSPFProtocolSpec")
	}

	output, err := r.vtyshAgent.ExecuteCommand("show ip ospf json")
	if err != nil {
		return false, fmt.Errorf("failed to execute command: %v", err)
	}

	if len(output) == 0 {
		r.needEnableOSPFv2Router = true
		return r.gatherAllUpdates(), nil
	}

	currentIntfList, err := r.getInterfaceList()
	if err != nil {
		return false, fmt.Errorf("failed to get interface list: %v", err)
	}
	specIntfList := make(map[string]interface{})
	if spec.Interfaces != nil {
		for _, intfSpec := range spec.Interfaces {
			specIntfList[intfSpec.InterfaceName] = &intfSpec
		}
	}

	addedIntfList := make(map[string]interface{})
	for intfName, intfSpec := range specIntfList {
		if _, ok := currentIntfList[intfName]; !ok {
			addedIntfList[intfName] = intfSpec
		}
	}

	removedIntfList := make(map[string]*intf)
	for intfName, intfObj := range currentIntfList {
		if _, ok := specIntfList[intfName]; !ok {
			removedIntfList[intfName] = intfObj
		}
	}

	r.addedIntfList = addedIntfList
	r.removedIntfList = removedIntfList

	return r.gatherAllUpdates(), nil
}

func (r *FRROSPFv2Reconciler) ApplyReconcile(ctx context.Context, desiredState interface{}) error {

	spec, ok := desiredState.(networkingv1alpha1.OSPFProtocolSpec)
	if !ok {
		return fmt.Errorf("desiredState is not a OSPFProtocolSpec")
	}

	if r.needEnableOSPFv2Router {
		// Enable OSPFv2 router
		routerId := spec.RouterID
		if routerId == "" {
			return fmt.Errorf("routerID is required")
		}

	}

	if r.removedIntfList != nil {
		for intfName := range r.removedIntfList {
			intfobj := r.removedIntfList[intfName]
			if err := r.deleteInterface(intfName, intfobj); err != nil {
				return fmt.Errorf("failed to delete interface %s: %v", intfName, err)
			}
		}
	}

	if r.addedIntfList != nil {
		for intfName := range r.addedIntfList {
			intfSpec := r.addedIntfList[intfName].(*networkingv1alpha1.OSPFProtocolInterfaceSpec)
			if err := r.addInterface(intfName, intfSpec); err != nil {
				return fmt.Errorf("failed to add interface %s: %v", intfName, err)
			}
		}
	}

	return nil
}

func (r *FRROSPFv2Reconciler) deleteInterface(intfName string, intfobj *intf) error {

	cmd := `
	configure
		interface %s
		no ip ospf area %s
		exit
	`
	cmd = strings.TrimSpace(cmd)
	cmd = fmt.Sprintf(cmd, intfName, intfobj.Area)
	cmd = fmt.Sprintf("%s\n", cmd)
	cmdBuf := bytes.NewBufferString(cmd)
	_, err := r.vtyshAgent.ExecuteCommandIO(cmdBuf)
	if err != nil {
		return fmt.Errorf("failed to delete interface %s: %v", intfName, err)
	}
	return nil
}

func (r *FRROSPFv2Reconciler) addInterface(intfName string, intfSpec *networkingv1alpha1.OSPFProtocolInterfaceSpec) error {
	if intfSpec.Passive != nil && *intfSpec.Passive {
		cmd := `
		configure
			interface %s
				ip ospf area %s
				ip ospf passive
			exit
		exit
		`

		cmd = strings.TrimSpace(cmd)
		cmd = fmt.Sprintf(cmd, intfName, intfSpec.Area)
		cmd = fmt.Sprintf("%s\n", cmd)
		cmdBuf := bytes.NewBufferString(cmd)
		_, err := r.vtyshAgent.ExecuteCommandIO(cmdBuf)
		if err != nil {
			return fmt.Errorf("failed to add interface %s: %v", intfName, err)
		}
		return nil
	}

	cmd := `
	configure
		interface %s
		ip ospf area %s
		ip ospf network %s
	`
	cmd = strings.TrimSpace(cmd)
	cmd = fmt.Sprintf(cmd, intfName, intfSpec.Area, intfSpec.NetworkType)
	cmd = fmt.Sprintf("%s\n", cmd)
	cmdBuf := bytes.NewBufferString(cmd)
	_, err := r.vtyshAgent.ExecuteCommandIO(cmdBuf)
	if err != nil {
		return fmt.Errorf("failed to add interface %s: %v", intfName, err)
	}

	return nil
}

func (r *FRROSPFv2Reconciler) ResetState() {
	r.needEnableOSPFv2Router = false
	r.addedIntfList = nil
	r.removedIntfList = nil
}
