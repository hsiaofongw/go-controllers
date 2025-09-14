package frr

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

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
