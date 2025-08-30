package utils

import (
	"fmt"
	"os"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.zx2c4.com/wireguard/wgctrl"
)

func WithNetlinkHandle(pid *int, hook func(handle *netlink.Handle) error) error {
	if pid == nil {
		hostNsHandle, err := netlink.NewHandle()
		if err != nil {
			return fmt.Errorf("failed to get netlink handle: %s", err.Error())
		}
		defer hostNsHandle.Close()

		return hook(hostNsHandle)
	} else {
		nsHandle, err := netns.GetFromPid(*pid)
		if err != nil {
			return fmt.Errorf("failed to get ns handle of PID %d: %s", *pid, err.Error())
		}
		defer nsHandle.Close()

		nsLinkHandle, err := netlink.NewHandleAt(nsHandle)
		if err != nil {
			return fmt.Errorf("failed to get netlink at ns PID %d: %s", *pid, err.Error())
		}
		defer nsLinkHandle.Close()

		return hook(nsLinkHandle)
	}
}

func WithNetnsWGCli(containerPid *int, hook func(wgCtrlCli *wgctrl.Client) error) error {

	var wgCtrlCli *wgctrl.Client
	var err error

	if containerPid != nil {
		nsHandle, err := netns.GetFromPid(*containerPid)
		if err != nil {
			return fmt.Errorf("failed to get netns from pid: %s", err.Error())
		}
		defer nsHandle.Close()

		hostPid := os.Getpid()
		hostNsHandle, err := netns.GetFromPid(hostPid)
		if err != nil {
			return fmt.Errorf("failed to get host netns: %s", err.Error())
		}
		defer hostNsHandle.Close()

		netns.Set(nsHandle)
		defer netns.Set(hostNsHandle)

		wgCtrlCli, err = wgctrl.New()
		if err != nil {
			return fmt.Errorf("failed to get wgctrl client: %s", err.Error())
		}
		defer wgCtrlCli.Close()
	} else {
		wgCtrlCli, err = wgctrl.New()
		if err != nil {
			return fmt.Errorf("failed to get wgctrl client: %s", err.Error())
		}
		defer wgCtrlCli.Close()
	}

	return hook(wgCtrlCli)
}
