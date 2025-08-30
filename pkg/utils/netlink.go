package utils

import (
	"fmt"
	"net"
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

func FlagsToStrings(flags net.Flags) []string {
	flagStrs := make([]string, 0)
	if flags&net.FlagUp == net.FlagUp {
		flagStrs = append(flagStrs, "UP")
	}
	if flags&net.FlagBroadcast == net.FlagBroadcast {
		flagStrs = append(flagStrs, "BROADCAST")
	}

	if flags&net.FlagPointToPoint == net.FlagPointToPoint {
		flagStrs = append(flagStrs, "POINTOPOINT")
	}
	if flags&net.FlagMulticast == net.FlagMulticast {
		flagStrs = append(flagStrs, "MULTICAST")
	}

	if flags&net.FlagRunning == net.FlagRunning {
		flagStrs = append(flagStrs, "RUNNING")
	}
	if flags&net.FlagLoopback == net.FlagLoopback {
		flagStrs = append(flagStrs, "LOOPBACK")
	}
	return flagStrs
}

func AddrToString(addr netlink.Addr) string {
	if addr.Peer != nil {
		return fmt.Sprintf("%s -> %s", addr.IP.String(), addr.Peer.String())
	}

	if addr.IPNet != nil {
		return addr.IPNet.String()
	}

	if addr.IP != nil {
		return addr.IP.String()
	}

	return addr.String()
}
