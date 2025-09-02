package reconcile

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"sort"

	"github.com/vishvananda/netlink"
	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgutils "k8s.io/sample-controller/pkg/utils"
)

type WGDesiredState struct {
	MTU     *int
	IPAddrs []netlink.Addr
	Peers   []wgtypes.PeerConfig

	WGOuterConfig *wgtypes.Config

	// If this is true, means that the wg interface is expected to be
	// born in host netns then move to the container once created.
	MoveToContainer bool
}

type WGReconciler struct {
	interfaceName          string
	pid                    *int
	shouldUpdateAddr       bool
	shouldCreateInterface  bool
	shouldUpdateMTU        bool
	shouldUpdateAdminState bool

	wgOuterConfigDiff *WGOuterConfigDiff
	peersDiff         *PeersDiff
}

func NewWGReconciler(interfaceName string, pid *int) (*WGReconciler, error) {
	return &WGReconciler{interfaceName: interfaceName, pid: pid}, nil
}

func (r *WGReconciler) setStatus(status *networkingv1alpha1.WireGuardInterfaceStatus) error {
	netlinkHook := func(handle *netlink.Handle, wgLink netlink.Link) error {
		mtu := wgLink.Attrs().MTU
		status.MTU = &mtu

		addrObjs, err := handle.AddrList(wgLink, netlink.FAMILY_ALL)
		if err != nil {
			return fmt.Errorf("failed to get addresses: %s", err.Error())
		}

		status.Netlink = networkingv1alpha1.NewFromNetlinkLinkAttrs(wgLink.Attrs(), addrObjs)

		return nil
	}

	wgHook := func(wgCtrlCli *wgctrl.Client) error {
		device, err := wgCtrlCli.Device(r.interfaceName)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("device %s does not exist: %s", r.interfaceName, err.Error())
			}
			return fmt.Errorf("failed to get device: %s", err.Error())
		}
		status.WireGuard = networkingv1alpha1.NewWireGuardStatusWrapper(device)
		return nil
	}

	if err := pkgutils.WithNetnsWGCli(r.pid, wgHook); err != nil {
		return fmt.Errorf("failed to get wg status: %s", err.Error())
	}

	return pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(r.interfaceName)
		if err != nil {
			return fmt.Errorf("failed to get link by name: %s", err.Error())
		}

		return netlinkHook(handle, link)
	})

}

func (r *WGReconciler) DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error) {

	desiredConf, ok := desiredState.(*WGDesiredState)
	if !ok {
		return false, fmt.Errorf("desired state is not a *WGDesiredState")
	}

	var status *networkingv1alpha1.WireGuardInterfaceStatus
	if v, ok := statusPtr.(*networkingv1alpha1.WireGuardInterfaceStatus); ok {
		status = v
	}

	err := pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(r.interfaceName)
		if err != nil {
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("failed to get link by name %s: %s", r.interfaceName, err.Error())
			}

			r.shouldCreateInterface = true
			return nil
		}

		if status != nil {
			if err := r.setStatus(status); err != nil {
				return fmt.Errorf("failed to set status: %s", err.Error())
			}
		}

		updated, _, err := reconcileAddrs(handle, link, desiredConf.IPAddrs, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			r.shouldUpdateAddr = updated
		}

		updated, err = reconcileAdminState(ctx, handle, link, true, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}
		if updated {
			r.shouldUpdateAdminState = true
		}

		updated, err = reconcileMTU(handle, link, desiredConf.MTU, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}
		if updated {
			r.shouldUpdateMTU = true
		}

		err = pkgutils.WithNetnsWGCli(r.pid, func(wgCtrlCli *wgctrl.Client) error {
			device, err := wgCtrlCli.Device(r.interfaceName)
			if err != nil {
				return fmt.Errorf("failed to get device %s: %s", r.interfaceName, err.Error())
			}

			wgOuterConfigDiff, err := reconcileWGOuterConfig(desiredConf.WGOuterConfig, device)
			if err != nil {
				return fmt.Errorf("failed to reconcile outer config of link %s: %s", r.interfaceName, err.Error())
			}
			r.wgOuterConfigDiff = wgOuterConfigDiff

			peersDiff, err := reconcilePeers(desiredConf.Peers, device.Peers)
			if err != nil {
				return fmt.Errorf("failed to reconcile peers of link %s: %s", r.interfaceName, err.Error())
			}
			r.peersDiff = peersDiff

			return nil
		})

		return err
	})
	if err != nil {
		return false, err
	}

	return r.gatherAllUpdates(), nil
}

func (r *WGReconciler) ApplyReconcile(ctx context.Context, desiredState interface{}) error {
	desiredConf, ok := desiredState.(*WGDesiredState)
	if !ok {
		return fmt.Errorf("desired state is not a *WGDesiredState")
	}

	if r.shouldCreateInterface {
		if desiredConf.MoveToContainer {
			// First, create it in the host netns
			err := pkgutils.WithNetlinkHandle(nil, func(handle *netlink.Handle) error {
				return createWGLink(handle, r.interfaceName)
			})

			if err != nil {
				return fmt.Errorf("failed to create link %s: %s", r.interfaceName, err.Error())
			}

			err = pkgutils.WithNetnsWGCli(nil, func(wgCtrlCli *wgctrl.Client) error {
				return wgCtrlCli.ConfigureDevice(r.interfaceName, *desiredConf.WGOuterConfig)
			})

			if err != nil {
				return fmt.Errorf("failed to configure device %s: %s", r.interfaceName, err.Error())
			}

			err = pkgutils.WithNetlinkHandle(nil, func(handle *netlink.Handle) error {
				link, _ := handle.LinkByName(r.interfaceName)

				// wg on listens after the interface is up
				err := handle.LinkSetUp(link)
				if err != nil {
					return fmt.Errorf("failed to set up link %s: %s", r.interfaceName, err.Error())
				}

				err = handle.LinkSetNsPid(link, *r.pid)
				if err != nil {
					return fmt.Errorf("failed to set ns pid of link %s: %s", r.interfaceName, err.Error())
				}

				return nil
			})

			return err
		}

		err := pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
			return createWGLink(handle, r.interfaceName)
		})
		if err != nil {
			return fmt.Errorf("failed to create link %s: %s", r.interfaceName, err.Error())
		}
	}

	// If it reaches here, the interface must exists
	return pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		link, _ := handle.LinkByName(r.interfaceName)

		if r.shouldUpdateAdminState {
			_, err := reconcileAdminState(ctx, handle, link, true, false)
			if err != nil {
				return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
			}
		}

		if r.shouldUpdateMTU {
			_, err := reconcileMTU(handle, link, desiredConf.MTU, false)
			if err != nil {
				return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
			}
		}

		if r.shouldUpdateAddr {
			_, _, err := reconcileAddrs(handle, link, desiredConf.IPAddrs, false)
			if err != nil {
				return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
			}
		}

		if r.wgOuterConfigDiff != nil {
			err := pkgutils.WithNetnsWGCli(r.pid, func(wgCtrlCli *wgctrl.Client) error {
				return applyWGOuterConfigDiff(r.wgOuterConfigDiff, wgCtrlCli, r.interfaceName)
			})
			if err != nil {
				return fmt.Errorf("failed to apply outer config diff of link %s: %s", r.interfaceName, err.Error())
			}
		}

		if r.peersDiff != nil {
			err := pkgutils.WithNetnsWGCli(r.pid, func(wgCtrlCli *wgctrl.Client) error {
				return applyPeersDiff(r.peersDiff, wgCtrlCli, r.interfaceName)
			})
			if err != nil {
				return fmt.Errorf("failed to apply peers diff of link %s: %s", r.interfaceName, err.Error())
			}
		}

		return nil

	})

}

func (r *WGReconciler) ResetState() {
	r.shouldCreateInterface = false
	r.shouldUpdateAddr = false
	r.shouldUpdateMTU = false
	r.shouldUpdateAdminState = false
	r.peersDiff = nil
	r.wgOuterConfigDiff = nil
}

func (r *WGReconciler) gatherAllUpdates() bool {
	return r.shouldCreateInterface ||
		r.shouldUpdateAddr ||
		r.shouldUpdateMTU ||
		r.wgOuterConfigDiff != nil ||
		r.shouldUpdateAdminState ||
		r.peersDiff != nil
}

// return true if not equal
func checkWGPeersDiff(lhs wgtypes.PeerConfig, rhs wgtypes.Peer) bool {
	if lhs.PublicKey.String() != rhs.PublicKey.String() {
		return true
	}

	if getKeyStr(lhs.PresharedKey) != getKeyStr(&rhs.PresharedKey) {
		return true
	}

	if getEndpointStr(lhs.Endpoint) != getEndpointStr(rhs.Endpoint) {
		return true
	}

	lhsAllowedIPs := make([]string, 0)
	for _, ip := range lhs.AllowedIPs {
		lhsAllowedIPs = append(lhsAllowedIPs, ip.String())
	}
	sort.Strings(lhsAllowedIPs)

	rhsAllowedIPs := make([]string, 0)
	for _, ip := range rhs.AllowedIPs {
		rhsAllowedIPs = append(rhsAllowedIPs, ip.String())
	}
	sort.Strings(rhsAllowedIPs)

	if len(lhsAllowedIPs) != len(rhsAllowedIPs) {
		return true
	}
	for idx := range lhsAllowedIPs {
		if lhsAllowedIPs[idx] != rhsAllowedIPs[idx] {
			return true
		}
	}

	if lhs.PersistentKeepaliveInterval != nil {
		if math.Abs(lhs.PersistentKeepaliveInterval.Seconds()-rhs.PersistentKeepaliveInterval.Seconds()) >= 1.0 {
			return true
		}
	}

	return false
}

func getKeyStr(key *wgtypes.Key) string {
	if key == nil {
		k := wgtypes.Key{}
		return k.String()
	}

	return key.String()
}

func getEndpointStr(endpoint *net.UDPAddr) string {
	if endpoint == nil {
		return ""
	}
	return endpoint.String()
}

type PeersDiff struct {
	AddedPeers   map[string]*wgtypes.PeerConfig
	RemovedPeers map[string]*wgtypes.Peer
	UpdatedPeers map[string]*wgtypes.PeerConfig
}

func reconcilePeers(peerCfgs []wgtypes.PeerConfig, peers []wgtypes.Peer) (*PeersDiff, error) {
	diff := new(PeersDiff)

	allPeers := make(map[string]bool)
	specPeers := make(map[string]*wgtypes.PeerConfig)
	for _, peer := range peerCfgs {
		specPeers[peer.PublicKey.String()] = &peer
		allPeers[peer.PublicKey.String()] = true
	}

	currentPeers := make(map[string]*wgtypes.Peer)
	for _, peer := range peers {
		currentPeers[peer.PublicKey.String()] = &peer
		allPeers[peer.PublicKey.String()] = true
	}

	addedPeers := make(map[string]*wgtypes.PeerConfig)
	for k := range specPeers {
		if _, ok := currentPeers[k]; !ok {
			addedPeers[k] = specPeers[k]
		}
	}

	removedPeers := make(map[string]*wgtypes.Peer)
	for k := range currentPeers {
		if _, ok := specPeers[k]; !ok {
			removedPeers[k] = currentPeers[k]
		}
	}

	commonPeers := make(map[string]bool)
	for k := range allPeers {
		if _, ok := addedPeers[k]; !ok {
			if _, ok := removedPeers[k]; !ok {
				commonPeers[k] = true
			}
		}
	}

	updatedPeers := make(map[string]*wgtypes.PeerConfig)
	for k := range commonPeers {
		if checkWGPeersDiff(*specPeers[k], *currentPeers[k]) {
			updatedPeers[k] = specPeers[k]
		}
	}

	diff.AddedPeers = addedPeers
	diff.RemovedPeers = removedPeers
	diff.UpdatedPeers = updatedPeers

	if len(addedPeers)+len(removedPeers)+len(updatedPeers) > 0 {
		return diff, nil
	}

	return nil, nil
}

func applyPeersDiff(diff *PeersDiff, wgCtrlCli *wgctrl.Client, intfName string) error {
	if diff == nil {
		return fmt.Errorf("applyPeersDiff invoked but peers diff is nil")
	}

	devStatus, err := wgCtrlCli.Device(intfName)
	if err != nil {
		return fmt.Errorf("failed to get device %s: %s", intfName, err.Error())
	}

	wgConf := new(wgtypes.Config)
	wgConf.PrivateKey = &devStatus.PrivateKey
	wgConf.ListenPort = &devStatus.ListenPort
	wgConf.FirewallMark = &devStatus.FirewallMark
	wgConf.ReplacePeers = false
	peerConf := make([]wgtypes.PeerConfig, 0)

	for _, peer := range diff.UpdatedPeers {
		updatedConf := *peer
		updatedConf.UpdateOnly = true
		updatedConf.Remove = false
		peerConf = append(peerConf, updatedConf)
	}

	for _, peer := range diff.RemovedPeers {
		removePeerConf := wgtypes.PeerConfig{
			PublicKey: peer.PublicKey,
			Remove:    true,
		}
		peerConf = append(peerConf, removePeerConf)
	}

	wgConf.Peers = peerConf
	err = wgCtrlCli.ConfigureDevice(intfName, *wgConf)
	if err != nil {
		return fmt.Errorf("failed to configure device %s: %s", intfName, err.Error())
	}

	peerConf = make([]wgtypes.PeerConfig, 0)
	for _, peer := range diff.AddedPeers {
		addedPeerConf := *peer
		addedPeerConf.UpdateOnly = false
		addedPeerConf.Remove = false
		peerConf = append(peerConf, addedPeerConf)
	}
	wgConf.Peers = peerConf
	err = wgCtrlCli.ConfigureDevice(intfName, *wgConf)
	if err != nil {
		return fmt.Errorf("failed to configure device %s: %s", intfName, err.Error())
	}

	return nil
}

type WGOuterConfigDiff struct {
	PrivateKey   *wgtypes.Key
	ListenPort   int
	FirewallMark int
}

func reconcileWGOuterConfig(wgConf *wgtypes.Config, devStatus *wgtypes.Device) (*WGOuterConfigDiff, error) {
	if wgConf == nil || devStatus == nil {
		return nil, fmt.Errorf("reconcileWGOuterConfig invoked but wgConf or devStatus is nil")
	}

	diff := new(WGOuterConfigDiff)
	updated := false

	if devStatus.PrivateKey.String() != wgConf.PrivateKey.String() {
		diff.PrivateKey = &devStatus.PrivateKey
		updated = true
	}

	if devStatus.ListenPort != *wgConf.ListenPort {
		diff.ListenPort = *wgConf.ListenPort
		updated = true
	}

	if devStatus.FirewallMark != *wgConf.FirewallMark {
		diff.FirewallMark = *wgConf.FirewallMark
		updated = true
	}

	if updated {
		return diff, nil
	}

	return nil, nil
}

func applyWGOuterConfigDiff(diff *WGOuterConfigDiff, wgCtrlCli *wgctrl.Client, intfName string) error {
	if diff == nil {
		return fmt.Errorf("applyWGOuterConfigDiff invoked but diff is nil")
	}

	wgConf := new(wgtypes.Config)
	wgConf.PrivateKey = diff.PrivateKey
	wgConf.ListenPort = &diff.ListenPort
	wgConf.FirewallMark = &diff.FirewallMark
	wgConf.ReplacePeers = false
	err := wgCtrlCli.ConfigureDevice(intfName, *wgConf)
	if err != nil {
		return fmt.Errorf("failed to configure device %s: %s", intfName, err.Error())
	}

	return nil
}

func createWGLink(handle *netlink.Handle, intfName string) error {
	wgLink := new(netlink.Wireguard)
	err := handle.LinkSetName(wgLink, intfName)
	if err != nil {
		return fmt.Errorf("failed to set name of link %s: %s", intfName, err.Error())
	}

	err = handle.LinkAdd(wgLink)
	if err != nil {
		return fmt.Errorf("failed to add link %s: %s", intfName, err.Error())
	}

	return nil
}
