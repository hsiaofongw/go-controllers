package reconcile

import (
	"context"
	"fmt"
	"net"

	"github.com/vishvananda/netlink"
	"k8s.io/klog/v2"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgutils "k8s.io/sample-controller/pkg/utils"
)

// Returns: (updated, error)
func reconcileMTU(handle *netlink.Handle, link netlink.Link, mtu *int, dryRun bool) (bool, error) {
	hasUpdated := false

	if mtu != nil {
		if *mtu != link.Attrs().MTU {
			hasUpdated = true
			if dryRun {
				return true, nil
			}

			if err := handle.LinkSetMTU(link, *mtu); err != nil {
				return false, fmt.Errorf("failed to set mtu: %s", err.Error())
			}
		}
	}
	return hasUpdated, nil
}

func getAddrSpecKey(addrSpec *networkingv1alpha1.NetlinkInterfaceAddressSpec) string {
	if addrSpec.PeerCIDR != nil {
		return fmt.Sprintf("%s -> %s", addrSpec.IPCIDR, *addrSpec.PeerCIDR)
	}
	return addrSpec.IPCIDR
}

func getNlAddrKey(addr *netlink.Addr) string {
	if addr == nil {
		return ""
	}
	return pkgutils.AddrToString(*addr)
}

func getReconciliationPlan(addrSpecs []networkingv1alpha1.NetlinkInterfaceAddressSpec, nlAddrs []netlink.Addr) (*NetlinkAddrDifferenceSet, error) {
	lhsSet := make(map[string]*networkingv1alpha1.NetlinkInterfaceAddressSpec)
	for _, addrSpec := range addrSpecs {
		lhsSet[getAddrSpecKey(&addrSpec)] = &addrSpec
	}

	rhsSet := make(map[string]*netlink.Addr)
	for _, addr := range nlAddrs {
		rhsSet[getNlAddrKey(&addr)] = &addr
	}

	addedSet := make(map[string]*networkingv1alpha1.NetlinkInterfaceAddressSpec)
	for k, v := range lhsSet {
		if _, ok := rhsSet[k]; !ok {
			addedSet[k] = v
		}
	}

	removedSet := make(map[string]*netlink.Addr)
	for k, v := range rhsSet {
		if _, ok := lhsSet[k]; !ok {
			removedSet[k] = v
		}
	}

	result := new(NetlinkAddrDifferenceSet)
	result.Added = addedSet
	result.Removed = removedSet
	return result, nil
}

func toNetlinkAddr(addrSpec *networkingv1alpha1.NetlinkInterfaceAddressSpec) (*netlink.Addr, error) {
	if addrSpec.PeerCIDR == nil {
		addrObj, err := netlink.ParseAddr(addrSpec.IPCIDR)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ipcidr %s: %s", addrSpec.IPCIDR, err.Error())
		}
		return addrObj, nil
	}

	_, peeripnet, err := net.ParseCIDR(*addrSpec.PeerCIDR)
	if err != nil {
		return nil, fmt.Errorf("failed to parse peerCidr %s: %s", *addrSpec.PeerCIDR, err.Error())
	}

	localIp := net.ParseIP(addrSpec.IPCIDR)
	if localIp == nil {
		return nil, fmt.Errorf("failed to parse ipcidr %s", addrSpec.IPCIDR)
	}

	addrObj := new(netlink.Addr)
	addrObj.IPNet = new(net.IPNet)
	addrObj.IP = localIp
	addrObj.Peer = peeripnet
	return addrObj, nil
}

func applyReconciliationPlan(handle *netlink.Handle, link netlink.Link, diffSet *NetlinkAddrDifferenceSet) error {
	for _, staleAddrPtr := range diffSet.Removed {
		if err := handle.AddrDel(link, staleAddrPtr); err != nil {
			return fmt.Errorf("failed to remove address %s: %s", staleAddrPtr.String(), err.Error())
		}
	}
	for _, newAddrSpecPtr := range diffSet.Added {
		addrObj, err := toNetlinkAddr(newAddrSpecPtr)
		if err != nil {
			return fmt.Errorf("failed to convert address spec %s to netlink address: %s", newAddrSpecPtr.IPCIDR, err.Error())
		}
		if err := handle.AddrAdd(link, addrObj); err != nil {
			return fmt.Errorf("failed to add address %s: %s", addrObj.String(), err.Error())
		}
	}
	return nil
}

// Returns: (updated, error)
func reconcileAddrs(handle *netlink.Handle, link netlink.Link, addrs []networkingv1alpha1.NetlinkInterfaceAddressSpec, dryRun bool) (bool, *NetlinkAddrDifferenceSet, error) {
	hasUpdated := false
	var diffSet *NetlinkAddrDifferenceSet

	if addrs != nil {
		nlAddrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			return false, diffSet, fmt.Errorf("failed to get addresses: %s", err.Error())
		}

		diffSet, err = getReconciliationPlan(addrs, nlAddrs)
		if err != nil {
			return false, nil, fmt.Errorf("failed to calculate the difference between the spec and the current netlink interface's addresses: %s", err.Error())
		}

		hasUpdated = len(diffSet.Added) > 0 || len(diffSet.Removed) > 0

		if dryRun {
			return hasUpdated, diffSet, nil
		}

		if err := applyReconciliationPlan(handle, link, diffSet); err != nil {
			return true, nil, fmt.Errorf("failed to apply the reconciliation plan: %s", err.Error())
		}
	}

	return hasUpdated, diffSet, nil
}

// Returns: (updated, error)
func reconcileAdminState(ctx context.Context, handle *netlink.Handle, link netlink.Link, up bool, dryRun bool) (bool, error) {
	logger := klog.FromContext(ctx)

	if up {
		if link.Attrs().Flags&net.FlagUp == 0 {
			// The admin state in spec is 'Up', but the link's admin state is 'Down'

			if dryRun {
				return true, nil
			}

			logger.Info("Setting up link", link.Attrs().Name)
			if err := handle.LinkSetUp(link); err != nil {
				return false, fmt.Errorf("failed to set up link %s: %s", link.Attrs().Name, err.Error())
			}
			return true, nil
		}
	} else {
		if link.Attrs().Flags&net.FlagUp == net.FlagUp {
			// The admin state in spec is 'Down', but the link's admin state is 'Up'
			if dryRun {
				return true, nil
			}

			logger.Info("Setting down link", link.Attrs().Name)
			if err := handle.LinkSetDown(link); err != nil {
				return false, fmt.Errorf("failed to set down link %s: %s", link.Attrs().Name, err.Error())
			}
			return true, nil
		}
	}

	// The admin state in spec is the same as the link's admin state
	return false, nil
}

// Returns: (updated, error)
func reconcileEnslavedLinks(handle *netlink.Handle, master netlink.Link, slaves []string, dryRun bool) (bool, *EnslavedLinksDifferenceSet, error) {
	diffSet := new(EnslavedLinksDifferenceSet)

	enslavedNLLinks, err := pkgutils.GetEnslavedLinks(handle, master)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get enslaved links: %s", err.Error())
	}

	specEnslaveSet := make(map[string]bool)

	// the 'addedEnslavedLinks' set are those interfaces that
	// specified as the slaves of the bridge but not really enslaved
	addedEnslavedLinks := make(map[string]netlink.Link)
	for _, ifname := range slaves {
		specEnslaveSet[ifname] = true
		if _, ok := enslavedNLLinks[ifname]; !ok {
			newslave, err := handle.LinkByName(ifname)
			if err != nil {
				return false, nil, fmt.Errorf("failed to get link %s: %s", ifname, err.Error())
			}
			addedEnslavedLinks[ifname] = newslave
		}
	}

	// the 'removedEnslavedLinks' set are those that are already enslaved,
	// but not present in the spec
	removedEnslavedLinks := make(map[string]netlink.Link)
	for ifname, lk := range enslavedNLLinks {
		if _, ok := specEnslaveSet[ifname]; !ok {
			removedEnslavedLinks[ifname] = lk
		}
	}

	updated := len(addedEnslavedLinks) > 0 || len(removedEnslavedLinks) > 0
	diffSet.Added = addedEnslavedLinks
	diffSet.Removed = removedEnslavedLinks

	if dryRun {
		return updated, diffSet, nil
	}

	// Now, worked out these two sets, we are going to
	// un-enslave all the interfaces in the 'removedEnslavedLinks' set,
	// and enslave all the interfaces in the 'addedEnslavedLinks' set
	for _, lk := range addedEnslavedLinks {
		if err := handle.LinkSetMaster(lk, master); err != nil {
			return true, nil, fmt.Errorf("failed to enslave link %s to bridge %s: %s", lk.Attrs().Name, master.Attrs().Name, err.Error())
		}
	}

	for _, lk := range removedEnslavedLinks {
		if err := handle.LinkSetNoMaster(lk); err != nil {
			return true, nil, fmt.Errorf("failed to un-enslave link %s from bridge %s: %s", lk.Attrs().Name, master.Attrs().Name, err.Error())
		}
	}

	return updated, diffSet, nil
}
