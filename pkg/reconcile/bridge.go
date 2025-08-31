package reconcile

import (
	"context"
	"fmt"

	"github.com/vishvananda/netlink"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgutils "k8s.io/sample-controller/pkg/utils"
)

type BridgeReconciler struct {
	interfaceName             string
	pid                       *int
	shouldUpdateMTU           *int
	shouldUpdateAdminState    *bool
	shouldAddAddrs            map[string]*networkingv1alpha1.NetlinkInterfaceAddressSpec
	shouldRemoveAddrs         map[string]*netlink.Addr
	shouldAddEnslavedLinks    map[string]netlink.Link
	shouldRemoveEnslavedLinks map[string]netlink.Link
	shouldCreateInterface     bool
}

func NewBridgeReconciler(interfaceName string, pid *int) (*BridgeReconciler, error) {
	bridgeReconciler := new(BridgeReconciler)
	bridgeReconciler.interfaceName = interfaceName
	bridgeReconciler.pid = pid
	return bridgeReconciler, nil
}

func (r *BridgeReconciler) gatherAllUpdates() bool {
	return r.shouldUpdateMTU != nil ||
		r.shouldUpdateAdminState != nil ||
		len(r.shouldAddAddrs) > 0 ||
		len(r.shouldRemoveAddrs) > 0 ||
		len(r.shouldAddEnslavedLinks) > 0 ||
		len(r.shouldRemoveEnslavedLinks) > 0 ||
		r.shouldCreateInterface
}

func (r *BridgeReconciler) DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error) {
	bridgeSpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return false, fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
	}

	var status *networkingv1alpha1.NetlinkInterfaceStatus
	if v, ok := statusPtr.(*networkingv1alpha1.NetlinkInterfaceStatus); ok {
		status = v
	}

	err := pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		_, err := handle.LinkByName(r.interfaceName)
		if err != nil {
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
			}

			r.shouldCreateInterface = true
		}
		return nil
	})

	if err != nil {
		return r.gatherAllUpdates(), err
	}

	err = pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(r.interfaceName)
		if err != nil {
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
			}

			return nil
		}

		if status != nil {
			mtu := link.Attrs().MTU
			status.MTU = &mtu

			addrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
			if err != nil {
				return fmt.Errorf("failed to get addresses of link %s: %s", r.interfaceName, err.Error())
			}
			attrs := link.Attrs()
			status.Netlink = networkingv1alpha1.NewFromNetlinkLinkAttrs(attrs, addrs)
			status.OperState = attrs.OperState.String()
			status.Flags = pkgutils.FlagsToStrings(link.Attrs().Flags)

			addrsStrs := make([]string, 0)
			for _, addr := range addrs {
				addrsStrs = append(addrsStrs, pkgutils.AddrToString(addr))
			}
			status.Addresses = addrsStrs

			enslavedNLLinks, err := pkgutils.GetEnslavedLinks(handle, link)
			if err != nil {
				return fmt.Errorf("failed to get enslaved links of link %s: %s", r.interfaceName, err.Error())
			}

			status.Bridge = &networkingv1alpha1.NetlinkInterfaceBridgeStatus{
				EnslavedLinks: make([]string, 0),
			}
			for _, lk := range enslavedNLLinks {
				status.Bridge.EnslavedLinks = append(status.Bridge.EnslavedLinks, lk.Attrs().Name)
			}
		}

		updated, err := reconcileMTU(handle, link, bridgeSpec.MTU, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			mtu := *bridgeSpec.MTU
			r.shouldUpdateMTU = &mtu
		}

		updated, diffSet, err := reconcileAddrs(handle, link, bridgeSpec.Addresses, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			r.shouldRemoveAddrs = diffSet.Removed
			r.shouldAddAddrs = diffSet.Added
		}

		updated, err = reconcileAdminState(ctx, handle, link, bridgeSpec.Up, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			desiredAdminState := bridgeSpec.Up
			r.shouldUpdateAdminState = &desiredAdminState
		}

		if bridgeSpec.Bridge != nil {
			updated, diffSet, err := reconcileEnslavedLinks(handle, link, bridgeSpec.Bridge.Slaves, true)
			if err != nil {
				return fmt.Errorf("failed to reconcile enslaved links of link %s: %s", r.interfaceName, err.Error())
			}

			if updated {
				r.shouldAddEnslavedLinks = diffSet.Added
				r.shouldRemoveEnslavedLinks = diffSet.Removed
			}

		}

		return nil
	})

	return r.gatherAllUpdates(), err
}

func (r *BridgeReconciler) ApplyReconcile(ctx context.Context, desiredState interface{}) error {
	bridgeSpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
	}

	return pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		if r.shouldCreateInterface {
			link := new(netlink.Dummy)
			if err := handle.LinkSetName(link, r.interfaceName); err != nil {
				return fmt.Errorf("failed to set name of link %s: %s", r.interfaceName, err.Error())
			}

			if err := handle.LinkAdd(link); err != nil {
				return fmt.Errorf("failed to add link %s: %s", r.interfaceName, err.Error())
			}

			// one should run `DetectChanges` again after `ApplyReconcile` until it's converged.
			return nil
		}

		link, _ := handle.LinkByName(r.interfaceName)
		if _, err := reconcileMTU(handle, link, r.shouldUpdateMTU, false); err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if _, _, err := reconcileAddrs(handle, link, bridgeSpec.Addresses, false); err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if _, err := reconcileAdminState(ctx, handle, link, bridgeSpec.Up, false); err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		if _, _, err := reconcileEnslavedLinks(handle, link, bridgeSpec.Bridge.Slaves, false); err != nil {
			return fmt.Errorf("failed to reconcile enslaved links of link %s: %s", r.interfaceName, err.Error())
		}

		return nil
	})
}

func (r *BridgeReconciler) ResetState() {
	r.shouldUpdateMTU = nil
	r.shouldUpdateAdminState = nil
	r.shouldAddAddrs = nil
	r.shouldRemoveAddrs = nil
	r.shouldAddEnslavedLinks = nil
	r.shouldRemoveEnslavedLinks = nil
	r.shouldCreateInterface = false
}
