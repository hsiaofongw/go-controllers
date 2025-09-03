package reconcile

import (
	"context"
	"fmt"

	"github.com/vishvananda/netlink"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgutils "k8s.io/sample-controller/pkg/utils"
)

// A VXLANReconciler implements the Reconciler interface
type VXLANReconciler struct {
	interfaceName          string
	pid                    *int
	shouldCreateInterface  bool
	shouldUpdateMTU        *int
	shouldUpdateAdminState *bool
	shouldUpdateAddrs      *NetlinkAddrDifferenceSet
}

func NewVXLANReconciler(interfaceName string, pid *int) (*VXLANReconciler, error) {
	reconciler := new(VXLANReconciler)
	reconciler.interfaceName = interfaceName
	reconciler.pid = pid
	return reconciler, nil
}

func (r *VXLANReconciler) gatherAllUpdates() bool {
	return r.shouldCreateInterface ||
		r.shouldUpdateAddrs != nil ||
		r.shouldUpdateMTU != nil ||
		r.shouldUpdateAdminState != nil

}

// Returns: (hasUpdates, error)
func (r *VXLANReconciler) DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error) {
	netlinkSpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
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
		}

		updated, err := reconcileMTU(handle, link, netlinkSpec.MTU, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			mtu := *netlinkSpec.MTU
			r.shouldUpdateMTU = &mtu
		}

		nlAddrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			return fmt.Errorf("failed to get addrs of link %s: %s", r.interfaceName, err.Error())
		}

		specAddrs, err := toNetlinkAddrList(netlinkSpec.Addresses)
		if err != nil {
			return fmt.Errorf("failed to convert address specs to netlink addresses: %s", err.Error())
		}

		r.shouldUpdateAddrs, err = getAddrReconciliationPlan(specAddrs, nlAddrs)
		if err != nil {
			return fmt.Errorf("failed to get addr reconciliation plan of link %s: %s", r.interfaceName, err.Error())
		}

		updated, err = reconcileAdminState(ctx, handle, link, netlinkSpec.Up, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			desiredAdminState := netlinkSpec.Up
			r.shouldUpdateAdminState = &desiredAdminState
		}

		return nil
	})

	return r.gatherAllUpdates(), err
}

// Returns: (converged, error)
func (r *VXLANReconciler) ApplyReconcile(ctx context.Context, desiredState interface{}) error {
	netlinkSpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
	}

	if netlinkSpec.Vxlan == nil {
		return fmt.Errorf("vxlan spec is nil")
	}

	return pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		if r.shouldCreateInterface {
			link := new(netlink.Vxlan)

			link.VxlanId = int(netlinkSpec.Vxlan.VNI)
			if netlinkSpec.Vxlan.Port != nil {
				link.Port = *netlinkSpec.Vxlan.Port
			}

			link.Learning = !netlinkSpec.Vxlan.NoLearning

			if netlinkSpec.Vxlan.Dev != nil {
				vtepDev, err := handle.LinkByName(*netlinkSpec.Vxlan.Dev)
				if err != nil {
					// Specified a vtep dev that is not exists IS an error here,
					// so abort the creation of vxlan interface.
					return fmt.Errorf("failed to get link %s: %s", *netlinkSpec.Vxlan.Dev, err.Error())
				} else {
					link.VtepDevIndex = vtepDev.Attrs().Index
				}
			}

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

		if r.shouldUpdateAddrs != nil {
			err := applyAddrReconciliationPlan(handle, link, r.shouldUpdateAddrs)
			if err != nil {
				return fmt.Errorf("failed to apply addr reconciliation plan of link %s: %s", r.interfaceName, err.Error())
			}
		}

		if _, err := reconcileAdminState(ctx, handle, link, netlinkSpec.Up, false); err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		return nil
	})
}

func (r *VXLANReconciler) ResetState() {
	r.shouldCreateInterface = false
	r.shouldUpdateAddrs = nil
	r.shouldUpdateMTU = nil
	r.shouldUpdateAdminState = nil
}
