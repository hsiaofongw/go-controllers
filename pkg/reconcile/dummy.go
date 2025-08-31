package reconcile

import (
	"context"
	"fmt"

	"github.com/vishvananda/netlink"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgutils "k8s.io/sample-controller/pkg/utils"
)

// A DummyReconciler implements the Reconciler interface
type DummyReconciler struct {
	interfaceName          string
	pid                    *int
	shouldCreateInterface  bool
	shouldRemoveAddrs      map[string]*netlink.Addr
	shouldAddAddrs         map[string]*netlink.Addr
	shouldUpdateMTU        *int
	shouldUpdateAdminState *bool
}

func NewDummyReconciler(interfaceName string, pid *int) (*DummyReconciler, error) {
	dummyReconciler := new(DummyReconciler)
	dummyReconciler.interfaceName = interfaceName
	dummyReconciler.pid = pid
	return dummyReconciler, nil
}

func (r *DummyReconciler) gatherAllUpdates() bool {
	return r.shouldCreateInterface ||
		len(r.shouldRemoveAddrs) > 0 ||
		len(r.shouldAddAddrs) > 0 ||
		r.shouldUpdateMTU != nil ||
		r.shouldUpdateAdminState != nil
}

// Returns: (hasUpdates, error)
func (r *DummyReconciler) DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error) {
	dummySpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return false, fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
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

		updated, err := reconcileMTU(handle, link, dummySpec.MTU, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			mtu := *dummySpec.MTU
			r.shouldUpdateMTU = &mtu
		}

		specAddrs, err := toNetlinkAddrList(dummySpec.Addresses)
		if err != nil {
			return fmt.Errorf("failed to convert address specs to netlink addresses: %s", err.Error())
		}

		updated, diffSet, err := reconcileAddrs(handle, link, specAddrs, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			r.shouldRemoveAddrs = diffSet.Removed
			r.shouldAddAddrs = diffSet.Added
		}

		updated, err = reconcileAdminState(ctx, handle, link, dummySpec.Up, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			desiredAdminState := dummySpec.Up
			r.shouldUpdateAdminState = &desiredAdminState
		}

		return nil
	})

	return r.gatherAllUpdates(), err
}

// Returns: (converged, error)
func (r *DummyReconciler) ApplyReconcile(ctx context.Context, desiredState interface{}) error {
	dummySpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
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

		specAddrs, err := toNetlinkAddrList(dummySpec.Addresses)
		if err != nil {
			return fmt.Errorf("failed to convert address specs to netlink addresses: %s", err.Error())
		}

		if _, _, err := reconcileAddrs(handle, link, specAddrs, false); err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if _, err := reconcileAdminState(ctx, handle, link, dummySpec.Up, false); err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		return nil
	})
}

func (r *DummyReconciler) ResetState() {
	r.shouldCreateInterface = false
	r.shouldRemoveAddrs = nil
	r.shouldAddAddrs = nil
	r.shouldUpdateMTU = nil
	r.shouldUpdateAdminState = nil
}
