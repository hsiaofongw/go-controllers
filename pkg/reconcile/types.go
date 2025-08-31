package reconcile

import (
	"context"

	"github.com/vishvananda/netlink"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
)

// Reconciler interface, its primary purpose is to decouple the
// detection of changes from the act of applying changes
type Reconciler interface {
	// DetectChanges is used to detect changes from the spec,
	// It should be guaranteed to not actually alter the underlying system state.
	// Returns: (hasUpdates, error)
	DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error)

	// ApplyReconcile is used to apply the changes to the underlying system state,
	// It acts based on the result of DetectChanges, which are store in the internal state of the reconciler.
	ApplyReconcile(ctx context.Context, desiredState interface{}) error

	// ResetState is used to reset the state of the reconciler,
	// it resets the internal state of the reconciler to the initial state.
	ResetState()
}

type ReconcilerFactory = func() (Reconciler, error)

type EnslavedLinksDifferenceSet struct {
	Added   map[string]netlink.Link
	Removed map[string]netlink.Link
}

type NetlinkAddrDifferenceSet struct {
	// the 'Added' addresses are those present in the spec but not in the current netlink interface's addresses list
	// Once should always ignore the key and treat it as the opaque.
	Added map[string]*networkingv1alpha1.NetlinkInterfaceAddressSpec

	// the 'Removed' addresses are those present in the current netlink interface but not in the spec.
	// Once should always ignore the key and treat it as the opaque.
	Removed map[string]*netlink.Addr

	// And there is no 'Updated' set since we never try to 'update' the address entry,
	// whenever there is a mismatch we just remove it and append the new one.
}
