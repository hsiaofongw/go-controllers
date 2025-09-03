package reconcile

import (
	"context"
	"fmt"

	dockerUtil "example.com/go-util/pkg/util/docker"
	dockerSDK "github.com/docker/docker/client"
	"github.com/vishvananda/netlink"
	"k8s.io/client-go/tools/record"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgutils "k8s.io/sample-controller/pkg/utils"
)

type changeset struct {
	shouldUpdateMTU        *int
	shouldUpdateAdminState *bool
	shouldUpdateAddrs      *NetlinkAddrDifferenceSet
	shouldCreateInterface  bool
}

// A VethReconciler implements the Reconciler interface
type VethReconciler struct {
	interfaceName string
	pid           *int
	dockerClient  *dockerSDK.Client
	recorder      record.EventRecorder

	localChanges          *changeset
	peerChanges           *changeset
	shouldCreateInterface bool
}

func NewVethReconciler(interfaceName string, pid *int, recorder record.EventRecorder) (*VethReconciler, error) {
	reconciler := new(VethReconciler)
	reconciler.interfaceName = interfaceName
	reconciler.pid = pid

	dockerClient, err := dockerUtil.NewDefaultDockerClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err.Error())
	}
	reconciler.dockerClient = dockerClient
	reconciler.recorder = recorder

	return reconciler, nil
}

func (cs *changeset) gatherAllUpdates() bool {
	return cs.shouldUpdateMTU != nil ||
		cs.shouldUpdateAdminState != nil ||
		cs.shouldUpdateAddrs != nil
}

func (r *VethReconciler) gatherAllUpdates() bool {
	return r.shouldCreateInterface ||
		(r.localChanges != nil && r.localChanges.gatherAllUpdates()) ||
		(r.peerChanges != nil && r.peerChanges.gatherAllUpdates())
}

type concernedSpec struct {
	MTU       *int
	Addresses []netlink.Addr
	Up        *bool
}

func (r *VethReconciler) setStatus(status *networkingv1alpha1.NetlinkInterfaceStatus, spec *networkingv1alpha1.NetlinkInterfaceSpec) error {
	localPid, peerPid, err := r.getVethPairPIDs(spec)
	if err != nil {
		return fmt.Errorf("failed to get veth pair pids: %s", err.Error())
	}

	localName, peerName, err := getInterfaceNames(spec)
	if err != nil {
		return fmt.Errorf("failed to get interface names: %s", err.Error())
	}

	err = pkgutils.WithNetlinkHandle(localPid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(localName)
		if err != nil {
			return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
		}

		attrs := link.Attrs()
		status.MTU = &attrs.MTU
		addrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			return fmt.Errorf("failed to get addresses of link %s: %s", r.interfaceName, err.Error())
		}
		status.Netlink = networkingv1alpha1.NewFromNetlinkLinkAttrs(attrs, addrs)
		addrsStrs := make([]string, 0)
		for _, addr := range addrs {
			addrsStrs = append(addrsStrs, pkgutils.AddrToString(addr))
		}
		status.Addresses = addrsStrs
		status.OperState = attrs.OperState.String()
		status.Flags = pkgutils.FlagsToStrings(link.Attrs().Flags)

		return nil
	})

	if err != nil {
		return err
	}

	return pkgutils.WithNetlinkHandle(peerPid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(peerName)
		if err != nil {
			return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
		}
		peerStatus := new(networkingv1alpha1.NetlinkInterfaceVethPeerStatus)
		vethStatus := new(networkingv1alpha1.NetlinkInterfaceVethStatus)

		attrs := link.Attrs()
		peerStatus.MTU = &attrs.MTU
		addrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			return fmt.Errorf("failed to get addresses of link %s: %s", r.interfaceName, err.Error())
		}
		addrsStrs := make([]string, 0)
		for _, addr := range addrs {
			addrsStrs = append(addrsStrs, pkgutils.AddrToString(addr))
		}
		peerStatus.Addresses = addrsStrs
		peerStatus.OperState = attrs.OperState.String()
		peerStatus.Flags = pkgutils.FlagsToStrings(link.Attrs().Flags)

		vethStatus.Peer = peerStatus
		status.Veth = vethStatus
		return nil
	})
}

func (r *VethReconciler) getLocalSpec(spec *networkingv1alpha1.NetlinkInterfaceSpec) (*concernedSpec, error) {
	addrs, _, err := r.getPairAddrs(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to get current addrs of link: %s", err.Error())
	}

	res := new(concernedSpec)
	res.Addresses = addrs
	res.Up = &spec.Up
	res.MTU = spec.MTU

	return res, nil
}

func (r *VethReconciler) getPeerSpec(spec *networkingv1alpha1.NetlinkInterfaceSpec) (*concernedSpec, error) {
	_, addrs, err := r.getPairAddrs(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to get current addrs of link: %s", err.Error())
	}

	res := new(concernedSpec)
	res.Addresses = addrs
	res.Up = &spec.Up
	res.MTU = spec.MTU

	return res, nil
}

// If not changes is happen, return nil on the first value
func (r *VethReconciler) detectChangeSet(pid *int, intfName string, spec *concernedSpec) (*changeset, error) {
	type result struct {
		changes *changeset
	}
	res := new(result)

	err := pkgutils.WithNetlinkHandle(pid, func(handle *netlink.Handle) error {

		res.changes = new(changeset)

		link, err := handle.LinkByName(intfName)
		if err != nil {
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("failed to get link %s: %s", intfName, err.Error())
			}

			res.changes.shouldCreateInterface = true

			return nil
		}

		if spec.MTU != nil {
			updated, err := reconcileMTU(handle, link, spec.MTU, true)
			if err != nil {
				return fmt.Errorf("failed to reconcile mtu of link %s: %s", intfName, err.Error())
			}

			if updated {
				res.changes.shouldUpdateMTU = spec.MTU
			}
		}

		addrDiff, err := getAddrReconciliationPlan(spec.Addresses, spec.Addresses)
		if err != nil {
			return fmt.Errorf("failed to get addr reconciliation plan of link %s: %s", intfName, err.Error())
		}
		if addrDiff != nil {
			res.changes.shouldUpdateAddrs = addrDiff
		}

		if spec.Up != nil {
			updated, err := detectAdminStateChange(handle, link, *spec.Up, true)
			if err != nil {
				return fmt.Errorf("failed to detect admin state change of link %s: %s", intfName, err.Error())
			}

			if updated {
				res.changes.shouldUpdateAdminState = spec.Up
			}
		}

		return nil
	})

	if res.changes.gatherAllUpdates() {
		return res.changes, nil
	}

	return nil, err
}

func (r *VethReconciler) applyChangeSet(pid *int, intfName string, changes *changeset) error {
	return pkgutils.WithNetlinkHandle(pid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(intfName)
		if err != nil {
			return fmt.Errorf("failed to get link %s: %s", intfName, err.Error())
		}

		if changes == nil || !changes.gatherAllUpdates() {
			return nil
		}
		if changes.shouldUpdateMTU != nil {
			err := handle.LinkSetMTU(link, *changes.shouldUpdateMTU)
			if err != nil {
				return fmt.Errorf("failed to set mtu of link %s: %s", intfName, err.Error())
			}
		}

		if changes.shouldUpdateAdminState != nil {
			if *changes.shouldUpdateAdminState {
				err := handle.LinkSetUp(link)
				if err != nil {
					return fmt.Errorf("failed to set up link %s: %s", intfName, err.Error())
				}
			} else {
				err := handle.LinkSetDown(link)
				if err != nil {
					return fmt.Errorf("failed to set down link %s: %s", intfName, err.Error())
				}
			}
		}

		if changes.shouldUpdateAddrs != nil {
			err := applyAddrReconciliationPlan(handle, link, changes.shouldUpdateAddrs)
			if err != nil {
				return fmt.Errorf("failed to apply addr reconciliation plan of link %s: %s", intfName, err.Error())
			}
		}

		return nil
	})
}

// Returns: (hasUpdates, error)
func (r *VethReconciler) DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error) {
	netlinkSpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return false, fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
	}

	localPid, peerPid, err := r.getVethPairPIDs(netlinkSpec)
	if err != nil {
		return false, fmt.Errorf("failed to get veth pair pids: %s", err.Error())
	}

	var status *networkingv1alpha1.NetlinkInterfaceStatus
	if v, ok := statusPtr.(*networkingv1alpha1.NetlinkInterfaceStatus); ok {
		status = v
	}

	localSpec, err := r.getLocalSpec(netlinkSpec)
	if err != nil {
		return false, fmt.Errorf("failed to get local spec: %s", err.Error())
	}

	peerSpec, err := r.getPeerSpec(netlinkSpec)
	if err != nil {
		return false, fmt.Errorf("failed to get peer spec: %s", err.Error())
	}

	localName, peerName, err := getInterfaceNames(netlinkSpec)
	if err != nil {
		return false, fmt.Errorf("failed to get interface names: %s", err.Error())
	}

	r.localChanges, err = r.detectChangeSet(localPid, localName, localSpec)
	if err != nil {
		return false, fmt.Errorf("failed to detect changes for local side of veth pair: %s", err.Error())
	}

	if r.localChanges != nil && r.localChanges.shouldCreateInterface {
		r.shouldCreateInterface = true
		return r.gatherAllUpdates(), nil
	}

	r.peerChanges, err = r.detectChangeSet(peerPid, peerName, peerSpec)
	if err != nil {
		return false, fmt.Errorf("failed to detect changes for peer side of veth pair: %s", err.Error())
	}

	if status != nil {
		err := r.setStatus(status, netlinkSpec)
		if err != nil {
			return r.gatherAllUpdates(), fmt.Errorf("failed to set status: %s", err.Error())
		}
	}

	return r.gatherAllUpdates(), nil
}

func getInterfaceNames(spec *networkingv1alpha1.NetlinkInterfaceSpec) (string, string, error) {
	localName := spec.InterfaceName
	peerName := fmt.Sprintf("%s-peer", spec.InterfaceName)

	if spec.Veth != nil {
		if spec.Veth.Local != nil {
			if spec.Veth.Local.InterfaceName != nil && *spec.Veth.Local.InterfaceName != "" {
				localName = *spec.Veth.Local.InterfaceName
			}
		}

		if spec.Veth.Peer != nil {
			if spec.Veth.Peer.InterfaceName != nil && *spec.Veth.Peer.InterfaceName != "" {
				peerName = *spec.Veth.Peer.InterfaceName
			}
		}
	}

	return localName, peerName, nil
}

func (r *VethReconciler) getVethPairPIDs(spec *networkingv1alpha1.NetlinkInterfaceSpec) (*int, *int, error) {
	localPid := r.pid
	peerPid := r.pid

	if spec.Veth != nil {
		if spec.Veth.Local != nil {
			if spec.Veth.Local.Container != nil {
				v, err := r.getInterfacePid(spec.Veth.Local.Container)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to get pid of container: %s", err.Error())
				}
				localPid = v
			}
		}

		if spec.Veth.Peer != nil {
			if spec.Veth.Peer.Container != nil {
				v, err := r.getInterfacePid(spec.Veth.Peer.Container)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to get pid of container: %s", err.Error())
				}
				peerPid = v
			}
		}
	}

	return localPid, peerPid, nil
}

func (r *VethReconciler) getPairAddrs(spec *networkingv1alpha1.NetlinkInterfaceSpec) ([]netlink.Addr, []netlink.Addr, error) {
	localAddrSpecs := spec.Addresses
	var peerAddrSpecs []networkingv1alpha1.NetlinkInterfaceAddressSpec
	if spec.Veth != nil {
		if spec.Veth.Local != nil && spec.Veth.Local.Addresses != nil {
			localAddrSpecs = spec.Veth.Local.Addresses
		}

		if spec.Veth.Peer != nil && spec.Veth.Peer.Addresses != nil {
			peerAddrSpecs = spec.Veth.Peer.Addresses
		}
	}

	localAddrs, err := toNetlinkAddrList(localAddrSpecs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert local address specs to netlink addresses: %s", err.Error())
	}

	peerAddrs, err := toNetlinkAddrList(peerAddrSpecs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert peer address specs to netlink addresses: %s", err.Error())
	}

	return localAddrs, peerAddrs, nil
}

// Returns: (converged, error)
func (r *VethReconciler) ApplyReconcile(ctx context.Context, desiredState interface{}) error {
	netlinkSpec, ok := desiredState.(*networkingv1alpha1.NetlinkInterfaceSpec)
	if !ok {
		return fmt.Errorf("desired state is not a *networkingv1alpha1.NetlinkInterfaceSpec")
	}

	if netlinkSpec.Veth == nil {
		return fmt.Errorf("veth spec is nil")
	}

	localPid, peerPid, err := r.getVethPairPIDs(netlinkSpec)
	if err != nil {
		return fmt.Errorf("failed to get veth pair pids: %s", err.Error())
	}

	localName, peerName, err := getInterfaceNames(netlinkSpec)
	if err != nil {
		return fmt.Errorf("failed to get interface names: %s", err.Error())
	}

	return pkgutils.WithNetlinkHandle(nil, func(handle *netlink.Handle) error {
		if r.shouldCreateInterface {
			link := new(netlink.Veth)

			link.PeerName = peerName
			if peerPid != nil {
				link.PeerNamespace = netlink.NsPid(*peerPid)
			}

			link.Attrs().Name = localName

			if err := handle.LinkAdd(link); err != nil {
				return fmt.Errorf("failed to add link %s: %s", localName, err.Error())
			}

			if localPid != nil {
				err := handle.LinkSetNsPid(link, *localPid)
				if err != nil {
					return fmt.Errorf("failed to set ns pid of link %s: %s", localName, err.Error())
				}
			}

			// one should run `DetectChanges` again after `ApplyReconcile` until it's converged.
			return nil
		}

		if r.localChanges != nil && r.localChanges.gatherAllUpdates() {
			err := r.applyChangeSet(localPid, localName, r.localChanges)
			if err != nil {
				return fmt.Errorf("failed to apply changes for local side of veth pair: %s", err.Error())
			}
		}

		if r.peerChanges != nil && r.peerChanges.gatherAllUpdates() {
			err := r.applyChangeSet(peerPid, peerName, r.peerChanges)
			if err != nil {
				return fmt.Errorf("failed to apply changes for peer side of veth pair: %s", err.Error())
			}
		}

		return nil
	})
}

func (r *VethReconciler) ResetState() {
	r.shouldCreateInterface = false
	r.localChanges = nil
	r.peerChanges = nil
}

// if the interface should be placed in current namespace, return nil
// use this function to determine where to look for the interface: is it in the current namespace or in a container?
// for example, if it returns a nil, look for the interface in the current namespace
// otherwise, look for the interface in the container specified by the pid
func (c *VethReconciler) getInterfacePid(nlContainerSpec *networkingv1alpha1.NetlinkInterfaceContainerSpec) (*int, error) {
	if nlContainerSpec == nil {
		return nil, nil
	}

	if nlContainerSpec.Docker != nil && nlContainerSpec.Docker.Name != "" {
		pid, err := c.getDockerContainerPid(nlContainerSpec.Docker.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get pid of container %s: %s", nlContainerSpec.Docker.Name, err.Error())
		}
		return &pid, nil
	}

	if nlContainerSpec.NetNS != nil && nlContainerSpec.NetNS.PID != nil {
		return nlContainerSpec.NetNS.PID, nil
	}

	return nil, nil
}

func (c *VethReconciler) getDockerContainerPid(containerName string) (int, error) {
	if containerName == "" {
		return -1, fmt.Errorf("moveToContainer is true but no container name is provided")
	}

	p, err := dockerUtil.GetPidOfContainer(context.Background(), c.dockerClient, containerName)
	if err != nil {
		return -1, err
	}

	return p, nil
}
