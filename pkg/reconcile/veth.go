package reconcile

import (
	"context"
	"fmt"

	dockerUtil "example.com/go-util/pkg/util/docker"
	dockerSDK "github.com/docker/docker/client"
	"github.com/vishvananda/netlink"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgutils "k8s.io/sample-controller/pkg/utils"
)

// A VethReconciler implements the Reconciler interface
type VethReconciler struct {
	interfaceName          string
	pid                    *int
	shouldCreateInterface  bool
	shouldUpdateMTU        *int
	shouldUpdateAdminState *bool
	dockerClient           *dockerSDK.Client

	localAddrDiffSet *NetlinkAddrDifferenceSet
	peerAddrDiffSet  *NetlinkAddrDifferenceSet
}

func NewVethReconciler(interfaceName string, pid *int) (*VethReconciler, error) {
	reconciler := new(VethReconciler)
	reconciler.interfaceName = interfaceName
	reconciler.pid = pid

	dockerClient, err := dockerUtil.NewDefaultDockerClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err.Error())
	}
	reconciler.dockerClient = dockerClient

	return reconciler, nil
}

func (r *VethReconciler) gatherAllUpdates() bool {
	return r.shouldCreateInterface ||
		r.localAddrDiffSet != nil ||
		r.peerAddrDiffSet != nil ||
		r.shouldUpdateMTU != nil ||
		r.shouldUpdateAdminState != nil

}

func (r *VethReconciler) getCurrentAddrs(spec *networkingv1alpha1.NetlinkInterfaceSpec) ([]netlink.Addr, []netlink.Addr, error) {
	localPid, peerPid, err := r.getVethPairPIDs(spec)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get veth pair pids of link %s: %s", r.interfaceName, err.Error())
	}

	localName, peerName, err := getInterfaceNames(spec)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get interface names of link %s: %s", r.interfaceName, err.Error())
	}

	type result struct {
		localAddrs []netlink.Addr
		peerAddrs  []netlink.Addr
	}

	res := new(result)

	err = pkgutils.WithNetlinkHandle(localPid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(localName)
		if err != nil {
			return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
		}

		localAddrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			return fmt.Errorf("failed to get local addrs of link %s: %s", r.interfaceName, err.Error())
		}

		res.localAddrs = localAddrs

		return nil
	})

	err = pkgutils.WithNetlinkHandle(peerPid, func(handle *netlink.Handle) error {
		link, err := handle.LinkByName(peerName)
		if err != nil {
			return fmt.Errorf("failed to get link %s: %s", r.interfaceName, err.Error())
		}

		peerAddrs, err := handle.AddrList(link, netlink.FAMILY_ALL)
		if err != nil {
			return fmt.Errorf("failed to get peer addrs of link %s: %s", r.interfaceName, err.Error())
		}

		res.peerAddrs = peerAddrs

		return nil
	})

	return res.localAddrs, res.peerAddrs, err
}

// Returns: (hasUpdates, error)
func (r *VethReconciler) DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error) {
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

		localAddrs, peerAddrs, err := r.getPairAddrs(netlinkSpec)
		if err != nil {
			return fmt.Errorf("failed to get pair addrs of link %s: %s", r.interfaceName, err.Error())
		}

		nlLocalAddrs, nlPeerAddrs, err := r.getCurrentAddrs(netlinkSpec)
		if err != nil {
			return fmt.Errorf("failed to get current addrs of link %s: %s", r.interfaceName, err.Error())
		}

		r.localAddrDiffSet, err = getAddrReconciliationPlan(localAddrs, nlLocalAddrs)
		if err != nil {
			return fmt.Errorf("failed to get addr reconciliation plan of link %s: %s", r.interfaceName, err.Error())
		}

		r.peerAddrDiffSet, err = getAddrReconciliationPlan(peerAddrs, nlPeerAddrs)
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
	// TODO: implement this

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
		return fmt.Errorf("vxlan spec is nil")
	}

	return pkgutils.WithNetlinkHandle(nil, func(handle *netlink.Handle) error {
		if r.shouldCreateInterface {
			link := new(netlink.Veth)

			localName, peerName, err := getInterfaceNames(netlinkSpec)
			if err != nil {
				return fmt.Errorf("failed to get interface names: %s", err.Error())
			}

			localPid, peerPid, err := r.getVethPairPIDs(netlinkSpec)
			if err != nil {
				return fmt.Errorf("failed to get veth pair pids: %s", err.Error())
			}

			link.PeerName = peerName
			if peerPid != nil {
				link.PeerNamespace = netlink.NsPid(*peerPid)
			}

			if err := handle.LinkSetName(link, localName); err != nil {
				return fmt.Errorf("failed to set name of link %s: %s", r.interfaceName, err.Error())
			}

			if err := handle.LinkAdd(link); err != nil {
				return fmt.Errorf("failed to add link %s: %s", r.interfaceName, err.Error())
			}

			if localPid != nil {
				err := handle.LinkSetNsPid(link, *localPid)
				if err != nil {
					return fmt.Errorf("failed to set ns pid of link %s: %s", r.interfaceName, err.Error())
				}
			}

			// one should run `DetectChanges` again after `ApplyReconcile` until it's converged.
			return nil
		}

		link, _ := handle.LinkByName(r.interfaceName)
		if _, err := reconcileMTU(handle, link, r.shouldUpdateMTU, false); err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		if r.localAddrDiffSet != nil {
			err := applyAddrReconciliationPlan(handle, link, r.localAddrDiffSet)
			if err != nil {
				return fmt.Errorf("failed to apply local addr reconciliation plan of link %s: %s", r.interfaceName, err.Error())
			}
		}

		if r.peerAddrDiffSet != nil {
			err := applyAddrReconciliationPlan(handle, link, r.peerAddrDiffSet)
			if err != nil {
				return fmt.Errorf("failed to apply peer addr reconciliation plan of link %s: %s", r.interfaceName, err.Error())
			}
		}

		if _, err := reconcileAdminState(ctx, handle, link, netlinkSpec.Up, false); err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		return nil
	})
}

func (r *VethReconciler) ResetState() {
	r.shouldCreateInterface = false
	r.shouldUpdateMTU = nil
	r.shouldUpdateAdminState = nil
	r.localAddrDiffSet = nil
	r.peerAddrDiffSet = nil
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
