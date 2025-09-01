package reconcile

import (
	"context"
	"fmt"

	"github.com/vishvananda/netlink"
	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
	pkgutils "k8s.io/sample-controller/pkg/utils"
)

type WGDesiredState struct {
	MTU     *int
	IPAddrs []netlink.Addr
	Peers   []wgtypes.PeerConfig

	PrivateKey wgtypes.Key

	// ListenPort must not be omitted, even when its behind a NAT.
	ListenPort int

	// If this is true, means that the wg interface is expected to be
	// born in host netns then move to the container once created.
	MoveToContainer bool
}

type WGReconciler struct {
	interfaceName          string
	pid                    *int
	shouldUpdateAddr       *NetlinkAddrDifferenceSet
	shouldCreateInterface  bool
	shouldUpdateMTU        bool
	shouldUpdateListenPort bool
	shouldUpdatePrivateKey bool
	shouldUpdateAdminState bool
}

func NewWGReconciler(interfaceName string, pid *int) *WGReconciler {
	return &WGReconciler{interfaceName: interfaceName, pid: pid}
}

func (r *WGReconciler) DetectChanges(ctx context.Context, desiredState interface{}, statusPtr interface{}) (bool, error) {

	desiredConf, ok := desiredState.(*WGDesiredState)
	if !ok {
		return false, fmt.Errorf("desired state is not a *WGDesiredState")
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

		updated, diffSet, err := reconcileAddrs(handle, link, desiredConf.IPAddrs, true)
		if err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		if updated {
			r.shouldUpdateAddr = diffSet
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

			if device.ListenPort != desiredConf.ListenPort {
				r.shouldUpdateListenPort = true
			}

			if device.PrivateKey.String() != desiredConf.PrivateKey.String() {
				r.shouldUpdatePrivateKey = true
			}

			// todo: reconcile peers

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
				wgLink := new(netlink.Wireguard)
				err := handle.LinkAdd(wgLink)
				if err != nil {
					return fmt.Errorf("failed to add link %s: %s", r.interfaceName, err.Error())
				}

				return nil
			})

			if err != nil {
				return err
			}

			err = pkgutils.WithNetnsWGCli(nil, func(wgCtrlCli *wgctrl.Client) error {
				wgConf := new(wgtypes.Config)
				wgConf.ListenPort = &desiredConf.ListenPort
				wgConf.PrivateKey = &desiredConf.PrivateKey
				err := wgCtrlCli.ConfigureDevice(r.interfaceName, *wgConf)
				if err != nil {
					return fmt.Errorf("failed to configure device %s: %s", r.interfaceName, err.Error())
				}

				return nil
			})
			if err != nil {
				return err
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
		} else {
			return pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
				wgLink := new(netlink.Wireguard)
				err := handle.LinkSetName(wgLink, r.interfaceName)
				if err != nil {
					return fmt.Errorf("failed to set name of link %s: %s", r.interfaceName, err.Error())
				}

				return nil
			})
		}
	}

	// If it reaches here, the interface must exists
	return pkgutils.WithNetlinkHandle(r.pid, func(handle *netlink.Handle) error {
		link, _ := handle.LinkByName(r.interfaceName)

		_, err := reconcileAdminState(ctx, handle, link, true, false)
		if err != nil {
			return fmt.Errorf("failed to reconcile admin state of link %s: %s", r.interfaceName, err.Error())
		}

		_, err = reconcileMTU(handle, link, desiredConf.MTU, false)
		if err != nil {
			return fmt.Errorf("failed to reconcile mtu of link %s: %s", r.interfaceName, err.Error())
		}

		_, _, err = reconcileAddrs(handle, link, desiredConf.IPAddrs, false)
		if err != nil {
			return fmt.Errorf("failed to reconcile addresses of link %s: %s", r.interfaceName, err.Error())
		}

		// todo: reconcile more

		return nil

	})

}

func (r *WGReconciler) ResetState() {
	r.shouldCreateInterface = false
	r.shouldUpdateAddr = nil
	r.shouldUpdateMTU = false
	r.shouldUpdateListenPort = false
	r.shouldUpdatePrivateKey = false
	r.shouldUpdateAdminState = false
}

func (r *WGReconciler) gatherAllUpdates() bool {
	return r.shouldCreateInterface ||
		r.shouldUpdateAddr != nil ||
		r.shouldUpdateMTU ||
		r.shouldUpdateListenPort ||
		r.shouldUpdatePrivateKey ||
		r.shouldUpdateAdminState
}
