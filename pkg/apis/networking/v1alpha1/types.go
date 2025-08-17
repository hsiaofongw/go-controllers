/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"encoding/base64"
	"fmt"
	"net"
	"time"

	"github.com/vishvananda/netlink"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:resource:scope=Cluster,shortName=wg;wgi;wgif
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardInterface is a specification for a WireGuardInterface resource
type WireGuardInterface struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WireGuardInterfaceSpec   `json:"spec"`
	Status WireGuardInterfaceStatus `json:"status"`
}

type InetFamily string

const (
	InetFamilyInet  InetFamily = "inet"
	InetFamilyInet6 InetFamily = "inet6"
)

type PrivateKeySecretRef struct {
	Name      string  `json:"name"`
	Key       string  `json:"key"`
	Namespace *string `json:"namespace,omitempty"`
}

type WireGuardPeerSpec struct {
	PublicKey             string               `json:"publicKey"`
	PresharedKeySecretRef *PrivateKeySecretRef `json:"presharedKeySecretRef,omitempty"`
	AllowedIPs            []string             `json:"allowedIPs,omitempty"`
	Endpoint              *string              `json:"endpoint,omitempty"`
	PersistentKeepalive   *int                 `json:"persistentKeepalive,omitempty"`
}

type WireGuardInterfaceContainerNetNSSpec struct {
	PID *int `json:"pid,omitempty"`
}

type WireGuardInterfaceContainerDockerSpec struct {
	Name string `json:"name"`
}

type WireGuardInterfaceContainerSpec struct {
	NetNS  *WireGuardInterfaceContainerNetNSSpec  `json:"netns,omitempty"`
	Docker *WireGuardInterfaceContainerDockerSpec `json:"docker,omitempty"`
}

// WireGuardInterfaceSpec is the spec for a WireGuardInterface resource
type WireGuardInterfaceSpec struct {
	Node                string                           `json:"node"`
	MoveToContainer     bool                             `json:"moveToContainer"`
	Container           *WireGuardInterfaceContainerSpec `json:"container,omitempty"`
	InterfaceName       string                           `json:"interfaceName"`
	PrivateKeySecretRef *PrivateKeySecretRef             `json:"privateKeySecretRef,omitempty"`
	Addresses           []WireGuardInterfaceAddressSpec  `json:"addresses"`
	ListenPort          int                              `json:"listenPort"`
	MTU                 *int                             `json:"mtu,omitempty"`
	Peers               []WireGuardPeerSpec              `json:"peers"`
}

type WireGuardInterfaceAddressSpec struct {
	Family        InetFamily `json:"family"`
	Local         string     `json:"local"`
	Peer          string     `json:"peer"`
	Prefixlen     int        `json:"prefixlen"`
	NoPrefixRoute bool       `json:"noPrefixRoute"`
}

type NetlinkInterfaceAddressStatus struct {
	Family    InetFamily `json:"family"`
	Local     string     `json:"local"`
	Address   *string    `json:"address,omitempty"`
	Prefixlen int        `json:"prefixlen"`
}

type PeerStatus struct {
	PublicKey       string  `json:"publicKey"`
	LatestHandshake *int64  `json:"latestHandshake,omitempty"`
	Endpoint        *string `json:"endpoint,omitempty"`
}

type NetlinkInterfaceAddressStatusWrapper struct {
	Label       string     `json:"label,omitempty"`
	Flags       int        `json:"flags,omitempty"`
	Scope       int        `json:"scope,omitempty"`
	Local       string     `json:"local,omitempty"`
	Peer        *string    `json:"peer,omitempty"`
	Broadcast   string     `json:"broadcast,omitempty"`
	PreferedLft int        `json:"preferedLft,omitempty"`
	ValidLft    int        `json:"validLft,omitempty"`
	Family      InetFamily `json:"family,omitempty"`
	LinkIndex   int        `json:"linkIndex,omitempty"`
	Prefixlen   int        `json:"prefixlen,omitempty"`
}

func NewFromNetlinkAddr(addr *netlink.Addr) NetlinkInterfaceAddressStatusWrapper {
	addrWrapper := NetlinkInterfaceAddressStatusWrapper{}
	if addr.IP.To4() != nil {
		addrWrapper.Family = InetFamilyInet
	} else {
		addrWrapper.Family = InetFamilyInet6
	}
	addrWrapper.Local = addr.IP.String()
	if addr.Peer != nil {
		peer := addr.Peer.String()
		addrWrapper.Peer = &peer
	}

	broadcast := addr.Broadcast.String()
	addrWrapper.Broadcast = broadcast

	addrWrapper.Label = addr.Label
	addrWrapper.Flags = addr.Flags
	addrWrapper.Scope = addr.Scope
	addrWrapper.PreferedLft = addr.PreferedLft
	addrWrapper.ValidLft = addr.ValidLft
	addrWrapper.LinkIndex = addr.LinkIndex
	mask := addr.IPNet.Mask
	ones, _ := mask.Size()
	addrWrapper.Prefixlen = ones

	return addrWrapper
}

type NetlinkStatisticsWrapper struct {
	netlink.LinkStatistics `json:"-"`

	RxPackets uint64 `json:"rxPackets,omitempty"`
	TxPackets uint64 `json:"txPackets,omitempty"`
	RxBytes   uint64 `json:"rxBytes,omitempty"`
	TxBytes   uint64 `json:"txBytes,omitempty"`
	RxErrors  uint64 `json:"rxErrors,omitempty"`
	TxErrors  uint64 `json:"txErrors,omitempty"`
	RxDropped uint64 `json:"rxDropped,omitempty"`
}

func NewFromNetlinkLinkStatistics(stats *netlink.LinkStatistics) *NetlinkStatisticsWrapper {
	if stats == nil {
		return nil
	}

	return &NetlinkStatisticsWrapper{
		LinkStatistics: *stats,
		RxPackets:      stats.RxPackets,
		TxPackets:      stats.TxPackets,
		RxBytes:        stats.RxBytes,
		TxBytes:        stats.TxBytes,
		RxErrors:       stats.RxErrors,
		TxErrors:       stats.TxErrors,
		RxDropped:      stats.RxDropped,
	}
}

type NetlinkStatusWrapper struct {
	Index        int                                    `json:"index"`
	MTU          int                                    `json:"mtu"`
	Name         string                                 `json:"name"`
	HardwareAddr string                                 `json:"hardwareAddr,omitempty"`
	Flags        net.Flags                              `json:"flags,omitempty"`
	RawFlags     uint32                                 `json:"rawFlags,omitempty"`
	ParentIndex  int                                    `json:"parentIndex,omitempty"`
	MasterIndex  int                                    `json:"masterIndex,omitempty"`
	Alias        string                                 `json:"alias,omitempty"`
	AltNames     []string                               `json:"altNames,omitempty"`
	Statistics   *NetlinkStatisticsWrapper              `json:"statistics,omitempty"`
	Addrs        []NetlinkInterfaceAddressStatusWrapper `json:"addrs,omitempty"`
}

func NewFromNetlinkLinkAttrs(attrs *netlink.LinkAttrs, addrs []netlink.Addr) *NetlinkStatusWrapper {
	nlStatusWrapper := &NetlinkStatusWrapper{
		Name:         attrs.Name,
		Index:        attrs.Index,
		MTU:          attrs.MTU,
		HardwareAddr: attrs.HardwareAddr.String(),
		Flags:        attrs.Flags,
		RawFlags:     attrs.RawFlags,
		ParentIndex:  attrs.ParentIndex,
		MasterIndex:  attrs.MasterIndex,
		Alias:        attrs.Alias,
		AltNames:     attrs.AltNames,
		Statistics:   NewFromNetlinkLinkStatistics(attrs.Statistics),
	}

	addrWrappers := make([]NetlinkInterfaceAddressStatusWrapper, 0)
	for _, addr := range addrs {
		addrWrappers = append(addrWrappers, NewFromNetlinkAddr(&addr))
	}
	nlStatusWrapper.Addrs = addrWrappers

	return nlStatusWrapper
}

type WireGuardInterfaceStatus struct {
	//  Hostname of the node where the interface is provisioned,
	// or the hostname of the host of the container in case of containerization.
	// This is used to identify the node where the interface is provisioned.
	Hostname string `json:"hostname"`
	// Node name where the interface is provisioned.
	// The node name can be overridden by the operator running on the node.
	Nodename  string                  `json:"nodename"`
	WireGuard *WireGuardStatusWrapper `json:"wireguard,omitempty"`
	MTU       *int                    `json:"mtu,omitempty"`
	Netlink   *NetlinkStatusWrapper   `json:"netlink,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardInterfaceList is a list of WireGuardInterface resources
type WireGuardInterfaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []WireGuardInterface `json:"items"`
}

func (peerSpec *WireGuardPeerSpec) ToZX2c4WGPeerConf(presharedKey *string) (*wgtypes.PeerConfig, error) {
	wgPeerConf := new(wgtypes.PeerConfig)
	if peerSpec.PublicKey == "" {
		return nil, fmt.Errorf("public key is required")
	}

	pubkeyObj, err := wgtypes.ParseKey(peerSpec.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("invalid peer public key: %s", err.Error())
	}

	wgPeerConf.PublicKey = pubkeyObj

	if presharedKey != nil && *presharedKey != "" {
		pskObj, err := wgtypes.ParseKey(*presharedKey)
		if err != nil {
			return nil, fmt.Errorf("preshared provided but invalid: %s (note it is optional)", err.Error())
		}
		wgPeerConf.PresharedKey = &pskObj
	}

	if peerSpec.PersistentKeepalive != nil {
		intv := time.Duration(*peerSpec.PersistentKeepalive) * time.Second
		wgPeerConf.PersistentKeepaliveInterval = &intv
	}

	if peerSpec.Endpoint != nil && *peerSpec.Endpoint != "" {
		peerUDPAddr, err := net.ResolveUDPAddr("udp", *peerSpec.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve peer endpoint %s: %s", *peerSpec.Endpoint, err.Error())
		}
		wgPeerConf.Endpoint = peerUDPAddr
	}

	if len(peerSpec.AllowedIPs) > 0 {
		for _, iprange := range peerSpec.AllowedIPs {
			_, ipNet, err := net.ParseCIDR(iprange)
			if err != nil {
				return nil, fmt.Errorf("invalid allowed ip cidr: %s: %s", iprange, err.Error())
			}
			wgPeerConf.AllowedIPs = append(wgPeerConf.AllowedIPs, *ipNet)
		}
	}

	return wgPeerConf, nil
}

func (wgi *WireGuardInterfaceAddressSpec) MakeNetlinkAddrObject() (*netlink.Addr, error) {
	family := wgi.Family
	local := wgi.Local
	peer := wgi.Peer
	prefixlen := wgi.Prefixlen
	if prefixlen == 0 {
		return nil, fmt.Errorf("invalid prefix length: %d", prefixlen)
	}

	bits := 32
	if family == InetFamilyInet6 {
		bits = 128
	}

	addrObj := new(netlink.Addr)
	addrObj.IPNet = new(net.IPNet)
	addrObj.IP = net.ParseIP(local)
	addrObj.Peer = new(net.IPNet)
	addrObj.Peer.IP = net.ParseIP(peer)
	addrObj.Peer.Mask = net.CIDRMask(prefixlen, bits)

	return addrObj, nil
}

func (wgi *WireGuardInterfaceSpec) ToZX2c4WGConf(privateKey *string) (*wgtypes.Config, error) {
	if privateKey == nil || *privateKey == "" {
		return nil, fmt.Errorf("private key is required")
	}

	wgConf := new(wgtypes.Config)
	if wgi.ListenPort != 0 {
		wgConf.ListenPort = &wgi.ListenPort
	}

	wgConf.PrivateKey = nil
	privKeyStr := base64.StdEncoding.EncodeToString([]byte(*privateKey))
	privKeyObj, err := wgtypes.ParseKey(privKeyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain the private key, either not provided or invalid: %s", err.Error())
	}

	wgConf.PrivateKey = &privKeyObj

	return wgConf, nil
}
