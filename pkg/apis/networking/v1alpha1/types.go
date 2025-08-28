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
	"net"

	"github.com/vishvananda/netlink"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=wgi;wgif
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardInterface is a specification for a WireGuardInterface resource
type WireGuardInterface struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec WireGuardInterfaceSpec `json:"spec"`

	// +optional
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
	Node            string                           `json:"node"`
	MoveToContainer bool                             `json:"moveToContainer"`
	Container       *WireGuardInterfaceContainerSpec `json:"container,omitempty"`
	InterfaceName   string                           `json:"interfaceName"`
	// If PrivateKey is provided, the PrivateKeySecretRef will not be used.
	PrivateKey          string                          `json:"privateKey,omitempty"`
	PrivateKeySecretRef *PrivateKeySecretRef            `json:"privateKeySecretRef,omitempty"`
	Addresses           []WireGuardInterfaceAddressSpec `json:"addresses"`
	ListenPort          *int                            `json:"listenPort,omitempty"`
	MTU                 *int                            `json:"mtu,omitempty"`
	Peers               []WireGuardPeerSpec             `json:"peers"`
}

type WireGuardInterfaceAddressSpec struct {
	Family    InetFamily `json:"family"`
	Local     string     `json:"local"`
	Peer      string     `json:"peer"`
	Prefixlen int        `json:"prefixlen"`
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

	if addr.Peer != nil {
		peer := addr.Peer.String()
		addrWrapper.Peer = &peer
		ones, _ := addr.Peer.Mask.Size()
		addrWrapper.Prefixlen = ones
	}

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
		// Statistics:   NewFromNetlinkLinkStatistics(attrs.Statistics),
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
	// The Hostname is not necessarily publicly reachable, since it is most likely collected from the system's hostname.
	Hostname string `json:"hostname"`

	// Node name where the interface is provisioned.
	// The node name can be overridden by the operator running on the node.
	Nodename string `json:"nodename"`

	WireGuard *WireGuardStatusWrapper `json:"wireguard,omitempty"`
	MTU       *int                    `json:"mtu,omitempty"`
	Netlink   *NetlinkStatusWrapper   `json:"netlink,omitempty"`

	// The most recent generation observed by the controller.
	ObservedGeneration int64 `json:"observedGeneration"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardInterfaceList is a list of WireGuardInterface resources
type WireGuardInterfaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []WireGuardInterface `json:"items"`
}
