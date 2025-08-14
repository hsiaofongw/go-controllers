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
	"fmt"
	"net"
	"time"

	"github.com/vishvananda/netlink"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
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

type WireGuardInterfaceStatus struct {
	//  Hostname of the node where the interface is provisioned,
	// or the hostname of the host of the container in case of containerization.
	// This is used to identify the node where the interface is provisioned.
	Hostname string `json:"hostname"`

	// Node name where the interface is provisioned.
	// The node name can be overridden by the operator running on the node.
	Nodename   string                          `json:"nodename"`
	PublicKey  string                          `json:"publicKey"`
	ListenPort *int                            `json:"listenPort,omitempty"`
	Peers      []PeerStatus                    `json:"peers"`
	Addresses  []NetlinkInterfaceAddressStatus `json:"addresses"`
	MTU        *int                            `json:"mtu,omitempty"`
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
	wgConf := new(wgtypes.Config)
	if wgi.ListenPort != 0 {
		wgConf.ListenPort = &wgi.ListenPort
	}

	wgConf.PrivateKey = nil
	if privateKey != nil && *privateKey != "" {
		if privKeyObj, err := wgtypes.ParseKey(*privateKey); err == nil {
			wgConf.PrivateKey = &privKeyObj
		}
	}

	if wgConf.PrivateKey == nil {
		return nil, fmt.Errorf("Failed to obtain the private key, either not provided or invalid")
	}

	return wgConf, nil
}
