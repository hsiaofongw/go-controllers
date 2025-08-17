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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:resource:scope=Cluster,shortName=nl;nli;nlif
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetlinkInterface is a specification for a NetlinkInterface resource
type NetlinkInterface struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NetlinkInterfaceSpec   `json:"spec"`
	Status NetlinkInterfaceStatus `json:"status"`
}

type NetlinkInterfaceContainerNetNSSpec struct {
	PID *int `json:"pid,omitempty"`
}

type NetlinkInterfaceContainerDockerSpec struct {
	Name string `json:"name"`
}

type NetlinkInterfaceContainerSpec struct {
	NetNS  *NetlinkInterfaceContainerNetNSSpec  `json:"netns,omitempty"`
	Docker *NetlinkInterfaceContainerDockerSpec `json:"docker,omitempty"`
}

type NetlinkInterfaceType string

const (
	NetlinkInterfaceTypeBridge NetlinkInterfaceType = "bridge"
	NetlinkInterfaceTypeVeth   NetlinkInterfaceType = "veth"
	NetlinkInterfaceTypeVxlan  NetlinkInterfaceType = "vxlan"
	NetlinkInterfaceTypeDummy  NetlinkInterfaceType = "dummy"
)

type NetlinkInterfaceBridgeSpec struct {
	InterfaceName string                         `json:"interfaceName"`
	Container     *NetlinkInterfaceContainerSpec `json:"container,omitempty"`
	MTU           *int                           `json:"mtu,omitempty"`
	Addresses     []NetlinkInterfaceAddressSpec  `json:"addresses,omitempty"`
}

type NetlinkInterfaceVxlanSpec struct {
	InterfaceName string                         `json:"interfaceName"`
	Container     *NetlinkInterfaceContainerSpec `json:"container,omitempty"`
	MTU           *int                           `json:"mtu,omitempty"`
	Addresses     []NetlinkInterfaceAddressSpec  `json:"addresses,omitempty"`
}

type NetlinkInterfaceDummySpec struct {
	InterfaceName string                         `json:"interfaceName"`
	Container     *NetlinkInterfaceContainerSpec `json:"container,omitempty"`
	MTU           *int                           `json:"mtu,omitempty"`
	Addresses     []NetlinkInterfaceAddressSpec  `json:"addresses,omitempty"`
}

type NetlinkInterfaceVethPeerSpec struct {
	InterfaceName string                         `json:"interfaceName"`
	Container     *NetlinkInterfaceContainerSpec `json:"container,omitempty"`
	Addresses     []NetlinkInterfaceAddressSpec  `json:"addresses,omitempty"`
	MTU           *int                           `json:"mtu,omitempty"`

	// Master is the name of the bridge interface to which the veth pair is connected.
	Master *string `json:"master,omitempty"`
}

type NetlinkInterfaceVethSpec struct {
	Local *NetlinkInterfaceVethPeerSpec `json:"localPeer,omitempty"`
	Peer  *NetlinkInterfaceVethPeerSpec `json:"peerPeer,omitempty"`
}

// NetlinkInterfaceSpec is the spec for a NetlinkInterface resource
type NetlinkInterfaceSpec struct {
	Node          string                      `json:"node"`
	InterfaceName string                      `json:"interfaceName"`
	Type          NetlinkInterfaceType        `json:"type"`
	Bridge        *NetlinkInterfaceBridgeSpec `json:"bridge,omitempty"`
	Vxlan         *NetlinkInterfaceVxlanSpec  `json:"vxlan,omitempty"`
	Dummy         *NetlinkInterfaceDummySpec  `json:"dummy,omitempty"`
	Veth          *NetlinkInterfaceVethSpec   `json:"veth,omitempty"`
}

type NetlinkInterfaceAddressSpec struct {
	Family        InetFamily `json:"family"`
	Local         string     `json:"local"`
	Peer          string     `json:"peer"`
	Prefixlen     int        `json:"prefixlen"`
	NoPrefixRoute bool       `json:"noPrefixRoute"`
}

type NetlinkInterfaceStatus struct {
	//  Hostname of the node where the interface is provisioned,
	// or the hostname of the host of the container in case of containerization.
	// This is used to identify the node where the interface is provisioned.
	Hostname string `json:"hostname"`
	// Node name where the interface is provisioned.
	// The node name can be overridden by the operator running on the node.
	Nodename string                `json:"nodename"`
	MTU      *int                  `json:"mtu,omitempty"`
	Netlink  *NetlinkStatusWrapper `json:"netlink,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetlinkInterfaceList is a list of NetlinkInterface resources
type NetlinkInterfaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []NetlinkInterface `json:"items"`
}
