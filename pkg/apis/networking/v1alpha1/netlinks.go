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

// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=nl;nli;nlif
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetlinkInterface is a specification for a NetlinkInterface resource
type NetlinkInterface struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec NetlinkInterfaceSpec `json:"spec"`

	// +optional
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
	NetlinkInterfaceTypeVXLAN  NetlinkInterfaceType = "vxlan"
	NetlinkInterfaceTypeDummy  NetlinkInterfaceType = "dummy"
)

type NetlinkInterfaceBridgeSpec struct {
	// Names of the interfaces that are enslaved to this bridge.
	Slaves []string `json:"slaves"`
}

type NetlinkInterfaceVxlanSpec struct {
	VNI int64 `json:"vni"`

	// Name of the dataplane interface
	// It is useful when you want the vxlan take some vrf-enslaved interface as the dataplane,
	// or you want it automatically deduce the correct MTU.
	Dev *string `json:"dev,omitempty"`

	// The src IP of the outer encapsulated ip packet.
	Local *string `json:"local,omitempty"`

	// Specifies the multicast IP address to join. This parameter cannot be specified with the remote parameter.
	Group *string `json:"group,omitempty"`

	// NoLearning is usefull when you want to take over the controlplane of vxlan, such as
	// you setup your own BGPEVPN to distribute the L2 reachability information.
	// +optional
	NoLearning bool `json:"noLearning"`

	// The port of the vxlan interface.
	Port *int `json:"port,omitempty"`

	// Specifies the TTL value to use in outgoing packets.
	TTL *int `json:"ttl,omitempty"`

	// Specifies the TOS value to use in outgoing packets.
	TOS *int `json:"tos,omitempty"`

	ProxyARP *bool `json:"proxyARP,omitempty"`

	NoAge *bool `json:"noAge,omitempty"`

	// Specifies the range of port numbers to use as UDP source ports to communicate to the remote VXLAN tunnel endpoint.
	// Must be a pair of integers [portLow, portHigh]
	PortRange []int `json:"portRange,omitempty"`
}

type NetlinkInterfaceDummySpec struct {
}

type NetlinkInterfaceVethPeerSpec struct {
	// When present, this field will take precedence over the interface name specified in the NetlinkInterfaceSpec.
	InterfaceName *string `json:"interfaceName,omitempty"`

	// When present, these addresses will take precedence over the addresses specified in the NetlinkInterfaceSpec.
	Addresses []NetlinkInterfaceAddressSpec `json:"addresses,omitempty"`

	// When present, this field will take precedence over the container specified in the NetlinkInterfaceSpec.
	Container *NetlinkInterfaceContainerSpec `json:"container,omitempty"`
}

type NetlinkInterfaceVethSpec struct {
	Local *NetlinkInterfaceVethPeerSpec `json:"localPeer,omitempty"`
	Peer  *NetlinkInterfaceVethPeerSpec `json:"peerPeer,omitempty"`
}

// NetlinkInterfaceSpec is the spec for a NetlinkInterface resource
type NetlinkInterfaceSpec struct {
	Node          string               `json:"node"`
	InterfaceName string               `json:"interfaceName"`
	Type          NetlinkInterfaceType `json:"type"`

	Bridge *NetlinkInterfaceBridgeSpec `json:"bridge,omitempty"`

	// Note: all vxlan-specific specs are not-modifiable once the interface is created.
	// To change the value of this field, you must delete the resource and create a new one.
	Vxlan *NetlinkInterfaceVxlanSpec `json:"vxlan,omitempty"`

	Dummy *NetlinkInterfaceDummySpec `json:"dummy,omitempty"`
	Veth  *NetlinkInterfaceVethSpec  `json:"veth,omitempty"`

	Addresses []NetlinkInterfaceAddressSpec `json:"addresses,omitempty"`
	MTU       *int                          `json:"mtu,omitempty"`

	// Up is the administrative state of the interface.
	// This will give the user the flexibility to turn it up or down on-demand.
	Up bool `json:"up"`

	// It specific where to place the interface, if it's nil, the interface will be placed in the host netns,
	// otherwise, the interface will be placed in the container's netns.
	// Change of this field won't take effect (this field is not intended to be modified)
	// The only way to specify a different value is to delete the old resource and create a new one.
	Container *NetlinkInterfaceContainerSpec `json:"container,omitempty"`
}

type NetlinkInterfaceAddressSpec struct {
	// If PeerCIDR is not nil, the '/prefixlen' suffix shall be omitted.
	IPCIDR   string  `json:"ipcidr"`
	PeerCIDR *string `json:"peerCidr,omitempty"`
}

type NetlinkInterfaceBridgeStatus struct {
	EnslavedLinks []string `json:"enslavedLinks"`
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

	// The most recent generation observed by the controller.
	ObservedGeneration int64 `json:"observedGeneration"`

	// OperState, see https://docs.kernel.org/networking/operstates.html
	// This represents the operational state of the interface.
	// Operational state is all about how it currently looks like.
	// +optional
	OperState string `json:"operState,omitempty"`

	// Flags, these represent the administrative state of the interface.
	// Administrative state is all about what you want it to be.
	Flags []string `json:"flags,omitempty"`

	Addresses []string `json:"addresses,omitempty"`

	Bridge *NetlinkInterfaceBridgeStatus `json:"bridge,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetlinkInterfaceList is a list of NetlinkInterface resources
type NetlinkInterfaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []NetlinkInterface `json:"items"`
}
