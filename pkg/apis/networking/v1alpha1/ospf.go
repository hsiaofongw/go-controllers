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
// +kubebuilder:resource:scope=Cluster,shortName=ospf
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OSPFProtocol is a specification for a OSPFProtocol resource
type OSPFProtocol struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec OSPFProtocolSpec `json:"spec"`

	// +optional
	Status OSPFProtocolStatus `json:"status"`
}

type OSPFProtocolDriverType string

const (
	OSPFProtocolTypeFRR OSPFProtocolDriverType = "frr"
)

type OSPFProtocolVersion string

const (
	OSPFProtocolVersion2 OSPFProtocolVersion = "v2"
	OSPFProtocolVersion3 OSPFProtocolVersion = "v3"
)

type OSPFNetworkType string

const (
	OSPFNetworkTypeBroadcast    OSPFNetworkType = "broadcast"
	OSPFNetworkTypeNBMA         OSPFNetworkType = "non-broadcast"
	OSPFNetworkTypePointToPoint OSPFNetworkType = "point-to-point"
	OSPFNetworkTypeNonBroadcast OSPFNetworkType = "point-to-multipoint"
)

type OSPFProtocolInterfaceSpec struct {
	// The name of the interface where the OSPF configuration is applied to.
	InterfaceName string `json:"interfaceName"`

	// Area is the area ID of the OSPF protocol.
	// It can be a string of 4-octet integer or a string of 4 dot-decimal integers,
	// like "0" or "0.0.0.0" or "1.2.3.4".
	Area string `json:"area"`

	// OSPF network type, it affects the way of doing neighbor discovery.
	NetworkType OSPFNetworkType `json:"networkType"`

	// Do not speak OSPF on the interface,
	// but do advertise the interface as a stub link in the router-LSA for this router.
	// So that the subnet where the interface is connected to will appear as a stub network in the lsdb.
	Passive *bool `json:"passive,omitempty"`
}

// OSPFProtocolSpec is the spec for a OSPFProtocol resource
type OSPFProtocolSpec struct {
	Node string `json:"node"`

	// Currently only frr is supported.
	Driver OSPFProtocolDriverType `json:"driver"`

	// Currently only v2 is supported.
	Version OSPFProtocolVersion `json:"version"`

	// VRF, VRF only supported in FRR
	VRF *string `json:"vrf,omitempty"`

	// RouterID used to identify the router and indicate the router that generate the LSA,
	// It is not necessary to be reachable, as long as it is unique across the network.
	// It can be a string of 4-octet integer or a string of 4 dot-decimal integers,
	// like "0" or "0.0.0.0" or "1.2.3.4".
	RouterID string `json:"routerID"`

	// These are interface-specific OSPF configurations.
	// Note: once applied, modify the content of a `OSPFProtocolInterfaceSpec` will not take effect,
	// the only way to alter the configuration is to delete the old `OSPFProtocolInterfaceSpec` and create a new one.
	Interfaces []OSPFProtocolInterfaceSpec `json:"interfaces"`

	// For Non-broadcast Multi-access (NBMA) networks or point-to-multipoint networks,
	// where the neighbor discovery can't be done by multicast flooding and one
	// have to manually specify the neighbors. Format: A.B.C.D.
	Neighbors []string `json:"neighbors,omitempty"`
}

type OSPFProtocolAreaStatus struct {
	Area     string `json:"area"`
	Backbone *bool  `json:"backbone,omitempty"`
}

type OSPFProtocolStatus struct {
	//  Hostname of the node where the interface is provisioned,
	// or the hostname of the host of the container in case of containerization.
	// This is used to identify the node where the interface is provisioned.
	Hostname string `json:"hostname"`

	// Node name where the interface is provisioned.
	// The node name can be overridden by the operator running on the node.
	Nodename string `json:"nodename"`

	// The most recent generation observed by the controller.
	ObservedGeneration int64 `json:"observedGeneration"`

	RouterId *string `json:"routerId,omitempty"`

	Areas []OSPFProtocolAreaStatus `json:"areas,omitempty"`

	VRF *string `json:"vrf,omitempty"`

	Driver *OSPFProtocolDriverType `json:"driver"`

	Version *OSPFProtocolVersion `json:"version"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OSPFProtocolList is a list of OSPFProtocol resources
type OSPFProtocolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []OSPFProtocol `json:"items"`
}
