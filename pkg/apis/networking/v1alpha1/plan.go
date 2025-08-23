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
// +kubebuilder:resource:scope=Cluster,shortName=wgplan;wgp
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardNetworkPlan is a specification for a WireGuardNetworkPlan resource
type WireGuardNetworkPlan struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec WireGuardNetworkPlanSpec `json:"spec"`

	// +optional
	Status WireGuardNetworkPlanStatus `json:"status"`
}

type WireGuardNetworkPlanPortRangeSpec struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type WireGuardNetworkPlanUnderlaySpec struct {
	// Hostname is the clearnet (or underlay) reachable DNS name or IP address of the node.
	Hostname string `json:"hostname"`

	// Port range is where to allocate for the WireGuard ListenPort. Inclusive.
	PortRange *WireGuardNetworkPlanPortRangeSpec `json:"portRange,omitempty"`
}

type WireGuardNetworkPlanNodeSpec struct {
	// NodeName is the advertised name of the node controller.
	NodeName string `json:"nodeName"`

	// If this node is behind a NAT, the `Underlay` field can be safely omitted.
	Underlay *WireGuardNetworkPlanUnderlaySpec `json:"underlay,omitempty"`
}

type WireGuardNetworkPlanDBSpec struct {
	Nodes []WireGuardNetworkPlanNodeSpec `json:"nodes,omitempty"`

	// DefaultPortRange is the default port range to allocate for the WireGuard ListenPort.
	// If a node has its own `Underlay` field, or the node is behind a NAT, the `DefaultPortRange` is not used.
	DefaultPortRange WireGuardNetworkPlanPortRangeSpec `json:"defaultPortRange"`
}

type WireGuardNetworkPlanLinkSpec struct {
	FromNode string `json:"fromNode"`
	ToNode   string `json:"toNode"`
}

type WireGuardNetworkPlanAdjacencySpec struct {
	Links []WireGuardNetworkPlanLinkSpec `json:"links,omitempty"`
}

// WireGuardNetworkPlanSpec is the spec for a WireGuardNetworkPlan resource
type WireGuardNetworkPlanSpec struct {
	DB        *WireGuardNetworkPlanDBSpec        `json:"db,omitempty"`
	Adjacency *WireGuardNetworkPlanAdjacencySpec `json:"adjacency,omitempty"`
}

type WireGuardNetworkPlanInterfaceStatus struct {
	NodeName      string  `json:"nodeName"`
	HostName      *string `json:"hostName,omitempty"`
	Endpoint      *string `json:"endpoint,omitempty"`
	LastHandshake *int64  `json:"lastHandshake,omitempty"`
	PublicKey     *string `json:"publicKey,omitempty"`
	PrivateKey    *string `json:"privateKey,omitempty"`
	ListenPort    *int    `json:"listenPort,omitempty"`
}

// Note: after the spec is formalized, this controller will first generate the underlying
// WireGuardInterface resources, and collect the statuses from the underlying WireGuardInterface resources.
type WireGuardNetworkPlanStatus struct {
	Interfaces []WireGuardNetworkPlanInterfaceStatus `json:"interfaces,omitempty"`

	// The most recent generation observed by the controller.
	ObservedGeneration int64 `json:"observedGeneration"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardNetworkPlanList is a list of WireGuardNetworkPlan resources
type WireGuardNetworkPlanList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []WireGuardNetworkPlan `json:"items"`
}
