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
	pkgnetapplybird "github.com/internetworklab/netapply/pkg/bird"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=birdbgp
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BirdBGPProtocol is a specification for a BirdBGPProtocol resource
type BirdBGPProtocol struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec BirdBGPProtocolSpec `json:"spec"`

	// +optional
	Status BirdBGPProtocolStatus `json:"status"`
}

// BirdBGPProtocolSpec is the spec for a BirdBGPProtocol resource
type BirdBGPProtocolSpec struct {
	Node         string  `json:"node"`
	Name         string  `yaml:"name" json:"name" bson:"name"`
	Template     *string `yaml:"template,omitempty" json:"template,omitempty" bson:"template,omitempty"`
	Interface    *string `yaml:"interface,omitempty" json:"interface,omitempty" bson:"interface,omitempty"`
	LocalAddress *string `yaml:"local_address,omitempty" json:"local_address,omitempty" bson:"local_address,omitempty"`
	PeerAddress  *string `yaml:"peer_address,omitempty" json:"peer_address,omitempty" bson:"peer_address,omitempty"`
	LocalASN     *string `yaml:"local_asn,omitempty" json:"local_asn,omitempty" bson:"local_asn,omitempty"`
	PeerASN      *string `yaml:"peer_asn,omitempty" json:"peer_asn,omitempty" bson:"peer_asn,omitempty"`
	PeerExternal *bool   `yaml:"peer_external,omitempty" json:"peer_external,omitempty" bson:"peer_external,omitempty"`
	PeerInternal *bool   `yaml:"peer_internal,omitempty" json:"peer_internal,omitempty" bson:"peer_internal,omitempty"`
}

type BirdBGPProtocolStatus struct {

	// Nodename is name of the node where the interface is provisioned.
	// The node name can be overridden by the operator running on the node.
	Nodename string `json:"nodename"`

	// Status of underlying resource in the node's system.
	// +k8s:deepcopy-gen=true
	Resource *pkgnetapplybird.BirdBGPProtocolStatus `json:"resource,omitempty"`

	// The UNIX timestamp when the status was generated, in unit of seconds.
	// Use this field to determine the interval between two consecutive status generation,
	// when it gets too quick, the controller might slow down the updating of the status.
	GeneratedAt int64 `json:"generatedAt"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BirdBGPProtocolList is a list of BirdBGPProtocol resources
type BirdBGPProtocolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []BirdBGPProtocol `json:"items"`
}
