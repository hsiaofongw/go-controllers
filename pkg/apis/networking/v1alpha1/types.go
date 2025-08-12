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
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardInterface is a specification for a WireGuardInterface resource
type WireGuardInterface struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WireGuardInterfaceSpec `json:"spec"`
	Status FooStatus              `json:"status"`
}

type PrivateKeySecretRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type WireGuardPeerSpec struct {
	PublicKey             string              `json:"publicKey"`
	PresharedKeySecretRef PrivateKeySecretRef `json:"presharedKeySecretRef"`
	AllowedIPs            []string            `json:"allowedIPs"`
	Endpoint              string              `json:"endpoint"`
	PersistentKeepalive   *int                `json:"persistentKeepalive,omitempty"`
}

type WireGuardInterfaceContainerNetNSSpec struct {
	Path string `json:"path"`
	PID  int    `json:"pid"`
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
	PrivateKeySecretRef PrivateKeySecretRef              `json:"privateKeySecretRef"`
	Address             []string                         `json:"address"`
	ListenPort          int                              `json:"listenPort"`
	MTU                 int                              `json:"mtu"`
	Peers               []WireGuardPeerSpec              `json:"peers"`
}

type PeerStatus struct {
	PublicKey       string `json:"publicKey"`
	LatestHandshake string `json:"latestHandshake"`
	TransferRx      int64  `json:"transferRx"`
	TransferTx      int64  `json:"transferTx"`
}

// FooStatus is the status for a WireGuardInterface resource
type FooStatus struct {
	PublicKey      string       `json:"publicKey"`
	InterfaceState string       `json:"interfaceState"`
	LastSyncTime   string       `json:"lastSyncTime"`
	Peers          []PeerStatus `json:"peers"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardInterfaceList is a list of WireGuardInterface resources
type WireGuardInterfaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []WireGuardInterface `json:"items"`
}
