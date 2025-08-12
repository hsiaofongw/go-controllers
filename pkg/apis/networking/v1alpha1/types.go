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

	Spec   WireGuardInterfaceSpec   `json:"spec"`
	Status WireGuardInterfaceStatus `json:"status"`
}

type PrivateKeySecretRef struct {
	Name      string  `json:"name"`
	Key       string  `json:"key"`
	Namespace *string `json:"namespace,omitempty"`
}

type WireGuardPeerSpec struct {
	PublicKey             string               `json:"publicKey"`
	PresharedKeySecretRef *PrivateKeySecretRef `json:"presharedKeySecretRef,omitempty"`
	AllowedIPs            []string             `json:"allowedIPs,omitempty"`
	Endpoint              string               `json:"endpoint,omitempty"`
	PersistentKeepalive   *int                 `json:"persistentKeepalive,omitempty"`
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
	Node                  string                           `json:"node"`
	MoveToContainer       bool                             `json:"moveToContainer"`
	Container             *WireGuardInterfaceContainerSpec `json:"container,omitempty"`
	InterfaceName         string                           `json:"interfaceName"`
	PrivateKeySecretRef   PrivateKeySecretRef              `json:"privateKeySecretRef"`
	PresharedKeySecretRef *PrivateKeySecretRef             `json:"presharedKeySecretRef,omitempty"`
	Addresses             []WireGuardInterfaceAddressSpec  `json:"addresses"`
	ListenPort            int                              `json:"listenPort"`
	MTU                   int                              `json:"mtu"`
	Peers                 []WireGuardPeerSpec              `json:"peers"`
}

type WireGuardInterfaceAddressSpec struct {
	Family        string `json:"family"`
	Local         string `json:"local"`
	Peer          string `json:"peer"`
	Prefixlen     int    `json:"prefixlen"`
	NoPrefixRoute bool   `json:"noPrefixRoute"`
}

type NetlinkInterfaceAddressStatus struct {
	Family    string  `json:"family"`
	Local     string  `json:"local"`
	Address   *string `json:"address,omitempty"`
	Prefixlen int     `json:"prefixlen"`
}

type PeerStatus struct {
	PublicKey       string  `json:"publicKey"`
	PresharedKey    *string `json:"presharedKey,omitempty"`
	LatestHandshake *int64  `json:"latestHandshake,omitempty"`
	Endpoint        *string `json:"endpoint,omitempty"`
}

type WireGuardInterfaceStatus struct {
	PublicKey    string                          `json:"publicKey"`
	PresharedKey *string                         `json:"presharedKey,omitempty"`
	ListenPort   *int                            `json:"listenPort,omitempty"`
	Peers        []PeerStatus                    `json:"peers"`
	Addresses    []NetlinkInterfaceAddressStatus `json:"addresses"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardInterfaceList is a list of WireGuardInterface resources
type WireGuardInterfaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []WireGuardInterface `json:"items"`
}
