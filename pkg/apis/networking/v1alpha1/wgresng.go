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
	pkgnetapplycommon "github.com/internetworklab/netapply/pkg/interface/common"
	pkgnetapplywg "github.com/internetworklab/netapply/pkg/interface/wireguard"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=wging;wgifng
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardInterfaceNG is a specification for a WireGuardInterfaceNG resource
type WireGuardInterfaceNG struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec WireGuardInterfaceNGSpec `json:"spec"`

	// +optional
	Status WireGuardInterfaceNGStatus `json:"status"`
}

type PrivateStuffRef struct {
	// Get the secret from string literally.
	String *string `json:"string,omitempty"`

	// Get the secret from secret resource object if `String` is nil or empty.
	// Note when both specified, the `String` would take precedence.
	SecretRef *PrivateKeySecretRef `json:"secretRef,omitempty"`
}

type WireGuardPeerNGSpec struct {
	// When a peer is specified, the publickey must be specified, since it uniquely identifies a peer in a tunnel.
	// So if you don't know the public key of your peer, better add the peer later whenever you know it.
	PublicKey string `json:"publicKey"`

	// Reference to the secret resource object where the preshared key is kept.
	// Some wg users might prefer to use PresharedKey to gain enhanced security, though I do not know for what rationales.
	PresharedKeySecretRef *PrivateStuffRef `json:"presharedKeySecretRef,omitempty"`

	// AllowedIPs has something to do with the cryptokey-routing of WireGuard which is a wg-specific feature,
	// If you have your own routing preferences or you are running some dynamic routing protocols of your own, best to leave it empty
	// or set it to open-to-all like 0.0.0.0/0 and ::/0.
	// Because this cryptokey-routing feature is most useful at some special cases (or special architectures) like hub-and-spoke topology networks.
	AllowedIPs []string `json:"allowedIPs,omitempty"`

	// Endpoint is the clearnet endpoint of the peer, however is not mandatory and you can completely let wg working in passive mode
	// to let your peer play the proactive role.
	Endpoint *string `json:"endpoint,omitempty"`

	// PersistentKeepalive specifies how often wg sends keepalive packets to keep the tunnel busy,
	// at many cases this is not needed since the inner layer traffic of the tunnel is already sufficient to keep the tunnel busy.
	// However at some rare cases you might still want to try set it to some appripriate value to make it more robust to survive some bad network conditions.
	PersistentKeepalive *int `json:"persistentKeepalive,omitempty"`
}

// Used to specify where the resource is located.
type ContainerSelector struct {
	// Takes highest precedence if provided.
	NetNS *string `json:"netns,omitempty"`

	// Name of docker container.
	Docker *string `json:"docker,omitempty"`

	// Name of podman container.
	Podman *string `json:"podman,omitempty"`
}

// WireGuardInterfaceNGSpec is the spec for a WireGuardInterfaceNG resource
type WireGuardInterfaceNGSpec struct {
	Node string `json:"node"`
	// If set to nil, the resource would be provisioned in the host netns, and `MoveToContainer` would be ignored.
	Container *pkgnetapplycommon.ContainerInfo `json:"container,omitempty"`

	// If set to nil, would create the interface in the default VRF.
	// Otherwise, create the interface in the named VRF. Try not to use 'default' as the name of the VRF.
	VRF *string `json:"vrf,omitempty"`

	// Name of the interface to be created, it is required otherwise the controller would have no idea what to create.
	InterfaceName string `json:"interfaceName"`

	// If PrivateKey is provided, the PrivateKeySecretRef will not be used.
	// Note if both PrivateKey and PrivateKeySecretRef are provided, the former would take precedence.
	// And when both are ignored, the controller would try to generate one on-the-fly and won't store the generated key to anywhere.
	PrivateKey *PrivateStuffRef `json:"privateKey,omitempty"`

	// Addresses specifies the addresses that are gonna to be assigned to the interface.
	Addresses []pkgnetapplycommon.AddressConfig `json:"addresses,omitempty"`

	// ListenPort specifies the port that the interface would listen on.
	// If unspecified, or specified a value of 0, the controller would try to generate one in the range of [11024, 65535].
	ListenPort *int `json:"listenPort,omitempty"`

	// MTU specifies the MTU of the interface.
	// Under most cases, the system would automatically determine a most appropriate MTU value for you.
	MTU *int `json:"mtu,omitempty"`

	// Peers specifies the other ends of the tunnel, that is where the tunnel would be connected to.
	// However these are also not mandatory in the creation of the resource, one can completely add (or delete) peers later whenever needed.
	Peers []WireGuardPeerNGSpec `json:"peers,omitempty"`
}

type WireGuardInterfaceNGStatus struct {

	// Nodename is name of the node where the interface is provisioned.
	// The node name can be overridden by the operator running on the node.
	Nodename string `json:"nodename"`

	// Status of underlying resource in the node's system.
	// +k8s:deepcopy-gen=true
	Resource *pkgnetapplywg.WireGuardInterfaceStatus `json:"resource,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WireGuardInterfaceNGList is a list of WireGuardInterfaceNG resources
type WireGuardInterfaceNGList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []WireGuardInterfaceNG `json:"items"`
}
