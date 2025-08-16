package v1alpha1

import "golang.zx2c4.com/wireguard/wgctrl/wgtypes"

// A Device is a WireGuard device.
type WireGuardDeviceStatus struct {
	// Name is the name of the device.
	Name string `json:"name"`

	// Type specifies the underlying implementation of the device.
	Type wgtypes.DeviceType `json:"type"`

	// PrivateKey is the device's private key.
	PrivateKey *string `json:"private_key"`

	// PublicKey is the device's public key, computed from its PrivateKey.
	PublicKey *string `json:"public_key"`

	// ListenPort is the device's network listening port.
	ListenPort *int `json:"listen_port"`

	// FirewallMark is the device's current firewall mark.
	//
	// The firewall mark can be used in conjunction with firewall software to
	// take action on outgoing WireGuard packets.
	FirewallMark *int `json:"firewall_mark,omitempty"`

	// Peers is the list of network peers associated with this device.
	Peers []*WireGuardPeerStatus `json:"peers"`
}

func NewDeviceStatus(dev *wgtypes.Device) *WireGuardDeviceStatus {
	if dev == nil {
		return nil
	}

	devStatus := new(WireGuardDeviceStatus)
	devStatus.Name = dev.Name
	devStatus.Type = dev.Type
	privKey := dev.PrivateKey.String()
	devStatus.PrivateKey = &privKey
	pubKey := dev.PublicKey.String()
	devStatus.PublicKey = &pubKey
	devStatus.ListenPort = &dev.ListenPort
	devStatus.FirewallMark = &dev.FirewallMark
	peerStatuses := make([]*WireGuardPeerStatus, 0)
	for _, peer := range dev.Peers {
		peerStatus := NewWireGuardPeerStatus(&peer)
		if peerStatus == nil {
			continue
		}
		peerStatuses = append(peerStatuses, peerStatus)
	}
	devStatus.Peers = peerStatuses

	return devStatus
}

// A PeerStatus is a WireGuard peer to a Device.
type WireGuardPeerStatus struct {

	// PublicKey is the public key of a peer, computed from its private key.
	//
	// PublicKey is always present in a Peer.
	PublicKey *string `json:"public_key,omitempty"`

	// PresharedKey is an optional preshared key which may be used as an
	// additional layer of security for peer communications.
	//
	// A zero-value Key means no preshared key is configured.
	PresharedKey *string `json:"preshared_key,omitempty"`

	// Endpoint is the most recent source address used for communication by
	// this Peer.
	Endpoint *string `json:"endpoint,omitempty"`

	// PersistentKeepaliveInterval specifies how often an "empty" packet is sent
	// to a peer to keep a connection alive.
	//
	// A value of 0 indicates that persistent keepalives are disabled.
	PersistentKeepaliveInterval *int64 `json:"persistent_keepalive_interval,omitempty"`

	// LastHandshakeTime indicates the most recent time a handshake was performed
	// with this peer.
	//
	// A zero-value time.Time indicates that no handshake has taken place with
	// this peer.
	LastHandshakeTime *int64 `json:"last_handshake_time,omitempty"`

	// ReceiveBytes indicates the number of bytes received from this peer.
	ReceiveBytes *int64 `json:"receive_bytes,omitempty"`

	// TransmitBytes indicates the number of bytes transmitted to this peer.
	TransmitBytes *int64 `json:"transmit_bytes,omitempty"`

	// AllowedIPs specifies which IPv4 and IPv6 addresses this peer is allowed
	// to communicate on.
	//
	// 0.0.0.0/0 indicates that all IPv4 addresses are allowed, and ::/0
	// indicates that all IPv6 addresses are allowed.
	AllowedIPs []string `json:"allowed_ips,omitempty"`

	// ProtocolVersion specifies which version of the WireGuard protocol is used
	// for this Peer.
	//
	// A value of 0 indicates that the most recent protocol version will be used.
	ProtocolVersion int
}

func NewWireGuardPeerStatus(peer *wgtypes.Peer) *WireGuardPeerStatus {
	if peer == nil {
		return nil
	}

	peerStatus := new(WireGuardPeerStatus)
	pubKey := peer.PublicKey.String()
	peerStatus.PublicKey = &pubKey
	psk := peer.PresharedKey.String()
	peerStatus.PresharedKey = &psk
	endpoint := peer.Endpoint.String()
	peerStatus.Endpoint = &endpoint
	pkl := peer.PersistentKeepaliveInterval.Milliseconds()
	peerStatus.PersistentKeepaliveInterval = &pkl
	lastHS := peer.LastHandshakeTime.UnixMilli()
	peerStatus.LastHandshakeTime = &lastHS
	recvBytes := peer.ReceiveBytes
	peerStatus.ReceiveBytes = &recvBytes
	txBytes := peer.TransmitBytes
	peerStatus.TransmitBytes = &txBytes
	allowedIPs := make([]string, 0)
	for _, ip := range peer.AllowedIPs {
		allowedIPs = append(allowedIPs, ip.String())
	}
	peerStatus.AllowedIPs = allowedIPs
	peerStatus.ProtocolVersion = peer.ProtocolVersion

	return peerStatus
}
