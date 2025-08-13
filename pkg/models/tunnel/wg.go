package tunnel

type WireGuardPeerObject struct {
	PublicKey    string
	PresharedKey *string
	Endpoint     string
}

type WireGuardInterfaceObject struct {
	Name         string
	ListenPort   int
	PrivateKey   string
	PublicKey    string
	PresharedKey *string
	Peers        []WireGuardPeerObject
	MTU          int
}
