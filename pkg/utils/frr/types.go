package frr

type FRRVtyshAgent struct {
	vtyshPath string
}

type FRROSPFManager struct {
	vtyshAgent *FRRVtyshAgent
}

type FRROSPFAreaDetail struct {
	Backbone *bool `json:"backbone,omitempty"`
}

type FRROSPFVRFDetail struct {
	VRFName  *string                      `json:"vrfName,omitempty"`
	VRFID    *string                      `json:"vrfId,omitempty"`
	RouterID *string                      `json:"routerId,omitempty"`
	Areas    map[string]FRROSPFAreaDetail `json:"areas,omitempty"`
}

type FRROSPFVRFList map[string]FRROSPFVRFDetail

type FRROSPFVRFBrief struct {
	VRFId    *int    `json:"vrfId,omitempty"`
	RouterID *string `json:"routerId,omitempty"`
}

type FRROSPFVRFBriefList struct {
	// key is the vrf name, the name for the default vrf is 'default'
	VRFs map[string]FRROSPFVRFBrief `json:"vrfs,omitempty"`

	TotalVRFs *int `json:"totalVrfs,omitempty"`
}

type FRROSPFVRFIfaceList map[string]FRROSPFIfaceList

type FRROSPFIfaceNWType string

const (
	FRRIFACE_NETWORK_TYPE_BROADCAST           = "BROADCAST"
	FRRIFACE_NETWORK_TYPE_POINT_TO_POINT      = "POINTTOPOINT"
	FRRIFACE_NETWORK_TYPE_POINT_TO_MULTIPOINT = "POINTTOMULTIPOINT"
)

type FRROSPFIface struct {
	Area              *string             `json:"area,omitempty"`
	TimerPassiveIface *bool               `json:"timerPassiveIface,omitempty"`
	NetworkType       *FRROSPFIfaceNWType `json:"networkType,omitempty"`
	RouterID          *string             `json:"routerId,omitempty"`
}

type FRROSPFIfaceList struct {
	VRFName    *string                 `json:"vrfName,omitempty"`
	VRFID      *string                 `json:"vrfId,omitempty"`
	Interfaces map[string]FRROSPFIface `json:"interfaces,omitempty"`
}

// In FRR vtysh, sometimes you can explicitly specify the vrf context,
// you can explicitly specify the default vrf e.g. `interface <iface> vrf default`,
// if you leave the vrf name empty, it can be deduced from the interface properties queried from
// underlying netlink subsystem, e.g. `interface veth1` could be in vrf default or other vrf,
// depending on which vrf the interface veth1 is/has been enslaved to.
//
// All to say, VRFUnspecified doesn't necessarily refers to vrf default,
// and vrf 'default' does explicitly refers to the default vrf (or vrf default).
//
// However, in some contexts, such as `router ospf <vrf>`, the VRFUnspecified here does
// always refers to vrf default no matter what.
//
// Also, we assume that no one will ever try to use some special vrf name like 'default' or 'all',
// since they serves as special purpose.
// For example, create and use a vrf with specialized name like 'default' or 'all'
// `ip l add default type vrf` or `ip l add all type vrf` will lead to un-expected behaviors.
const FRRVRFUnspecified string = ""
const FRRVRFDefault string = "default"
