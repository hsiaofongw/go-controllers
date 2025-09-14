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

const FRRVRFUnspecified string = ""
const FRRVRFDefault string = "default"
