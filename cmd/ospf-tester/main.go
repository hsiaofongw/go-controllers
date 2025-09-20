package main

import (
	"fmt"

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgutilsfrr "k8s.io/sample-controller/pkg/utils/frr"
)

// To prepare the environment:
//
// ip l add v1 type vrf table 101
// ip l add v2 type vrf table 102
// ip l set v1 up
// ip l set v2 up

// # create a pair of veth interfaces: va, vb
// ip l add va type veth peer name vb
// ip l set va master v1
// ip l set vb master v2
// ip a add 10.0.4.1 peer 10.0.4.2/32 dev va
// ip a add 10.0.4.2 peer 10.0.4.1/32 dev vb
// ip l set va up
// ip l set vb up

// # ping between va and vb
// ip vrf exec v1 ping -c1 10.0.4.2
// ip vrf exec v2 ping -c1 10.0.4.1

// # now that v1 and v2 have direct connectivity, we can let them peer with each other

// # create two dummy interfaces representing the subnets in v1 and v2 being announced
// ip l add d1 type dummy
// ip l add d2 type dummy
// ip l set d1 master v1
// ip l set d2 master v2
// ip a add 10.0.2.1/24 dev d1
// ip a add 10.0.3.1/24 dev d2
// ip l set d1 up
// ip l set d2 up
//
// To verify:
//
// vtysh -c 'show running-config'
// vtysh -c 'show ip ospf vrf v1 neighbor'
// vtysh -c 'show ip ospf vrf v2 neighbor'
// ip vrf exec v1 ping -c1 10.0.3.1
// ip r show vrf v1    # v1 should learnt the route to 10.0.3.0/24
// ip r show vrf v2    # v2 should learnt the route to 10.0.2.0/24
//
//
// To clean up the environment:
//
// ip l del d1
// ip l del d2
// ip l del v1
// ip l del v2
// ip l del va # or ip l del vb
//
// vtysh commands to do the clean up job:
//
// configure
//   no router ospf vrf v1
//   no router ospf vrf v2
//   !
//   no vrf v1
//   no vrf v2
//   !
//   interface va
//     no ip ospf area
//     no ip ospf network
//   exit
//   !
//   interface vb
//     no ip ospf area
//     no ip ospf network
//   exit
//   !
//   interface d1
//     no ip ospf area
//     no ip ospf passive
//   exit
//   !
//   interface d2
//     no ip ospf area
//     no ip ospf passive
//   exit
// exit
//

func main() {

	pathToVtysh := "/usr/bin/vtysh"

	manager, err := pkgutilsfrr.NewFRROSPFManager(pathToVtysh)
	if err != nil {
		fmt.Println("Error creating FRR OSPF manager:", err)
		return
	}

	routerId1 := "0.0.0.1"
	vrf1 := "v1"
	routerId2 := "0.0.0.2"
	vrf2 := "v2"
	passive := true
	ptpNw := networkingv1alpha1.OSPFNetworkTypePointToPoint

	ifaceSpecs := []networkingv1alpha1.OSPFProtocolInterfaceSpec{
		{InterfaceName: "va", Area: "0.0.0.0", NetworkType: &ptpNw},
		{InterfaceName: "d1", Area: "0.0.0.0", Passive: &passive},
		{InterfaceName: "vb", Area: "0.0.0.0", NetworkType: &ptpNw},
		{InterfaceName: "d2", Area: "0.0.0.0", Passive: &passive},
	}

	err = manager.EnableOSPFv2Router(routerId1, &vrf1)
	if err != nil {
		fmt.Println("Error enabling OSPFv2 router:", err)
		return
	}

	err = manager.EnableOSPFv2Router(routerId2, &vrf2)
	if err != nil {
		fmt.Println("Error enabling OSPFv2 router:", err)
		return
	}

	for _, intf := range ifaceSpecs {
		err = manager.AddInterface(intf.InterfaceName, &intf, pkgutilsfrr.FRRVRFUnspecified)
		if err != nil {
			fmt.Println("Error adding interface:", err)
			return
		}
	}

	fmt.Println("Task is successfully completed")
}
