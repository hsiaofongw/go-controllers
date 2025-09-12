package main

import (
	"fmt"

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgreconcile "k8s.io/sample-controller/pkg/reconcile"
)

func main() {

	pathToVtysh := "/usr/bin/vtysh"

	manager, err := pkgreconcile.NewFRROSPFManager(pathToVtysh)
	if err != nil {
		fmt.Println("Error creating FRR OSPF manager:", err)
		return
	}

	routerId1 := "0.0.0.1"
	vrf1 := "v1"
	routerId2 := "0.0.0.2"
	vrf2 := "v2"
	passive := true

	ifaceSpecs := []networkingv1alpha1.OSPFProtocolInterfaceSpec{
		{InterfaceName: "va", Area: "0.0.0.0", NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint},
		{InterfaceName: "d1", Area: "0.0.0.0", Passive: &passive},
		{InterfaceName: "vb", Area: "0.0.0.0", NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint},
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
		err = manager.AddInterface(intf.InterfaceName, &intf)
		if err != nil {
			fmt.Println("Error adding interface:", err)
			return
		}
	}

	fmt.Println("Task is successfully completed")
}
