package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	yamlv3 "gopkg.in/yaml.v3"
	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgreconcile "k8s.io/sample-controller/pkg/reconcile"
	pkgutilsfrr "k8s.io/sample-controller/pkg/utils/frr"
)

// This is a tester program for testing the OSPF reconciliation capability of the OSPF controller.
// First, you must prepare the environment by running the following commands.
// then, compile and run the program (or just go run <path-to-this-file>),
// it will detect any deviations between the desired state and the current state, and reconverge both.
// At the end, it will print the latest status object in YAML format.
//
// Synopsis:
// go run <path-to-this-file> --spec1
// go run <path-to-this-file> --spec2

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

func getSpec1() *networkingv1alpha1.OSPFProtocolSpec {
	routerId1 := "0.0.0.1"
	vrf1 := "v1"
	routerId2 := "0.0.0.2"
	vrf2 := "v2"
	passive := true
	routerId3 := "10.3.82.1"
	vrfDefault := pkgutilsfrr.FRRVRFDefault
	area0 := "0.0.0.0"

	ifaceSpecs := []networkingv1alpha1.OSPFProtocolInterfaceSpec{

		// vrf v1
		{InterfaceName: "va", Area: area0, NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint, VRF: &vrf1},
		{InterfaceName: "d1", Area: area0, Passive: &passive, VRF: &vrf1},

		// vrf v2
		{InterfaceName: "vb", Area: area0, NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint, VRF: &vrf2},
		{InterfaceName: "d2", Area: area0, Passive: &passive, VRF: &vrf2},

		// vrf default
		{InterfaceName: "dummy-wien1", Area: area0, Passive: &passive, VRF: &vrfDefault},
		{InterfaceName: "wg-wien1-frank1", Area: area0, VRF: &vrfDefault, NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint},
		{InterfaceName: "wg-wien1-mnz1", Area: area0, VRF: &vrfDefault, NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint},
		{InterfaceName: "wg-wien1-sgp1", Area: area0, VRF: &vrfDefault, NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint},
	}

	routerSpecs := []networkingv1alpha1.OSPFProtocolRouterSpec{
		// vrf v1
		{RouterID: routerId1, VRF: &vrf1},

		// vrf v2
		{RouterID: routerId2, VRF: &vrf2},

		// vrf default (aka. default vrf)
		{RouterID: routerId3, VRF: &vrfDefault},
	}

	ospfSpec := &networkingv1alpha1.OSPFProtocolSpec{
		Driver:     networkingv1alpha1.OSPFProtocolTypeFRR,
		Version:    networkingv1alpha1.OSPFProtocolVersion2,
		Interfaces: ifaceSpecs,
		Routers:    routerSpecs,
	}

	return ospfSpec
}

// the only difference between getSpec1() and getSpec2() is that getSpec2()
// removes the VRF-enslaved routers and interfaces from the spec.
func getSpec2() *networkingv1alpha1.OSPFProtocolSpec {

	passive := true
	routerId3 := "10.3.82.1"
	vrfDefault := pkgutilsfrr.FRRVRFDefault
	area0 := "0.0.0.0"

	ifaceSpecs := []networkingv1alpha1.OSPFProtocolInterfaceSpec{
		// vrf default
		{InterfaceName: "dummy-wien1", Area: area0, Passive: &passive, VRF: &vrfDefault},
		{InterfaceName: "wg-wien1-frank1", Area: area0, VRF: &vrfDefault, NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint},
		{InterfaceName: "wg-wien1-mnz1", Area: area0, VRF: &vrfDefault, NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint},
		{InterfaceName: "wg-wien1-sgp1", Area: area0, VRF: &vrfDefault, NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint},
	}

	routerSpecs := []networkingv1alpha1.OSPFProtocolRouterSpec{
		// vrf default (aka. default vrf)
		{RouterID: routerId3, VRF: &vrfDefault},
	}

	ospfSpec := &networkingv1alpha1.OSPFProtocolSpec{
		Driver:     networkingv1alpha1.OSPFProtocolTypeFRR,
		Version:    networkingv1alpha1.OSPFProtocolVersion2,
		Interfaces: ifaceSpecs,
		Routers:    routerSpecs,
	}

	return ospfSpec
}

func readSpecFromStdin() (*networkingv1alpha1.OSPFProtocolSpec, error) {
	var ospfSpec networkingv1alpha1.OSPFProtocolSpec
	err := yamlv3.NewDecoder(os.Stdin).Decode(&ospfSpec)
	if err != nil {
		return nil, fmt.Errorf("error decoding spec from stdin: %s", err.Error())
	}
	return &ospfSpec, nil
}

// It selects what spec to use based on the CLI arguments,
// If multiple arguments are given, the first one takes precedence.
// If no effective argument is given, it will use the spec1 by default.
// You can also pass the spec from stdin by using the --spec-from-stdin argument.
// Example:
// cat spec.yaml | go run main.go --spec-from-stdin
//
// The spec must be in YAML format, see code in networkingv1alpha1 for the spec schema.
func getSpec(args []string) (*networkingv1alpha1.OSPFProtocolSpec, string, error) {
	for _, arg := range args {
		if arg == "--spec1" {
			return getSpec1(), "spec1", nil
		}
		if arg == "--spec2" {
			return getSpec2(), "spec2", nil
		}
		if arg == "--spec-from-stdin" {
			specObj, err := readSpecFromStdin()
			if err != nil {
				return nil, "", fmt.Errorf("error reading spec from stdin: %s", err.Error())
			}
			return specObj, "spec-from-stdin", nil
		}
	}
	return getSpec1(), "spec1", nil
}

func main() {

	ospfSpec, specName, err := getSpec(os.Args[1:])
	if err != nil {
		fmt.Println("Error getting spec:", err)
		return
	}
	log.Println("Using spec:", specName)

	pathToVtysh := "/usr/bin/vtysh"
	frrManager, err := pkgutilsfrr.NewFRROSPFManager(pathToVtysh)
	if err != nil {
		fmt.Println("Error creating FRR OSPF manager:", err)
		return
	}

	reconciler, err := pkgreconcile.NewFRROSPFv2Reconciler(frrManager)
	if err != nil {
		fmt.Println("Error creating FRR OSPF reconciler:", err)
		return
	}

	statusObj := new(networkingv1alpha1.OSPFProtocolStatus)

	log.Println("Detecting changes")
	hasUpdates, err := reconciler.DetectChanges(context.Background(), ospfSpec, statusObj)
	if err != nil {
		fmt.Println("Error detecting changes:", err)
		return
	}

	log.Println("hasUpdates:", hasUpdates)

	maxLoops := 10
	for hasUpdates && maxLoops > 0 {
		time.Sleep(1 * time.Second)

		log.Println("Applying reconcile", "maxLoops", maxLoops)
		err = reconciler.ApplyReconcile(context.Background(), ospfSpec)
		if err != nil {
			fmt.Println("Error applying reconcile:", err)
			return
		}

		log.Println("Resetting state")
		reconciler.ResetState()

		log.Println("Detecting changes")
		hasUpdates, err = reconciler.DetectChanges(context.Background(), ospfSpec, statusObj)
		if err != nil {
			fmt.Println("Error detecting changes:", err)
			return
		}
		log.Println("hasUpdates:", hasUpdates)

		maxLoops--
	}

	if hasUpdates && maxLoops == 0 {
		panic("Max loops reached")
	}

	log.Println("Done")

	fmt.Println("latest Status obj:")
	var statusObjYaml []byte
	statusObjYaml, err = yamlv3.Marshal(statusObj)
	if err != nil {
		fmt.Println("Error marshalling status obj:", err)
		return
	}
	statusObjYamlStr := string(statusObjYaml)
	fmt.Println(statusObjYamlStr)
}
