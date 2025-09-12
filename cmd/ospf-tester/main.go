package main

import (
	"context"
	"fmt"

	networkingv1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
	pkgreconcile "k8s.io/sample-controller/pkg/reconcile"
)

func main() {
	frrReconciler, err := pkgreconcile.NewFRROSPFv2Reconciler("/usr/bin/vtysh")
	if err != nil {
		fmt.Println("Error creating FRRVtyshAgent:", err)
		return
	}

	vrf := "v1"
	passive := true

	desiredState := networkingv1alpha1.OSPFProtocolSpec{
		Driver:   networkingv1alpha1.OSPFProtocolTypeFRR,
		Version:  networkingv1alpha1.OSPFProtocolVersion2,
		VRF:      &vrf,
		RouterID: "0.0.0.1",
		Interfaces: []networkingv1alpha1.OSPFProtocolInterfaceSpec{
			{InterfaceName: "va", Area: "0.0.0.0", NetworkType: networkingv1alpha1.OSPFNetworkTypePointToPoint},
			{InterfaceName: "d1", Area: "0.0.0.0", Passive: &passive},
		},
	}

	hasUpdated, err := frrReconciler.DetectChanges(context.Background(), desiredState, nil)
	if err != nil {
		fmt.Println("Error detecting changes:", err)
		return
	}
	fmt.Println("Has updated:", hasUpdated)

	if hasUpdated {
		maxLoop := 10
		for hasUpdated && maxLoop > 0 {
			err = frrReconciler.ApplyReconcile(context.Background(), desiredState)
			if err != nil {
				fmt.Println("Error applying reconcile:", err)
				return
			}

			frrReconciler.ResetState()
			hasUpdated, err = frrReconciler.DetectChanges(context.Background(), desiredState, nil)
			if err != nil {
				fmt.Println("Error detecting changes:", err)
				return
			}

			maxLoop--
		}

		if hasUpdated && maxLoop == 0 {
			fmt.Println("Failed to apply reconcile: out of max loops")
			return
		}
	}

}
