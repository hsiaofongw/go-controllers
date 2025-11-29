package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	pkgnetapplywg "github.com/internetworklab/netapply/pkg/interface/wireguard"
	pkgnetapplymdls "github.com/internetworklab/netapply/pkg/models"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sJson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	v1alpha1networking "k8s.io/sample-controller/pkg/apis/networking"
	v1alpha1 "k8s.io/sample-controller/pkg/apis/networking/v1alpha1"
)

var (
	resourceFile       = flag.String("resource-file", "", "the resource file to convert from")
	birdBGPResourceDir = flag.String("bird-bgp-resource-dir", "", "the directory where the bird bgp resource files are send to")
	wgResourceDir      = flag.String("wg-resource-dir", "", "the directory where the wireguard resource files are send to")
	nodeName           = flag.String("node-name", "", "the name of the node to convert from")
	namespace          = flag.String("namespace", "default", "the namespace of the node to convert from")
)

func init() {
	flag.Parse()
}

const labelKeyPeerASN = "networking.dn42.io/peer-asn"

func main() {
	if *resourceFile == "" || *birdBGPResourceDir == "" || *wgResourceDir == "" || *nodeName == "" || *namespace == "" {
		flag.Usage()
		os.Exit(1)
	}

	if *wgResourceDir == *birdBGPResourceDir {
		log.Fatalf("wireguard resource directory and bird bgp resource directory cannot be the same")
	}

	f, err := os.Open(*resourceFile)
	if err != nil {
		log.Fatalf("failed to open resource file: %v", err)
	}
	defer f.Close()

	nodeConfig := new(pkgnetapplymdls.NodeConfig)
	if err := yaml.NewDecoder(f).Decode(nodeConfig); err != nil {
		log.Fatalf("failed to decode resource file: %v", err)
	}

	if nodeConfig.Resources == nil {
		log.Println("Resource file is empty, nothing to convert from")
		return
	}

	wgMap := make(map[string]pkgnetapplywg.WireGuardConfig)

	if nodeConfig.Resources.WireGuard != nil {
		log.Println("Start converting wireguard resources to resource files")
		for _, wgCfg := range nodeConfig.Resources.WireGuard.WireGuardConfigs {
			wgMap[wgCfg.Name] = wgCfg
			resName := fmt.Sprintf("%s-%s", *nodeName, wgCfg.Name)
			filebasename := resName + ".yaml"
			wireguardRes := v1alpha1.WireGuardInterfaceNG{
				TypeMeta: v1.TypeMeta{
					Kind:       "WireGuardInterfaceNG",
					APIVersion: "networking.dn42.io/v1alpha1",
				},
				ObjectMeta: v1.ObjectMeta{
					Name:      resName,
					Namespace: *namespace,
					Finalizers: []string{
						v1alpha1networking.WGNetworkingFinalizer,
					},
					Annotations: wgCfg.Additionals,
				},
				Spec: v1alpha1.WireGuardInterfaceNGSpec{
					Node:          *nodeName,
					Container:     wgCfg.Container,
					VRF:           wgCfg.VRF,
					InterfaceName: wgCfg.Name,
					Addresses:     wgCfg.Addresses,
					ListenPort:    wgCfg.ListenPort,
					MTU:           wgCfg.MTU,
				},
			}
			if wgCfg.Additionals != nil {
				if asn, ok := wgCfg.Additionals[pkgnetapplywg.WGAdditionalKeyASN]; ok {
					wireguardRes.ObjectMeta.Labels = map[string]string{
						labelKeyPeerASN: asn,
					}
				}
			}
			if wgCfg.PrivateKey != "" {
				wireguardRes.Spec.PrivateKey = &v1alpha1.PrivateStuffRef{
					String: &wgCfg.PrivateKey,
				}
			}
			for _, peer := range wgCfg.Peers {
				peerRes := v1alpha1.WireGuardPeerNGSpec{
					PublicKey:           peer.PublicKey,
					AllowedIPs:          peer.AllowedIPs,
					Endpoint:            peer.Endpoint,
					PersistentKeepalive: peer.PersistentKeepalive,
				}
				if peer.PresharedKey != "" {
					peerRes.PresharedKeySecretRef = &v1alpha1.PrivateStuffRef{
						String: &peer.PresharedKey,
					}
				}
				wireguardRes.Spec.Peers = append(wireguardRes.Spec.Peers, peerRes)
			}
			fullpath := filepath.Join(*wgResourceDir, filebasename)
			func() {
				outf, err := os.Create(fullpath)
				if err != nil {
					log.Fatalf("failed to create wireguard resource file: %v", err)
				}
				defer outf.Close()
				log.Printf("Writing wireguard resource to %s", fullpath)
				serializer := k8sJson.NewSerializerWithOptions(
					k8sJson.DefaultMetaFactory, nil, nil,
					k8sJson.SerializerOptions{Yaml: true, Pretty: true, Strict: true},
				)
				if err := serializer.Encode(&wireguardRes, outf); err != nil {
					log.Fatalf("failed to encode bird bgp resource: %v", err)
				}
			}()
		}
	}

	if nodeConfig.Resources.BirdBGP != nil {
		log.Println("Start converting bird bgp resources to resource files")
		for _, bgpCfg := range nodeConfig.Resources.BirdBGP.EBGPProtocols {
			resName := fmt.Sprintf("%s-%s", *nodeName, bgpCfg.Name)
			filebasename := resName + ".yaml"
			birdBGPRes := v1alpha1.BirdBGPProtocol{
				TypeMeta: v1.TypeMeta{
					Kind:       "BirdBGPProtocol",
					APIVersion: "networking.dn42.io/v1alpha1",
				},
				ObjectMeta: v1.ObjectMeta{
					Name:      resName,
					Namespace: *namespace,
					Finalizers: []string{
						v1alpha1networking.BirdBGPFinalizer,
					},
				},
				Spec: v1alpha1.BirdBGPProtocolSpec{
					Node:         *nodeName,
					Name:         bgpCfg.Name,
					Template:     bgpCfg.Template,
					Interface:    bgpCfg.Interface,
					LocalAddress: bgpCfg.LocalAddress,
					PeerAddress:  bgpCfg.PeerAddress,
					LocalASN:     bgpCfg.LocalASN,
					PeerASN:      bgpCfg.PeerASN,
					PeerExternal: bgpCfg.PeerExternal,
					PeerInternal: bgpCfg.PeerInternal,
				},
			}

			if bgpCfg.Interface != nil && *bgpCfg.Interface != "" {
				if wgCfg, ok := wgMap[*bgpCfg.Interface]; ok {
					if wgCfg.Additionals != nil {
						if peerASN, ok := wgCfg.Additionals[pkgnetapplywg.WGAdditionalKeyASN]; ok {
							birdBGPRes.ObjectMeta.Labels = map[string]string{
								labelKeyPeerASN: peerASN,
							}
						}
					}
				}
			}

			fullpath := filepath.Join(*birdBGPResourceDir, filebasename)
			func() {
				outf, err := os.Create(fullpath)
				if err != nil {
					log.Fatalf("failed to create bird bgp resource file: %v", err)
				}
				defer outf.Close()
				log.Printf("Writing bird bgp resource to %s", fullpath)
				serializer := k8sJson.NewSerializerWithOptions(
					k8sJson.DefaultMetaFactory, nil, nil,
					k8sJson.SerializerOptions{Yaml: true, Pretty: true, Strict: true},
				)
				if err := serializer.Encode(&birdBGPRes, outf); err != nil {
					log.Fatalf("failed to encode bird bgp resource: %v", err)
				}
			}()
		}
	}

}
