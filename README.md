# Network controller

> "What if the desired state of the network can be defined in a single place, and declaratively?"

## Overview

This is a Kubernetes-based WireGuard controller (operator). Its job is to ensure that the state of WireGuard interfaces in nodes is consistent with the desired state that you define. 

You define the desired state by creating a `WireGuardNetworkPlan` resource object (see [./example/wgp/wgp1.yaml](./example/wgp/wgp1.yaml)) and posting it to the API server. The controllers will carry out your intention and converge the node's actual state toward the desired state, all in a declarative manner.

Alternatively, you can manually create a few `WireGuardInterface` resource objects (see [./example/wgi/lax1-wg1.yaml](./example/wgi/lax1-wg1.yaml)) and post them to the API server. Doing so gives you more granular control than the `WireGuardNetworkPlan` approach.

## Core Features

1. Intention-oriented, declarative linux network management.
2. Multi-nodes support and container-awareness.
3. Flexible configuration (network-wide or per-node customization).
4. Self-healing: gracefully deals with abrupt misconfiguration and reconverges automatically.
5. Multi-tenants and RBAC (on its way).

## Install Dependencies

1. golang
2. Docker
3. Kubernetes
4. `controller-gen` for generating CRD manifests:

```sh
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest
```

Note: If `$GOPATH` is not defined in your shell profile, define it in the shell's startup script. If `$GOPATH/bin` is not in the `$PATH`, include it as well.

## Build

After all dependencies are in place, pull the whole monorepo code base:

```sh
cd ~/projects
git clone --recurse-submodules https://gitea.exploro.one/admin/go-projects
```

Then cd into the project's directory and build:


```sh
cd ~/projects/go-projects/go-controllers

./build-all.sh

# If there is any updates in the generated CRDs, remember to re-apply the new CRDs to the cluster
kubectl apply ./crds
```

## Give It a Try:

### Setting up WireGuard interfaces on nodes

```sh
cd ./lab
docker build -t agent:test . 
docker compose up -d
```

Now you will have three containers: agentx, agent1 and agent2 if everything goes well.

Note that agentx is the privileged container that runs in the host netns and shares the host pid namespace. We will run controllers inside the agentx container.


Start controller for node 'lax1':

```sh
docker exec -w /root/projects/go-projects/go-controllers/bin -it agentx --namespace default \
    ./wg-controller --kubeconfig /root/.kube/config -nodename lax1 -v 4
```

Start controller for node 'lax2':

```sh
docker exec -w /root/projects/go-projects/go-controllers/bin -it agentx --namespace default \
    ./wg-controller --kubeconfig /root/.kube/config -nodename lax2 -v 4
```

Start the controller that is responsible for the WireGuardNetworkPlan resources:

```sh
docker exec -w /root/projects/go-projects/go-controllers/bin -it agentx --namespace default \
    ./wgplan-controller --kubeconfig=/root/.kube/config -v 4
```

Don't forget to ensure that /root/.kube/config actually exists and is valid before launching all of these controllers.

Try these examples:

```sh
kubectl apply -f ./example/wgp/wgp1.yaml
kubectl apply -f ./example/wgi/lax1-wg1.yaml
kubectl apply -f ./example/wgi/lax1-wg2.yaml
```

If everything works as expected, you should find that the interfaces are created and moved into the container's netns:

```sh
docker exec -it agent1 ip a show type wireguard

# 37: wg-lax1-lax2-0: <POINTOPOINT,NOARP,UP,LOWER_UP> mtu 1420 qdisc noqueue state UNKNOWN group default 
#     link/none 
#     inet 10.4.0.1 peer 10.4.0.2/32 scope global wg-lax1-lax2-0
#        valid_lft forever preferred_lft forever
# 38: wg1: <POINTOPOINT,NOARP,UP,LOWER_UP> mtu 1420 qdisc noqueue state UNKNOWN group default 
#     link/none 
#     inet6 fe80::1771 peer fe80::a:1771/64 scope link 
#        valid_lft forever preferred_lft forever
```

You can now ping the other end of the tunnel:

```sh
docker exec -it agent1 ping -c 3 10.4.0.2

# PING 10.4.0.2 (10.4.0.2) 56(84) bytes of data.
# 64 bytes from 10.4.0.2: icmp_seq=1 ttl=64 time=0.299 ms
# 64 bytes from 10.4.0.2: icmp_seq=2 ttl=64 time=0.848 ms
# 64 bytes from 10.4.0.2: icmp_seq=3 ttl=64 time=0.808 ms

# --- 10.4.0.2 ping statistics ---
# 3 packets transmitted, 3 received, 0% packet loss, time 2044ms
# rtt min/avg/max/mdev = 0.299/0.651/0.848/0.249 ms

docker exec -it agent1 ping -c 3 fe80::a:1771%wg1

# PING fe80::a:1771%wg1 (fe80::a:1771%wg1) 56 data bytes
# 64 bytes from fe80::a:1771%wg1: icmp_seq=1 ttl=64 time=0.318 ms
# 64 bytes from fe80::a:1771%wg1: icmp_seq=2 ttl=64 time=0.750 ms
# 64 bytes from fe80::a:1771%wg1: icmp_seq=3 ttl=64 time=0.418 ms

# --- fe80::a:1771%wg1 ping statistics ---
# 3 packets transmitted, 3 received, 0% packet loss, time 2066ms
# rtt min/avg/max/mdev = 0.318/0.495/0.750/0.184 ms
```

### Setting up various netlink interfaces:

Start netlink controller on node lax1:

```sh
docker exec -w /root/projects/go-projects/go-controllers/bin -it agentx --namespace default \
    ./nl-controller --kubeconfig /root/.kube/config -nodename lax1 -v 4
```

Apply [./example/nl/nl1-dummy.yaml](./example/nl/nl1-dummy.yaml) to create an interface of type dummy in node lax1:

```sh
kubectl apply -f ./example/nl/nl1-dummy.yaml
```

Apply [./example/nl/nl2-bridge.yaml](./example/nl/nl2-bridge.yaml) to create an interface of type bridge in node lax1:

```sh
kubectl apply -f ./example/nl/nl2-bridge.yaml
```

To create and apply netlink configurations for node lax2, launch the controller on node lax2 as well.

Once controller in node lax2 is in position, apply [./example/nl/vxlan.yaml](./example/nl/vxlan.yaml) to create VTEPs on node lax1 and node lax2:

```sh
kubectl apply -f ./example/nl/vxlan.yaml
```

Doing so will create two VTEPs, one on container agent1, the another one on container agent2, named 'vxlan1' and 'vxlan2' respectively, assigned IPv6 link-local address as `fe80::1%vxlan1` and `fe80::2%vxlan2`.

Afterwards, you can ping the multicast 'All Nodes' address `ff02::1` to discover other VTEPs:

```sh
docker exec -it agent1 ping -c 3 ff02::1%vxlan1

# PING ff02::1%vxlan1 (ff02::1%vxlan1) 56 data bytes
# 64 bytes from fe80::1%vxlan1: icmp_seq=1 ttl=64 time=0.067 ms
# 64 bytes from fe80::2%vxlan1: icmp_seq=1 ttl=64 time=1.33 ms
# 64 bytes from fe80::1%vxlan1: icmp_seq=2 ttl=64 time=0.075 ms
# 64 bytes from fe80::2%vxlan1: icmp_seq=2 ttl=64 time=0.395 ms
# 64 bytes from fe80::1%vxlan1: icmp_seq=3 ttl=64 time=0.106 ms

# --- ff02::1%vxlan1 ping statistics ---
# 3 packets transmitted, 3 received, +2 duplicates, 0% packet loss, time 2018ms
# rtt min/avg/max/mdev = 0.067/0.395/1.334/0.484 ms
```

Note that vxlan1 on agent1 and vxlan2 on agent2 relies on the WireGuard interfaces created earlier on this guide.

Apply [./example/nl/veth.yaml](./example/nl/veth.yaml) would create a pair of veth interfaces, one is at container agent1, the another one is at container agent2:

```sh
kubectl apply -f ./example/nl/veth.yaml
```

Ping `ff02::1` to discover the other end of the veth pair:

```sh
docker exec -it agent1 ping -c 3 ff02::1%veth1-a

# PING ff02::1%veth1-a (ff02::1%veth1-a) 56 data bytes
# 64 bytes from fe80::eeee:1%veth1-a: icmp_seq=1 ttl=64 time=0.175 ms
# 64 bytes from fe80::eeee:2%veth1-a: icmp_seq=1 ttl=64 time=0.191 ms
# 64 bytes from fe80::eeee:1%veth1-a: icmp_seq=2 ttl=64 time=0.122 ms
# 64 bytes from fe80::eeee:2%veth1-a: icmp_seq=2 ttl=64 time=0.184 ms
# 64 bytes from fe80::eeee:1%veth1-a: icmp_seq=3 ttl=64 time=0.077 ms

# --- ff02::1%veth1-a ping statistics ---
# 3 packets transmitted, 3 received, +2 duplicates, 0% packet loss, time 2030ms
# rtt min/avg/max/mdev = 0.077/0.149/0.191/0.043 ms
```

## CRDs and Controller Design

The networking controller system consists of three main components:

1. **WireGuardNetworkPlan Controller** (`wgplan-controller`): Manages high-level network topology definitions
2. **WireGuard Interface Controller** (`wg-controller`): Runs on each node to manage local WireGuard interfaces and ensure they match the desired state
3. **NetlinkInterface Controller** (`netlink-controller`): Runs on each node to manage various types of Linux network interfaces (bridge, vxlan, veth, dummy) with declarative configuration

### Custom Resources
- `WireGuardNetworkPlan`: Defines complete network topologies with nodes, links, and configurations. See [./pkg/apis/networking/v1alpha1/plan.go](./pkg/apis/networking/v1alpha1/plan.go).
- `WireGuardInterface`: Defines desired WireGuard configurations on each node with granular control See [./pkg/apis/networking/v1alpha1/types.go](./pkg/apis/networking/v1alpha1/types.go).
- `NetlinkInterface`: Defines desired configurations for various types of netlink interfaces (bridge, vxlan, veth, dummy) on each node. See [./pkg/apis/networking/v1alpha1/netlinks.go](./pkg/apis/networking/v1alpha1/netlinks.go).

