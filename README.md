# WireGuard controller

> "What if the desired state of the network can be defined in a single place, and declaratively?"

## Overview

This is a Kubernetes-based WireGuard controller (operator). Its job is to ensure that the state of WireGuard interfaces in nodes is consistent with the desired state that you define. 

You define the desired state by creating a `WireGuardNetworkPlan` resource object (see [./example/wgp/wgp1.yaml](./example/wgp/wgp1.yaml)) and posting it to the API server. The controllers will carry out your intention and converge the node's actual state toward the desired state, all in a declarative manner.

Alternatively, you can manually create a few `WireGuardInterface` resource objects (see [./example/wgi/lax1-wg1.yaml](./example/wgi/lax1-wg1.yaml)) and post them to the API server. Doing so gives you more granular control than the `WireGuardNetworkPlan` approach.

## Core Features

1. Intention-oriented, declarative WireGuard network management.
2. Multi-node support and container-awareness.
3. Flexible configuration (network-wide or per-node customization).
4. Self-healing: gracefully deals with abrupt misconfiguration and reconverges automatically.

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

After all dependencies are in place:

```sh
# NOTE: PICK A TEST Kubernetes CLUSTER for testing.
# It will apply some CRD manifests to the API server.
# **Be aware that** it might override the already applied CRDs in your k8s cluster with the same name.
./build-all.sh
```

## Give It a Try:

```sh
cd ./lab
docker build -t agent:test . 
docker compose up -d
```

Now you will have three containers: agentx, agent1 and agent2 if everything goes well.

Note that agentx is the privileged container that runs in the host netns and shares the host pid namespace. We will run controllers inside the agentx container.


Start controller for node 'lax1':

```sh
docker exec -w /root/projects/go-projects/go-controllers/bin -it agentx \
    ./wg-controller --kubeconfig /root/.kube/config -nodename lax1 -v 4
```

Start controller for node 'lax2':

```sh
docker exec -w /root/projects/go-projects/go-controllers/bin -it agentx \
    ./wg-controller --kubeconfig /root/.kube/config -nodename lax2 -v 4
```

Start the controller that is responsible for the WireGuardNetworkPlan resources:

```sh
docker exec -w /root/projects/go-projects/go-controllers/bin -it agentx \
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

## CRDs and Controller Design

The networking controller system consists of three main components:

1. **WireGuardNetworkPlan Controller** (`wgplan-controller`): Manages high-level network topology definitions
2. **WireGuard Interface Controller** (`wg-controller`): Runs on each node to manage local WireGuard interfaces and ensure they match the desired state
3. **NetlinkInterface Controller** (`netlink-controller`): Runs on each node to manage various types of Linux network interfaces (bridge, veth, dummy) with declarative configuration

### Custom Resources
- `WireGuardNetworkPlan`: Defines complete network topologies with nodes, links, and configurations
- `WireGuardInterface`: Defines desired WireGuard configurations on each node with granular control
- `NetlinkInterface`: Defines desired configurations for various types of netlink interfaces (bridge, veth, dummy) on each node

