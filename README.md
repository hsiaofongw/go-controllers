# WireGuard controller

This is a Kubernetes-based WireGuard controller (operator), it's job is to ensure that the state of WireGuard interfaces in nodes are consistent with the desired state.

## Install Dependencies

1. golang
2. Docker
3. Kubernetes
4. `controller-gen` for generating CRD manifests:

```sh
go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest
```

Note: if `$GOPATH` is not defined in your shell profile, define it in the shell's startup script, if `$GOPATH/bin` is not in the `$PATH`, include it as well.

## Build

After all dependencies are in position:

```sh
# NOTE: PICK A TEST Kubernetes CLUSTER for testing,
# It will apply some CRD manifests to the api-server.
# ** Be aware that ** it might overrides the already applied CRDs in your k8s cluster with the same name.
./build-all.sh
```

## Give It A Try:

```sh
cd ./lab
docker build -t agent:test . 
docker compose up -d
```

Now you will have three containers: agentx, agent1 and agent2 if everything goes well.

Where agentx is the privileged container that runs in the host netns and shares the host pid namespace, we will run containers in agentx container:


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
docker exec -w /root/projects/go-projects/go-controller/bin -it agentx \
    ./wgplan-controller --kubeconfig=/root/.kube/config -v 4
```

Don't forget to ensure that /root/.kube/config is actually exist and valid before launch all of these.

