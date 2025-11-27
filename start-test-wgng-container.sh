#!/bin/bash

scriptPath=$(realpath $0)
scriptDir=$(dirname $scriptPath)

docker run \
  --pull always \
  --rm \
  --pid=host \
  --name=test-wgng-controller \
  -it \
  -v /root/.kube/config:/root/.kube/config:ro \
  -v "/var/run/docker.sock:/var/run/docker.sock:ro" \
  -v "/etc/bird/ebgp_peers:/etc/bird/ebgp_peers" \
  -v "/:/host-rootfs:ro" \
  -v /run/netns:/run/netns \
  -v $scriptDir/bin/wgng-controller:/usr/local/bin/wgng-controller \
  --privileged \
  debian:trixie \
  wgng-controller -v=4 -kubeconfig=/root/.kube/config -namespace=default -nodename=vie1

# note if something still goes wrong, try add --ipc=host to the docker run arguments as well
# if that still doesn't work, it's bug.
