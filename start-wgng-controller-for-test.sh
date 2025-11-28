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
  -v "/:/host-rootfs:ro" \
  -v /run/netns:/run/netns \
  -v $scriptDir/bin/wgng-controller:/usr/local/bin/wgng-controller \
  --privileged \
  debian:trixie \
  wgng-controller \
  -v=4 \
  -kubeconfig=/root/.kube/config \
  -namespace=default \
  -nodename=vie1
