#!/bin/bash

scriptPath=$(realpath $0)
scriptDir=$(dirname $scriptPath)

# please make sure that the controller
# will controls a test bird instance before starting.

docker run \
  --pull always \
  --rm \
  --pid=host \
  --name=test-birdbgp-controller \
  -it \
  -v /root/.kube/config:/root/.kube/config:ro \
  -v "/var/run/docker.sock:/var/run/docker.sock:ro" \
  -v "/etc/bird/ebgp_peers:/etc/bird/ebgp_peers" \
  -v "/:/host-rootfs:ro" \
  -v $scriptDir/bin/birdbgp-controller:/usr/local/bin/birdbgp-controller \
  --privileged \
  debian:trixie \
  birdbgp-controller \
    -v=4 \
    -kubeconfig=/root/.kube/config \
    -namespace=default \
    -nodename=vie1 \
    -bird-socket-path=/var/run/bird/bird.ctl \
    -bird-config-dir=/etc/bird/ebgp_peers 
