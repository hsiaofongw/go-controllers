#!/bin/bash

scriptPath=$(realpath $0)
scriptDir=$(dirname $scriptPath)

cd "$scriptDir"

./bin/controller \
    --kubeconfig=/root/.kube/config \
    -v 4 \
    --nodename=lax1
