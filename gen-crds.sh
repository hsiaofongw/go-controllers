#!/bin/bash

scriptPath=$(realpath $0)
scriptDir=$(dirname $scriptPath)

cd "$scriptDir" && controller-gen crd paths=./pkg/apis/networking/v1alpha1 output:stdout output:crd:dir=../crds
