#!/bin/bash

set -e

scriptPath=$(realpath $0)
scriptDir=$(dirname $scriptPath)

echo "Generating clientset and deepcopy code ..."
cd $scriptDir/hack
./update-codegen.sh

echo "Generating CRDs ..."
cd $scriptDir
./gen-crds.sh

# echo "Applying CRDs ..."
kubectl apply -f $scriptDir/../crds

echo "Building executables ..."
for cmd in cmd/*; do
    if [ -d $cmd ] && [ -f $cmd/main.go ]; then
        cmdname=$(basename $cmd)
        echo "Building $cmdname ..."
        go build -o "$scriptDir/bin/$cmdname" "$scriptDir/cmd/$cmdname/main.go"
    fi
done

echo "Done"
