FROM --platform=$BUILDPLATFORM golang:1.25-trixie AS builder
ARG TARGETOS
ARG TARGETARCH

RUN \
  apt-get update && apt-get install -y git && \
  go install k8s.io/code-generator/cmd/deepcopy-gen@latest && \
  go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest

RUN \
  mkdir -p /app && \
  git -C /app clone --recurse-submodules https://github.com/hsiaofongw/go-util.git && \
  git -C /app clone --recurse-submodules https://github.com/internetworklab/netapply.git && \
  git -C /app/netapply checkout dev && \
  cd /app/netapply && \
    deepcopy-gen ./pkg/interface/wireguard && \
    deepcopy-gen ./pkg/bird && \
    deepcopy-gen ./pkg/interface/common

WORKDIR /app/go-controllers

COPY go.mod go.mod
COPY go.sum go.sum

RUN \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} go mod download
    

WORKDIR /app/go-controllers
COPY . .

RUN \
    (cd /app/go-controllers/hack && ./update-codegen.sh) && \
    (cd /app/go-controllers && ./gen-crds.sh)

RUN \
    cd /app/go-controllers && \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o bin/wgng-controller ./cmd/wgng-controller && \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o bin/birdbgp-controller ./cmd/birdbgp-controller

FROM debian:trixie

COPY --from=builder /app/go-controllers/bin/wgng-controller /usr/local/bin/wgng-controller
COPY --from=builder /app/go-controllers/bin/birdbgp-controller /usr/local/bin/birdbgp-controller
