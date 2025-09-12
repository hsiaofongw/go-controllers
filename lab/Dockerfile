FROM ubuntu:24.04

ARG TARGETPLATFORM

# Add FRRouting GPG key
ADD https://deb.frrouting.org/frr/keys.gpg /usr/share/keyrings/frrouting.gpg

ARG FRRVER="frr-stable"

RUN \
  echo deb '[signed-by=/usr/share/keyrings/frrouting.gpg]' https://deb.frrouting.org/frr \
  noble $FRRVER | tee -a /etc/apt/sources.list.d/frr.list

# Set Go environment variables
ENV GOPATH=/root/go
ENV GOBIN=$GOPATH/bin
ENV PATH=$PATH:$GOBIN

RUN \
  apt-get -y update && \
  apt-get -y install --no-install-recommends \
    curl \
    golang \
    ca-certificates \
    iproute2 \
    iputils-ping \
    dnsutils \
    wireguard \
    frr frr-pythontools \
    git && \
    rm -rf /var/lib/apt/lists/*

# Install Go development tools
RUN mkdir -p $GOBIN && \
    go install golang.org/x/tools/gopls@latest && \
    go install honnef.co/go/tools/cmd/staticcheck@latest

# Install kubectl
ARG KUBECTL_VERSION=v1.33.4
RUN echo "Installing kubectl ${KUBECTL_VERSION} for ${TARGETPLATFORM} ..."
ADD "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/${TARGETPLATFORM}/kubectl" /usr/local/bin/kubectl
RUN chmod +x /usr/local/bin/kubectl

