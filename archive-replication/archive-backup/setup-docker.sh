#!/bin/bash

echo "debconf debconf/frontend select noninteractive" | debconf-set-selections

apt-get update --quiet

apt-get dist-upgrade --quiet --assume-yes

apt-get install \
    --quiet \
    --assume-yes \
    --no-install-recommends \
    bash \
    wget \
    iproute2 \
    less \
    dnsutils \
    procps \
    sysstat

mkdir /root/aeron
mkdir /root/jar

wget https://repo1.maven.org/maven2/io/aeron/aeron-all/1.54.0/aeron-all-1.54.0.jar -P /root/aeron/

apt-get remove wget --quiet --assume-yes
