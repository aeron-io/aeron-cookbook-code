#!/bin/sh

java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED \
  -Daeron.event.log=admin \
  -Djava.net.preferIPv4Stack=true \
  -jar /root/jar/aeron-mdc-subscriber-uber.jar
