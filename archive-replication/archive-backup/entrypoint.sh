#!/bin/sh

java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED \
  -Daeron.event.log=admin \
  -Daeron.event.archive.log=all \
  -Djava.net.preferIPv4Stack=true \
  -jar /root/jar/archive-backup-uber.jar
