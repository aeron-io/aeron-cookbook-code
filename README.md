# Aeron Cookbook Code Samples

Samples which should be reviewed alongside Aeron Cookbook

> **Note**: This code in this repo is for demonstration purposes only and is not representative of a production system. Please contact info@aeron.io for help configuring your system.

> **Note**: Aeron is supported in production on Linux. Windows and macOS can be used for local development, but are not supported in production.

> **Note**: JDK 17+ requires that `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED` is added to the VM Options to run the examples.

Projects:
- `sbe-core` and `sbe-protocol` - a project defining an SBE schema, along with some tests showing how to use it.
- `ipc-core` and `async` - focused on Aeron IPC samples. Also contains the minimal Aeron one file sample.
- `aeron-core` and `aeron-mdc` - a sample Aeron UDP client and server, with basic UDP and Multi-destination cast. Multi-destination cast example includes docker.
- `archive-core` and `archive-multi-host` and `archive-replciation` - Aeron Archive samples, including multiple hosts under docker and archive replication across multiple nodes.
- `cluster-rsm` and `rfq` - Aeron Cluster samples, using SBE for the protocol
- `theory` - examples from the Distributed Systems Basics section

Requires:
- Java 21
- Gradle 8.5
- Docker (to run some samples)

 ![build](https://github.com/aeron-io/aeron-cookbook-code/workflows/JavaCI/badge.svg)
 
Sample code from Aeron cookbook.
