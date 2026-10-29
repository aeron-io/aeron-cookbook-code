# Cluster RFQ

A simple RFQ cluster and command line client built using [Adaptive's Aeron Cluster QuickStart](https://github.com/AdaptiveConsulting/aeron-io-samples).

![RFQ Cluster](image/rfq-sample-flow.gif)

# Runtime requirements

- Linux - Aeron is supported in production on Linux only. macOS can be used to run these samples locally for development only (Windows is currently untested).
- Docker Compose 2.x - see [docker readme](docker/readme.md) for more details
- Kubernetes 1.26.x  - see [kubernetes readme](kubernetes/readme.md) for more details
- Minikube 1.31.x - if running Kubernetes with minikube. See [kubernetes readme](kubernetes/readme.md) for more details
