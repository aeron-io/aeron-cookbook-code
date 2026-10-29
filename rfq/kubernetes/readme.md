# Kubernetes

## Running the sample

> Note: DNS can be slow in kubernetes, so you may need to wait a few seconds (typically 5 to 10) for the cluster to start up.

Step 1: Apply both the cluster and admin deployments

Step 2: Connect to the cluster from admin

By manually interacting with `kubectl`:

- Find the admin container's name with `kubectl get pods -n aeron-io-sample-admin`
- Connect to the admin container with `kubectl exec -it <POD NAME FROM ABOVE> -n aeron-io-sample-admin -- java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED -jar admin-uber.jar`

Using the provided scripts:

- Run `./k8s_connect_admin.sh` to connect to the admin container and connect to the cluster.

Step 3:

- within Admin, use the `connect` command to connect to the cluster

Step 4:

- interact with the cluster. See the [Admin](../admin/readme.md) for more details.

## Local Kubernetes

> **Note**: These steps are for local development only. Aeron is supported in production on Linux.

These assume that you are running either on a Linux machine with minikube installed, or on an Intel or Apple Silicon Mac with Docker Desktop and Kubernetes enabled or minikube installed.

### MiniKube

Step 1:
- install Docker Desktop
- allocate at least 6 cores and 15GB of RAM to Docker Desktop

Step 2:
- install minikube, as appropriate for your OS and CPU following instructions here https://minikube.sigs.k8s.io/docs/start/

Step 3:
- install the correct version of Kubernetes: `minikube start --kubernetes-version="v1.26.5" --driver="docker" --memory="15G" --cpus="6" --addons="registry" --embed-certs="true"`

Run `./minikube-run.sh` to build, deploy and run the cluster and admin.

### Docker Desktop with Kubernetes enabled

> **Note**: tested with Kubernetes `1.26.5` - there is no way to override the version of Kubernetes used by Docker Desktop. Minikube is recommended.

As with minikube, allocate at least 6 cores and 15GB of RAM to Docker Desktop.

Run `./docker-desktop-k8s-run.sh` to build, deploy and run the cluster and admin.

