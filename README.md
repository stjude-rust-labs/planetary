<img style="margin: 0px" alt="Repository Header Image"
src="./assets/repo-header.png" />

<hr/>

<p align="center">
  <p align="center">
    <a href="https://github.com/stjude-rust-labs/planetary/actions/workflows/CI.yml" target="_blank">
      <img alt="CI: Status" src="https://github.com/stjude-rust-labs/planetary/actions/workflows/CI.yml/badge.svg" />
    </a>
    <a href="https://crates.io/crates/planetary" target="_blank">
      <img alt="crates.io version" src="https://img.shields.io/crates/v/planetary">
    </a>
    <img alt="crates.io downloads" src="https://img.shields.io/crates/d/planetary">
    <a href="https://github.com/stjude-rust-labs/planetary/blob/main/LICENSE-APACHE" target="_blank">
      <img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache 2.0-blue.svg" />
    </a>
    <a href="https://github.com/stjude-rust-labs/planetary/blob/main/LICENSE-MIT" target="_blank">
      <img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg" />
    </a>
  </p>

  <p align="center">
    A Kubernetes-based task executor for the Task Execution Service (TES) specification.
    <br />
    <br />
    <a href="https://github.com/stjude-rust-labs/planetary/issues/new?assignees=&title=Descriptive%20Title&labels=enhancement">Request Feature</a>
    ·
    <a href="https://github.com/stjude-rust-labs/planetary/issues/new?assignees=&title=Descriptive%20Title&labels=bug">Report Bug</a>
    ·
    ⭐ Consider starring the repo! ⭐
    <br />
  </p>
</p>

## 📚 About Planetary

Planetary is an implementation of the [GA4GH Task Execution Service (TES) API](https://github.com/ga4gh/task-execution-schemas)
that executes tasks in a [Kubernetes](https://kubernetes.io/) cluster.

### ⚠️ Security Notice

Though we've made a best-effort attempt to incorporate security best practices
into Planetary and its associated Helm chart where possible, Planetary itself
does not claim to be an end-to-end, fully secure application and
deployment.

Ensuring a high level of security is inherently contextual, and requirements
vary significantly depending on your environment and operational policies. As
such, several additional steps are needed to effectively deploy and secure your
Kubernetes environment, the Planetary Helm chart, and the Planetary application.

**It is your responsibility** to implement, verify, and continuously monitor all
aspects of security to ensure they meet your standards and adequately protect
your environment.

As a starting point, we recommend you review the following aspects of your
deployment and the Helm chart. Keep in mind that this list is non-exhaustive, so
you should do your own research to create a complete list of concerns to
consider in your situation.

- **Ingress and external exposure.** The services provided by Planetary
  include **no authentication or authorization at all and will accept and
  execute unauthenticated requests if you do not take action to secure them
  further**. It is imperative that you restrict these services using external
  authentication and authorization mechanisms. Further, we encourage you to
  review any Services or Ingress resources included in the chart to confirm they
  are exposed only as needed. Ensure TLS termination, authentication,
  authorization, and rate-limiting are configured according to your policies,
  and integrate the service with a web application firewall.
- **RBAC permissions.** We've scoped roles and bindings according to what we
  believe to be least-privilege access. Review these to ensure they align with
  your internal access control policies and compliance standards.
- **Pod security contexts.** We’ve configured workloads to run as non-root
  and use read-only file systems where possible. These measures help limit
  container privileges and reduce the attack surface. Confirm these settings
  meet your container hardening requirements for all deployments and containers.
- **Network policies.** We include policies to restrict pod-to-pod ingress
  and egress traffic, balancing security with ease of configuration. Validate
  that these meet your segmentation, isolation, and ingress/egress control
  goals.
- **Secrets management.** We've defined Kubernetes Secrets where needed and
  structured their use for secure injection. Verify these secrets meet your
  standards for confidentiality, access control, and rotation.
- **Credentials.** This chart does not provide default passwords or API
  keys. You must set secure, unique credentials for all components before
  deployment.
- **Resource limits and requests.** We've set default CPU and memory
  constraints to encourage efficient scheduling and prevent resource exhaustion.
  Adjust these for your expected workloads.
- **Container images and supply chain security.** We use version-pinned
  images from known registries. Review these to ensure they come from sources
  you trust, are vulnerability-scanned, and, where appropriate, have verified
  signatures.
- **Monitoring, logging, and alerting.** Implement robust observability for
  timely incident detection and response. Configure logging to avoid exposing
  sensitive information, and ensure log retention complies with your policies.
- **Automated compliance scanning.** Use automated Kubernetes security
  scanners to verify compliance with your organizational policies.
- **Upgrades and patch management.** Keep Planetary and its dependencies
  up-to-date. We may publish chart updates in response to vulnerabilities, but
  you are responsible for monitoring for published updates and applying them
  promptly.
- **Cluster environment assumptions.** This chart assumes it is deployed
  into a hardened Kubernetes cluster (e.g., restricted API server access,
  encrypted etcd, secure CSI drivers). Ensure your cluster meets these
  prerequisites.
- **Backups and disaster recovery.** If Planetary stores persistent state in
  your environment, implement a tested backup and restore process.

We recommend regularly auditing your deployment to ensure continued security and
operational integrity. Default settings may not be sufficient for all use
cases—review and customize configurations as appropriate for your environment.

### Architecture

Planetary is made up of five components:

* _TES API Service_ - this component is responsible for handling requests from
  TES API clients; it interacts with the _Orchestrator Service_ and the
  _Planetary Database_.

* _Orchestrator Service_ - this component is responsible for creating and
  managing the Kubernetes resources used to execute tasks; it interacts with
  the _Kubernetes API Server_ and the _Planetary Database_.

* _Task Monitoring Service_ - this component is responsible for watching for
  orphaned pods and requesting an orchestrator to adopt the pod; it interacts
  with the _Orchestrator Service_ and the _Planetary Database_.

* _Planetary Database_ - this component stores the state of the TES tasks and
  the Kubernetes pods used to execute tasks; [PostgreSQL](https://www.postgresql.org)
  is currently the only supported database.

* _Transporter_ - this component is a responsible for downloading
  task inputs from cloud storage to the Kubernetes storage before a task
  starts and also for uploading task outputs from Kubernetes storage to cloud
  storage after a task has completed.

There are currently four images created for use with Planetary:

* `stjude-rust-labs/planetary-api` - implements the _TES API Service_.

* `stjude-rust-labs/planetary-orchestrator` - implements the _Orchestrator
  Service_.

* `stjude-rust-labs/planetary-monitor` - implements the _Task Monitor Service_.

* `stjude-rust-labs/planetary-transporter` - implements the _Transporter_ used
  for downloading task inputs and uploading task outputs.

### API Server Request Authentication

The Planetary API server ***performs no authentication of requests***.

However, requests must be associated with a username in one of two ways (in
order of precedence):

* The request includes a `X-Forwarded-User` header. This is typically set by
  upstream authentication proxies that sit between the TES client and Planetary.

* The request includes a basic `Authorization` header with a non-empty
  username (***the password is ignored***). This can be used for testing
  purposes with TES clients that support basic authentication.

If a TES API request has neither header present, a 403 is returned from the
Planetary API Server.

Planetary uses the username specified in the header to associate tasks with
that user.

The user may only retrieve, list, and cancel tasks with the _same_
username.

### Supported Cloud Storage

Planetary supports the following cloud storage services:

* [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/)
  : authentication is performed via signed requests using a Azure Storage
  shared access key.

* [AWS S3](https://aws.amazon.com/s3/): authentication is performed via signed
  requests using an AWS Access Key Id and AWS Secret Access Key.

* [Google Cloud Storage](https://cloud.google.com/storage): authentication is
  performed via signed requests using an [HMAC Access Key and HMAC Secret](https://cloud.google.com/storage/docs/authentication/hmackeys).

See the [`transporter.storage` Helm chart values](./chart/values.yaml) for
configuring a Planetary deployment.

### Task Execution

Planetary runs each TES task by evaluating a template file for creating
Kubernetes resources associated with the task.

The task resource template is stored in a `ConfigMap` resource named
`templates`. See [`configmap.yaml`](./chart/templates/configmap.yaml)
for more information.

By default, the template creates a `PersistentVolumeClaim` resource to
provision storage for the task and a single `Pod` resource for executing the
task.

The task pod consists of several containers:

* An _init container_ for downloading inputs.
* An _init container_ for each TES task executor.
* A _main container_ for uploading outputs.

Because [TES task executors are expected to be run sequentially on the same node](https://github.com/ga4gh/task-execution-schemas/issues/208#issuecomment-2341853166),
the executors cannot be sidecar or main containers of the pod as they run
in parallel.

As a result of running executors as _init containers_, a TES task state may be
incongruent with the Kubernetes pod state.

If you run `kubectl get pods -n planetary-tasks`, you may see task pods in one
of the following states:

* `Pending` - this state means the TES task is `QUEUED`.
* `Init:0/N` - this state means the TES task is `INITIALIZING` and the inputs
  init container may be running.
* `Init:X/N` (where `X` >= 1) - this state means the TES task is `RUNNING` and
  one of the executor init containers may be running.
* `Running` - this state means the TES task is `RUNNING` and the outputs
  container is executing.
* `Completed` - this state means the TES task is in the `COMPLETED` state.
* `Failed` - this state means the TES task is likely in the `EXECUTOR_ERROR` or
  `SYSTEM_ERROR` state.

Running a TES task as a single pod has some performance benefits:

* Fewer Kubernetes resources are created to execute the task, reducing load on
  the Kubernetes control plane.
* A task's execution is "atomic" in the sense that the task is scheduled to a
  single node and runs its executors immediately one after the other in
  sequence.
* Because a persistent volume must be shared between the input, executors, and
  output containers, using a single pod greatly reduces the load on a Container
  Storage Interface (CSI) driver that may be responsible for provisioning
  volumes via cloud storage APIs.
* By attaching a volume at most once to a node for a task, a pod being
  scheduled on one node doesn't need to wait for the same volume to detach from
  a node where a task pod was executing previously.

#### RBAC Authorization

RBAC authorization is used to grant permissions to the Planetary orchestrator
and monitor for creating and deleting Kubernetes resources, respectively.

If additional resource kinds are required in the task template, ensure that the
Planetary orchestrator is granted the `create` verb and that the Planetary
monitor is granted the `delete` verb for the resource.

See [`rbac.yaml`](./chart/templates/rbac.yaml) for more information.

## 🚀 Getting Started

### Cloning the Repository

To clone the repository, please use the following command:

```bash
git clone git@github.com:stjude-rust-labs/planetary.git
cd planetary
```

### Installing Docker

Local container tooling is required for building the images that can be
deployed to the Kubernetes cluster.

This guide assumes [Docker](https://www.docker.com/) is installed locally for
building container images.

### Installing `kind`

This guide uses [`kind`](https://kind.sigs.k8s.io/) for running a local
Kubernetes cluster for testing Planetary.

To install `kind`, please consult the [installation instructions](https://kind.sigs.k8s.io/docs/user/quick-start#installation) for your operating system.

### Installing `kubectl`

The `kubectl` tool is used to manage a Kubernetes cluster.

To install `kubectl`, please consult the [installation instructions](https://kubernetes.io/docs/tasks/tools/#kubectl) for your operating system.

### Installing `helm`

This guide uses [`helm`](https://helm.sh/) for installing Kubernetes packages
(called _charts_).

To install `helm`, please consult the [installation instructions](https://helm.sh/docs/intro/install/) for your operating system.

### Creating the Kubernetes Cluster

To create a local Kubernetes cluster using `kind`, run the following command:

```bash
kind create cluster --config kind-config.yml
```

This guide will be installing Planetary into a Kubernetes namespace named
`planetary`.

The installation of Planetary will also create a Kubernetes namespace named
`planetary-tasks` for managing resources relating to executing TES tasks.

## 🏗️ Building Planetary

### Building the Container Images

To build the `stjude-rust-labs/planetary-api` container image, run the
following command:

```bash
docker build . --target planetary-api --tag stjude-rust-labs/planetary-api:staging
```

To build the `stjude-rust-labs/planetary-orchestrator` container image, run the
following command:

```bash
docker build . --target planetary-orchestrator --tag stjude-rust-labs/planetary-orchestrator:staging
```

To build the `stjude-rust-labs/planetary-orchestrator` container image, run the
following command:

```bash
docker build . --target planetary-monitor --tag stjude-rust-labs/planetary-monitor:staging
```

To build the `stjude-rust-labs/planetary-transporter` container image,
run the following command:

```bash
docker build . --target planetary-transporter --tag stjude-rust-labs/planetary-transporter:staging
```

To build the `stjude-rust-labs/planetary-migration` container image (for database migrations),
run the following command:

```bash
docker build . --target planetary-migration --tag stjude-rust-labs/planetary-migration:staging
```

### Loading the Container Images

As `kind` cluster nodes run isolated from the host system, local container
images must be explicitly loaded onto the nodes. To load the images, run the
following command:

```bash
kind load docker-image -n planetary \
  stjude-rust-labs/planetary-api:staging \
  stjude-rust-labs/planetary-orchestrator:staging \
  stjude-rust-labs/planetary-monitor:staging \
  stjude-rust-labs/planetary-transporter:staging \
  stjude-rust-labs/planetary-migration:staging
```

## ✨ Deploying Planetary

> [!NOTE]
> The Planetary Helm chart includes an optional pod-based PostgreSQL database
> for local development and testing (enabled with `postgresql.enabled=true` in
> `./chart/examples/staging.yaml`).
>
> In this guide, we'll deploy Planetary using this
> ephemeral database. **This is for demonstration purposes only—for anything
> other than local testing, we recommend using an external managed database
> service that is backed up rather than the included pod-based database.**

### Installing NFS

The `planetary` orchestrator uses shared storage to communicate with task pods
to coordinate the transfer of remote inputs and outputs into the storage
attached to the pod.

It is up to the cluster administrator to provide this storage to the
orchestrator and task pods.

For the purpose of deploying `planetary` into a development Kubernetes cluster,
this guide will use an NFS server to provide the shared storage.

Install [`nfs-operator`](https://github.com/krestomatio/nfs-operator)
into the cluster:

```bash
kubectl apply -k 'https://github.com/krestomatio/nfs-operator/config/default?ref=v0.4.25'
```

`nfs-operator` will define a new Custom Resource Definition named `Ganesha` for
managing [nfs-ganesha](https://github.com/nfs-ganesha/nfs-ganesha) services.

### Installing Planetary

To deploy Planetary with the included PostgreSQL database into a local
Kubernetes cluster, run the following command:

```bash
helm upgrade --install --create-namespace -n planetary planetary chart \
  --set postgresql.password='<mypassword>' \
  -f chart/examples/staging.yaml
```

**Note:** Replace `<mypassword>` with a secure password of your choice.

The database schema will be automatically set up by a post-install Helm hook
that runs after the chart is deployed. The migration job waits for the database
to be ready (up to 10 minutes) and then runs the necessary migrations.

### Verifying the Deployment

Check the status of the Planetary pods to see if they are `Running`:

```bash
kubectl get pods -n planetary
```
Note: it may take a little while for the `planetary-orchestrator` and
`planetary-monitor` pods to become `Running` as they have dependencies on the
NFS service.

Once the Planetary pods are running, you may forward the service port for
accessing the TES API locally:

```bash
kubectl port-forward -n planetary service/planetary-api 8080:8080
```

Send a test request for getting the service information:

```bash
curl -v http://localhost:8080/v1/service-info
```

Congratulations, Planetary is now ready to receive requests 🎉!

### Deploying Development Changes

To deploy development changes, rebuild and load the container images from the
[Building the Container Images](#building-the-container-images) and [Loading the Container Images](#loading-the-container-images)
sections above.

Restart the Planetary deployment with the following commands:

```
kubectl rollout restart -n planetary deployments/planetary-api
kubectl rollout restart -n planetary deployments/planetary-orchestrator
kubectl rollout restart -n planetary deployments/planetary-monitor
```

## 🧠 Running Automated Tests

This section coming soon!

## ✅ Submitting Pull Requests

Before submitting any pull requests, please make sure the code passes the
following checks (from the root directory).

```bash
# Run the project's tests.
cargo test --all-features

# Run the tests for the examples.
cargo test --examples --all-features

# Ensure the project doesn't have any linting warnings.
cargo clippy --all-features

# Ensure the project passes `cargo fmt`.
cargo fmt --check

# Ensure the docs build.
cargo doc
```

## 🤝 Contributing

Contributions, issues and feature requests are welcome! Feel free to check
[issues page](https://github.com/stjude-rust-labs/planetary/issues).

## 📝 License

This project is licensed as either [Apache 2.0][license-apache] or
[MIT][license-mit] at your discretion. Additionally, please see [the
disclaimer](https://github.com/stjude-rust-labs#disclaimer) that applies to all
crates and command line tools made available by St. Jude Rust Labs.

Copyright © 2024-Present [St. Jude Children's Research
Hospital](https://github.com/stjude).

[license-apache]: https://github.com/stjude-rust-labs/planetary/blob/main/LICENSE-APACHE
[license-mit]: https://github.com/stjude-rust-labs/planetary/blob/main/LICENSE-MIT
[`sprocket`]: https://github.com/stjude-rust-labs/sprocket
