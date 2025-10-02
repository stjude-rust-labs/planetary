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

### Supported Cloud Storage

Planetary supports the following cloud storage services:

* [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/)
  : authentication is performed via the client sending input and outputs URLs 
  with SAS tokens; no configuration is required for Planetary to support Azure 
  Blob Storage.

* [AWS S3](https://aws.amazon.com/s3/) - authentication is performed via signed 
  requests using an AWS Access Key Id and AWS Secret Access Key.

* [Google Cloud Storage](https://cloud.google.com/storage) - authentication is 
  performed via signed requests using an [HMAC Access Key and HMAC Secret](https://cloud.google.com/storage/docs/authentication/hmackeys).

See the [Helm chart values](./chart/values.yaml) for configuring a Planetary deployment.

## ⚠️ Security Notice

Though we've made a best-effort attempt to incorporate security best practices
into Planetary and its associated Helm chart where possible, Planetary itself
does not claim to be include an end-to-end, fully secure application and
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

- [ ] **Ingress and external exposure.** The services provided by Planetary
  include **no authentication or authorization at all and will accept and
  execute unauthenticated requests if you do not take action to secure them
  further**. It is imperitive that you restrict these services using external
  authentication and authorization mechanisms. Further, we encourage you to
  review any Services or Ingress resources included in the chart to confirm they
  are exposed only as needed. Ensure TLS termination, authentication,
  authorization, and rate-limiting are configured according to your policies,
  and integrate the service with a web application firewall.
- [ ] **RBAC permissions.** We've scoped roles and bindings according to what we
  believe to be least-privilege access. Review these to ensure they align with
  your internal access control policies and compliance standards.
- [ ] **Pod security contexts.** We’ve configured workloads to run as non-root
  and use read-only file systems where possible. These measures help limit
  container privileges and reduce the attack surface. Confirm these settings
  meet your container hardening requirements for all deployments and containers.
- [ ] **Network policies.** We include policies to restrict pod-to-pod ingress
  and egress traffic, balancing security with ease of configuration. Validate
  that these meet your segmentation, isolation, and ingress/egress control
  goals.
- [ ] **Secrets management.** We've defined Kubernetes Secrets where needed and
  structured their use for secure injection. Verify these secrets meet your
  standards for confidentiality, access control, and rotation.
- [ ] **Credentials.** This chart does not provide default passwords or API
  keys. You must set secure, unique credentials for all components before
  deployment.
- [ ] **Resource limits and requests.** We've set default CPU and memory
  constraints to encourage efficient scheduling and prevent resource exhaustion.
  Adjust these for your expected workloads.
- [ ] **Container images and supply chain security.** We use version-pinned
  images from known registries. Review these to ensure they come from sources
  you trust, are vulnerability-scanned, and, where appropriate, have verified
  signatures.
- [ ] **Monitoring, logging, and alerting.** Implement robust observability for
  timely incident detection and response. Configure logging to avoid exposing
  sensitive information, and ensure log retention complies with your policies.
- [ ] **Automated compliance scanning.** Use automated Kubernetes security
  scanners to verify compliance with your organizational policies.
- [ ] **Upgrades and patch management.** Keep Planetary and its dependencies
  up-to-date. We may publish chart updates in response to vulnerabilities, but
  you are responsible for monitoring for published updates and applying them
  promptly.
- [ ] **Cluster environment assumptions.** This chart assumes it is deployed
  into a hardened Kubernetes cluster (e.g., restricted API server access,
  encrypted etcd, secure CSI drivers). Ensure your cluster meets these
  prerequisites.
- [ ] **Backups and disaster recovery.** If Planetary stores persistent state in
  your environment, implement a tested backup and restore process.

We recommend regularly auditing your deployment to ensure continued security and
operational integrity. Default settings may not be sufficient for all use
cases—review and customize configurations as appropriate for your environment.

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

To crate a local Kubernetes cluster using `kind`, run the following command:

```bash
kind create cluster --config kind-config.yml
```

This guide will be installing Planetary and its database server into a
Kubernetes namespace named `planetary`.

The installation of Planetary will also create a Kubernetes namespace named
`planetary-tasks` for managing resources relating to executing TES tasks.

### Installing PostgreSQL

For local development, it is best to install a small PostgreSQL server into the
test cluster.

To install PostgreSQL into your local cluster, run the following command:

```bash
helm install --create-namespace -n planetary planetary-database oci://registry-1.docker.io/bitnamicharts/postgresql --set image.repository=bitnamilegacy/postgresql --set image.tag=latest
```

If you wish to interact with the database from outside of the cluster, forward the PostgreSQL port:

```bash
kubectl port-forward -n planetary svc/planetary-database-postgresql 5432:5432
```

The database password is available as a secret:

```bash
kubectl get secret -n planetary planetary-database-postgresql -o jsonpath="{.data.postgres-password}" | base64 --decode && echo
```

### Installing `diesel`

[`diesel`](https://diesel.rs) is a ORM used by Planetary for storing and retrieving data from PostgreSQL.

It has commands for setting up a database and performing migrations.

To install `diesel`, please consult the [installation instructions](https://diesel.rs/guides/getting-started#installing-diesel-cli) for your operating system.

### Setting Up the Planetary Database

To create the Planetary database, run the following command, replacing `$PASSWORD` with the database server password retrieved from the secret above:

```bash
DATABASE_URL=postgres://postgres:$PASSWORD@localhost/planetary diesel setup
```

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

### Loading the Container Images

As `kind` cluster nodes run isolated from the host system, local container images must be explicitly loaded onto the nodes. To load the images, run the following command:

```bash
kind load docker-image -n planetary stjude-rust-labs/planetary-api:staging stjude-rust-labs/planetary-orchestrator:staging stjude-rust-labs/planetary-monitor:staging stjude-rust-labs/planetary-transporter:staging
```

## ✨ Deploying Planetary

To deploy Planetary into a local Kubernetes cluster, run the following command:

```bash
cd chart
helm install -n planetary planetary . --set api.image.tag=staging --set orchestrator.image.tag=staging --set monitor.image.tag=staging --set transporter.image.tag=staging
```

Check the status of a `planetary-api` pod to see if it is `Running`:

```bash
kubectl get pods -n planetary
```

Once the Planetary service is running, you may forward the service port for
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
