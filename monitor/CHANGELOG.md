# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

* Added an opt-in task resource usage sampler (`--usage-sample-interval` /
  `USAGE_SAMPLE_INTERVAL`) that reads the kubelet `/metrics/resource`
  endpoints of the nodes hosting task pods and records per-container
  aggregates in the database. Nodes are fetched concurrently (bounded, so a
  slow or unreachable node cannot delay sampling the rest). Kubelets are
  contacted directly at each node's address with the monitor's service
  account token, authorized via `get` on `nodes/metrics`, and their serving
  certificates are verified against a certificate authority bundle to the
  exclusion of any other trust anchor: the in-cluster bundle by default, or
  an override path (`--kubelet-ca-path` / `KUBELET_CA_PATH`) for clusters
  whose kubelets are issued certificates by a different certificate
  authority (`--kubelet-insecure-tls` / `KUBELET_INSECURE_TLS` instead skips
  verification entirely, for clusters with self-signed kubelet
  certificates, and logs a warning when enabled; `--kubelet-port` /
  `KUBELET_PORT` overrides the default port of 10250). Kubelet client
  initialization retries every sampling interval until it succeeds, rather
  than disabling sampling for the life of the process on the first failure
  ([#48](https://github.com/stjude-rust-labs/planetary/pull/48)).
* Added creating Kubernetes resources via a template ([#38](https://github.com/stjude-rust-labs/planetary/pull/38)).

#### Fixed

* Corrected several bugs in the monitor that prevented it from aborting tasks
  that no longer have running pods ([#29](https://github.com/stjude-rust-labs/planetary/pull/29)).

#### Dependencies

* Updated dependencies to latest ([#37](https://github.com/stjude-rust-labs/planetary/pull/37)).

## v0.1.0 (2025-10-13)

#### Added

* Log errors to the database ([#15](https://github.com/stjude-rust-labs/planetary/pull/15)).
* Added monitor service ([#9](https://github.com/stjude-rust-labs/planetary/pull/9)).
