# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

* Added an opt-in task resource usage sampler (`--usage-sample-interval` /
  `USAGE_SAMPLE_INTERVAL`) that reads the kubelet `/metrics/resource`
  endpoints of the nodes hosting task pods and records per-container
  aggregates in the database. Kubelets are contacted directly at each node's
  address with the monitor's service account token, authorized via `get` on
  `nodes/metrics`, and their serving certificates are verified against the
  cluster certificate authority (`--kubelet-insecure-tls` /
  `KUBELET_INSECURE_TLS` skips verification for clusters with self-signed
  kubelet certificates, and `--kubelet-port` / `KUBELET_PORT` overrides the
  default port of 10250)
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
