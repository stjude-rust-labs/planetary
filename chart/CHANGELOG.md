# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

* Added `local.storage` to support local inputs and outputs ([#41](https://github.com/stjude-rust-labs/planetary/pull/41)).
* Added `transporter.storage.azure` values for Azure Storage authentication ([#27](https://github.com/stjude-rust-labs/planetary/pull/27)).
* Added a 15 minute TTL on the migration job ([#25](https://github.com/stjude-rust-labs/planetary/pull/25)).
* Addes dynamic egress network policy additions for cloud and user exceptions ([#34](https://github.com/stjude-rust-labs/planetary/pull/34)).
* Added `monitor.usageSampleInterval` to enable per-container task resource
  usage sampling from the kubelet `/metrics/resource` endpoints of the nodes
  hosting task pods, along with a conditional cluster role granting the
  monitor `get` on `nodes/metrics` and a `Recreate` rollout strategy for the
  monitor. Kubelets are contacted directly and their serving certificates
  are verified against a certificate authority bundle to the exclusion of
  any other trust anchor: the in-cluster bundle by default, or the bundle
  named by `monitor.kubeletCaSecretName` for clusters whose kubelets are
  issued certificates by a different certificate authority.
  `monitor.kubeletInsecureTls` instead skips verification entirely for
  clusters whose kubelets serve self-signed certificates (for example,
  `kind`), and `monitor.kubeletPort` overrides the default port of 10250.
  The task-level `peak_memory_bytes`, `avg_memory_bytes`, and `cpu_time_ms`
  task log metadata keys cover the task's executor containers, and the
  `resource_usage` key carries the per-container breakdown (`inputs`,
  `executor-N`, `outputs`). The `nodes/metrics` grant is the chart's only
  cluster-scoped permission; kubelet authorization maps it exclusively to
  the read-only `/metrics/*` paths, and the role is not created while
  sampling is disabled (the default)
  ([#48](https://github.com/stjude-rust-labs/planetary/pull/48)).

### Fixed

* Fixed the task pod's `outputs` container so that it also mounts the task's
  input files at the same paths the executors used. Previously, a task's
  work directory could still contain symlinks pointing at input files (for
  example, if the task's own cleanup step never ran because the task
  failed), and the `outputs` container had no way to resolve such
  symlinks, causing the output sweep to fail with a misleading "no such
  file or directory" error that masked the task's real failure. ([#50](https://github.com/stjude-rust-labs/planetary/pull/50))


## v0.1.0 (2025-10-13)

### Added

* Added automatic database migrations via a Kubernetes Job that runs on chart installation and upgrade ([#24](https://github.com/stjude-rust-labs/planetary/pull/24)).
* Added optional pod-based PostgreSQL database to Helm chart ([#23](https://github.com/stjude-rust-labs/planetary/pull/23)).
