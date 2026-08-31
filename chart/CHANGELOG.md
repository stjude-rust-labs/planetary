# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

* Added `monitor.usageSampleInterval` to enable per-container task resource
  usage sampling from the kubelet `/metrics/resource` endpoints of the nodes
  hosting task pods (through the Kubernetes API server's node proxy), along
  with a conditional cluster role granting the monitor `get` on `nodes/proxy`
  and a `Recreate` rollout strategy for the monitor. The task-level
  `peak_memory_bytes`, `avg_memory_bytes`, and `cpu_time_ms` task log
  metadata keys cover the task's executor containers, and the
  `resource_usage` key carries the per-container breakdown (`inputs`,
  `executor-N`, `outputs`). **Security note**: `get` on `nodes/proxy` is the
  chart's only cluster-scoped permission and permits read access to any
  kubelet endpoint on any node (pod specs, container logs, node stats; not
  exec/attach); the role is not created while sampling is disabled (the
  default)
  ([#48](https://github.com/stjude-rust-labs/planetary/pull/48)).
* Added `local.storage` to support local inputs and outputs ([#41](https://github.com/stjude-rust-labs/planetary/pull/41)).
* Added `transporter.storage.azure` values for Azure Storage authentication ([#27](https://github.com/stjude-rust-labs/planetary/pull/27)).
* Added a 15 minute TTL on the migration job ([#25](https://github.com/stjude-rust-labs/planetary/pull/25)).
* Addes dynamic egress network policy additions for cloud and user exceptions ([#34](https://github.com/stjude-rust-labs/planetary/pull/34)).

## v0.1.0 (2025-10-13)

### Added

* Added automatic database migrations via a Kubernetes Job that runs on chart installation and upgrade ([#24](https://github.com/stjude-rust-labs/planetary/pull/24)).
* Added optional pod-based PostgreSQL database to Helm chart ([#23](https://github.com/stjude-rust-labs/planetary/pull/23)).
