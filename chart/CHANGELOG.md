# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## v0.1.0 (2025-10-13)

### Added

* Added automatic database migrations via a Kubernetes Job that runs on chart installation and upgrade ([#24](https://github.com/stjude-rust-labs/planetary/pull/24)).
* Added optional pod-based PostgreSQL database to Helm chart ([#23](https://github.com/stjude-rust-labs/planetary/pull/23)).
