# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

* Added support for Google Cloud Storage as a storage backend ([#12](https://github.com/stjude-rust-labs/planetary/pull/12)).
* Added support for S3 as a storage backend ([#11](https://github.com/stjude-rust-labs/planetary/pull/11)).
* Added support for downloading remote files not from a supported cloud service ([#8](https://github.com/stjude-rust-labs/planetary/pull/8)).
* Initial implementation of the `cloud::copy` API with support for Azure blob
  storage ([#4](https://github.com/stjude-rust-labs/planetary/pull/4)).

#### Fixed

* Apply timeouts to `reqwest::Client` ([#10](https://github.com/stjude-rust-labs/planetary/pull/10)).

#### Changed

* Default parallelism is now 4 times the available parallelism ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).
* Refactored crate to a single `FileTransfer` implementation with cloud storage
  backends ([#9](https://github.com/stjude-rust-labs/planetary/pull/9)).
