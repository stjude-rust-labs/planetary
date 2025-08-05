# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

* Added support for S3 as a storage backend (#[11](https://github.com/stjude-rust-labs/planetary/pull/11)).
* Added signal handling ([#9](https://github.com/stjude-rust-labs/planetary/pull/9)).
* Initial implementation of inputs and outputs using `cloud::copy` ([#4](https://github.com/stjude-rust-labs/planetary/pull/4)).

#### Fixed

* Fixed incorrect upload URLs when uploading an output directory ([#8](https://github.com/stjude-rust-labs/planetary/pull/8)).

#### Changed

* Refactored progress event handling to the `cloud` crate ([#9](https://github.com/stjude-rust-labs/planetary/pull/9)).
