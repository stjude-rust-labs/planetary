# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

* Added support for Google Cloud Storage as a storage backend ([#12](https://github.com/stjude-rust-labs/planetary/pull/12)).
* Added support for S3 as a storage backend ([#11](https://github.com/stjude-rust-labs/planetary/pull/11)).
* Added `--transporter-image` CLI option to specify the `planetary-transporter`
  image to use for inputs and outputs pods ([#8](https://github.com/stjude-rust-labs/planetary/pull/8)).
* Initial implementation of the full TES API and database abstraction ([#4](https://github.com/stjude-rust-labs/planetary/pull/4)).

#### Changed

* Refactored the orchestrator into its own service ([#9](https://github.com/stjude-rust-labs/planetary/pull/9)).
