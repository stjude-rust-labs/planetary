# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Fixed

* Corrected several bugs in the monitor that prevented it from aborting tasks
  that no longer have running pods ([#29](https://github.com/stjude-rust-labs/planetary/pull/29)).

## v0.1.0 (2025-10-13)

#### Added

* Log errors to the database ([#15](https://github.com/stjude-rust-labs/planetary/pull/15)).
* Added monitor service ([#9](https://github.com/stjude-rust-labs/planetary/pull/9)).
