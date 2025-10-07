# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

* Added retry for Kubernetes API calls ([#21](https://github.com/stjude-rust-labs/planetary/pull/21)).
* Log errors to the database ([#15](https://github.com/stjude-rust-labs/planetary/pull/15)).
* Added endpoints for getting task I/O and updating a task's outputs, which is
  now used from the transporter ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).
* Added support for Google Cloud Storage as a storage backend ([#12](https://github.com/stjude-rust-labs/planetary/pull/12)).
* Added support for S3 as a storage backend ([#11](https://github.com/stjude-rust-labs/planetary/pull/11)).
* Added orchestrator service ([#9](https://github.com/stjude-rust-labs/planetary/pull/9)).

#### Fixed

* A failure to start a task now properly transitions the task to
  `SYSTEM_ERROR` ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).
* All pods created by the orchestrator now contain resource requests ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).
* The `start_task` endpoint now runs its future from a new tokio task ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).
* Fixed comments related to memory constants that were using the wrong unit ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).
* Limit database connection pool to 10 connections and remove connections from
  the pool that haven't been used for 60 seconds ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).
