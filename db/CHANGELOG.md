# Changelog

## Unreleased


All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.1.0 (2025-10-13)

#### Added

* Added `errors` table and ability to log errors to the database ([#15](https://github.com/stjude-rust-labs/planetary/pull/15)).
* Added support for draining executing pods ([#9](https://github.com/stjude-rust-labs/planetary/pull/9)).
* Initial implementation PostgreSQL database support ([#4](https://github.com/stjude-rust-labs/planetary/pull/4)).

#### Changed

* Draining pod rows now only returns rows that are older than 5 minutes ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).

#### Fixed

* Inserting a pod now checks for the `SYSTEM_ERROR` task state ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).
