# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

#### Added

* Added per-container task resource usage aggregation: a new
  `task_container_usage` table keyed by task and container name, a batched
  `add_task_resource_usage_samples` database method, and reporting of the
  aggregates in task log metadata — the `peak_memory_bytes`,
  `avg_memory_bytes`, and `cpu_time_ms` keys cover the task's executor
  containers and the `resource_usage` key carries the per-container
  breakdown ([#48](https://github.com/stjude-rust-labs/planetary/pull/48)).
* Added `name` column to containers table ([#43](https://github.com/stjude-rust-labs/planetary/pull/43)).
* Added support retrieving usernames of tasks for template rendering ([#41](https://github.com/stjude-rust-labs/planetary/pull/41)).
* Added `username` column to `tasks` table to associate tasks with TES API
  users ([#40](https://github.com/stjude-rust-labs/planetary/pull/40)).

#### Dependencies

* Updated dependencies to latest ([#37](https://github.com/stjude-rust-labs/planetary/pull/37)).

## v0.1.0 (2025-10-13)

#### Added

* Added `errors` table and ability to log errors to the database ([#15](https://github.com/stjude-rust-labs/planetary/pull/15)).
* Added support for draining executing pods ([#9](https://github.com/stjude-rust-labs/planetary/pull/9)).
* Initial implementation PostgreSQL database support ([#4](https://github.com/stjude-rust-labs/planetary/pull/4)).

#### Changed

* Draining pod rows now only returns rows that are older than 5 minutes ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).

#### Fixed

* Inserting a pod now checks for the `SYSTEM_ERROR` task state ([#13](https://github.com/stjude-rust-labs/planetary/pull/13)).
