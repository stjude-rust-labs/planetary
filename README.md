<img style="margin: 0px" alt="Repository Header Image"
src="./assets/repo-header.png" />

<hr/>

<p align="center">
  <p align="center">
    <a href="https://github.com/stjude-rust-labs/planetary/actions/workflows/CI.yml" target="_blank">
      <img alt="CI: Status" src="https://github.com/stjude-rust-labs/planetary/actions/workflows/CI.yml/badge.svg" />
    </a>
    <a href="https://crates.io/crates/planetary" target="_blank">
      <img alt="crates.io version" src="https://img.shields.io/crates/v/planetary">
    </a>
    <img alt="crates.io downloads" src="https://img.shields.io/crates/d/planetary">
    <a href="https://github.com/stjude-rust-labs/planetary/blob/main/LICENSE-APACHE" target="_blank">
      <img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache 2.0-blue.svg" />
    </a>
    <a href="https://github.com/stjude-rust-labs/planetary/blob/main/LICENSE-MIT" target="_blank">
      <img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg" />
    </a>
  </p>

  <p align="center">
    A Kubernetes-based task executor for the Task Execution Service (TES) specification.
    <br />
    <br />
    <a href="https://github.com/stjude-rust-labs/planetary/issues/new?assignees=&title=Descriptive%20Title&labels=enhancement">Request Feature</a>
    ·
    <a href="https://github.com/stjude-rust-labs/planetary/issues/new?assignees=&title=Descriptive%20Title&labels=bug">Report Bug</a>
    ·
    ⭐ Consider starring the repo! ⭐
    <br />
  </p>
</p>

## 🖥️ Development

To bootstrap a development environment, please use the following commands.

```bash
# Clone the repository
git clone git@github.com:stjude-rust-labs/planetary.git
cd planetary

# Build the crate in release mode
cargo build --release
```

## 🚧️ Tests

Before submitting any pull requests, please make sure the code passes the
following checks (from the root directory).

```bash
# Run the project's tests.
cargo test --all-features

# Run the tests for the examples.
cargo test --examples --all-features

# Ensure the project doesn't have any linting warnings.
cargo clippy --all-features

# Ensure the project passes `cargo fmt`.
cargo fmt --check

# Ensure the docs build.
cargo doc
```

## 🤝 Contributing

Contributions, issues and feature requests are welcome! Feel free to check
[issues page](https://github.com/stjude-rust-labs/planetary/issues).

## 📝 License

This project is licensed as either [Apache 2.0][license-apache] or
[MIT][license-mit] at your discretion. Additionally, please see [the
disclaimer](https://github.com/stjude-rust-labs#disclaimer) that applies to all
crates and command line tools made available by St. Jude Rust Labs.

Copyright © 2024-Present [St. Jude Children's Research
Hospital](https://github.com/stjude).

[license-apache]: https://github.com/stjude-rust-labs/planetary/blob/main/LICENSE-APACHE
[license-mit]: https://github.com/stjude-rust-labs/planetary/blob/main/LICENSE-MIT
[`sprocket`]: https://github.com/stjude-rust-labs/sprocket
