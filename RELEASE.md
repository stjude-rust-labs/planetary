# Release Process

This document describes how to create a new release of Planetary.

## Pre-Release Checklist

Before triggering the release workflow, ensure the following are completed:

- [ ] All tests pass: `cargo test --all-features`
- [ ] Linting passes: `cargo clippy --all-features -- -D warnings`
- [ ] Formatting is correct: `cargo fmt --check`
- [ ] Documentation builds: `cargo doc`
- [ ] Dependencies are up to date (run `cargo update` if needed)

## Triggering a Release

Planetary uses an on-demand GitHub Actions workflow to create releases.

### Steps

1. Go to the [Actions tab](../../actions) in the GitHub repository
2. Select the "Release" workflow from the left sidebar
3. Click "Run workflow" button
4. Select the version bump type: `patch`, `minor`, or `major`
5. Click "Run workflow"

## Post-Release

After the release workflow completes:

1. Verify the release appears on the [Releases page](../../releases)
2. Verify Docker images are available in [Packages](../../packages)
3. Verify crates are published on [crates.io](https://crates.io)
4. Test the release by installing or upgrading the Helm chart:
   ```bash
   helm upgrade --install planetary ./chart
   ```