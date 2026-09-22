# Python release

The Python package is published by `.github/workflows/python-release.yml` using
PyPI Trusted Publishing. No PyPI API token is stored in GitHub.

## One-time PyPI setup

Sign in to PyPI with an account protected by two-factor authentication. Before
the first release, create a pending trusted publisher for the `shardpack`
project; after the project exists, the same values appear under the project's
Publishing settings:

```text
PyPI project:       shardpack
GitHub owner:       SunDoge
GitHub repository:  shardpack
Workflow filename:  python-release.yml
Environment name:   pypi
```

Every value is identity-sensitive. In particular, the workflow filename is only
the basename, while the environment must exactly match the workflow's `pypi`
environment. A pending publisher creates the project when the first trusted
release succeeds.

In the GitHub repository, create an environment named `pypi`. Protect it with a
required reviewer if releases should require manual approval. Do not add a
`PYPI_API_TOKEN` secret: the publish job requests a short-lived OIDC token with
the narrowly scoped `id-token: write` permission.

## Release procedure

1. Ensure the release commit is on `main` and the worktree is clean.
2. Set the same version in the workspace `Cargo.toml` and `python/pyproject.toml`.
3. Run the retained local checks:

   ```bash
   cargo test --workspace
   cargo clippy --workspace --all-targets -- -D warnings
   mise run test:python
   mise run test:python-torch
   mise run build:python
   ```

4. Commit the version change and create an annotated version tag:

   ```bash
   git tag -a v0.1.0 -m "ShardPack Python 0.1.0"
   git push origin main
   git push origin v0.1.0
   ```

The tag starts two independent builds: one CPython 3.11+ abi3
manylinux 2.17 x86-64 wheel and one source distribution. A separate job downloads
the exact artifacts, runs strict metadata checks, installs the wheel under the
minimum supported Python version, and exercises both CLI entry points. Only
those verified artifacts reach the `pypi` environment and the publish job.

`workflow_dispatch` builds and verifies artifacts without publishing them. Use
it to validate workflow changes before creating a release tag.

## Scope of the first release

Version 0.1.0 publishes a Linux x86-64 wheel because that is the platform tested
for the training workload. The source distribution remains available for users
with a Rust toolchain. Add aarch64, macOS, or Windows wheels only after their
native reader, encryption, OpenDAL backends, and CLI have run in corresponding
CI environments; building a cross-compiled wheel alone is not treated as support.

PyPI distributions are immutable. If a published artifact is wrong, increment
the package version and publish a new release rather than attempting to replace
the file.
