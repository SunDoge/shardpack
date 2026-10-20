# Python release

The Python package is published by `.github/workflows/python-release.yml` using
PyPI Trusted Publishing. No PyPI API token is stored in GitHub.

## Version ownership and automation

`Cargo.toml` → `[workspace.package].version` is the single product version.
All Rust crates inherit it; `python/pyproject.toml` declares `dynamic = ["version"]`
so maturin derives the Python distribution version from the binding crate.
For example, Cargo `0.2.0-rc.1` becomes Python `0.2.0rc1`; the Git tag remains
`v0.2.0-rc.1`. The `.spk` wire-format version is independent and must not be
bumped just because the package version changes.

The pinned release-plz CLI is managed by mise. `release-plz.toml` uses
`git_only = true`: **nothing is published to crates.io**, and only
`shardpack-python` anchors the shared `vX.Y.Z` tag and GitHub Release.

Release preparation deliberately takes an explicit version. release-plz's
automatic change detection is Cargo-package-scoped and does not reliably cover
the sibling `python/` tree. We use `release-plz set-version` plus a repository-wide
changelog, then `create-pull-request` to open a `release-plz-product` branch.
`release-plz release` recognizes this prefix with `release_always = false`;
ordinary pushes to main do not release.

References: [release-plz git-only configuration](https://release-plz.dev/docs/config),
[set-version](https://release-plz.dev/docs/usage/set-version),
[maturin metadata](https://www.maturin.rs/metadata.html).

## One-time GitHub App setup

Create and install a GitHub App restricted to this repository, with repository
permissions **Contents: Read and write** and **Pull requests: Read and write**.
Set these repository Actions settings:

```text
Variable RELEASE_APP_ID          App ID
Secret   RELEASE_APP_PRIVATE_KEY App private key (PEM)
```

Both release workflows use short-lived installation tokens. Unlike the default
`GITHUB_TOKEN`, these allow the generated PR and tag to trigger subsequent CI
workflows. See [release-plz token guidance](https://release-plz.dev/docs/github/token).
Without `RELEASE_APP_ID`, automatic release is disabled; preparing a PR requires
both settings. No App or external repository settings are created by these files.

Protect `main`, require release checks / Rust CI / wheel verification before
merging, and restrict the `release-plz-*` branch prefix to release automation.
Do not reuse that prefix for ordinary work: release-plz uses it to identify
release PRs. Review the release PR before merging; it is the publication approval.

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

1. Run the retained local checks:

   ```bash
   cargo test --workspace
   cargo clippy --workspace --all-targets -- -D warnings
   mise run test:python
   mise run test:python-torch
   mise run build:python
   ```

2. In Actions, run **Prepare release PR** on `main`, entering `0.1.0` for the
   initial release or a newer version afterward. Supported prereleases are
   `X.Y.Z-alpha.N`, `X.Y.Z-beta.N`, and `X.Y.Z-rc.N`. Build metadata is rejected.
3. Review the generated version/lock changes and `CHANGELOG.md`. Changelog entries
   cover all non-merge commits since the last reachable release tag, including
   Python-only fixes. Edit the notes on the PR if needed.
4. Wait for CI, then merge the PR. **Release product** creates the shared tag and
   GitHub Release; the App-created tag triggers **Publish Python package**.
5. Approve the `pypi` environment if required. Verify the published distribution.

To preview preparation locally on a clean checkout (changes files but does not
push or publish):

```bash
mise exec -- python3 scripts/prepare-release.py 0.1.0
```

Do not run `release-plz release-pr` directly: it can miss Python-only changes.
Repeated dispatch updates the same PR and regenerates its notes, so retain manual
edits elsewhere before rerunning. Only prepare one release at a time.

The tag starts two independent builds: one CPython 3.11+ abi3
manylinux 2.17 x86-64 wheel and one source distribution. A separate job downloads
the exact artifacts, runs strict metadata checks, installs the wheel under the
minimum supported Python version, and exercises both CLI entry points. Only
those verified artifacts reach the `pypi` environment and the publish job.
Tag, Cargo version, wheel/sdist metadata, and the installed native extension
version are checked for consistency. Release PRs also build and verify artifacts
without publishing.

`workflow_dispatch` builds and verifies artifacts without publishing them. Use
it to validate workflow changes before creating a release tag. If PyPI publishing
fails after a tag was created, rerun that tag's failed workflow; do not move the
tag. Check whether any artifacts were already accepted before retrying.

Automation regression tests (isolated temporary Git repositories, no publishing):

```bash
mise exec -- python3 -m unittest discover -s scripts -p test_release.py -v
```

## Scope of the first release

Version 0.1.0 publishes a Linux x86-64 wheel because that is the platform tested
for the training workload. The source distribution remains available for users
with a Rust toolchain. Add aarch64, macOS, or Windows wheels only after their
native reader, encryption, OpenDAL backends, and CLI have run in corresponding
CI environments; building a cross-compiled wheel alone is not treated as support.

PyPI distributions are immutable. If a published artifact is wrong, increment
the package version and publish a new release rather than attempting to replace
the file.
