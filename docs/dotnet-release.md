# NuGet release

`SunDoge.ShardPack` currently ships a .NET 8 assembly and a **Linux x64 glibc**
native library. Windows, macOS, ARM64 and musl support are deferred. Package
versions follow `[workspace.package].version` in `Cargo.toml`. Native code is
built on Ubuntu 24.04; this is not a manylinux 2.17 build. Older glibc deployments
need separate compatibility validation.

## One-time configuration

On NuGet.org, sign in and open **Trusted Publishing**, then add a GitHub policy:

| Field | Value |
|---|---|
| Policy Name | `shardpack-release` (a descriptive label) |
| Repository Owner | `SunDoge` |
| Repository | `shardpack` |
| Workflow File | `dotnet-release.yml` |
| Environment | `nuget` |
| Package Glob Pattern | `SunDoge.ShardPack` |
| Scopes | Push new packages and package versions |

Allow creating new packages for the first publication, as well as updating
existing packages. Scope the policy to the exact package name above. No manual
first upload or long-lived API key is needed when the policy allows new packages.
Use the account/organization owner that should own the package.

In GitHub repository settings:

1. Under **Secrets and variables → Actions → Variables**, add `NUGET_USER` with
   your NuGet.org profile username, **not your email address**. This is a repository
   variable, not a secret or GitHub username unless your names happen to match.
2. Under **Environments**, create `nuget`. Configure reviewers if desired and
   permit the branch used to manually run the workflow (normally `main`).

The publish job uses `NuGet/login@v1` and `id-token: write` to exchange its GitHub
OIDC token for a short-lived NuGet API key immediately before uploading. Neither
a GitHub App nor a PAT is required. See [NuGet Trusted Publishing](https://learn.microsoft.com/en-us/nuget/nuget-org/trusted-publishing).

## Publish

1. Merge and tag a reviewed commit containing the .NET SDK, this workflow and
   its validation script. If using release-plz, wait for its GitHub release to
   finish. The old Python-only `v0.1.0` tag does not contain the .NET SDK; choose
   a new product version containing these changes instead of moving that tag.
2. Open **Actions → Publish NuGet package → Run workflow**. Leave the workflow
   branch on `main` and fill the required **tag** input, for example `v0.1.1`.
   You do not need to select a tag in GitHub's branch picker.
3. CI checks out `refs/tags/<input>`, validates the Cargo version, builds and tests
   the SDK, packs it, checks package identity and Linux x64 assets, and runs the
   tests as a consumer of the actual NuGet package.
4. The publish job downloads those verified artifacts and pushes to NuGet.org.
   Approve the `nuget` environment if configured. Wait for NuGet validation and
   indexing before installing the package.

The CLI equivalent, after replacing the example tag:

```sh
gh workflow run dotnet-release.yml --repo SunDoge/shardpack --ref main -f tag=v0.1.1
```

Ordinary pushes, tags, PRs, and the existing **.NET CI** workflow do not publish.
Invalid/missing tags or a version mismatch fail explicitly. Publishing is
immutable: never replace a published package/version or move a release tag.
If a run fails, inspect whether the package was accepted before retrying; a
duplicate upload intentionally fails rather than silently accepting different
bytes under the same version.

Local preparation (no upload):

```sh
mise run test:dotnet
dotnet pack dotnet/src/SunDoge.ShardPack -c Release -o dotnet/artifacts
python3 scripts/check-nuget-release.py --tag v0.1.0 --dist dotnet/artifacts
```

Use the tag matching your current Cargo version in the last command. Local
artifacts inherit the build host's architecture and libc; the release workflow
is the source of official Linux x64 artifacts.
