"""Prepare local release changes; never pushes, tags, or publishes."""

import argparse
import datetime
import subprocess

from release_version import ROOT, version_key, workspace_version


def run(*args: str) -> str:
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    args = parser.parse_args()
    target = version_key(args.version)
    if run("git", "status", "--porcelain"):
        raise SystemExit("Release preparation requires a clean worktree")
    if target < version_key(workspace_version()):
        raise SystemExit("Cannot lower the workspace version")
    tags = run("git", "tag", "--merged", "HEAD", "--list", "v*").splitlines()
    versions = []
    for tag in tags:
        try:
            versions.append((version_key(tag[1:]), tag))
        except ValueError:
            continue
    previous = max(versions, default=None)
    if previous and target <= previous[0]:
        raise SystemExit("Version must be newer than the latest reachable release tag")
    if run("git", "tag", "--list", f"v{args.version}"):
        raise SystemExit("Release tag already exists")
    revision = f"{previous[1]}..HEAD" if previous else "HEAD"
    entries = run("git", "log", "--no-merges", "--format=- %s (%h)", revision)
    if not entries:
        raise SystemExit("No commits to release")
    changelog = ROOT / "CHANGELOG.md"
    existing = changelog.read_text() if changelog.exists() else "# Changelog\n"
    date = datetime.datetime.now(datetime.UTC).date().isoformat()
    heading = f"## [{args.version}] - {date}"
    changelog.write_text(
        "# Changelog\n\n"
        + heading
        + "\n\n"
        + entries
        + "\n\n"
        + existing.removeprefix("# Changelog").lstrip()
    )
    # set-version also updates changelogs; write the new entry first so it
    # cannot rename the previous release's heading.
    run("release-plz", "set-version", args.version)
    subprocess.run(["uv", "lock", "--project", "python"], cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
