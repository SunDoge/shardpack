"""Validate Cargo/tag identity and the Linux x64 NuGet distribution."""

import argparse
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

from release_version import version_key, workspace_version


def verify_package(package: Path, version: str) -> None:
    with zipfile.ZipFile(package) as archive:
        names = archive.namelist()
        manifests = [name for name in names if name.endswith(".nuspec")]
        if len(manifests) != 1:
            raise ValueError("Expected exactly one NuGet manifest")
        root = ET.fromstring(archive.read(manifests[0]))
        metadata = root.find("{*}metadata")
        if metadata is None:
            raise ValueError("Missing NuGet metadata")
        if metadata.findtext("{*}id") != "SunDoge.ShardPack":
            raise ValueError("Unexpected NuGet package ID")
        if metadata.findtext("{*}version") != version:
            raise ValueError("NuGet version does not match Cargo workspace version")
        native = [name for name in names if name.startswith("runtimes/")]
        expected = "runtimes/linux-x64/native/libshardpack_ffi.so"
        if native != [expected]:
            raise ValueError(
                f"Expected only the Linux x64 native asset, found {native}"
            )
        header = archive.read(expected)[:20]
        # ELF64, little endian, shared object, x86-64 (not a renamed foreign binary).
        if (
            len(header) != 20
            or header[:6] != b"\x7fELF\x02\x01"
            or header[16:20] != b"\x03\x00\x3e\x00"
        ):
            raise ValueError("Native asset is not an x86-64 ELF shared library")
        managed = "lib/net8.0/SunDoge.ShardPack.dll"
        if managed not in names or not archive.read(managed):
            raise ValueError("Missing .NET 8 SDK assembly")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--dist", type=Path)
    args = parser.parse_args()
    version = workspace_version()
    version_key(version)
    if args.tag != f"v{version}":
        raise SystemExit(f"Tag {args.tag!r} does not match Cargo version {version!r}")
    if args.dist:
        packages = list(args.dist.glob("*.nupkg"))
        if len(packages) != 1:
            raise SystemExit("Expected exactly one NuGet package")
        verify_package(packages[0], version)
    print(f"Verified SunDoge.ShardPack {version} (linux-x64)")


if __name__ == "__main__":
    main()
