"""Check tag, wheel/sdist metadata, and optionally the installed extension."""

import argparse
import email
import tarfile
import zipfile
from pathlib import Path

from release_version import python_version, workspace_version

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--tag")
parser.add_argument("--dist", type=Path)
parser.add_argument("--installed", action="store_true")
args = parser.parse_args()
version = workspace_version()
expected = python_version(version)
if args.tag and args.tag != f"v{version}":
    raise SystemExit(f"Tag {args.tag!r} does not match Cargo version {version!r}")
if args.dist:
    wheels = list(args.dist.glob("*.whl"))
    sdists = list(args.dist.glob("*.tar.gz"))
    if not wheels or not sdists:
        raise SystemExit("Both a wheel and an sdist are required")
    for artifact in wheels + sdists:
        if artifact.suffix == ".whl":
            with zipfile.ZipFile(artifact) as archive:
                names = [
                    n for n in archive.namelist() if n.endswith(".dist-info/METADATA")
                ]
                if len(names) != 1:
                    raise SystemExit(f"Ambiguous wheel metadata: {artifact}")
                raw = archive.read(names[0])
        else:
            with tarfile.open(artifact) as archive:
                names = [
                    n
                    for n in archive.getnames()
                    if n.count("/") == 1 and n.endswith("/PKG-INFO")
                ]
                if len(names) != 1:
                    raise SystemExit(f"Ambiguous sdist metadata: {artifact}")
                stream = archive.extractfile(names[0])
                if stream is None:
                    raise SystemExit(f"Missing sdist metadata: {artifact}")
                raw = stream.read()
        metadata = email.message_from_bytes(raw)
        if metadata["Name"] != "shardpack" or metadata["Version"] != expected:
            raise SystemExit(f"Unexpected distribution identity: {artifact}")
if args.installed:
    import importlib.metadata

    import shardpack

    if importlib.metadata.version("shardpack") != expected:
        raise SystemExit("Installed Python version differs from Cargo")
    if python_version(shardpack.__version__) != expected:
        raise SystemExit("Native extension version differs from Cargo")
print(f"Verified ShardPack {version} (Python {expected})")
