"""Shared Cargo/SemVer -> Python version validation (Python 3.11+, no deps)."""

import re
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(
    r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-(alpha|beta|rc)\.(0|[1-9]\d*))?"
)


def version_key(value: str) -> tuple[int, ...]:
    match = VERSION.fullmatch(value)
    if not match:
        raise ValueError(
            "Use X.Y.Z or X.Y.Z-{alpha,beta,rc}.N; build metadata is not supported"
        )
    major, minor, patch, stage, number = match.groups()
    return (
        int(major),
        int(minor),
        int(patch),
        {"alpha": 0, "beta": 1, "rc": 2, None: 3}[stage],
        int(number or 0),
    )


def python_version(value: str) -> str:
    version_key(value)
    return value.replace("-alpha.", "a").replace("-beta.", "b").replace("-rc.", "rc")


def workspace_version() -> str:
    return tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"][
        "version"
    ]
