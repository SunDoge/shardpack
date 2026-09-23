"""Python entry point for the native ShardPack command-line interface."""

from __future__ import annotations

import sys
from typing import NoReturn

from ._native import cli_main


def main() -> NoReturn:
    """Run the native CLI with Python's argument vector."""
    raise SystemExit(cli_main(sys.argv))
