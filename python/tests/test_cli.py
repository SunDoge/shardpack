import subprocess
import sys

import pytest

import shardpack
from shardpack._native import cli_main


def test_module_cli_reports_package_version() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "shardpack", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == f"shardpack {shardpack.__version__}"


def test_module_cli_propagates_usage_error_code_without_traceback() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "shardpack", "not-a-command"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "unrecognized subcommand" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["--help"], "Operate immutable ShardPack datasets"),
        (["import", "--help"], "Usage: shardpack import"),
    ],
)
def test_native_cli_renders_usage_rs_help(
    args: list[str], expected: str, capfd: pytest.CaptureFixture[str]
) -> None:
    assert cli_main(["shardpack", *args]) == 0
    captured = capfd.readouterr()
    assert expected in captured.out
    assert captured.err == ""
