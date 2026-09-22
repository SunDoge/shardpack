import subprocess
import sys

import shardpack


def test_module_cli_reports_package_version() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "shardpack", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == f"shardpack {shardpack.__version__}"


def test_module_cli_propagates_clap_error_code_without_traceback() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "shardpack", "not-a-command"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "unrecognized subcommand" in result.stderr
    assert "Traceback" not in result.stderr
