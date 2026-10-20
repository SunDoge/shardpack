"""Run with mise exec -- python -m unittest discover -s scripts -p 'test_release.py'."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import tomllib
from release_version import ROOT, python_version, version_key


class VersionTests(unittest.TestCase):
    def test_workspace_release_configuration(self):
        workspace = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]
        config = tomllib.loads((ROOT / "release-plz.toml").read_text())
        packages = {package["name"]: package for package in config["package"]}
        for member in workspace["members"]:
            package = tomllib.loads((ROOT / member / "Cargo.toml").read_text())[
                "package"
            ]
            self.assertTrue(package["version"]["workspace"])
            self.assertEqual(
                packages[package["name"]]["changelog_path"], "CHANGELOG.md"
            )
        self.assertTrue(config["workspace"]["git_only"])
        self.assertFalse(config["workspace"]["publish"])
        self.assertFalse(config["workspace"]["release"])
        self.assertFalse(config["workspace"]["release_always"])
        self.assertEqual(
            [name for name, package in packages.items() if package.get("release")],
            ["shardpack-python"],
        )

    def test_pep440(self):
        for semver, pep440 in [
            ("0.1.0", "0.1.0"),
            ("1.2.3-alpha.0", "1.2.3a0"),
            ("1.2.3-beta.2", "1.2.3b2"),
            ("1.2.3-rc.1", "1.2.3rc1"),
        ]:
            self.assertEqual(python_version(semver), pep440)

    def test_order(self):
        versions = ["0.1.0", "0.2.0-alpha.1", "0.2.0-beta.1", "0.2.0-rc.1", "0.2.0"]
        self.assertEqual(sorted(versions, key=version_key), versions)

    def test_invalid(self):
        for value in ["v0.1.0", "01.0.0", "1.0", "1.0.0+local", "1.0.0-dev", "$(id)"]:
            with self.assertRaises(ValueError):
                version_key(value)


@unittest.skipUnless(
    shutil.which("release-plz") and shutil.which("uv"), "requires mise/uv"
)
class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="shardpack-release-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        files = {
            "Cargo.toml": """[workspace]
members = ["crates/shardpack-python", "crates/shardpack-core"]
resolver = "2"
[workspace.package]
version = "0.1.0"
""",
            "crates/shardpack-python/Cargo.toml": """[package]
name = "shardpack-python"
version.workspace = true
edition = "2021"
[lib]
crate-type = ["cdylib"]
""",
            "crates/shardpack-python/src/lib.rs": "pub fn placeholder() {}\n",
            "crates/shardpack-core/Cargo.toml": """[package]
name = "shardpack-core"
version.workspace = true
edition = "2021"
""",
            "crates/shardpack-core/src/lib.rs": "pub fn placeholder() {}\n",
            "python/pyproject.toml": """[build-system]
requires = ["maturin>=1.9,<2"]
build-backend = "maturin"
[project]
name = "shardpack"
dynamic = ["version"]
requires-python = ">=3.11"
[tool.maturin]
manifest-path = "../crates/shardpack-python/Cargo.toml"
bindings = "cffi"
""",
            ".gitignore": "target/\n**/__pycache__/\n",
        }
        for name, content in files.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        (self.root / "scripts").mkdir()
        for name in ["prepare-release.py", "release_version.py", "check-release.py"]:
            shutil.copy(ROOT / "scripts" / name, self.root / "scripts" / name)
        shutil.copy(ROOT / "release-plz.toml", self.root / "release-plz.toml")
        self.run_command("git", "init", "-b", "main")
        self.run_command("git", "config", "user.name", "Release Test")
        self.run_command("git", "config", "user.email", "test@example.invalid")
        self.run_command("cargo", "generate-lockfile")
        self.commit("feat: initial")

    def run_command(self, *args, success=True):
        result = subprocess.run(
            args,
            check=False,
            cwd=self.root,
            text=True,
            capture_output=True,
            env={**os.environ, "UV_PYTHON_DOWNLOADS": "never"},
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return result

    def commit(self, message):
        self.run_command("git", "add", ".")
        self.run_command("git", "commit", "-m", message)

    def prepare(self, version, **kwargs):
        return self.run_command(
            "python3", "scripts/prepare-release.py", version, **kwargs
        )

    def test_initial_release(self):
        self.prepare("0.1.0")
        self.assertIn("feat: initial", (self.root / "CHANGELOG.md").read_text())

    def test_python_only_release_and_prerelease(self):
        (self.root / "CHANGELOG.md").write_text(
            "# Changelog\n\n## [0.1.0] - 2026-01-01\n\nOriginal release.\n"
        )
        self.commit("chore: initial release notes")
        self.run_command("git", "tag", "v0.1.0")
        (self.root / "python" / "reader.py").write_text("# Python-only fix\n")
        self.commit("fix: Python reader")
        self.prepare("0.2.0-rc.1")
        self.assertIn('version = "0.2.0-rc.1"', (self.root / "Cargo.toml").read_text())
        self.assertIn('version = "0.2.0-rc.1"', (self.root / "Cargo.lock").read_text())
        self.assertIn("fix: Python reader", (self.root / "CHANGELOG.md").read_text())
        self.assertNotIn("feat: initial", (self.root / "CHANGELOG.md").read_text())
        self.assertIn(
            "## [0.1.0] - 2026-01-01", (self.root / "CHANGELOG.md").read_text()
        )
        self.run_command("python3", "scripts/check-release.py", "--tag", "v0.2.0-rc.1")
        self.run_command(
            "python3", "scripts/check-release.py", "--tag", "v0.2.0", success=False
        )

    def test_duplicate_and_dirty_rejected(self):
        self.run_command("git", "tag", "v0.1.0")
        self.prepare("0.1.0", success=False)
        (self.root / "uncommitted").touch()
        self.prepare("0.2.0", success=False)


if __name__ == "__main__":
    unittest.main()
