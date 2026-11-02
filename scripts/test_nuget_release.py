"""Regression coverage for the package publication gate."""

import runpy
import tempfile
import unittest
import zipfile
from pathlib import Path

VERIFY = runpy.run_path(str(Path(__file__).with_name("check-nuget-release.py")))[
    "verify_package"
]


class NuGetReleaseTests(unittest.TestCase):
    def package(
        self,
        directory,
        *,
        version="0.1.0",
        package_id="SunDoge.ShardPack",
        native_path="runtimes/linux-x64/native/libshardpack_ffi.so",
        machine=b"\x3e\x00",
        include_assembly=True,
    ):
        path = Path(directory) / "test.nupkg"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr(
                "SunDoge.ShardPack.nuspec",
                '<package xmlns="http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd">'
                f"<metadata><id>{package_id}</id><version>{version}</version></metadata></package>",
            )
            archive.writestr(
                native_path, b"\x7fELF\x02\x01" + bytes(10) + b"\x03\x00" + machine
            )
            if include_assembly:
                archive.writestr("lib/net8.0/SunDoge.ShardPack.dll", b"MZ")
        return path

    def test_valid_package_and_prerelease(self):
        with tempfile.TemporaryDirectory() as directory:
            for version in ["0.1.0", "0.2.0-rc.1"]:
                VERIFY(self.package(directory, version=version), version)

    def test_rejects_wrong_identity_platform_or_missing_assembly(self):
        cases = [
            {"version": "0.1.1"},
            {"package_id": "Other.Package"},
            {"native_path": "runtimes/win-x64/native/shardpack_ffi.dll"},
            {"machine": b"\xb7\x00"},  # ARM64 disguised under a linux-x64 path
            {"include_assembly": False},
        ]
        with tempfile.TemporaryDirectory() as directory:
            for options in cases:
                with self.subTest(options=options), self.assertRaises(ValueError):
                    VERIFY(self.package(directory, **options), "0.1.0")


if __name__ == "__main__":
    unittest.main()
