"""Reject mismatched release identity and store packaging before publication."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import zipfile

version = os.environ["PLUGIN_VERSION"]
assert re.fullmatch(r"\d+\.\d+\.\d+-plana\.\d+", version), version
if os.environ.get("GITHUB_REF_TYPE") == "tag":
    assert os.environ["GITHUB_REF_NAME"] == "v" + version
registry = json.loads(Path("registry.json").read_text())
assert [p["version"] for p in registry["plugins"] if p["id"] == "clickhouse-sink"] == [version]
doc = Path("PLANA.md").read_text()
assert set(re.findall(r"\d+\.\d+\.\d+-plana\.\d+", doc)) == {version}
assert "version `" + version + "`" in doc
name = "clickhouse-sink_" + version + "_linux_arm64.zip"
assert name in doc
if len(sys.argv) > 1:
    dist = Path(sys.argv[1])
    expected = hashlib.sha256((dist / name).read_bytes()).hexdigest() + "  " + name + "\n"
    assert (dist / "checksums.txt").read_text() == expected
    with zipfile.ZipFile(dist / name) as z, tempfile.TemporaryDirectory() as unpacked:
        assert z.namelist() == ["clickhouse-sink.so"], z.namelist()
        z.extractall(unpacked)
        subprocess.run([sys.executable, "scripts/verify_arm64_release.py", str(Path(unpacked) / "clickhouse-sink.so")], check=True)
print("Release identity verified, " + version)
