"""Run with `cd python && uv run python ../scripts/generate-config-schema.py`."""

import argparse
import json
from pathlib import Path

from shardpack.config import config_json_schema

parser = argparse.ArgumentParser(description="Generate the ShardPack configuration JSON Schema")
parser.add_argument("--check", action="store_true", help="Fail if the committed schema is stale")
args = parser.parse_args()
path = Path(__file__).resolve().parents[1] / "schemas" / "shardpack-config.schema.json"
text = json.dumps(config_json_schema(), indent=2, ensure_ascii=False) + "\n"
if args.check:
    if not path.exists() or path.read_text() != text:
        raise SystemExit("Configuration schema is stale; run scripts/generate-config-schema.py")
else:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
