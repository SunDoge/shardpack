"""Copy the prepared COCO train layout into an isolated encrypted S3 prefix.

Credentials are loaded from an existing private TOML, never serialized to results.
The key is created with mode 0600 on tmpfs. Remote test objects are retained.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import time
import tomllib
import uuid
from array import array
from pathlib import Path

from shardpack import Dataset, DatasetWriter


def scoped_config(spec):
    with Path(spec["config_path"]).open("rb") as file:
        base = tomllib.load(file)
    store = dict(base["stores"][spec["store"]])
    if store["type"] != "s3":
        raise ValueError("expected S3 store")
    suffix = spec["root_suffix"]
    if not suffix.startswith("experiments/coco-s3-") or ".." in suffix.split("/"):
        raise ValueError("invalid experiment prefix")
    store["root"] = "/".join(filter(None, [(store.get("root") or "").strip("/"), suffix]))
    return {"default_store": spec["store"], "stores": {spec["store"]: store}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--config-path", type=Path, required=True)
    parser.add_argument("--store", default="internal")
    args = parser.parse_args()
    run_id = "coco-s3-" + uuid.uuid4().hex
    local = args.work / run_id
    local.mkdir(mode=0o700)
    cache = local / "plaintext-cache"
    cache.mkdir(mode=0o700)
    key_path = Path("/dev/shm") / f"shardpack-{run_id}.key"
    fd = os.open(key_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "wb") as file:
        file.write(secrets.token_bytes(32))
    spec = {
        "config_path": str(args.config_path.resolve()),
        "store": args.store,
        "root_suffix": "experiments/" + run_id,
        "encryption_key_id": run_id,
        "encryption_key_path": str(key_path),
        "cache_dir": str(cache),
        "state": "building",
    }
    path = local / "source.json"
    path.write_text(json.dumps(spec, indent=2))
    print(f"source_spec={path}", flush=True)
    remote = scoped_config(spec)
    manifest = json.loads((args.work / "experiment.json").read_text())
    train = manifest["splits"]["train"]
    source = Dataset(
        train["layout"],
        config={
            "default_store": "local",
            "stores": {"local": {"type": "filesystem", "root": args.work / "objects"}},
        },
    )
    writer = DatasetWriter(
        bytes.fromhex(train["selection_sha256"]),
        local / "staging",
        config=remote,
        target_shard_bytes=128 * 1024**2,
        target_block_bytes=4 * 1024**2,
        encryption_key_id=run_id,
        encryption_key_path=key_path,
    )
    started = time.perf_counter()
    count = payload_bytes = 0
    for record in source.records():
        writer.write_batch(record.payload, array("Q", [0, len(record)]), record.sample_id)
        count += 1
        payload_bytes += len(record)
        if count % 1024 == 0:
            print(f"queued_records={count}", flush=True)
    layout = writer.finish()
    assert count == source.record_count
    uploaded = Dataset(layout, config=remote)
    assert uploaded.record_count == count
    spec.update(
        state="ready",
        train_layout=layout,
        records=count,
        payload_bytes=payload_bytes,
        stored_bytes=uploaded.stored_bytes,
        shards=uploaded.shard_count,
        upload_seconds=time.perf_counter() - started,
    )
    path.write_text(json.dumps(spec, indent=2))
    print(
        json.dumps(
            {k: spec[k] for k in ("state", "records", "shards", "stored_bytes", "upload_seconds")}
        )
    )


if __name__ == "__main__":
    main()
