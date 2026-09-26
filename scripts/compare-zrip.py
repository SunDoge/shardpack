"""Alternate two prebuilt zrip_upgrade benchmarks on one CPU; preserve raw CSV."""

import argparse
import csv
import statistics
import subprocess
from collections import defaultdict
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--old", required=True)
parser.add_argument("--new", required=True)
parser.add_argument("--jpeg-dir", required=True)
parser.add_argument("--cpu", default="0")
parser.add_argument("--pairs", type=int, default=5)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
results = defaultdict(list)
args.output.parent.mkdir(parents=True, exist_ok=True)
with args.output.open("w", newline="") as output:
    writer = csv.writer(output)
    writer.writerow(["pair", "version", "input", "level", "operation", "stored_bytes", "gib_s"])
    for pair in range(args.pairs):
        versions = ["old", "new"] if pair % 2 == 0 else ["new", "old"]
        for version in versions:
            command = ["taskset", "-c", args.cpu, getattr(args, version), args.jpeg_dir]
            completed = subprocess.run(command, check=True, text=True, capture_output=True)
            for row in csv.reader(completed.stdout.splitlines()):
                writer.writerow([pair, version, *row])
                name, level, operation, stored, speed = row
                results[(name, level, operation, version)].append(float(speed))
            output.flush()
            print(f"pair {pair + 1}/{args.pairs}: {version} completed", flush=True)
print("input,level,operation,old_median,new_median,change_pct")
for name, level, operation in sorted({key[:3] for key in results}):
    old = statistics.median(results[name, level, operation, "old"])
    new = statistics.median(results[name, level, operation, "new"])
    print(f"{name},{level},{operation},{old:.3f},{new:.3f},{(new / old - 1) * 100:+.1f}")
