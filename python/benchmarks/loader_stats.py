"""Compare baseline, disabled and enabled instrumentation on GIL-releasing decode."""

import argparse
import json
import subprocess
import sys
import time
import types
import zlib

from shardpack import ThreadedLoader


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-ref", required=True)
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    source = subprocess.check_output(
        ["git", "show", f"{args.baseline_ref}:python/src/shardpack/loader.py"], text=True
    )
    baseline = types.ModuleType("shardpack._benchmark_baseline")
    sys.modules[baseline.__name__] = baseline
    # Benchmark only: baseline-ref must identify trusted local repository code.
    exec(compile(source, "baseline-loader.py", "exec"), baseline.__dict__)  # noqa: S102
    compressed = zlib.compress(bytes(range(256)) * 256)

    def decode(payload):
        return len(zlib.decompress(payload))

    for pair in range(args.rounds):
        modes = ["baseline", "disabled", "enabled"]
        modes = modes[pair % 3 :] + modes[: pair % 3]
        for mode in modes:
            cls = baseline.ThreadedLoader if mode == "baseline" else ThreadedLoader
            options = {} if mode == "baseline" else {"enable_stats": mode == "enabled"}
            loader = cls(
                [compressed] * 8192,
                decode=decode,
                batch_size=64,
                decode_workers=4,
                decode_prefetch=128,
                **options,
            )
            assert sum(sum(batch) for batch in loader) == 8192 * 65536
            started = time.perf_counter()
            iterations = 0
            while time.perf_counter() - started < 1.0:
                total = sum(sum(batch) for batch in loader)
                assert total == 8192 * 65536
                iterations += 1
            seconds = time.perf_counter() - started
            print(
                json.dumps(
                    {
                        "round": pair,
                        "mode": mode,
                        "seconds": seconds,
                        "records_per_second": iterations * 8192 / seconds,
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    main()
