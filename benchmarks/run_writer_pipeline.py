#!/usr/bin/env python3
"""Build, verify, warm up, and compare experimental writer pipelines."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent
MODES = ("direct", "staged", "payload")


def run(binary, mode, options, events, verify=False):
    command = [str(binary), "--mode", mode, "--events", str(events)]
    for key, value in options.items():
        command.extend(["--" + key, str(value)])
    if verify:
        command.append("--verify")
    result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout)


def report(rows):
    lines = [
        "# Writer pipeline experiment", "",
        "Medians across repetitions; brackets show min–max. CPU includes all worker threads,",
        "queue polling, pacing, and drain. Latencies are sampled successful recording calls.",
        "Rows with drops do not measure equal completed work. These are preliminary",
        "capture/encoding results, not full VIREL writer performance.", "",
        "| Scenario | Mode | CPU ns/accepted [range] | Producer p99 ns [range] | Dropped % median / max | Lossless runs | Accepted M/s |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for scenario in dict.fromkeys(r["scenario"] for r in rows):
        for mode in MODES:
            group = [r for r in rows if r["scenario"] == scenario and r["mode"] == mode]
            if not group:
                continue

            def distribution(key):
                values = [r[key] for r in group]
                return f"{statistics.median(values):.0f} [{min(values):.0f}–{max(values):.0f}]"

            drops = [100 * r["dropped"] / (r["accepted"] + r["dropped"]) for r in group]
            lossless = sum(r["dropped"] == 0 for r in group)
            throughput = statistics.median(r["accepted_per_second"] for r in group) / 1e6
            lines.append(f"| {scenario} | {mode} | {distribution('cpu_ns_per_accepted')} | "
                         f"{distribution('producer_p99_ns')} | {statistics.median(drops):.3f} / "
                         f"{max(drops):.3f} | {lossless}/{len(group)} | {throughput:.3f} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=200000, help="attempts per producer per run")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--compiler", default=os.environ.get("CXX", "clang++"))
    parser.add_argument("--output", type=Path, required=True, help="JSON results; Markdown written alongside")
    args = parser.parse_args()
    if args.events < 10000 or args.events > 100000000 or args.repeats < 1:
        parser.error("events must be 10000..100000000; repeats must be positive")
    scenarios = {
        "tiny": {"payload": 0, "rate": 200000},
        "string64": {"payload": 64, "rate": 200000},
        "string256-diverse": {"payload": 256, "cardinality": 4096, "rate": 200000},
        "rebase": {"payload": 64, "rotate": 4096, "rate": 200000},
        "four-producers": {"payload": 64, "producers": 4, "rate": 200000},
        "saturated": {"payload": 64, "rate": 0},
        "stalled": {"payload": 64, "rate": 200000, "stall-ms": 100},
    }
    rows = []
    with tempfile.TemporaryDirectory(prefix="virel-pipeline-") as directory:
        binary = Path(directory) / "writer_pipeline"
        build = [args.compiler, "-std=c++17", "-O3", "-DNDEBUG", "-Wall", "-Wextra",
                 "-Wpedantic", "-pthread", str(ROOT / "writer_pipeline.cpp"), "-o", str(binary)]
        subprocess.run(build, check=True)
        compiler = subprocess.run([args.compiler, "--version"], check=True,
                                  capture_output=True, text=True).stdout
        # Decode every accepted frame, including strings, producer order, and negative offsets.
        for mode in MODES:
            for payload in (0, 64, 256):
                options = {"payload": payload, "cardinality": 4096, "producers": 2, "rate": 100000}
                if mode != "direct":
                    options["rotate"] = 100
                result = run(binary, mode, options, 10000, verify=True)
                if not result["accepted"]:
                    raise RuntimeError("verification accepted no events")
            result = run(binary, mode, {"rate": 0, "stall-ms": 100}, 10000, verify=True)
            if not result["dropped"] or not result["accepted"]:
                raise RuntimeError("stalled verification must exercise acceptance and drops")
            run(binary, mode, {"payload": 64, "rate": 0}, 100000)  # unreported warm-up
        jobs = [(repeat, name, mode) for repeat in range(args.repeats)
                for name in scenarios for mode in MODES
                if not (name == "rebase" and mode == "direct")]
        random.Random(20261004).shuffle(jobs)
        for index, (repeat, name, mode) in enumerate(jobs):
            print(f"{index + 1}/{len(jobs)}: {name} {mode} repeat {repeat + 1}", flush=True)
            row = run(binary, mode, scenarios[name], args.events)
            row.update(scenario=name, repeat=repeat)
            rows.append(row)
    rows.sort(key=lambda r: (list(scenarios).index(r["scenario"]), MODES.index(r["mode"]), r["repeat"]))
    output = {
        "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "platform": platform.platform(), "machine": platform.machine(),
        "logical_cpus": os.cpu_count(), "compiler": compiler,
        "source_sha256": hashlib.sha256((ROOT / "writer_pipeline.cpp").read_bytes()).hexdigest(),
        "build_flags": build[1:-3], "events_per_producer": args.events,
        "repeats": args.repeats, "shuffle_seed": 20261004,
        "verification": "12 decoding/accounting runs passed; varint/ZigZag selftests on each invocation",
        "rows": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    args.output.with_suffix(".md").write_text(report(rows))
    print(report(rows))


if __name__ == "__main__":
    main()
