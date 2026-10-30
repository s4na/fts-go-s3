#!/usr/bin/env python3
"""Summarize comparable experiment results; do not invent a missing baseline."""
import json
import pathlib
import sys


def main():
    baseline = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
    current = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
    for key in ("documents", "shards", "layout", "bloom"):
        if baseline.get(key) != current.get(key):
            raise ValueError(f"incompatible {key}: {baseline.get(key)!r} != {current.get(key)!r}")
    print("| Metric | main | Current | Difference |")
    print("|---|---:|---:|---:|")
    for group in ("uncached", "cached_manifest"):
        for key in ("queries", "requests", "received_body_bytes", "false_negatives", "false_positives", "p50_ms", "p95_ms"):
            old = baseline[group][key]
            new = current[group][key]
            print(f"| {group}.{key} | {old} | {new} | {new-old:+} |")


if __name__ == "__main__":
    main()
