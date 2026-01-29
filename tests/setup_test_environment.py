#!/usr/bin/env python3
"""Minimal test environment setup helper.
Creates required directories and optionally clears generated CSVs in `incoming/`.
"""
import argparse
from pathlib import Path
import shutil


def main(clean: bool) -> None:
    project_root = Path(__file__).resolve().parents[1]
    incoming = project_root / "data" / "incoming"
    checkpoints = project_root / "data" / "checkpoints"
    logs_dir = project_root / "logs"

    incoming.mkdir(parents=True, exist_ok=True)
    checkpoints.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    if clean:
        for f in incoming.glob("ecommerce_events_*.csv"):
            try:
                f.unlink()
            except Exception:
                pass

    print(f"Incoming dir: {incoming}")
    print(f"Checkpoints dir: {checkpoints}")
    print(f"Logs dir: {logs_dir}")
    if clean:
        print("Removed existing generated CSVs from incoming/")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Setup test environment directories")
    parser.add_argument("--clean", action="store_true", help="Remove existing generated CSV files in incoming/")
    args = parser.parse_args()
    main(args.clean)
