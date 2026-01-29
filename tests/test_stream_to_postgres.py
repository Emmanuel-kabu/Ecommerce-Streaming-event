#!/usr/bin/env python3
"""Integration smoke test: CSV -> Spark Structured Streaming -> PostgreSQL.

What this test proves
- Spark can read CSV files from a directory as a stream
- Spark can write the transformed stream to PostgreSQL via JDBC
- Rows actually land in the target DB table

Prereqs
- PostgreSQL running and reachable
- Spark runtime available (pyspark installed + Java)
- Environment variables set (or pass via your shell/.env):

  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

Optional
- POSTGRES_JDBC_JAR: path to a PostgreSQL JDBC jar; if not set, Spark will try to download via Maven.

Run
  python tests/test_stream_to_postgres.py --count 50 --timeout-seconds 90
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import psycopg2


def build_jdbc_url(host: str, port: int, database: str) -> str:
    return f"jdbc:postgresql://{host}:{port}/{database}"


def pg_connect():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    database = os.getenv("POSTGRES_DB", "ecommerce_analytics")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is required for this test")

    return psycopg2.connect(host=host, port=port, database=database, user=user, password=password)


def wait_for_rows(table: str, min_rows: int, timeout_seconds: int) -> int:
    deadline = time.time() + timeout_seconds
    last_count = 0

    while time.time() < deadline:
        with pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                last_count = int(cur.fetchone()[0])

        if last_count >= min_rows:
            return last_count

        time.sleep(2)

    return last_count


def terminate_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return

    try:
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.send_signal(signal.SIGINT)
    except Exception:
        proc.terminate()

    try:
        proc.wait(timeout=20)
    except Exception:
        proc.kill()


def main(count: int, timeout_seconds: int, keep_table: bool) -> int:
    project_root = Path(__file__).resolve().parents[1]
    run_id = str(int(time.time()))

    input_dir = project_root / "data" / "incoming_test" / run_id
    checkpoint_dir = project_root / "data" / "checkpoints_test" / run_id

    input_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Isolate this test from any existing pipeline tables/files
    table = f"ecommerce_events_test_{run_id}"

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    database = os.getenv("POSTGRES_DB", "ecommerce_analytics")

    os.environ["DATABASE_URL"] = build_jdbc_url(host, port, database)
    os.environ["DB_TABLE"] = table
    os.environ["INPUT_DATA_DIR"] = str(input_dir)
    os.environ["CHECKPOINT_DIR"] = str(checkpoint_dir)

    # Local Spark is simplest for a smoke test; override if you have a cluster.
    os.environ.setdefault("SPARK_MASTER_URL", "local[*]")

    # 1) Generate controlled CSV(s)
    gen_cmd = [
        sys.executable,
        str(project_root / "tests" / "generate_test_data.py"),
        "--count",
        str(count),
        "--batch-size",
        str(min(50, count)),
        "--out-dir",
        str(input_dir.relative_to(project_root)).replace("\\", "/"),
        "--seed",
        "42",
    ]
    print("Generating test CSV files...")
    subprocess.check_call(gen_cmd, cwd=str(project_root))

    # 2) Start Spark streaming job
    spark_cmd = [sys.executable, str(project_root / "spark_streaming_to_postgres" / "spark_to_postgres.py")]
    print(f"Starting Spark streaming job (table={table})...")

    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    proc = subprocess.Popen(
        spark_cmd,
        cwd=str(project_root),
        env=os.environ.copy(),
        creationflags=creationflags,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    # 3) Wait for rows to land
    try:
        # The Spark job creates the table on startup, so wait/poll for rows.
        inserted = wait_for_rows(table, min_rows=min(1, count), timeout_seconds=timeout_seconds)
        if inserted < 1:
            print("No rows detected in PostgreSQL within timeout.")
            return 2

        print(f"OK: detected {inserted} row(s) in {table}")
        return 0

    finally:
        terminate_process(proc)

        # Print last ~100 lines of output to help debugging
        try:
            if proc.stdout is not None:
                output = proc.stdout.read()
                if output:
                    tail = "\n".join(output.splitlines()[-100:])
                    print("\n--- Spark job output (tail) ---")
                    print(tail)
        except Exception:
            pass

        if not keep_table:
            try:
                with pg_connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"DROP TABLE IF EXISTS {table};")
                    conn.commit()
                print(f"Dropped test table {table}")
            except Exception as e:
                print(f"Warning: failed to drop test table {table}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Integration smoke test: stream CSV data into PostgreSQL")
    parser.add_argument("--count", type=int, default=50, help="Number of events to generate")
    parser.add_argument("--timeout-seconds", type=int, default=90, help="How long to wait for rows in DB")
    parser.add_argument("--keep-table", action="store_true", help="Do not drop the test table after run")
    args = parser.parse_args()

    raise SystemExit(main(args.count, args.timeout_seconds, args.keep_table))
