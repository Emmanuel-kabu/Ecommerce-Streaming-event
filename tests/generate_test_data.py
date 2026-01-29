#!/usr/bin/env python3
"""Generate synthetic e-commerce CSV event files into `incoming/`.
Usage examples:
  python tests/generate_test_data.py --count 1000 --batch-size 100 --categories all
"""
import argparse
import csv
import uuid
import random
from pathlib import Path
from datetime import datetime, timedelta

from faker import Faker

EVENT_TYPES = ["view", "purchase", "add_to_cart", "add_to_wishlist", "order"]

# Matches the CSV schema expected by `spark_streaming_to_postgres/spark_to_postgres.py`
HEADER = [
    "event_id",
    "event_type",
    "product_id",
    "product_name",
    "category",
    "brand",
    "sku",
    "price",
    "customer_id",
    "customer_email",
    "customer_name",
    "customer_address",
    "session_id",
    "user_agent",
    "ip_address",
    "event_timestamp",
]

CATEGORIES = [
    "Electronics",
    "Fashion",
    "Home",
    "Beauty",
    "Sports",
    "Books",
    "Toys",
]

BRANDS = [
    "Acme",
    "Globex",
    "Umbrella",
    "Stark",
    "Wayne",
    "Initech",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/120.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
]


def random_ip() -> str:
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


def random_price() -> str:
    return f"{random.uniform(1, 500):.2f}"

def generate_records(fake: Faker, start_index: int, n: int, event_types: list[str]):
    now = datetime.utcnow()
    for i in range(n):
        product_id = random.randint(1, 10000)
        category = random.choice(CATEGORIES)
        brand = random.choice(BRANDS)
        customer_name = fake.name()
        customer_email = f"user{start_index + i}@example.com"

        yield {
            "event_id": str(uuid.uuid4()),
            "event_type": random.choice(event_types),
            "product_id": product_id,
            "product_name": f"{category} Item {product_id}",
            "category": category,
            "brand": brand,
            "sku": f"{brand[:3].upper()}-{product_id:05d}",
            "price": random_price(),
            "customer_id": str(uuid.uuid4()),
            "customer_email": customer_email,
            "customer_name": customer_name,
            "customer_address": fake.address().replace("\n", ", "),
            "session_id": str(uuid.uuid4()),
            "user_agent": random.choice(USER_AGENTS),
            "ip_address": random_ip(),
            "event_timestamp": (now - timedelta(seconds=random.randint(0, 3600))).isoformat(),
        }


def write_batch(out_dir: Path, idx: int, records):
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    fname = out_dir / f"ecommerce_events_{ts}_{idx:04d}.csv"
    with fname.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        for r in records:
            writer.writerow(r)
    return fname


def main(count: int, batch_size: int, categories: str, out_dir: Path, seed: int | None):
    out_dir.mkdir(parents=True, exist_ok=True)
    if seed is not None:
        random.seed(seed)

    fake = Faker()
    if seed is not None:
        Faker.seed(seed)

    event_types = EVENT_TYPES if categories == "all" else [c for c in EVENT_TYPES if c in categories.split(",")]
    total_written = 0
    batch_idx = 0
    start_index = 0

    while total_written < count:
        to_write = min(batch_size, count - total_written)
        records = list(generate_records(fake, start_index, to_write, event_types))
        fname = write_batch(out_dir, batch_idx, records)
        print(f"Wrote {len(records)} records -> {fname}")
        total_written += to_write
        start_index += to_write
        batch_idx += 1

    print(f"Generated {total_written} total records in {batch_idx} file(s)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate synthetic ecommerce CSV event files")
    parser.add_argument("--count", type=int, default=1000, help="Total number of records to generate")
    parser.add_argument("--batch-size", type=int, default=100, dest="batch_size", help="Records per CSV file")
    parser.add_argument("--categories", type=str, default="all", help="Comma-separated event types or 'all'")
    parser.add_argument("--out-dir", type=str, default="data/incoming", help="Output directory relative to project root")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for repeatable test data")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    out_dir = (project_root / args.out_dir).resolve()
    main(args.count, args.batch_size, args.categories, out_dir, args.seed)
