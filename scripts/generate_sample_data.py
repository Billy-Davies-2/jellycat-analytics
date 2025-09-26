#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, timedelta
from random import choice, randint

POS = [
    "I love this Jellycat! So soft and adorable.",
    "Great quality and super cute.",
    "Perfect gift, brings me joy!",
]
NEG = [
    "Overpriced and disappointing.",
    "Not as durable as I expected.",
    "Arrived late and felt cheap.",
]

NEU = [
    "Got a Jellycat yesterday.",
    "Saw a new plush at the store.",
    "Reading reviews about Jellycat.",
]

ALL = POS + NEG + NEU


def main(out_dir: str, count: int):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "jellycat_samples.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        now = datetime.utcnow()
        for i in range(count):
            ts = now - timedelta(minutes=randint(0, 10_000))
            rec = {
                "id": f"sample-{i}",
                "timestamp": ts.isoformat() + "Z",
                "text": choice(ALL),
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"Wrote {count} records to {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="./.data", help="Output directory (ignored by git)")
    parser.add_argument("--count", type=int, default=200)
    args = parser.parse_args()
    main(args.out, args.count)
