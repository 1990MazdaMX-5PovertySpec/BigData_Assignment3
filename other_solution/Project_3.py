"""
csv_to_mongo.py
---------------
Reads a CSV file line-by-line (no full load into memory) and uploads
the rows to a MongoDB sharded cluster using multiprocessing.

Usage:
    python csv_to_mongo.py --file data.csv \
                           --uri "mongodb://localhost:27017" \
                           --db mydb \
                           --collection ais_data \
                           --workers 4 \
                           --batch-size 500
"""

import csv
import argparse
import logging
import multiprocessing as mp
from datetime import datetime

from pymongo import MongoClient, errors

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)s] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Worker
# ─────────────────────────────────────────────
def worker(queue: mp.Queue, uri: str, db_name: str, collection_name: str) -> None:
    """
    Pulls batches of rows from the queue and inserts them into MongoDB.
    Each worker maintains its own MongoClient connection.
    Runs until it receives the sentinel value None.
    """
    client = MongoClient(uri)
    collection = client[db_name][collection_name]

    total_inserted = 0

    while True:
        batch = queue.get()

        if batch is None:           # Sentinel — time to shut down
            log.info("Worker done. Inserted %d documents.", total_inserted)
            client.close()
            return

        try:
            result = collection.insert_many(batch, ordered=False)
            total_inserted += len(result.inserted_ids)
        except errors.BulkWriteError as e:
            # Log but don't crash — partial inserts are still recorded
            log.warning("Bulk write error: %s", e.details)


# ─────────────────────────────────────────────
# CSV reader / dispatcher
# ─────────────────────────────────────────────
def read_and_dispatch(
    filepath: str,
    queue: mp.Queue,
    batch_size: int,
) -> int:
    """
    Reads the CSV file line-by-line and pushes batches onto the queue.
    Returns the total number of rows dispatched.
    """
    total_rows = 0
    batch: list[dict] = []

    with open(filepath, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)

        for row in reader:
            # Strip whitespace from keys and values; keep empty strings as None
            clean = {k.strip(): (v.strip() if v.strip() != "" else None) for k, v in row.items()}
            batch.append(clean)
            total_rows += 1

            if len(batch) >= batch_size:
                queue.put(batch)    # Blocks if queue is full (backpressure)
                batch = []

    if batch:                       # Flush the last partial batch
        queue.put(batch)

    return total_rows


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stream a CSV into MongoDB with multiprocessing."
    )
    parser.add_argument("--file",       required=True,
                        help="Path to the CSV file")
    parser.add_argument("--uri",        default="mongodb://localhost:27017",
                        help="MongoDB connection URI (points to mongos)")
    parser.add_argument("--db",         default="mydb",
                        help="Target database name")
    parser.add_argument("--collection", default="ais_data",
                        help="Target collection name")
    parser.add_argument("--workers",    type=int, default=4,
                        help="Number of worker processes")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Number of documents per insert batch")
    args = parser.parse_args()

    # Queue cap = workers * 4  →  limits how far ahead the reader runs,
    # keeping memory usage bounded regardless of file size.
    queue: mp.Queue = mp.Queue(maxsize=args.workers * 4)

    # ── Start worker processes ──────────────────────────────────────────
    processes = []
    for i in range(args.workers):
        p = mp.Process(
            target=worker,
            args=(queue, args.uri, args.db, args.collection),
            name=f"Worker-{i + 1}",
        )
        p.start()
        processes.append(p)

    log.info(
        "Started %d workers. Reading '%s' in batches of %d ...",
        args.workers, args.file, args.batch_size,
    )

    # ── Read CSV and dispatch batches ───────────────────────────────────
    start = datetime.now()
    total_rows = read_and_dispatch(args.file, queue, args.batch_size)

    # ── Send one sentinel (None) per worker to signal shutdown ──────────
    for _ in processes:
        queue.put(None)

    # ── Wait for all workers to finish ──────────────────────────────────
    for p in processes:
        p.join()

    elapsed = (datetime.now() - start).total_seconds()
    log.info(
        "Done. %d rows dispatched in %.1f s (%.0f rows/s).",
        total_rows, elapsed, total_rows / elapsed if elapsed > 0 else 0,
    )


if __name__ == "__main__":
    main()