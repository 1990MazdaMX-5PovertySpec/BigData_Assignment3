"""
Project_3.py
------------
Reads a CSV file line-by-line and uploads data to a MongoDB sharded cluster,
and/or filters the uploaded data into a cleaned collection.

Usage:
    # Upload CSV data
    python Project_3.py --uploadCSV --file data.csv

    # Filter already-uploaded data
    python Project_3.py --filterData

    # Do both in one run
    python Project_3.py --uploadCSV --file data.csv --filterData
"""

import csv
import math
import time
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
# Retry configuration
# ─────────────────────────────────────────────

# Exception types that are always retryable
RETRYABLE_ERRORS = (
    errors.ConnectionFailure,   # covers ServerSelectionTimeoutError, NetworkTimeout, etc.
    errors.AutoReconnect,
    errors.NotPrimaryError,     # stepped-down primary
    errors.ExecutionTimeout,
)

# OperationFailure error codes that signal a transient shard/primary issue.
# These are NOT subclasses of ConnectionFailure so must be checked separately.
# Full list: https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
RETRYABLE_OP_FAILURE_CODES = {
    6,      # HostUnreachable  ← the code that triggered this fix
    7,      # HostNotFound
    89,     # NetworkTimeout
    91,     # ShutdownInProgress
    189,    # PrimarySteppedDown
    216,    # ElectionInProgress
    262,    # ExceededTimeLimit
    9001,   # SocketException
    10107,  # NotWritablePrimary
    11600,  # InterruptedAtShutdown
    11602,  # InterruptedDueToReplStateChange
    13435,  # NotPrimaryNoSecondaryOk
    13436,  # NotPrimaryOrSecondary
}

MAX_RETRIES    = 8      # number of attempts before giving up
BASE_DELAY_S   = 2.0    # initial wait in seconds
MAX_DELAY_S    = 60.0   # cap on exponential backoff


def is_retryable(exc: Exception) -> bool:
    """Return True if the exception represents a transient failover condition."""
    if isinstance(exc, RETRYABLE_ERRORS):
        return True
    if isinstance(exc, errors.OperationFailure):
        return exc.code in RETRYABLE_OP_FAILURE_CODES
    return False


def insert_with_retry(collection, batch: list[dict]) -> int:
    """
    Attempt insert_many with exponential backoff on transient errors.
    Returns the number of documents successfully inserted.
    On a BulkWriteError (e.g. duplicate keys) the partial result is counted
    and execution continues — the batch is NOT retried.
    """
    delay = BASE_DELAY_S
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = collection.insert_many(batch, ordered=False)
            return len(result.inserted_ids)
        except errors.BulkWriteError as e:
            # Partial insert — not a connectivity issue, don't retry
            inserted = e.details.get("nInserted", 0)
            log.warning(
                "BulkWriteError: %d inserted, %d errors. Not retrying.",
                inserted, len(e.details.get("writeErrors", [])),
            )
            return inserted
        except Exception as e:
            if not is_retryable(e):
                raise
            if attempt == MAX_RETRIES:
                log.error(
                    "Giving up after %d attempts. Last error: %s", MAX_RETRIES, e
                )
                raise
            log.warning(
                "Transient MongoDB error (attempt %d/%d): %s. "
                "Retrying in %.1f s...",
                attempt, MAX_RETRIES, e, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, MAX_DELAY_S)

    return 0


# ─────────────────────────────────────────────
# Validation rules
# ─────────────────────────────────────────────

# Navigational statuses that are considered valid/meaningful
INVALID_NAV_STATUSES = {
    "unknown value",
    "unknown",
    "not defined",
    "",
}

# Numeric fields that must be present and a valid finite number
NUMERIC_FIELDS = ["ROT", "SOG", "COG", "Heading"]

# Range checks: (min_inclusive, max_inclusive)
FIELD_RANGES = {
    "Latitude":  (-90.0,   90.0),
    "Longitude": (-180.0, 180.0),
    "ROT":       (-127.0, 127.0),
    "SOG":       (0.0,   102.2),   # knots — 102.2 = "not available" sentinel
    "COG":       (0.0,   360.0),
    "Heading":   (0.0,   359.0),
}

# Minimum number of data points a vessel must have to be kept
MIN_VESSEL_POINTS = 100


def is_valid_document(doc: dict) -> bool:
    """
    Return True only if the document passes all field-level validation rules.
    """

    # ── MMSI: must be present and a 9-digit integer ──────────────────────
    mmsi = doc.get("MMSI")
    if not mmsi:
        return False
    try:
        mmsi_int = int(mmsi)
        if not (100_000_000 <= mmsi_int <= 999_999_999):
            return False
    except (ValueError, TypeError):
        return False

    # ── Navigational status ───────────────────────────────────────────────
    nav = (doc.get("Navigational status") or "").strip().lower()
    if nav in INVALID_NAV_STATUSES:
        return False

    # ── Latitude & Longitude: present, numeric, in range ─────────────────
    for field in ("Latitude", "Longitude"):
        val = doc.get(field)
        if val is None:
            return False
        try:
            f = float(val)
        except (ValueError, TypeError):
            return False
        lo, hi = FIELD_RANGES[field]
        if not (lo <= f <= hi):
            return False
        if math.isnan(f) or math.isinf(f):
            return False

    # ── Numeric fields: present and finite ───────────────────────────────
    for field in NUMERIC_FIELDS:
        val = doc.get(field)
        if val is None:
            return False
        try:
            f = float(val)
        except (ValueError, TypeError):
            return False
        if math.isnan(f) or math.isinf(f):
            return False
        # Range check where applicable
        if field in FIELD_RANGES:
            lo, hi = FIELD_RANGES[field]
            if not (lo <= f <= hi):
                return False

    return True


# ─────────────────────────────────────────────
# Upload workers  (CSV → ais_data)
# ─────────────────────────────────────────────

def upload_worker(queue: mp.Queue, uri: str, db_name: str, collection_name: str) -> None:
    """Inserts batches of raw CSV rows into MongoDB."""
    client = MongoClient(uri)
    collection = client[db_name][collection_name]
    total_inserted = 0

    while True:
        batch = queue.get()
        if batch is None:
            log.info("Upload worker done. Inserted %d documents.", total_inserted)
            client.close()
            return
        try:
            total_inserted += insert_with_retry(collection, batch)
        except RETRYABLE_ERRORS as e:
            log.error("Batch permanently failed after retries: %s. Skipping.", e)


def read_and_dispatch(filepath: str, queue: mp.Queue, batch_size: int) -> int:
    """Reads CSV line-by-line and pushes batches onto the queue."""
    total_rows = 0
    batch: list[dict] = []

    with open(filepath, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            clean = {
                k.strip(): (v.strip() if v.strip() != "" else None)
                for k, v in row.items()
            }
            batch.append(clean)
            total_rows += 1

            if len(batch) >= batch_size:
                queue.put(batch)
                batch = []

    if batch:
        queue.put(batch)

    return total_rows


# ─────────────────────────────────────────────
# Filter workers  (ais_data → ais_data_filtered)
# ─────────────────────────────────────────────

def filter_worker(
    queue: mp.Queue,
    uri: str,
    db_name: str,
    out_collection: str,
    valid_mmsis: set,
) -> None:
    """
    Receives batches of documents, discards those that fail field-level
    validation or belong to vessels with too few data points, and inserts
    the survivors into the output collection.
    """
    client = MongoClient(uri)
    collection = client[db_name][out_collection]
    total_inserted = 0
    total_dropped = 0

    while True:
        batch = queue.get()
        if batch is None:
            log.info(
                "Filter worker done. Kept %d, dropped %d documents.",
                total_inserted, total_dropped,
            )
            client.close()
            return

        keep = []
        for doc in batch:
            mmsi = doc.get("MMSI")
            if mmsi not in valid_mmsis:
                total_dropped += 1
                continue
            if not is_valid_document(doc):
                total_dropped += 1
                continue
            # Remove the internal MongoDB _id so insert_many generates a fresh one
            doc.pop("_id", None)
            keep.append(doc)

        if keep:
            try:
                total_inserted += insert_with_retry(collection, keep)
            except Exception as e:
                if is_retryable(e):
                    log.error("Batch permanently failed after retries: %s. Skipping.", e)
                else:
                    raise


def fetch_and_dispatch(
    uri: str,
    db_name: str,
    in_collection: str,
    queue: mp.Queue,
    batch_size: int,
) -> int:
    """
    Reads all documents from ais_data via a cursor and pushes them onto
    the queue in batches. Runs in the main process.
    Retries cursor iteration on transient failover errors.
    """
    client = MongoClient(uri)
    collection = client[db_name][in_collection]
    total = 0
    batch: list[dict] = []
    delay = BASE_DELAY_S

    cursor = collection.find({}, batch_size=batch_size)
    while True:
        try:
            doc = next(cursor)
            batch.append(doc)
            total += 1
            if len(batch) >= batch_size:
                queue.put(batch)
                batch = []
            delay = BASE_DELAY_S    # reset backoff after a successful read
        except StopIteration:
            break
        except Exception as e:
            if not is_retryable(e):
                raise
            log.warning(
                "Cursor interrupted by failover: %s. Retrying in %.1f s...", e, delay
            )
            time.sleep(delay)
            delay = min(delay * 2, MAX_DELAY_S)
            # Re-open cursor from where we left off using skip
            try:
                cursor.close()
            except Exception:
                pass
            cursor = collection.find({}, batch_size=batch_size).skip(total)

    if batch:
        queue.put(batch)

    client.close()
    return total


def build_valid_mmsi_set(uri: str, db_name: str, in_collection: str) -> set:
    """
    Counts data points per MMSI using MongoDB aggregation and returns
    the set of MMSIs that have at least MIN_VESSEL_POINTS records.
    This runs once in the main process before filtering begins.
    """
    log.info(
        "Counting data points per vessel (min threshold: %d)...",
        MIN_VESSEL_POINTS,
    )
    client = MongoClient(uri)
    collection = client[db_name][in_collection]

    pipeline = [
        {"$group": {"_id": "$MMSI", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": MIN_VESSEL_POINTS}}},
        {"$project": {"_id": 1}},
    ]

    delay = BASE_DELAY_S
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            valid = {doc["_id"] for doc in collection.aggregate(pipeline, allowDiskUse=True)}
            break
        except RETRYABLE_ERRORS as e:
            if attempt == MAX_RETRIES:
                client.close()
                raise
            log.warning(
                "Aggregation failed (attempt %d/%d): %s. Retrying in %.1f s...",
                attempt, MAX_RETRIES, e, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, MAX_DELAY_S)
        except errors.OperationFailure as e:
            if e.code not in RETRYABLE_OP_FAILURE_CODES or attempt == MAX_RETRIES:
                client.close()
                raise
            log.warning(
                "Aggregation failed (attempt %d/%d): %s. Retrying in %.1f s...",
                attempt, MAX_RETRIES, e, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, MAX_DELAY_S)
    client.close()

    log.info("Found %d vessels with >= %d data points.", len(valid), MIN_VESSEL_POINTS)
    return valid


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stream a CSV into MongoDB and/or filter the data."
    )
    parser.add_argument("--file",
                        help="Path to the CSV file (required with --uploadCSV)")
    parser.add_argument("--uri",             default="mongodb://localhost:27017",
                        help="MongoDB connection URI (points to mongos)")
    parser.add_argument("--db",              default="mydb",
                        help="Target database name")
    parser.add_argument("--collection",      default="ais_data",
                        help="Source collection name")
    parser.add_argument("--out-collection",  default="ais_data_filtered",
                        help="Output collection for filtered data")
    parser.add_argument("--workers",         type=int, default=4,
                        help="Number of worker processes")
    parser.add_argument("--batch-size",      type=int, default=500,
                        help="Number of documents per batch")
    # Flags are independent — neither is required on its own
    parser.add_argument("--uploadCSV",  action="store_true",
                        help="Upload data from the CSV file into --collection")
    parser.add_argument("--filterData", action="store_true",
                        help="Filter --collection and write results to --out-collection")
    args = parser.parse_args()

    if not args.uploadCSV and not args.filterData:
        parser.error("Specify at least one of --uploadCSV or --filterData.")

    # ══════════════════════════════════════════
    # STEP 1 — Upload CSV → ais_data
    # ══════════════════════════════════════════
    if args.uploadCSV:
        if not args.file:
            parser.error("--file is required when using --uploadCSV.")

        queue: mp.Queue = mp.Queue(maxsize=args.workers * 4)

        processes = []
        for i in range(args.workers):
            p = mp.Process(
                target=upload_worker,
                args=(queue, args.uri, args.db, args.collection),
                name=f"UploadWorker-{i + 1}",
            )
            p.start()
            processes.append(p)

        log.info(
            "Started %d upload workers. Reading '%s' in batches of %d ...",
            args.workers, args.file, args.batch_size,
        )

        start = datetime.now()
        total_rows = read_and_dispatch(args.file, queue, args.batch_size)

        for _ in processes:
            queue.put(None)
        for p in processes:
            p.join()

        elapsed = (datetime.now() - start).total_seconds()
        log.info(
            "Upload done. %d rows in %.1f s (%.0f rows/s).",
            total_rows, elapsed, total_rows / elapsed if elapsed > 0 else 0,
        )

    # ══════════════════════════════════════════
    # STEP 2 — Filter ais_data → ais_data_filtered
    # ══════════════════════════════════════════
    if args.filterData:
        # Phase A: aggregate per-vessel counts (fast, single-pass in Mongo)
        valid_mmsis = build_valid_mmsi_set(args.uri, args.db, args.collection)

        # Phase B: stream all documents through filter workers
        filter_queue: mp.Queue = mp.Queue(maxsize=args.workers * 4)

        filter_processes = []
        for i in range(args.workers):
            p = mp.Process(
                target=filter_worker,
                args=(
                    filter_queue,
                    args.uri,
                    args.db,
                    args.out_collection,
                    valid_mmsis,
                ),
                name=f"FilterWorker-{i + 1}",
            )
            p.start()
            filter_processes.append(p)

        log.info(
            "Started %d filter workers. Reading from '%s'...",
            args.workers, args.collection,
        )

        start = datetime.now()
        total_docs = fetch_and_dispatch(
            args.uri, args.db, args.collection, filter_queue, args.batch_size
        )

        for _ in filter_processes:
            filter_queue.put(None)
        for p in filter_processes:
            p.join()

        elapsed = (datetime.now() - start).total_seconds()
        log.info(
            "Filter done. %d documents processed in %.1f s (%.0f docs/s). "
            "Results are in '%s'.",
            total_docs, elapsed,
            total_docs / elapsed if elapsed > 0 else 0,
            args.out_collection,
        )


if __name__ == "__main__":
    main()
