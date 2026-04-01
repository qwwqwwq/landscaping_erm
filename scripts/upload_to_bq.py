"""Upload all Parquet files to BigQuery."""

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import bigquery

PROJECT_ID = "landscaping-erm"
DATASET_ID = "erm_data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def table_name_from_filename(filename: str) -> str:
    """Convert 'dbo.TableName.parquet' -> 'dbo_TableName'."""
    return filename.removesuffix(".parquet").replace(".", "_").replace("#", "_")


def count_parquet_rows(parquet_path: Path) -> int:
    """Get row count from parquet file metadata (fast, doesn't load data)."""
    parquet_file = pq.ParquetFile(parquet_path)
    return parquet_file.metadata.num_rows


def upload_parquet(client: bigquery.Client, parquet_path: Path, table_id: str):
    """Upload a single Parquet file to BigQuery."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(parquet_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)

    load_job.result()  # Wait for completion
    table = client.get_table(table_id)
    return table.num_rows


def main():
    parser = argparse.ArgumentParser(description="Upload Parquet files to BigQuery")
    parser.add_argument(
        "parquet_dir",
        type=Path,
        help="Directory containing Parquet files to upload",
    )
    args = parser.parse_args()

    parquet_dir = args.parquet_dir.resolve()
    if not parquet_dir.is_dir():
        log.error("Directory does not exist: %s", parquet_dir)
        sys.exit(1)

    log.info("Starting BigQuery upload to project=%s dataset=%s", PROJECT_ID, DATASET_ID)
    client = bigquery.Client(project=PROJECT_ID)

    # Create dataset if it doesn't exist
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        log.info("Dataset %s already exists", DATASET_ID)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        log.info("Created dataset %s", DATASET_ID)

    parquet_files = sorted(parquet_dir.glob("*.parquet"))
    log.info("Found %d Parquet files in %s", len(parquet_files), parquet_dir)

    # Pre-filter: skip files with 0 rows
    to_upload = []
    skipped_empty = 0
    for parquet_path in parquet_files:
        if parquet_path.stat().st_size == 0:
            skipped_empty += 1
            continue
        row_count = count_parquet_rows(parquet_path)
        if row_count <= 0:
            log.info("SKIP (no data rows): %s", parquet_path.name)
            skipped_empty += 1
            continue
        to_upload.append((parquet_path, row_count))

    log.info("%d tables to upload, %d skipped (empty)", len(to_upload), skipped_empty)

    # Fetch existing tables to skip
    existing_tables = {t.table_id for t in client.list_tables(f"{PROJECT_ID}.{DATASET_ID}")}
    if existing_tables:
        log.info("%d tables already exist in BigQuery, will skip", len(existing_tables))

    successes = 0
    skipped_existing = 0
    failures = []
    start_all = time.time()

    # Filter out already existing tables
    pending = []
    for parquet_path, row_count in to_upload:
        table_name = table_name_from_filename(parquet_path.name)
        if table_name in existing_tables:
            log.info("SKIP (already exists): %s", table_name)
            skipped_existing += 1
        else:
            pending.append((parquet_path, row_count, table_name))

    def upload_one(args):
        parquet_path, row_count, table_name = args
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        size_mb = parquet_path.stat().st_size / (1024 * 1024)
        log.info("Uploading %s (%.1f MB, %d rows)...", parquet_path.name, size_mb, row_count)
        t0 = time.time()
        try:
            num_rows = upload_parquet(client, parquet_path, table_id)
            elapsed = time.time() - t0
            log.info("OK %s -> %s (%d rows, %.1fs)", parquet_path.name, table_name, num_rows, elapsed)
            return (True, parquet_path.name, None)
        except Exception as e:
            elapsed = time.time() - t0
            error_msg = str(e).split("\n")[0]
            log.error("FAIL %s (%.1fs): %s", parquet_path.name, elapsed, error_msg)
            return (False, parquet_path.name, error_msg)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(upload_one, args) for args in pending]
        for future in as_completed(futures):
            success, name, error_msg = future.result()
            if success:
                successes += 1
            else:
                failures.append((name, error_msg))

    total_time = time.time() - start_all
    log.info("Done! %d uploaded, %d failed, %d skipped (empty), %d skipped (existing) in %.0fs",
             successes, len(failures), skipped_empty, skipped_existing, total_time)

    if failures:
        log.error("Failed tables:")
        for name, err in failures:
            log.error("  %s: %s", name, err)
        sys.exit(1)


if __name__ == "__main__":
    main()
