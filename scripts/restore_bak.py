#!/usr/bin/env python3
"""
Restore a SQL Server .bak file using Docker and export all tables to Parquet.

Usage:
    python restore_bak.py BBIT_3-30-26.bak
    python restore_bak.py BBIT_3-30-26.bak --output-dir ./parquet_output
    python restore_bak.py BBIT_3-30-26.bak --db-name BBIT
"""

import argparse
import os
import subprocess
import sys
import time

import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq

CONTAINER_NAME = "sqlserver_restore"
SA_PASSWORD = "RestorePass!123"
DOCKER_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
BACKUP_MOUNT = "/backup"
SQL_PORT = 1433

# SQL Server type to PyArrow type mapping
SQL_TO_ARROW = {
    "bigint": pa.int64(),
    "int": pa.int32(),
    "smallint": pa.int16(),
    "tinyint": pa.int8(),
    "bit": pa.bool_(),
    "decimal": pa.decimal128(38, 10),
    "numeric": pa.decimal128(38, 10),
    "money": pa.decimal128(19, 4),
    "smallmoney": pa.decimal128(10, 4),
    "float": pa.float64(),
    "real": pa.float32(),
    "date": pa.date32(),
    "datetime": pa.timestamp("ms"),
    "datetime2": pa.timestamp("us"),
    "smalldatetime": pa.timestamp("s"),
    "time": pa.time64("us"),
    "char": pa.string(),
    "varchar": pa.string(),
    "text": pa.string(),
    "nchar": pa.string(),
    "nvarchar": pa.string(),
    "ntext": pa.string(),
    "binary": pa.binary(),
    "varbinary": pa.binary(),
    "image": pa.binary(),
    "uniqueidentifier": pa.string(),
    "xml": pa.string(),
}


def run(cmd, check=True, capture=True):
    """Run a shell command and return stdout."""
    result = subprocess.run(cmd, capture_output=capture, text=True, check=check)
    return result.stdout if capture else None


def sqlcmd(query, database=None, timeout=120):
    """Execute a SQL query inside the container via sqlcmd."""
    cmd = [
        "docker", "exec", CONTAINER_NAME,
        "/opt/mssql-tools18/bin/sqlcmd",
        "-S", "localhost", "-U", "sa", "-P", SA_PASSWORD,
        "-C",  # trust server certificate
        "-W",  # remove trailing spaces
        "-s", "\t",  # tab separator
        "-h", "-1",  # no headers/dashes
        "-Q", query,
    ]
    if database:
        cmd.extend(["-d", database])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        print(f"SQL error: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"sqlcmd failed: {result.stderr}")
    return result.stdout


def sqlcmd_with_headers(query, database=None, timeout=120):
    """Execute a SQL query and return stdout with column headers."""
    cmd = [
        "docker", "exec", CONTAINER_NAME,
        "/opt/mssql-tools18/bin/sqlcmd",
        "-S", "localhost", "-U", "sa", "-P", SA_PASSWORD,
        "-C", "-W",
        "-s", "\t",
        "-Q", query,
    ]
    if database:
        cmd.extend(["-d", database])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        print(f"SQL error: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"sqlcmd failed: {result.stderr}")
    return result.stdout


def start_container(bak_dir):
    """Start SQL Server container with the backup directory mounted."""
    # Check if container already exists
    result = subprocess.run(
        ["docker", "inspect", CONTAINER_NAME],
        capture_output=True, text=True, check=False,
    )
    if result.returncode == 0:
        print(f"Container '{CONTAINER_NAME}' already exists, removing...")
        run(["docker", "rm", "-f", CONTAINER_NAME])

    print("Starting SQL Server container...")
    run([
        "docker", "run",
        "-e", "ACCEPT_EULA=Y",
        "-e", f"MSSQL_SA_PASSWORD={SA_PASSWORD}",
        "-v", f"{bak_dir}:{BACKUP_MOUNT}",
        "-p", f"{SQL_PORT}:1433",
        "--name", CONTAINER_NAME,
        "-d", DOCKER_IMAGE,
    ])

    # Wait for SQL Server to be ready
    print("Waiting for SQL Server to start", end="", flush=True)
    for _ in range(60):
        time.sleep(2)
        print(".", end="", flush=True)
        try:
            sqlcmd("SELECT 1", timeout=5)
            print(" ready!")
            return
        except (RuntimeError, subprocess.TimeoutExpired):
            continue
    print()
    raise RuntimeError("SQL Server did not start within 120 seconds")


def get_logical_names(bak_path_in_container):
    """Get logical file names from the backup."""
    query = f"RESTORE FILELISTONLY FROM DISK = '{bak_path_in_container}'"
    output = sqlcmd_with_headers(query)
    lines = [l for l in output.strip().splitlines() if l.strip()]
    if len(lines) < 2:
        raise RuntimeError(f"Unexpected FILELISTONLY output:\n{output}")

    # First line is headers, second is dashes, rest is data
    headers = lines[0].split("\t")
    # Find LogicalName and Type columns
    name_idx = next(i for i, h in enumerate(headers) if h.strip() == "LogicalName")
    type_idx = next(i for i, h in enumerate(headers) if h.strip() == "Type")

    files = []
    for line in lines[2:]:  # skip header and dash lines
        cols = line.split("\t")
        if len(cols) > max(name_idx, type_idx):
            files.append({
                "logical_name": cols[name_idx].strip(),
                "type": cols[type_idx].strip(),
            })
    return files


def restore_database(bak_filename, db_name):
    """Restore the .bak file into the container."""
    bak_path = f"{BACKUP_MOUNT}/{bak_filename}"

    print(f"Reading backup file list from {bak_filename}...")
    files = get_logical_names(bak_path)
    print(f"  Found logical files: {files}")

    move_clauses = []
    for f in files:
        if f["type"] == "D":
            dest = f"/var/opt/mssql/data/{db_name}.mdf"
        elif f["type"] == "L":
            dest = f"/var/opt/mssql/data/{db_name}.ldf"
        else:
            dest = f"/var/opt/mssql/data/{db_name}_{f['logical_name']}"
        move_clauses.append(f"MOVE '{f['logical_name']}' TO '{dest}'")

    move_sql = ", ".join(move_clauses)
    query = f"RESTORE DATABASE [{db_name}] FROM DISK = '{bak_path}' WITH {move_sql}, REPLACE"

    print(f"Restoring database as '{db_name}'...")
    sqlcmd(query)
    print("  Restore complete!")


def get_connection(db_name):
    """Get a pyodbc connection to the SQL Server container."""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER=localhost,{SQL_PORT};"
        f"DATABASE={db_name};"
        f"UID=sa;"
        f"PWD={SA_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def get_tables(db_name):
    """Get all user tables in the database."""
    conn = get_connection(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    tables = [(row[0], row[1]) for row in cursor.fetchall()]
    conn.close()
    return tables


def get_arrow_type(sql_type, precision=None, scale=None):
    """Map SQL Server type to PyArrow type."""
    sql_type = sql_type.lower()
    if sql_type in ("decimal", "numeric") and precision and scale:
        return pa.decimal128(precision, scale)
    return SQL_TO_ARROW.get(sql_type, pa.string())


BATCH_SIZE = 100_000


def export_table(db_name, schema, table, output_dir):
    """Export a single table to Parquet with proper types, using batched fetching."""
    conn = get_connection(db_name)
    cursor = conn.cursor()

    # Get column metadata
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    col_info = cursor.fetchall()

    columns = [c[0] for c in col_info]
    arrow_types = [get_arrow_type(c[1], c[2], c[3]) for c in col_info]

    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
    row_count = cursor.fetchone()[0]

    filename = f"{schema}.{table}.parquet"
    filepath = os.path.join(output_dir, filename)

    # Fetch and write in batches using ParquetWriter
    cursor.execute(f"SELECT * FROM [{schema}].[{table}]")

    pa_schema = pa.schema([(col, t) for col, t in zip(columns, arrow_types)])
    writer = None
    rows_written = 0

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        # Transpose rows to columns
        col_data = list(zip(*rows))
        arrays = []
        for i, (data, arrow_type) in enumerate(zip(col_data, arrow_types)):
            try:
                arrays.append(pa.array(data, type=arrow_type))
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
                # Fall back to string if type conversion fails
                arrays.append(pa.array([str(v) if v is not None else None for v in data], type=pa.string()))

        batch_table = pa.table(dict(zip(columns, arrays)))

        if writer is None:
            writer = pq.ParquetWriter(filepath, batch_table.schema, compression="snappy")
        writer.write_table(batch_table)
        rows_written += len(rows)

    conn.close()

    if writer:
        writer.close()
    else:
        # Empty table - write empty parquet with schema
        empty_table = pa.table({col: pa.array([], type=t) for col, t in zip(columns, arrow_types)})
        pq.write_table(empty_table, filepath, compression="snappy")

    print(f"  {schema}.{table} -> {filename} ({row_count} rows, {len(columns)} cols)")


def cleanup():
    """Stop and remove the container."""
    print("Cleaning up container...")
    run(["docker", "rm", "-f", CONTAINER_NAME], check=False)
    print("Done!")


def main():
    parser = argparse.ArgumentParser(description="Restore a SQL Server .bak and export tables to Parquet")
    parser.add_argument("bak_file", help="Path to the .bak file")
    parser.add_argument("--output-dir", default="./parquet_output", help="Directory for Parquet output (default: ./parquet_output)")
    parser.add_argument("--db-name", default=None, help="Database name to restore as (default: derived from filename)")
    parser.add_argument("--keep-container", action="store_true", help="Don't remove the container when done")
    args = parser.parse_args()

    bak_path = os.path.abspath(args.bak_file)
    if not os.path.exists(bak_path):
        print(f"Error: {bak_path} not found", file=sys.stderr)
        sys.exit(1)

    bak_dir = os.path.dirname(bak_path)
    bak_filename = os.path.basename(bak_path)
    db_name = args.db_name or os.path.splitext(bak_filename)[0].replace("-", "_").replace(" ", "_")

    os.makedirs(args.output_dir, exist_ok=True)

    try:
        start_container(bak_dir)
        restore_database(bak_filename, db_name)

        print(f"\nDiscovering tables in '{db_name}'...")
        tables = get_tables(db_name)
        print(f"Found {len(tables)} tables\n")

        print(f"Exporting to {args.output_dir}/")
        for schema, table in tables:
            filename = f"{schema}.{table}.parquet"
            filepath = os.path.join(args.output_dir, filename)
            if os.path.exists(filepath):
                print(f"  {schema}.{table} -> {filename} (already exists, skipping)")
                continue
            export_table(db_name, schema, table, args.output_dir)

        print(f"\nAll tables exported to {args.output_dir}/ as Parquet files")
    finally:
        if not args.keep_container:
            cleanup()


if __name__ == "__main__":
    main()
