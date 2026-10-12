"""Object storage (MinIO/S3), Delta tables and DuckDB connections. The only place that knows endpoints."""
from __future__ import annotations
import boto3
import duckdb
import pyarrow as pa
from botocore.client import Config
from botocore.exceptions import ClientError
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from . import config as C


def s3():
    return boto3.client("s3", endpoint_url=C.S3_ENDPOINT, aws_access_key_id=C.S3_KEY, aws_secret_access_key=C.S3_SECRET,
                        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))


def ensure_bucket():
    try:
        s3().head_bucket(Bucket=C.BUCKET)
    except ClientError:
        s3().create_bucket(Bucket=C.BUCKET)


def key_exists(key: str) -> bool:
    try:
        s3().head_object(Bucket=C.BUCKET, Key=key)
        return True
    except ClientError:
        return False


def delta_options() -> dict:
    return {"AWS_ACCESS_KEY_ID": C.S3_KEY, "AWS_SECRET_ACCESS_KEY": C.S3_SECRET, "AWS_ENDPOINT_URL": C.S3_ENDPOINT,
            "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true", "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}


def table_uri(layer: str, name: str) -> str:
    return C.lake(layer, "delta", name)


def open_table(layer: str, name: str) -> DeltaTable | None:
    try:
        return DeltaTable(table_uri(layer, name), storage_options=delta_options())
    except TableNotFoundError:
        return None


def read_table(layer: str, name: str, columns: list[str] | None = None) -> pa.Table:
    dt = open_table(layer, name)
    if dt is None:
        raise SystemExit(f"table {layer}.{name} does not exist yet; run the previous stage")
    return dt.to_pyarrow_table(columns=columns)


def write_table(layer: str, name: str, data: pa.Table, mode: str = "append", predicate: str | None = None,
                partition_by: list[str] | None = None, replace_schema: bool = False) -> None:
    kw = {"predicate": predicate} if predicate else {}
    if replace_schema:
        kw["schema_mode"] = "overwrite"
    write_deltalake(table_uri(layer, name), data, mode=mode, storage_options=delta_options(), partition_by=partition_by, **kw)


def duck(path: str | None = None) -> duckdb.DuckDBPyConnection:
    """DuckDB that can read s3:// Delta/Parquet and attach the Postgres warehouse."""
    con = duckdb.connect(path or ":memory:")
    for ext in ("httpfs", "delta", "postgres"):
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    host = C.S3_ENDPOINT.split("://", 1)[1]
    con.execute(f"""CREATE OR REPLACE SECRET lake (TYPE S3, KEY_ID '{C.S3_KEY}', SECRET '{C.S3_SECRET}', ENDPOINT '{host}',
                    URL_STYLE 'path', USE_SSL {'true' if C.S3_ENDPOINT.startswith('https') else 'false'}, REGION 'us-east-1')""")
    return con


def purge_lake() -> int:
    """Delete every object in the lake bucket (used by `cli reset` for a clean re-run)."""
    n, c = 0, s3()
    for page in c.get_paginator("list_objects_v2").paginate(Bucket=C.BUCKET):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            c.delete_objects(Bucket=C.BUCKET, Delete={"Objects": keys}); n += len(keys)
    return n
