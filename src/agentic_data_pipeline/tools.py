import boto3
import duckdb
import os
from dotenv import load_dotenv
from langchain_core.tools import tool
import pandas as pd
import io


load_dotenv()

duckdb_path = os.getenv("DUCKDB_PATH")
bucket_name = os.getenv("S3_BUCKET")

def _create_duckdb_connection():
    """Create and return a DuckDB connection."""
    return duckdb.connect(duckdb_path)

def _get_s3_client():
    """Create and return an S3 client using boto3."""
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("S3_ENDPOINT"),
        region_name=os.getenv("S3_REGION"),
        use_ssl=os.getenv("S3_SECURE")
    )

def _fetch_csv_as_df(bucket: str, key: str) -> pd.DataFrame:
    """Internal helper: fetch a CSV from S3/MinIO and return as DataFrame."""
    client = _get_s3_client()
    obj = client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

@tool
def list_s3_files() -> list[str]:
    """List all files in the specified S3 bucket."""
    s3 = _get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket_name)
    return [item['Key'] for item in response.get('Contents', [])]


@tool
def list_tables_in_duckdb(database_name:str) -> list[str] :
    """Return a list of all tables in DuckDB"""
    con = _create_duckdb_connection()
    try:
        return [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    finally:
        con.close()

@tool
def describe_table(table_name: str):
    """Return column names and types for a given table."""
    con = _create_duckdb_connection()
    try:
        return con.execute(f"DESCRIBE {table_name}").fetchall()
    finally:
        con.close()

@tool
def load_csv_to_duckdb(bucket: str, key: str, table: str) -> str:
    """
    Load a CSV from S3/MinIO into DuckDB as a table.
    Overwrites the table if it already exists.
    """
    df = _fetch_csv_as_df(bucket, key)
    con = _create_duckdb_connection()
    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.register("temp_df", df)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM temp_df")
        con.unregister("temp_df")
    finally:
        con.close()
    return table

@tool
def fetch_table_as_df(table: str) -> pd.DataFrame:
    """Return a DuckDB table as a Pandas DataFrame."""
    con = _create_duckdb_connection()
    try:
        return con.execute(f"SELECT * FROM {table}").fetchdf()
    finally:
        con.close()

@tool
def execute_sql(sql: str) -> str:
    """
    Executes arbitrary SQL against DuckDB and returns a stringified result.
    """
    con = _create_duckdb_connection()
    try:
        result = con.execute(sql).fetchall()
        return str(result)
    finally:
        con.close()
@tool
def get_table_schema(table: str) -> list[str]:
    """Return the column names of a given DuckDB table."""
    con = _create_duckdb_connection()
    cols = con.execute(f"PRAGMA table_info({table})").fetchall()
    con.close()
    return [c[1] for c in cols]  # column names
