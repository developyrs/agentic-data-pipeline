import boto3
import duckdb
import os
from dotenv import load_dotenv
from langchain_core.tools import tool


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


@tool
def list_s3_files() -> list[str]:
    """List all files in the specified S3 bucket."""
    s3 = _get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket_name)
    return [item['Key'] for item in response.get('Contents', [])]


# ---------------------------
# Core DB inspection & execution
# ---------------------------
@tool
def list_tables():
    """Return a list of all tables in DuckDB."""
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
