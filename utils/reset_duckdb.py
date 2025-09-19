# reset_duckdb.py
from src.agentic_data_pipeline.tools import _create_duckdb_connection

def reset_duckdb():
    con = _create_duckdb_connection()
    try:
        tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
        if not tables:
            print("No tables to drop — database is already empty.")
            return
        for table in tables:
            con.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"Dropped table: {table}")
        print("✅ DuckDB reset complete.")
    finally:
        con.close()

if __name__ == "__main__":
    reset_duckdb()
