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

def verify_duckdb():
    con = _create_duckdb_connection()
    tables = con.execute("SHOW TABLES").fetchall()
    if not tables:
        print("No tables found in DuckDB.")
        return

    print(f"Found {len(tables)} tables in DuckDB:\n")

    for (table_name,) in tables:
        print("=" * 60)
        print(f"Table: {table_name}")

        # Row count
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Row count: {count}")

        # Top 5 records
        rows = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        print("Top 5 rows:")
        print(rows.to_string(index=False))
        print()

    con.close()

if __name__ == "__main__":
    reset_duckdb()
    #verify_duckdb()