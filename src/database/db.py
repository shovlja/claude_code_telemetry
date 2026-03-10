from __future__ import annotations

import logging
from pathlib import Path

import duckdb


ROOT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT_DIR / "data" / "processed"

DB_PATH = PROCESSED_DIR / "claude_code_analytics.duckdb"
SCHEMA_PATH = ROOT_DIR / "src" / "database" / "schema.sql"

TABLE_SOURCES = {
    "events": PROCESSED_DIR / "events_clean.parquet",
    "api_requests": PROCESSED_DIR / "api_requests.parquet",
    "tool_results": PROCESSED_DIR / "tool_results.parquet",
    "sessions": PROCESSED_DIR / "sessions.parquet",
    "users": PROCESSED_DIR / "users.parquet",
}


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


def load_table(con: duckdb.DuckDBPyConnection, table_name: str, parquet_path: Path) -> None:
    require_file(parquet_path)
    logging.info("Loading table '%s' from %s", table_name, parquet_path)
    con.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet(?)",
        [str(parquet_path)],
    )


def run_schema(con: duckdb.DuckDBPyConnection) -> None:
    require_file(SCHEMA_PATH)
    logging.info("Applying SQL views from %s", SCHEMA_PATH)
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    con.execute(sql)


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    print("\n=== DUCKDB STORAGE SUMMARY ===")
    for table_name in TABLE_SOURCES:
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"{table_name}: {count:,}")

    print(f"\nDatabase created at: {DB_PATH}")

    overview = con.execute("SELECT * FROM analytics.overview").fetchdf()
    print("\n=== ANALYTICS OVERVIEW VIEW ===")
    print(overview.to_string(index=False))


def main() -> None:
    setup_logging()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(DB_PATH)) as con:
        for table_name, parquet_path in TABLE_SOURCES.items():
            load_table(con, table_name, parquet_path)

        con.execute("ANALYZE")
        run_schema(con)
        print_summary(con)


if __name__ == "__main__":
    main()