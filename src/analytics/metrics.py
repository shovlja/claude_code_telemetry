from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
DB_PATH = PROCESSED_DIR / "claude_code_analytics.duckdb"
OUTPUT_DIR = PROCESSED_DIR / "metrics"

QUERIES = {
    "overview": "SELECT * FROM analytics.overview",
    "token_usage_by_practice": "SELECT * FROM analytics.token_usage_by_practice",
    "cost_by_model": "SELECT * FROM analytics.cost_by_model",
    "usage_by_hour": "SELECT * FROM analytics.usage_by_hour",
    "usage_by_weekday": "SELECT * FROM analytics.usage_by_weekday",
    "daily_usage": "SELECT * FROM analytics.daily_usage",
    "sessions_by_practice": "SELECT * FROM analytics.sessions_by_practice",
    "top_users_by_tokens": "SELECT * FROM analytics.top_users_by_tokens",
    "top_users_by_cost": "SELECT * FROM analytics.top_users_by_cost",
    "error_rate_by_model": "SELECT * FROM analytics.error_rate_by_model",
    "tool_performance": "SELECT * FROM analytics.tool_performance",
    "session_outliers": "SELECT * FROM analytics.session_outliers",
}


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


def export_df(df: pd.DataFrame, name: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_DIR / f"{name}.csv", index=False)


def main() -> None:
    require_file(DB_PATH)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        results: dict[str, pd.DataFrame] = {}

        for name, query in QUERIES.items():
            df = con.execute(query).fetchdf()
            results[name] = df
            export_df(df, name)

        overview_df = results["overview"]
        overview_record = overview_df.iloc[0].to_dict() if not overview_df.empty else {}

        with open(OUTPUT_DIR / "overview.json", "w", encoding="utf-8") as f:
            json.dump(overview_record, f, indent=2, default=str)

        print("=== METRICS EXPORT COMPLETE ===")
        for name, df in results.items():
            print(f"{name}: {len(df):,} rows")

        if overview_record:
            print("\n=== OVERVIEW ===")
            for key, value in overview_record.items():
                if "cost" in key:
                    print(f"{key}: ${float(value):,.2f}")
                else:
                    print(f"{key}: {value}")


if __name__ == "__main__":
    main()