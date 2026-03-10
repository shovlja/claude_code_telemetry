from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]

INPUT_EVENTS = ROOT_DIR / "data" / "processed" / "events.parquet"
INPUT_EMPLOYEES = ROOT_DIR / "data" / "raw" / "employees.csv"
OUTPUT_DIR = ROOT_DIR / "data" / "processed"

OUTPUT_EVENTS = OUTPUT_DIR / "events_clean.parquet"
OUTPUT_API_REQUESTS = OUTPUT_DIR / "api_requests.parquet"
OUTPUT_TOOL_RESULTS = OUTPUT_DIR / "tool_results.parquet"
OUTPUT_SESSIONS = OUTPUT_DIR / "sessions.parquet"
OUTPUT_USERS = OUTPUT_DIR / "users.parquet"
OUTPUT_QUALITY_REPORT = OUTPUT_DIR / "data_quality_report.json"

VALID_EVENT_NAMES = {
    "api_request",
    "tool_decision",
    "tool_result",
    "user_prompt",
    "api_error",
}

REQUIRED_COLUMNS = {
    "event_type",
    "event_name",
    "timestamp",
    "session_id",
    "user_id",
    "email",
}

TEXT_COLUMNS = [
    "event_type",
    "event_name",
    "session_id",
    "user_id",
    "email",
    "model",
    "tool_name",
    "terminal_type",
    "organization_id",
    "practice",
]

NUMERIC_COLUMNS = [
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
    "cost_usd",
    "duration_ms",
    "prompt_length",
]


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s | %(message)s",
    )


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


def clean_text_series(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .fillna("")
        .str.strip()
    )


def to_bool_series(series: pd.Series) -> pd.Series:
    mapped = (
        series.astype("string")
        .str.strip()
        .str.lower()
        .map(
            {
                "true": True,
                "false": False,
                "1": True,
                "0": False,
                "yes": True,
                "no": False,
                "nan": False,
                "none": False,
                "": False,
            }
        )
        .fillna(False)
        .astype(bool)
    )
    return mapped


def first_non_empty(series: pd.Series) -> str:
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    if cleaned.empty:
        return ""
    return cleaned.iloc[0]


def mode_non_empty(series: pd.Series) -> str:
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    if cleaned.empty:
        return ""
    mode_values = cleaned.mode()
    if mode_values.empty:
        return cleaned.iloc[0]
    return mode_values.iloc[0]


def load_events() -> pd.DataFrame:
    require_file(INPUT_EVENTS)
    logging.info("Loading events from %s", INPUT_EVENTS)
    return pd.read_parquet(INPUT_EVENTS)


def load_employees() -> pd.DataFrame | None:
    if not INPUT_EMPLOYEES.exists():
        logging.warning("employees.csv not found, enrichment will be skipped.")
        return None

    logging.info("Loading employees from %s", INPUT_EMPLOYEES)
    employees = pd.read_csv(INPUT_EMPLOYEES)

    employees.columns = [col.strip().lower() for col in employees.columns]

    expected_cols = {"email", "full_name", "practice", "level", "location"}
    missing = expected_cols - set(employees.columns)
    if missing:
        logging.warning(
            "employees.csv is missing expected columns: %s. Enrichment will be partial.",
            sorted(missing),
        )

    for col in employees.columns:
        if employees[col].dtype == "object" or str(employees[col].dtype).startswith("string"):
            employees[col] = clean_text_series(employees[col])

    return employees


def standardize_events(events: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    logging.info("Standardizing raw events...")

    df = events.copy()
    df.columns = [col.strip() for col in df.columns]

    missing_required = REQUIRED_COLUMNS - set(df.columns)
    if missing_required:
        raise ValueError(f"Missing required columns in events dataset: {sorted(missing_required)}")

    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = clean_text_series(df[col])

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = 0

    if "success" in df.columns:
        df["success"] = to_bool_series(df["success"])
    else:
        df["success"] = False

    raw_row_count = len(df)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    invalid_timestamp_count = int(df["timestamp"].isna().sum())

    duplicate_count = int(df.duplicated().sum())
    if duplicate_count > 0:
        df = df.drop_duplicates()

    df = df.sort_values(["timestamp", "session_id", "event_name"], na_position="last").reset_index(drop=True)
    df["event_id"] = pd.RangeIndex(start=1, stop=len(df) + 1, step=1)

    df["input_tokens"] = df["input_tokens"].fillna(0)
    df["output_tokens"] = df["output_tokens"].fillna(0)
    df["cache_read_tokens"] = df["cache_read_tokens"].fillna(0)
    df["cache_creation_tokens"] = df["cache_creation_tokens"].fillna(0)
    df["cost_usd"] = df["cost_usd"].fillna(0.0)
    df["duration_ms"] = df["duration_ms"].fillna(0)
    df["prompt_length"] = df["prompt_length"].fillna(0)

    df["total_tokens"] = df["input_tokens"] + df["output_tokens"]
    df["cached_tokens"] = df["cache_read_tokens"] + df["cache_creation_tokens"]
    df["total_token_footprint"] = df["total_tokens"] + df["cached_tokens"]
    df["duration_seconds"] = df["duration_ms"] / 1000.0

    df["event_date"] = df["timestamp"].dt.floor("D")
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["month_label"] = df["timestamp"].dt.strftime("%Y-%m")
    df["hour"] = df["timestamp"].dt.hour
    df["weekday_num"] = df["timestamp"].dt.weekday
    df["weekday"] = df["timestamp"].dt.day_name()
    df["is_weekend"] = df["weekday_num"].isin([5, 6])
    df["is_business_hour"] = df["hour"].between(9, 17, inclusive="both")

    df["has_model"] = df["model"] != ""
    df["has_tool"] = df["tool_name"] != ""
    df["is_api_request"] = df["event_name"] == "api_request"
    df["is_tool_result"] = df["event_name"] == "tool_result"
    df["is_error"] = df["event_name"] == "api_error"

    invalid_event_name_count = int((~df["event_name"].isin(VALID_EVENT_NAMES)).sum())

    quality_summary = {
        "raw_row_count": int(raw_row_count),
        "clean_row_count": int(len(df)),
        "duplicate_rows_removed": int(duplicate_count),
        "invalid_timestamp_count": invalid_timestamp_count,
        "invalid_event_name_count": invalid_event_name_count,
    }

    return df, quality_summary


def enrich_with_employees(events: pd.DataFrame, employees: pd.DataFrame | None) -> pd.DataFrame:
    if employees is None or employees.empty:
        logging.info("Employee enrichment skipped.")
        if "practice" not in events.columns:
            events["practice"] = "Unknown"
        events["practice"] = clean_text_series(events["practice"]).replace("", "Unknown")
        return events

    logging.info("Enriching events with employee metadata...")

    df = events.copy()

    if "practice" in df.columns:
        df = df.rename(columns={"practice": "practice_resource"})

    employees = employees.copy()
    if "practice" in employees.columns:
        employees = employees.rename(columns={"practice": "practice_employee"})

    df = df.merge(employees, how="left", on="email")

    if "practice_employee" in df.columns or "practice_resource" in df.columns:
        df["practice"] = (
            df.get("practice_employee", pd.Series(index=df.index, dtype="string"))
            .combine_first(df.get("practice_resource", pd.Series(index=df.index, dtype="string")))
        )
        df["practice"] = clean_text_series(df["practice"]).replace("", "Unknown")
    else:
        df["practice"] = "Unknown"

    for col in ["full_name", "level", "location"]:
        if col not in df.columns:
            df[col] = "Unknown"
        df[col] = clean_text_series(df[col]).replace("", "Unknown")

    drop_cols = [col for col in ["practice_employee", "practice_resource"] if col in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    return df


def build_api_requests(events: pd.DataFrame) -> pd.DataFrame:
    api_requests = events[events["event_name"] == "api_request"].copy()

    if api_requests.empty:
        return api_requests

    api_requests["cost_per_1k_tokens"] = (
        api_requests["cost_usd"] / (api_requests["total_tokens"].replace(0, pd.NA) / 1000.0)
    ).fillna(0.0)

    api_requests["cache_ratio"] = (
        api_requests["cached_tokens"] / api_requests["total_token_footprint"].replace(0, pd.NA)
    ).fillna(0.0)

    return api_requests


def build_tool_results(events: pd.DataFrame) -> pd.DataFrame:
    tool_results = events[events["event_name"] == "tool_result"].copy()
    return tool_results


def build_sessions(events: pd.DataFrame, api_requests: pd.DataFrame, tool_results: pd.DataFrame) -> pd.DataFrame:
    logging.info("Building sessions table...")

    session_base = (
        events.groupby("session_id", dropna=False)
        .agg(
            user_id=("user_id", first_non_empty),
            email=("email", first_non_empty),
            full_name=("full_name", first_non_empty),
            practice=("practice", first_non_empty),
            level=("level", first_non_empty),
            location=("location", first_non_empty),
            terminal_type=("terminal_type", mode_non_empty),
            organization_id=("organization_id", first_non_empty),
            session_start=("timestamp", "min"),
            session_end=("timestamp", "max"),
            total_events=("event_id", "size"),
        )
        .reset_index()
    )

    session_base["session_duration_minutes"] = (
        (session_base["session_end"] - session_base["session_start"]).dt.total_seconds() / 60.0
    ).fillna(0.0)

    session_base["session_duration_seconds"] = (
        (session_base["session_end"] - session_base["session_start"]).dt.total_seconds()
    ).fillna(0.0)

    event_counts = pd.crosstab(events["session_id"], events["event_name"]).reset_index()
    event_counts = event_counts.rename(
        columns={
            "api_request": "count_api_request",
            "tool_decision": "count_tool_decision",
            "tool_result": "count_tool_result",
            "user_prompt": "count_user_prompt",
            "api_error": "count_api_error",
        }
    )

    api_summary = (
        api_requests.groupby("session_id", dropna=False)
        .agg(
            session_total_tokens=("total_tokens", "sum"),
            session_cached_tokens=("cached_tokens", "sum"),
            session_total_cost_usd=("cost_usd", "sum"),
            avg_api_duration_ms=("duration_ms", "mean"),
            unique_models=("model", "nunique"),
            primary_model=("model", mode_non_empty),
        )
        .reset_index()
    )

    tool_summary = (
        tool_results.groupby("session_id", dropna=False)
        .agg(
            tool_success_rate=("success", "mean"),
            avg_tool_duration_ms=("duration_ms", "mean"),
            unique_tools=("tool_name", "nunique"),
            most_used_tool=("tool_name", mode_non_empty),
        )
        .reset_index()
    )

    prompt_summary = (
        events[events["event_name"] == "user_prompt"]
        .groupby("session_id", dropna=False)
        .agg(
            avg_prompt_length=("prompt_length", "mean"),
        )
        .reset_index()
    )

    sessions = session_base.merge(event_counts, on="session_id", how="left")
    sessions = sessions.merge(api_summary, on="session_id", how="left")
    sessions = sessions.merge(tool_summary, on="session_id", how="left")
    sessions = sessions.merge(prompt_summary, on="session_id", how="left")

    numeric_fill_zero = [
        "count_api_request",
        "count_tool_decision",
        "count_tool_result",
        "count_user_prompt",
        "count_api_error",
        "session_total_tokens",
        "session_cached_tokens",
        "session_total_cost_usd",
        "avg_api_duration_ms",
        "tool_success_rate",
        "avg_tool_duration_ms",
        "unique_models",
        "unique_tools",
        "avg_prompt_length",
    ]
    for col in numeric_fill_zero:
        if col in sessions.columns:
            sessions[col] = sessions[col].fillna(0)

    for col in ["primary_model", "most_used_tool", "practice", "level", "location", "terminal_type"]:
        if col in sessions.columns:
            sessions[col] = clean_text_series(sessions[col]).replace("", "Unknown")

    sessions["tool_success_rate_pct"] = sessions["tool_success_rate"] * 100.0
    sessions["events_per_minute"] = (
        sessions["total_events"] / sessions["session_duration_minutes"].replace(0, pd.NA)
    ).fillna(sessions["total_events"])

    return sessions.sort_values("session_start").reset_index(drop=True)


def build_users(events: pd.DataFrame, api_requests: pd.DataFrame, tool_results: pd.DataFrame, sessions: pd.DataFrame) -> pd.DataFrame:
    logging.info("Building users table...")

    user_base = (
        events.groupby("user_id", dropna=False)
        .agg(
            email=("email", first_non_empty),
            full_name=("full_name", first_non_empty),
            practice=("practice", first_non_empty),
            level=("level", first_non_empty),
            location=("location", first_non_empty),
            organization_id=("organization_id", first_non_empty),
            terminal_type=("terminal_type", mode_non_empty),
            first_seen=("timestamp", "min"),
            last_seen=("timestamp", "max"),
            total_events=("event_id", "size"),
        )
        .reset_index()
    )

    user_sessions = (
        sessions.groupby("user_id", dropna=False)
        .agg(
            total_sessions=("session_id", "nunique"),
            avg_session_duration_minutes=("session_duration_minutes", "mean"),
            avg_events_per_session=("total_events", "mean"),
        )
        .reset_index()
    )

    user_api = (
        api_requests.groupby("user_id", dropna=False)
        .agg(
            total_api_requests=("event_id", "size"),
            total_tokens=("total_tokens", "sum"),
            total_cached_tokens=("cached_tokens", "sum"),
            total_cost_usd=("cost_usd", "sum"),
            avg_api_duration_ms=("duration_ms", "mean"),
            preferred_model=("model", mode_non_empty),
        )
        .reset_index()
    )

    user_tools = (
        tool_results.groupby("user_id", dropna=False)
        .agg(
            total_tool_runs=("event_id", "size"),
            tool_success_rate=("success", "mean"),
            favorite_tool=("tool_name", mode_non_empty),
        )
        .reset_index()
    )

    users = user_base.merge(user_sessions, on="user_id", how="left")
    users = users.merge(user_api, on="user_id", how="left")
    users = users.merge(user_tools, on="user_id", how="left")

    numeric_fill_zero = [
        "total_sessions",
        "avg_session_duration_minutes",
        "avg_events_per_session",
        "total_api_requests",
        "total_tokens",
        "total_cached_tokens",
        "total_cost_usd",
        "avg_api_duration_ms",
        "total_tool_runs",
        "tool_success_rate",
    ]
    for col in numeric_fill_zero:
        if col in users.columns:
            users[col] = users[col].fillna(0)

    for col in ["full_name", "practice", "level", "location", "terminal_type", "preferred_model", "favorite_tool"]:
        if col in users.columns:
            users[col] = clean_text_series(users[col]).replace("", "Unknown")

    users["tool_success_rate_pct"] = users["tool_success_rate"] * 100.0

    return users.sort_values(["practice", "total_tokens"], ascending=[True, False]).reset_index(drop=True)


def build_quality_report(
    events: pd.DataFrame,
    api_requests: pd.DataFrame,
    tool_results: pd.DataFrame,
    sessions: pd.DataFrame,
    users: pd.DataFrame,
    standardization_summary: dict[str, Any],
) -> dict[str, Any]:
    logging.info("Building data quality report...")

    negative_numeric_counts: dict[str, int] = {}
    for col in ["input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens", "cost_usd", "duration_ms"]:
        if col in events.columns:
            negative_numeric_counts[col] = int((events[col] < 0).sum())

    null_counts = {
        col: int(events[col].isna().sum())
        for col in ["timestamp", "session_id", "user_id", "email", "practice", "model", "tool_name"]
        if col in events.columns
    }

    report = {
        "summary": {
            **standardization_summary,
            "total_api_requests": int(len(api_requests)),
            "total_tool_results": int(len(tool_results)),
            "total_sessions": int(len(sessions)),
            "total_users": int(len(users)),
            "timestamp_min": str(events["timestamp"].min()) if not events.empty else None,
            "timestamp_max": str(events["timestamp"].max()) if not events.empty else None,
        },
        "validation": {
            "required_columns_present": sorted(list(REQUIRED_COLUMNS)),
            "known_event_names": sorted(list(VALID_EVENT_NAMES)),
            "event_name_distribution": {
                str(key): int(value)
                for key, value in events["event_name"].value_counts(dropna=False).to_dict().items()
            },
            "null_counts": null_counts,
            "negative_numeric_counts": negative_numeric_counts,
            "missing_practice_count": int((events["practice"] == "Unknown").sum()) if "practice" in events.columns else 0,
            "missing_level_count": int((events["level"] == "Unknown").sum()) if "level" in events.columns else 0,
            "missing_location_count": int((events["location"] == "Unknown").sum()) if "location" in events.columns else 0,
        },
    }

    return report


def save_outputs(
    events: pd.DataFrame,
    api_requests: pd.DataFrame,
    tool_results: pd.DataFrame,
    sessions: pd.DataFrame,
    users: pd.DataFrame,
    quality_report: dict[str, Any],
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logging.info("Saving processed datasets...")
    events.to_parquet(OUTPUT_EVENTS, index=False)
    api_requests.to_parquet(OUTPUT_API_REQUESTS, index=False)
    tool_results.to_parquet(OUTPUT_TOOL_RESULTS, index=False)
    sessions.to_parquet(OUTPUT_SESSIONS, index=False)
    users.to_parquet(OUTPUT_USERS, index=False)

    with open(OUTPUT_QUALITY_REPORT, "w", encoding="utf-8") as f:
        json.dump(quality_report, f, indent=2)

    logging.info("Saved: %s", OUTPUT_EVENTS)
    logging.info("Saved: %s", OUTPUT_API_REQUESTS)
    logging.info("Saved: %s", OUTPUT_TOOL_RESULTS)
    logging.info("Saved: %s", OUTPUT_SESSIONS)
    logging.info("Saved: %s", OUTPUT_USERS)
    logging.info("Saved: %s", OUTPUT_QUALITY_REPORT)


def print_summary(
    events: pd.DataFrame,
    api_requests: pd.DataFrame,
    tool_results: pd.DataFrame,
    sessions: pd.DataFrame,
    users: pd.DataFrame,
) -> None:
    print("\n=== DATA PROCESSING SUMMARY ===")
    print(f"Events: {len(events):,}")
    print(f"API requests: {len(api_requests):,}")
    print(f"Tool results: {len(tool_results):,}")
    print(f"Sessions: {len(sessions):,}")
    print(f"Users: {len(users):,}")
    print(f"Date range: {events['timestamp'].min()} -> {events['timestamp'].max()}")
    print(f"Total tokens: {events['total_tokens'].sum():,.0f}")
    print(f"Total cost: ${events['cost_usd'].sum():,.2f}")


def main() -> None:
    setup_logging()

    raw_events = load_events()
    employees = load_employees()

    standardized_events, standardization_summary = standardize_events(raw_events)
    enriched_events = enrich_with_employees(standardized_events, employees)

    api_requests = build_api_requests(enriched_events)
    tool_results = build_tool_results(enriched_events)
    sessions = build_sessions(enriched_events, api_requests, tool_results)
    users = build_users(enriched_events, api_requests, tool_results, sessions)
    quality_report = build_quality_report(
        enriched_events,
        api_requests,
        tool_results,
        sessions,
        users,
        standardization_summary,
    )

    save_outputs(
        enriched_events,
        api_requests,
        tool_results,
        sessions,
        users,
        quality_report,
    )
    print_summary(enriched_events, api_requests, tool_results, sessions, users)


if __name__ == "__main__":
    main()