import pandas as pd

INPUT_PATH = "data/processed/events.parquet"
OUTPUT_PATH = "data/processed/events_clean.parquet"


def transform_events():
    df = pd.read_parquet(INPUT_PATH)

    print("Rows loaded:", len(df))

    # timestamp -> datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # time features
    df["hour"] = df["timestamp"].dt.hour
    df["day"] = df["timestamp"].dt.date
    df["weekday"] = df["timestamp"].dt.day_name()

    # numeric columns
    numeric_cols = [
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_creation_tokens",
        "cost_usd",
        "duration_ms",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # total tokens
    df["total_tokens"] = (
        df["input_tokens"].fillna(0) + df["output_tokens"].fillna(0)
    )

    # success -> boolean
    if "success" in df.columns:
        df["success"] = (
            df["success"]
            .astype(str)
            .str.strip()
            .str.lower()
            .map({"true": True, "false": False, "none": False, "nan": False})
            .fillna(False)
            .astype(bool)
        )

    # optional: fill missing text columns to avoid mixed object issues
    text_cols = ["tool_name", "model", "practice", "terminal_type", "event_type", "event_name"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("")

    # remove duplicates
    df = df.drop_duplicates()

    print("Rows after cleaning:", len(df))

    df.to_parquet(OUTPUT_PATH, index=False)

    print("Saved cleaned dataset ->", OUTPUT_PATH)


if __name__ == "__main__":
    transform_events()