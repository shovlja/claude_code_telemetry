from pathlib import Path
import pandas as pd

INPUT_PATH = Path("data/processed/events_clean.parquet")
OUTPUT_DIR = Path("data/processed/metrics")


def load_events() -> pd.DataFrame:
    df = pd.read_parquet(INPUT_PATH)
    return df


def get_overview_metrics(df: pd.DataFrame) -> dict:
    api_requests = df[df["event_name"] == "api_request"].copy()

    return {
        "total_events": int(len(df)),
        "total_sessions": int(df["session_id"].nunique()),
        "total_users": int(df["user_id"].nunique()),
        "total_api_requests": int(len(api_requests)),
        "total_tokens": float(api_requests["total_tokens"].fillna(0).sum()),
        "total_cost_usd": float(api_requests["cost_usd"].fillna(0).sum()),
    }


def get_token_usage_by_practice(df: pd.DataFrame) -> pd.DataFrame:
    api_requests = df[df["event_name"] == "api_request"].copy()

    result = (
        api_requests.groupby("practice", dropna=False)["total_tokens"]
        .sum()
        .reset_index()
        .sort_values("total_tokens", ascending=False)
    )

    return result


def get_cost_by_model(df: pd.DataFrame) -> pd.DataFrame:
    api_requests = df[df["event_name"] == "api_request"].copy()

    result = (
        api_requests.groupby("model", dropna=False)["cost_usd"]
        .sum()
        .reset_index()
        .sort_values("cost_usd", ascending=False)
    )

    return result


def get_usage_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.groupby("hour", dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values("hour")
    )

    return result


def get_daily_usage(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.groupby("day", dropna=False)
        .size()
        .reset_index(name="event_count")
        .sort_values("day")
    )

    return result


def get_top_tools(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    tool_df = df[df["tool_name"].fillna("") != ""].copy()

    result = (
        tool_df.groupby("tool_name")
        .size()
        .reset_index(name="usage_count")
        .sort_values("usage_count", ascending=False)
        .head(top_n)
    )

    return result


def get_tool_success_rate(df: pd.DataFrame) -> pd.DataFrame:
    tool_results = df[df["event_name"] == "tool_result"].copy()

    if tool_results.empty:
        return pd.DataFrame(columns=["tool_name", "success_rate", "total_runs"])

    result = (
        tool_results.groupby("tool_name")
        .agg(
            success_rate=("success", "mean"),
            total_runs=("tool_name", "size"),
        )
        .reset_index()
        .sort_values(["success_rate", "total_runs"], ascending=[False, False])
    )

    result["success_rate"] = result["success_rate"] * 100

    return result


def export_metrics(df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    overview = get_overview_metrics(df)
    pd.DataFrame([overview]).to_csv(OUTPUT_DIR / "overview.csv", index=False)

    get_token_usage_by_practice(df).to_csv(
        OUTPUT_DIR / "token_usage_by_practice.csv", index=False
    )
    get_cost_by_model(df).to_csv(
        OUTPUT_DIR / "cost_by_model.csv", index=False
    )
    get_usage_by_hour(df).to_csv(
        OUTPUT_DIR / "usage_by_hour.csv", index=False
    )
    get_daily_usage(df).to_csv(
        OUTPUT_DIR / "daily_usage.csv", index=False
    )
    get_top_tools(df).to_csv(
        OUTPUT_DIR / "top_tools.csv", index=False
    )
    get_tool_success_rate(df).to_csv(
        OUTPUT_DIR / "tool_success_rate.csv", index=False
    )


def main():
    df = load_events()

    overview = get_overview_metrics(df)
    print("=== OVERVIEW ===")
    for key, value in overview.items():
        if key == "total_cost_usd":
            print(f"{key}: ${value:,.2f}")
        else:
            print(f"{key}: {value:,.0f}")

    print("\n=== TOKEN USAGE BY PRACTICE ===")
    print(get_token_usage_by_practice(df).head())

    print("\n=== COST BY MODEL ===")
    print(get_cost_by_model(df).head())

    print("\n=== USAGE BY HOUR ===")
    print(get_usage_by_hour(df).head())

    print("\n=== TOP TOOLS ===")
    print(get_top_tools(df).head())

    print("\n=== TOOL SUCCESS RATE ===")
    print(get_tool_success_rate(df).head())

    export_metrics(df)
    print(f"\nMetrics exported to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()