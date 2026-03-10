import json
import pandas as pd
from pathlib import Path


def parse_telemetry_logs(file_path: str) -> pd.DataFrame:
    """
    Parse telemetry_logs.jsonl into a flat pandas DataFrame.
    """

    events = []

    with open(file_path, "r") as f:
        for line in f:
            batch = json.loads(line)

            for log_event in batch.get("logEvents", []):
                message = json.loads(log_event["message"])

                attributes = message.get("attributes", {})
                resource = message.get("resource", {})

                event = {
                    "event_type": message.get("body"),
                    "event_name": attributes.get("event.name"),
                    "timestamp": attributes.get("event.timestamp"),
                    "session_id": attributes.get("session.id"),
                    "user_id": attributes.get("user.id"),
                    "email": attributes.get("user.email"),
                    "model": attributes.get("model"),
                    "input_tokens": attributes.get("input_tokens"),
                    "output_tokens": attributes.get("output_tokens"),
                    "cache_read_tokens": attributes.get("cache_read_tokens"),
                    "cache_creation_tokens": attributes.get("cache_creation_tokens"),
                    "cost_usd": attributes.get("cost_usd"),
                    "duration_ms": attributes.get("duration_ms"),
                    "tool_name": attributes.get("tool_name"),
                    "success": attributes.get("success"),
                    "terminal_type": attributes.get("terminal.type"),
                    "organization_id": attributes.get("organization.id"),
                    "practice": resource.get("user.practice"),
                }

                events.append(event)

    df = pd.DataFrame(events)

    return df


def main():

    raw_path = Path("data/raw/telemetry_logs.jsonl")

    df = parse_telemetry_logs(raw_path)

    print("Events parsed:", len(df))
    print(df.head())

    output_path = Path("data/processed/events.parquet")

    df.to_parquet(output_path, index=False)

    print("Saved to:", output_path)


if __name__ == "__main__":
    main()