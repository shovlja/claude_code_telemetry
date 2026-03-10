from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(
    page_title="Claude Code Usage Analytics",
    page_icon="📊",
    layout="wide",
)

DATA_PATH = Path("data/processed/events_clean.parquet")


@st.cache_data
def load_data() -> pd.DataFrame:
    return pd.read_parquet(DATA_PATH)


def format_number(value: float | int) -> str:
    return f"{int(value):,}"


def format_currency(value: float) -> str:
    return f"${value:,.2f}"


def build_insights(
    filtered_df: pd.DataFrame,
    filtered_api: pd.DataFrame,
    filtered_tools: pd.DataFrame,
) -> list[str]:
    insights = []

    if not filtered_api.empty:
        top_practice = (
            filtered_api.groupby("practice")["total_tokens"]
            .sum()
            .sort_values(ascending=False)
        )
        if not top_practice.empty:
            practice_name = top_practice.index[0]
            practice_tokens = top_practice.iloc[0]
            insights.append(
                f"Najveći token usage trenutno ima **{practice_name}** sa **{int(practice_tokens):,}** tokena."
            )

        top_model_cost = (
            filtered_api.groupby("model")["cost_usd"]
            .sum()
            .sort_values(ascending=False)
        )
        if not top_model_cost.empty:
            model_name = top_model_cost.index[0]
            model_cost = top_model_cost.iloc[0]
            insights.append(
                f"Najveći trošak generiše model **{model_name}** sa ukupno **${model_cost:,.2f}**."
            )

    if not filtered_df.empty:
        usage_by_hour = (
            filtered_df.groupby("hour")
            .size()
            .sort_values(ascending=False)
        )
        if not usage_by_hour.empty:
            peak_hour = usage_by_hour.index[0]
            peak_count = usage_by_hour.iloc[0]
            insights.append(
                f"Najveća aktivnost je oko **{peak_hour:02d}:00** sa **{int(peak_count):,}** eventova."
            )

    if not filtered_tools.empty:
        tool_usage = (
            filtered_tools.groupby("tool_name")
            .size()
            .sort_values(ascending=False)
        )
        if not tool_usage.empty:
            tool_name = tool_usage.index[0]
            tool_count = tool_usage.iloc[0]
            insights.append(
                f"Najkorišćeniji alat je **{tool_name}** sa **{int(tool_count):,}** izvršavanja."
            )

    return insights[:4]


df = load_data()

api_requests = df[df["event_name"] == "api_request"].copy()
tool_results = df[df["event_name"] == "tool_result"].copy()

# -----------------------------
# Styling
# -----------------------------
st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 2rem;
        }

        .hero {
            padding: 1.4rem 1.6rem;
            border-radius: 18px;
            background: linear-gradient(135deg, rgba(32,39,55,1) 0%, rgba(16,20,31,1) 100%);
            border: 1px solid rgba(255,255,255,0.08);
            margin-bottom: 1rem;
        }

        .hero h1 {
            margin: 0;
            font-size: 2rem;
        }

        .hero p {
            margin: 0.35rem 0 0 0;
            color: #B8C0CC;
            font-size: 0.98rem;
        }

        .section-title {
            font-size: 1.1rem;
            font-weight: 700;
            margin-top: 0.25rem;
            margin-bottom: 0.75rem;
        }

        .insight-card {
            padding: 1rem 1rem;
            border-radius: 16px;
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            min-height: 110px;
        }

        .insight-card h4 {
            margin: 0 0 0.4rem 0;
            font-size: 0.95rem;
        }

        .insight-card p {
            margin: 0;
            color: #D7DCE3;
            font-size: 0.92rem;
            line-height: 1.45;
        }

        div[data-testid="metric-container"] {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            padding: 0.85rem 1rem;
            border-radius: 16px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Header
# -----------------------------
st.markdown(
    """
    <div class="hero">
        <h1>Claude Code Usage Analytics</h1>
        <p>Interactive dashboard for telemetry analysis, developer behavior, token usage, cost trends, and tool performance.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Sidebar filters
# -----------------------------
st.sidebar.title("Controls")

if st.sidebar.button("Reset filters", use_container_width=True):
    st.session_state.pop("selected_practices", None)
    st.session_state.pop("selected_models", None)

with st.sidebar.expander("Filters", expanded=False):
    practices = sorted([p for p in df["practice"].dropna().unique() if p != ""])
    selected_practices = st.multiselect(
        "Engineering Practice",
        options=practices,
        default=st.session_state.get("selected_practices", practices),
        key="selected_practices",
    )

    models = sorted([m for m in api_requests["model"].dropna().unique() if m != ""])
    selected_models = st.multiselect(
        "Model",
        options=models,
        default=st.session_state.get("selected_models", models),
        key="selected_models",
    )

# -----------------------------
# Filtering
# -----------------------------
filtered_df = df.copy()
filtered_api = api_requests.copy()
filtered_tools = tool_results.copy()

if selected_practices:
    filtered_df = filtered_df[filtered_df["practice"].isin(selected_practices)]
    filtered_api = filtered_api[filtered_api["practice"].isin(selected_practices)]
    filtered_tools = filtered_tools[filtered_tools["practice"].isin(selected_practices)]

if selected_models:
    filtered_api = filtered_api[filtered_api["model"].isin(selected_models)]

valid_sessions = filtered_api["session_id"].unique()
if len(valid_sessions) > 0:
    filtered_df = filtered_df[filtered_df["session_id"].isin(valid_sessions)]
    filtered_tools = filtered_tools[filtered_tools["session_id"].isin(valid_sessions)]
else:
    filtered_df = filtered_df.iloc[0:0]
    filtered_tools = filtered_tools.iloc[0:0]

# -----------------------------
# KPI cards
# -----------------------------
total_events = len(filtered_df)
total_sessions = filtered_df["session_id"].nunique()
total_users = filtered_df["user_id"].nunique()
total_tokens = filtered_api["total_tokens"].fillna(0).sum()
total_cost = filtered_api["cost_usd"].fillna(0).sum()

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Events", format_number(total_events))
col2.metric("Total Sessions", format_number(total_sessions))
col3.metric("Total Users", format_number(total_users))
col4.metric("Total Tokens", format_number(total_tokens))
col5.metric("Total Cost", format_currency(total_cost))

st.markdown('<div class="section-title">Key Insights</div>', unsafe_allow_html=True)

insights = build_insights(filtered_df, filtered_api, filtered_tools)
insight_cols = st.columns(4)

for idx, col in enumerate(insight_cols):
    if idx < len(insights):
        col.markdown(
            f"""
            <div class="insight-card">
                <h4>Insight {idx + 1}</h4>
                <p>{insights[idx]}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        col.markdown(
            """
            <div class="insight-card">
                <h4>Insight</h4>
                <p>Nema dovoljno podataka za dodatni zaključak u trenutnom filteru.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.divider()

tab1, tab2, tab3 = st.tabs(["Usage & Cost", "Tools", "Data Preview"])

with tab1:
    col1, col2 = st.columns(2)

    with col1:
        tokens_by_practice = (
            filtered_api.groupby("practice", dropna=False)["total_tokens"]
            .sum()
            .reset_index()
            .sort_values("total_tokens", ascending=False)
        )
        fig = px.bar(
            tokens_by_practice,
            x="practice",
            y="total_tokens",
            title="Token Usage by Engineering Practice",
        )
        fig.update_layout(xaxis_title="", yaxis_title="Total Tokens")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        cost_by_model = (
            filtered_api.groupby("model", dropna=False)["cost_usd"]
            .sum()
            .reset_index()
            .sort_values("cost_usd", ascending=False)
        )
        fig = px.bar(
            cost_by_model,
            x="model",
            y="cost_usd",
            title="Cost by Model",
        )
        fig.update_layout(xaxis_title="", yaxis_title="Cost (USD)")
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        usage_by_hour = (
            filtered_df.groupby("hour", dropna=False)
            .size()
            .reset_index(name="event_count")
            .sort_values("hour")
        )
        fig = px.line(
            usage_by_hour,
            x="hour",
            y="event_count",
            markers=True,
            title="Usage by Hour of Day",
        )
        fig.update_layout(xaxis_title="Hour", yaxis_title="Event Count")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        daily_usage = (
            filtered_df.groupby("day", dropna=False)
            .size()
            .reset_index(name="event_count")
            .sort_values("day")
        )
        fig = px.line(
            daily_usage,
            x="day",
            y="event_count",
            title="Daily Usage Trend",
        )
        fig.update_layout(xaxis_title="Day", yaxis_title="Event Count")
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    col1, col2 = st.columns(2)

    with col1:
        top_tools = (
            filtered_df[filtered_df["tool_name"].fillna("") != ""]
            .groupby("tool_name")
            .size()
            .reset_index(name="usage_count")
            .sort_values("usage_count", ascending=False)
            .head(10)
        )
        fig = px.bar(
            top_tools,
            x="tool_name",
            y="usage_count",
            title="Top 10 Tools",
        )
        fig.update_layout(xaxis_title="", yaxis_title="Usage Count")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        success_rate = (
            filtered_tools.groupby("tool_name")
            .agg(
                success_rate=("success", "mean"),
                total_runs=("tool_name", "size"),
            )
            .reset_index()
        )

        if not success_rate.empty:
            success_rate["success_rate"] = success_rate["success_rate"] * 100
            success_rate = success_rate.sort_values(
                ["success_rate", "total_runs"],
                ascending=[False, False],
            ).head(10)

        fig = px.bar(
            success_rate,
            x="tool_name",
            y="success_rate",
            title="Tool Success Rate (%)",
        )
        fig.update_layout(xaxis_title="", yaxis_title="Success Rate (%)")
        st.plotly_chart(fig, use_container_width=True)

    tool_table = (
        filtered_tools.groupby("tool_name")
        .agg(
            total_runs=("tool_name", "size"),
            success_rate=("success", "mean"),
            avg_duration_ms=("duration_ms", "mean"),
        )
        .reset_index()
        .sort_values("total_runs", ascending=False)
    )

    if not tool_table.empty:
        tool_table["success_rate"] = (tool_table["success_rate"] * 100).round(2)
        tool_table["avg_duration_ms"] = tool_table["avg_duration_ms"].round(2)

    st.subheader("Tool Performance Summary")
    st.dataframe(tool_table, use_container_width=True, height=350)

with tab3:
    st.subheader("Filtered Data Preview")

    preview_cols = [
        "timestamp",
        "event_name",
        "practice",
        "model",
        "tool_name",
        "total_tokens",
        "cost_usd",
        "duration_ms",
        "success",
    ]
    available_cols = [c for c in preview_cols if c in filtered_df.columns]

    st.dataframe(
        filtered_df[available_cols].head(300),
        use_container_width=True,
        height=500,
    )