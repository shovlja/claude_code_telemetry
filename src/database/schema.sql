CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.overview AS
SELECT
    (SELECT COUNT(*) FROM events) AS total_events,
    (SELECT COUNT(*) FROM api_requests) AS total_api_requests,
    (SELECT COUNT(*) FROM tool_results) AS total_tool_results,
    (SELECT COUNT(*) FROM sessions) AS total_sessions,
    (SELECT COUNT(*) FROM users) AS total_users,
    ROUND(COALESCE((SELECT SUM(total_tokens) FROM api_requests), 0), 0) AS total_tokens,
    ROUND(COALESCE((SELECT SUM(cost_usd) FROM api_requests), 0), 2) AS total_cost_usd,
    ROUND(COALESCE((SELECT AVG(session_duration_minutes) FROM sessions), 0), 2) AS avg_session_duration_minutes,
    ROUND(COALESCE((SELECT AVG(session_total_tokens) FROM sessions), 0), 2) AS avg_tokens_per_session,
    ROUND(COALESCE((SELECT AVG(session_total_cost_usd) FROM sessions), 0), 4) AS avg_cost_per_session;

CREATE OR REPLACE VIEW analytics.token_usage_by_practice AS
SELECT
    practice,
    COUNT(*) AS api_requests,
    ROUND(SUM(total_tokens), 0) AS total_tokens,
    ROUND(SUM(cost_usd), 2) AS total_cost_usd,
    ROUND(AVG(total_tokens), 2) AS avg_tokens_per_request
FROM api_requests
GROUP BY practice
ORDER BY total_tokens DESC;

CREATE OR REPLACE VIEW analytics.cost_by_model AS
SELECT
    model,
    COUNT(*) AS api_requests,
    ROUND(SUM(cost_usd), 2) AS total_cost_usd,
    ROUND(SUM(total_tokens), 0) AS total_tokens,
    ROUND(AVG(cost_usd), 4) AS avg_cost_per_request
FROM api_requests
WHERE model IS NOT NULL AND model <> ''
GROUP BY model
ORDER BY total_cost_usd DESC;

CREATE OR REPLACE VIEW analytics.usage_by_hour AS
SELECT
    hour,
    COUNT(*) AS event_count
FROM events
GROUP BY hour
ORDER BY hour;

CREATE OR REPLACE VIEW analytics.usage_by_weekday AS
SELECT
    weekday_num,
    weekday,
    COUNT(*) AS event_count
FROM events
GROUP BY weekday_num, weekday
ORDER BY weekday_num;

CREATE OR REPLACE VIEW analytics.daily_usage AS
SELECT
    event_date,
    COUNT(*) AS event_count
FROM events
GROUP BY event_date
ORDER BY event_date;

CREATE OR REPLACE VIEW analytics.sessions_by_practice AS
SELECT
    practice,
    COUNT(*) AS total_sessions,
    ROUND(AVG(session_duration_minutes), 2) AS avg_session_duration_minutes,
    ROUND(AVG(session_total_tokens), 2) AS avg_tokens_per_session,
    ROUND(SUM(session_total_cost_usd), 2) AS total_cost_usd
FROM sessions
GROUP BY practice
ORDER BY total_sessions DESC;

CREATE OR REPLACE VIEW analytics.top_users_by_tokens AS
SELECT
    user_id,
    full_name,
    email,
    practice,
    level,
    location,
    total_sessions,
    ROUND(total_tokens, 0) AS total_tokens,
    ROUND(total_cost_usd, 2) AS total_cost_usd,
    preferred_model,
    favorite_tool
FROM users
ORDER BY total_tokens DESC
LIMIT 15;

CREATE OR REPLACE VIEW analytics.top_users_by_cost AS
SELECT
    user_id,
    full_name,
    email,
    practice,
    level,
    location,
    total_sessions,
    ROUND(total_tokens, 0) AS total_tokens,
    ROUND(total_cost_usd, 2) AS total_cost_usd,
    preferred_model,
    favorite_tool
FROM users
ORDER BY total_cost_usd DESC
LIMIT 15;

CREATE OR REPLACE VIEW analytics.error_rate_by_model AS
WITH requests AS (
    SELECT
        model,
        COUNT(*) AS api_requests
    FROM api_requests
    WHERE model IS NOT NULL AND model <> ''
    GROUP BY model
),
errors AS (
    SELECT
        model,
        COUNT(*) AS api_errors
    FROM events
    WHERE event_name = 'api_error'
      AND model IS NOT NULL
      AND model <> ''
    GROUP BY model
)
SELECT
    r.model,
    r.api_requests,
    COALESCE(e.api_errors, 0) AS api_errors,
    ROUND(COALESCE(e.api_errors, 0) * 100.0 / NULLIF(r.api_requests, 0), 2) AS error_rate_pct
FROM requests r
LEFT JOIN errors e USING (model)
ORDER BY error_rate_pct DESC, api_requests DESC;

CREATE OR REPLACE VIEW analytics.tool_performance AS
SELECT
    tool_name,
    COUNT(*) AS total_runs,
    ROUND(AVG(CASE WHEN success THEN 1 ELSE 0 END) * 100, 2) AS success_rate_pct,
    ROUND(AVG(duration_ms), 2) AS avg_duration_ms
FROM tool_results
WHERE tool_name IS NOT NULL AND tool_name <> ''
GROUP BY tool_name
ORDER BY total_runs DESC;

CREATE OR REPLACE VIEW analytics.session_outliers AS
SELECT
    session_id,
    user_id,
    full_name,
    practice,
    level,
    session_start,
    session_duration_minutes,
    session_total_tokens,
    session_total_cost_usd,
    primary_model,
    most_used_tool
FROM sessions
ORDER BY session_total_tokens DESC
LIMIT 20;