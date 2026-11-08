use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env, process,
};

use reqwest::Client;
use serde_json::{json, Value};

const WINDOWS: &[(&str, Option<i64>)] = &[
    ("5m", Some(300)),
    ("10m", Some(600)),
    ("1h", Some(3600)),
    ("6h", Some(21600)),
    ("1d", Some(86400)),
    ("all", None),
];
const STUCK_WINDOW_MS: i64 = 3_600_000;
const STUCK_SECONDS: i64 = 300;

fn usage() {
    eprintln!("Rustash delivery health check\n\nUsage: rs-check [--window 5m|10m|1h|6h|1d|all] [--all] [--json] [--url URL] [--token TOKEN]\n\nDefaults: URL=http://127.0.0.1:8080, window=10m, token=QSTASH_TOKEN");
}

fn arg_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn short_url(url: &str) -> String {
    let s = url.split("/api/").last().unwrap_or(url);
    s.trim_end_matches('/').to_string()
}

async fn get_json(client: &Client, base: &str, token: &str, path: &str) -> Result<Value, String> {
    let response = client
        .get(format!("{base}{path}"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("{path}: {e}"))?;
    let status = response.status();
    let body = response.text().await.map_err(|e| format!("{path}: {e}"))?;
    if !status.is_success() {
        return Err(format!("{path} -> {status}: {body}"));
    }
    serde_json::from_str(&body).map_err(|e| format!("{path}: invalid JSON: {e}"))
}

async fn paginate(
    client: &Client,
    base: &str,
    token: &str,
    path: &str,
    field: &str,
) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut url = format!("{base}{path}?count=1000");
        if let Some(c) = &cursor {
            url.push_str("&cursor=");
            url.push_str(&urlencoding(c));
        }
        let response = client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| format!("{path}: {e}"))?;
        let status = response.status();
        let body = response.text().await.map_err(|e| format!("{path}: {e}"))?;
        if !status.is_success() {
            return Err(format!("{path} -> {status}: {body}"));
        }
        let page: Value =
            serde_json::from_str(&body).map_err(|e| format!("{path}: invalid JSON: {e}"))?;
        if let Some(items) = page.get(field).and_then(Value::as_array) {
            out.extend(items.iter().cloned());
        }
        cursor = page
            .get("cursor")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        if cursor.is_none() || out.len() >= 200_000 {
            break;
        }
    }
    Ok(out)
}

fn urlencoding(input: &str) -> String {
    input
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn analyze(events: &[Value], runs: &[Value]) -> Value {
    let mut states = BTreeMap::<String, usize>::new();
    let mut by_message = HashMap::<String, Vec<&Value>>::new();
    let mut endpoints = HashMap::<String, usize>::new();
    let mut duplicated = 0usize;
    let mut latest_error = 0i64;
    for event in events {
        let state = event
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN");
        *states.entry(state.to_string()).or_default() += 1;
        let id = event.get("messageId").and_then(Value::as_str).unwrap_or("");
        by_message.entry(id.to_string()).or_default().push(event);
        if state == "ERROR" || state == "FAILED" {
            latest_error = latest_error.max(event.get("time").and_then(Value::as_i64).unwrap_or(0));
        }
        if state == "DELIVERED" {
            let url = event.get("url").and_then(Value::as_str).unwrap_or("");
            *endpoints.entry(short_url(url)).or_default() += 1;
        }
        if event
            .get("header")
            .and_then(|h| h.get("Content-Type"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains(", ")
        {
            duplicated += 1;
        }
    }
    let mut delivered = 0usize;
    let mut failed = 0usize;
    let mut heavy = 0usize;
    for rows in by_message.values() {
        let mut rows = rows.clone();
        rows.sort_by_key(|e| e.get("time").and_then(Value::as_i64).unwrap_or(0));
        if rows
            .iter()
            .any(|e| e.get("state").and_then(Value::as_str) == Some("DELIVERED"))
        {
            delivered += 1;
        }
        if rows
            .last()
            .and_then(|e| e.get("state"))
            .and_then(Value::as_str)
            == Some("FAILED")
        {
            failed += 1;
        }
        let errors = rows
            .iter()
            .filter(|e| e.get("state").and_then(Value::as_str) == Some("ERROR"))
            .count();
        let retries = rows
            .iter()
            .filter_map(|e| e.get("retried").and_then(Value::as_i64))
            .max()
            .unwrap_or(0);
        if errors >= 2 || retries >= 2 {
            heavy += 1;
        }
    }
    let total = delivered + failed;
    let success_rate = if total == 0 {
        Value::Null
    } else {
        json!(delivered as f64 / total as f64)
    };
    let mut top_endpoints: Vec<(String, usize)> = endpoints.into_iter().collect();
    top_endpoints.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    top_endpoints.truncate(6);
    let mut run_states = BTreeMap::<String, usize>::new();
    for run in runs {
        *run_states
            .entry(
                run.get("workflowState")
                    .and_then(Value::as_str)
                    .unwrap_or("UNKNOWN")
                    .to_string(),
            )
            .or_default() += 1;
    }
    json!({
        "events": events.len(), "messages": by_message.len(), "states": states,
        "delivered": delivered, "failed": failed, "successRate": success_rate,
        "heavy": heavy, "duplicated": duplicated,
        "latestError": if latest_error > 0 { json!(latest_error) } else { Value::Null },
        "endpoints": top_endpoints.iter().map(|(url,n)| json!({"url":url,"count":n})).collect::<Vec<_>>(),
        "runStates": run_states, "runTotal": runs.len()
    })
}

fn find_stuck(events: &[Value], now: i64) -> Vec<Value> {
    let mut by_message = HashMap::<String, Vec<&Value>>::new();
    for event in events {
        let id = event.get("messageId").and_then(Value::as_str).unwrap_or("");
        by_message.entry(id.to_string()).or_default().push(event);
    }
    let mut stuck = Vec::new();
    for rows in by_message.values() {
        let last = rows
            .iter()
            .max_by_key(|e| e.get("time").and_then(Value::as_i64).unwrap_or(0))
            .copied();
        let Some(last) = last else { continue };
        let state = last.get("state").and_then(Value::as_str).unwrap_or("");
        if !["ACTIVE", "RETRY", "ERROR", "FAILED"].contains(&state) {
            continue;
        }
        let time = last.get("time").and_then(Value::as_i64).unwrap_or(now);
        let age = (now - time) / 1000;
        if age <= STUCK_SECONDS {
            continue;
        }
        stuck.push(json!({
            "ageSeconds": age, "messageId": last.get("messageId").cloned().unwrap_or(Value::Null),
            "state": state, "url": short_url(last.get("url").and_then(Value::as_str).unwrap_or("")),
            "detail": last.get("error").or_else(|| last.get("responseStatus")).cloned().unwrap_or(Value::Null)
        }));
    }
    stuck.sort_by(|a, b| {
        b.get("ageSeconds")
            .and_then(Value::as_i64)
            .cmp(&a.get("ageSeconds").and_then(Value::as_i64))
    });
    stuck
}

fn timestamp_utc(value: &Value) -> Value {
    value
        .as_i64()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|date| json!(date.format("%Y-%m-%d %H:%M:%S UTC").to_string()))
        .unwrap_or(Value::Null)
}

fn schedule_summary(schedule: &Value) -> Value {
    json!({
        "scheduleId": schedule.get("scheduleId"),
        "destination": schedule.get("destination"),
        "cron": schedule.get("cron"),
        "method": schedule.get("method"),
        "isPaused": schedule.get("isPaused"),
        "nextScheduleTime": schedule.get("nextScheduleTime"),
        "nextScheduleTimeUtc": schedule.get("nextScheduleTime").map(timestamp_utc).unwrap_or(Value::Null),
        "lastScheduleTime": schedule.get("lastScheduleTime"),
        "lastScheduleTimeUtc": schedule.get("lastScheduleTime").map(timestamp_utc).unwrap_or(Value::Null),
        "lastScheduleStates": schedule.get("lastScheduleStates"),
        "retries": schedule.get("retries"),
        "queueName": schedule.get("queueName")
    })
}

fn workflow_summary(runs: &[Value]) -> Value {
    let mut states = BTreeMap::<String, usize>::new();
    for run in runs {
        *states
            .entry(
                run.get("workflowState")
                    .and_then(Value::as_str)
                    .unwrap_or("UNKNOWN")
                    .to_string(),
            )
            .or_default() += 1;
    }
    let mut recent: Vec<&Value> = runs.iter().collect();
    recent.sort_by_key(|run| {
        std::cmp::Reverse(
            run.get("workflowRunCreatedAt")
                .and_then(Value::as_i64)
                .unwrap_or(0),
        )
    });
    let details = |run: &Value| {
        json!({
            "workflowRunId": run.get("workflowRunId"),
            "workflowUrl": run.get("workflowUrl"),
            "workflowState": run.get("workflowState"),
            "workflowRunCreatedAt": run.get("workflowRunCreatedAt"),
            "workflowRunCreatedAtUtc": run.get("workflowRunCreatedAt").map(timestamp_utc).unwrap_or(Value::Null),
            "workflowRunCompletedAt": run.get("workflowRunCompletedAt"),
            "workflowRunCompletedAtUtc": run.get("workflowRunCompletedAt").map(timestamp_utc).unwrap_or(Value::Null),
            "dlqId": run.get("dlqId"),
            "flowControlKey": run.get("flowControlKey")
        })
    };
    let active: Vec<Value> = recent
        .iter()
        .filter(|run| run.get("workflowState").and_then(Value::as_str) == Some("RUN_STARTED"))
        .map(|run| details(run))
        .collect();
    let failed: Vec<Value> = recent
        .iter()
        .filter(|run| run.get("workflowState").and_then(Value::as_str) == Some("RUN_FAILED"))
        .take(20)
        .map(|run| details(run))
        .collect();
    let recent: Vec<Value> = recent.into_iter().take(20).map(details).collect();
    json!({"total": runs.len(), "states": states, "activeRuns": active, "failedRuns": failed, "recentRuns": recent})
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        usage();
        return;
    }
    let token = arg_value(&args, "--token")
        .or_else(|| env::var("QSTASH_TOKEN").ok())
        .unwrap_or_default();
    if token.is_empty() {
        eprintln!("缺少 token: 设置 QSTASH_TOKEN 或 --token");
        process::exit(2);
    }
    let base = arg_value(&args, "--url")
        .or_else(|| env::var("RS_CHECK_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:8080".into())
        .trim_end_matches('/')
        .to_string();
    let json_out = args.iter().any(|a| a == "--json");
    let selected: Vec<String> = if args.iter().any(|a| a == "--all") {
        WINDOWS
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect()
    } else {
        vec![arg_value(&args, "--window").unwrap_or_else(|| "10m".to_string())]
    };
    for window in &selected {
        if !WINDOWS.iter().any(|(name, _)| name == window) {
            eprintln!("未知窗口: {window}（可选 5m / 10m / 1h / 6h / 1d / all）");
            process::exit(2);
        }
    }
    let client = match Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("创建 HTTP 客户端失败: {e}");
            process::exit(2);
        }
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let events = match paginate(&client, &base, &token, "/v2/logs", "logs").await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("检查失败: {e}");
            process::exit(2);
        }
    };
    let runs = match paginate(&client, &base, &token, "/v2/workflows/logs", "runs").await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("检查失败: {e}");
            process::exit(2);
        }
    };
    let schedules = match get_json(&client, &base, &token, "/v2/schedules").await {
        Ok(v) => v.as_array().cloned().unwrap_or_default(),
        Err(e) => {
            eprintln!("检查失败: {e}");
            process::exit(2);
        }
    };
    let dlq = match get_json(&client, &base, &token, "/v2/dlq?count=1000").await {
        Ok(v) => v
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        Err(e) => {
            eprintln!("检查失败: {e}");
            process::exit(2);
        }
    };
    let stuck: Vec<Value> = events
        .iter()
        .filter(|e| now - e.get("time").and_then(Value::as_i64).unwrap_or(now) <= STUCK_WINDOW_MS)
        .cloned()
        .collect();
    let stuck = find_stuck(&stuck, now);
    let mut window_stats = serde_json::Map::new();
    let mut failed = false;
    for name in &selected {
        let seconds = WINDOWS
            .iter()
            .find(|(n, _)| n == name)
            .and_then(|(_, s)| *s);
        let scoped_events: Vec<Value> = events
            .iter()
            .filter(|e| {
                seconds
                    .map(|s| e.get("time").and_then(Value::as_i64).unwrap_or(0) >= now - s * 1000)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        let scoped_runs: Vec<Value> = runs
            .iter()
            .filter(|r| {
                seconds
                    .map(|s| {
                        r.get("workflowRunCreatedAt")
                            .and_then(Value::as_i64)
                            .unwrap_or(0)
                            >= now - s * 1000
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        let mut stats = analyze(&scoped_events, &scoped_runs);
        let mut problems = Vec::<String>::new();
        let dup = stats.get("duplicated").and_then(Value::as_u64).unwrap_or(0);
        let fail_count = stats.get("failed").and_then(Value::as_u64).unwrap_or(0);
        let rate = stats.get("successRate").and_then(Value::as_f64);
        let run_failed = stats
            .get("runStates")
            .and_then(|v| v.get("RUN_FAILED"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if dup > 0 {
            problems.push(format!(
                "{dup} messages have duplicated Content-Type headers"
            ));
        }
        if fail_count > 0 {
            problems.push(format!("{fail_count} messages ultimately failed"));
        }
        if !stuck.is_empty() {
            problems.push(format!(
                "{} messages stuck for over {STUCK_SECONDS}s",
                stuck.len()
            ));
        }
        if rate.is_some_and(|r| r < 0.9) {
            problems.push(format!(
                "success rate is only {:.0}%",
                rate.unwrap() * 100.0
            ));
        }
        if run_failed > 0 {
            problems.push(format!("{run_failed} workflow runs failed"));
        }
        failed |= !problems.is_empty();
        stats["problems"] = json!(problems);
        window_stats.insert((*name).to_string(), stats);
    }
    let destinations: HashSet<String> = schedules
        .iter()
        .filter_map(|s| s.get("destination").and_then(Value::as_str))
        .map(|s| s.split("/api/").next().unwrap_or(s).to_string())
        .collect();
    let paused: Vec<String> = schedules
        .iter()
        .filter(|s| s.get("isPaused").and_then(Value::as_bool) == Some(true))
        .filter_map(|s| {
            s.get("scheduleId")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    let schedule_details: Vec<Value> = schedules.iter().map(schedule_summary).collect();
    let workflow_info = workflow_summary(&runs);
    let output = json!({
        "now": chrono::DateTime::from_timestamp_millis(now).map(|d| d.to_rfc3339()).unwrap_or_default(),
        "schedules": {"count": schedules.len(), "destinations": destinations, "paused": paused, "items": schedule_details},
        "workflows": workflow_info,
        "dlq": dlq.len(), "stuck": stuck, "windows": window_stats
    });
    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&output).unwrap_or_else(|_| output.to_string())
        );
    } else {
        println!("Time: {}", output["now"].as_str().unwrap_or("unknown"));
        println!(
            "Schedules: {}, destinations: {}, paused: {}",
            schedules.len(),
            destinations.len(),
            paused.len()
        );
        for schedule in output["schedules"]["items"]
            .as_array()
            .into_iter()
            .flatten()
        {
            println!(
                "  {} {} {} cron={} next={} last={} lastStates={}",
                schedule["scheduleId"].as_str().unwrap_or("?"),
                if schedule["isPaused"].as_bool() == Some(true) {
                    "PAUSED"
                } else {
                    "ACTIVE"
                },
                schedule["method"].as_str().unwrap_or("POST"),
                schedule["cron"].as_str().unwrap_or("?"),
                schedule["nextScheduleTimeUtc"].as_str().unwrap_or("n/a"),
                schedule["lastScheduleTimeUtc"].as_str().unwrap_or("never"),
                schedule["lastScheduleStates"]
            );
            println!("    -> {}", schedule["destination"].as_str().unwrap_or("?"));
        }
        let workflows = &output["workflows"];
        println!(
            "Workflows: {} total; states={}",
            workflows["total"], workflows["states"]
        );
        for run in workflows["activeRuns"].as_array().into_iter().flatten() {
            println!(
                "  ACTIVE {} {} created={}",
                run["workflowRunId"],
                run["workflowUrl"],
                run["workflowRunCreatedAtUtc"].as_str().unwrap_or("unknown")
            );
        }
        for run in workflows["failedRuns"].as_array().into_iter().flatten() {
            println!(
                "  FAILED {} {} created={} dlq={}",
                run["workflowRunId"],
                run["workflowUrl"],
                run["workflowRunCreatedAtUtc"].as_str().unwrap_or("unknown"),
                run["dlqId"]
            );
        }
        println!("DLQ: {}", dlq.len());
        for name in &selected {
            let s = &output["windows"][name.as_str()];
            let rate = s["successRate"]
                .as_f64()
                .map(|v| format!("{:.1}%", v * 100.0))
                .unwrap_or_else(|| "n/a".into());
            println!(
                "{} events, {} messages, delivered {}, failed {}, success rate {}",
                s["events"], s["messages"], s["delivered"], s["failed"], rate
            );
            println!(
                "States: {}; retries>=2: {}; duplicated Content-Type: {}",
                s["states"], s["heavy"], s["duplicated"]
            );
            println!("Workflow runs: {}", s["runStates"]);
            if !s["problems"].as_array().unwrap_or(&Vec::new()).is_empty() {
                println!(
                    "WARNING: {}",
                    s["problems"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join("; ")
                );
            } else {
                println!("OK");
            }
        }
        if !stuck.is_empty() {
            println!("Stuck >{STUCK_SECONDS}s in last hour: {}", stuck.len());
            for item in stuck.iter().take(8) {
                println!(
                    "  {}s {} {}",
                    item["ageSeconds"], item["url"], item["detail"]
                );
            }
        }
    }
    process::exit(if failed { 1 } else { 0 });
}
