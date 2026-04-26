use crate::server::prelude::*;

use crate::server::config::{EventsRetentionConfig, MetricsConfig};
use crate::server::constants::RPC_DURATION_BUCKETS_SECS;
use crate::server::state::AppState;

fn prometheus_escape_label(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}
pub(crate) async fn monitor_background_tasks(
    state: Arc<AppState>,
    retention: EventsRetentionConfig,
    metrics: MetricsConfig,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut retention_tick = tokio::time::interval(retention.sweep_interval);
    let mut metrics_tick = tokio::time::interval(metrics.log_interval);
    // Avoid a cleanup storm right at startup.
    retention_tick.tick().await;
    metrics_tick.tick().await;

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("background monitor shutting down");
                break;
            }
            _ = retention_tick.tick() => {
                state
                    .runtime_metrics
                    .retention_runs_total
                    .fetch_add(1, Ordering::Relaxed);
                match state.index_backend.cleanup_events_retention(
                    retention.max_age,
                    retention.min_events_per_workspace,
                ) {
                    Ok(deleted) => {
                        state
                            .runtime_metrics
                            .retention_deleted_total
                            .fetch_add(deleted, Ordering::Relaxed);
                        state
                            .runtime_metrics
                            .retention_last_deleted
                            .store(deleted, Ordering::Relaxed);
                        if deleted > 0 {
                            info!(deleted_events = deleted, "events retention cleanup completed");
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, "events retention cleanup failed");
                    }
                }
            }
            _ = metrics_tick.tick() => {
                state
                    .runtime_metrics
                    .metrics_snapshot_runs_total
                    .fetch_add(1, Ordering::Relaxed);
                match state.index_backend.indexing_metrics_snapshot() {
                    Ok(snapshot) => {
                        info!(
                            queued_jobs = snapshot.queued_jobs,
                            running_jobs = snapshot.running_jobs,
                            completed_jobs = snapshot.completed_jobs,
                            failed_jobs = snapshot.failed_jobs,
                            avg_running_latency_ms = snapshot.avg_running_latency_ms.unwrap_or(0),
                            avg_completion_latency_ms = snapshot.avg_completion_latency_ms.unwrap_or(0),
                            "indexing metrics snapshot"
                        );
                    }
                    Err(err) => {
                        warn!(error = %err, "failed to read indexing metrics snapshot");
                    }
                }
            }
        }
    }
}

pub(crate) fn build_metrics_text(state: &AppState) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# HELP scry_index_open_total Open index stores by layout."
    );
    let _ = writeln!(out, "# TYPE scry_index_open_total gauge");
    let _ = writeln!(
        out,
        "scry_index_open_total{{layout=\"{}\"}} {}",
        state.index_backend.layout_label(),
        state.index_backend.open_store_count()
    );

    let uptime_secs = state.runtime_metrics.started_at.elapsed().as_secs_f64();
    let _ = writeln!(
        out,
        "# HELP scryd_uptime_seconds Process uptime in seconds."
    );
    let _ = writeln!(out, "# TYPE scryd_uptime_seconds gauge");
    let _ = writeln!(out, "scryd_uptime_seconds {}", uptime_secs);

    let _ = writeln!(
        out,
        "# HELP scryd_started_unix_seconds Process start timestamp in unix seconds."
    );
    let _ = writeln!(out, "# TYPE scryd_started_unix_seconds gauge");
    let _ = writeln!(
        out,
        "scryd_started_unix_seconds {}",
        state.runtime_metrics.started_unix_seconds
    );

    let _ = writeln!(
        out,
        "# HELP scryd_auth_success_total Successful auth checks."
    );
    let _ = writeln!(out, "# TYPE scryd_auth_success_total counter");
    let _ = writeln!(
        out,
        "scryd_auth_success_total {}",
        state
            .runtime_metrics
            .auth_success_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_auth_unauthenticated_total Unauthenticated auth failures."
    );
    let _ = writeln!(out, "# TYPE scryd_auth_unauthenticated_total counter");
    let _ = writeln!(
        out,
        "scryd_auth_unauthenticated_total {}",
        state
            .runtime_metrics
            .auth_unauthenticated_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_auth_denied_total Permission-denied auth failures."
    );
    let _ = writeln!(out, "# TYPE scryd_auth_denied_total counter");
    let _ = writeln!(
        out,
        "scryd_auth_denied_total {}",
        state
            .runtime_metrics
            .auth_denied_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_retention_runs_total Events retention sweep runs."
    );
    let _ = writeln!(out, "# TYPE scryd_retention_runs_total counter");
    let _ = writeln!(
        out,
        "scryd_retention_runs_total {}",
        state
            .runtime_metrics
            .retention_runs_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_retention_deleted_total Events deleted by retention sweeps."
    );
    let _ = writeln!(out, "# TYPE scryd_retention_deleted_total counter");
    let _ = writeln!(
        out,
        "scryd_retention_deleted_total {}",
        state
            .runtime_metrics
            .retention_deleted_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_retention_last_deleted Last retention sweep deleted events."
    );
    let _ = writeln!(out, "# TYPE scryd_retention_last_deleted gauge");
    let _ = writeln!(
        out,
        "scryd_retention_last_deleted {}",
        state
            .runtime_metrics
            .retention_last_deleted
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_metrics_snapshot_runs_total Indexing snapshot sampling runs."
    );
    let _ = writeln!(out, "# TYPE scryd_metrics_snapshot_runs_total counter");
    let _ = writeln!(
        out,
        "scryd_metrics_snapshot_runs_total {}",
        state
            .runtime_metrics
            .metrics_snapshot_runs_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_embedding_billed_credits_total Credits billed for embedding calls (ceil of token-based charge)."
    );
    let _ = writeln!(out, "# TYPE scryd_embedding_billed_credits_total counter");
    let _ = writeln!(
        out,
        "scryd_embedding_billed_credits_total {}",
        state
            .runtime_metrics
            .embedding_billed_credits_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_embedding_billed_tokens_total Tokens used for embedding credit settlement (upstream usage or estimate)."
    );
    let _ = writeln!(out, "# TYPE scryd_embedding_billed_tokens_total counter");
    let _ = writeln!(
        out,
        "scryd_embedding_billed_tokens_total {}",
        state
            .runtime_metrics
            .embedding_billed_tokens_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_embedding_settlement_upstream_usage_total Embedding calls settled using provider usage."
    );
    let _ = writeln!(
        out,
        "# TYPE scryd_embedding_settlement_upstream_usage_total counter"
    );
    let _ = writeln!(
        out,
        "scryd_embedding_settlement_upstream_usage_total {}",
        state
            .runtime_metrics
            .embedding_settlement_upstream_usage_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_embedding_settlement_estimated_total Embedding calls settled using local token estimates."
    );
    let _ = writeln!(
        out,
        "# TYPE scryd_embedding_settlement_estimated_total counter"
    );
    let _ = writeln!(
        out,
        "scryd_embedding_settlement_estimated_total {}",
        state
            .runtime_metrics
            .embedding_settlement_estimated_total
            .load(Ordering::Relaxed)
    );

    let _ = writeln!(
        out,
        "# HELP scryd_rpc_requests_total gRPC unary/stream responses by service, method, and grpc-status code."
    );
    let _ = writeln!(out, "# TYPE scryd_rpc_requests_total counter");
    if let Ok(guard) = state.runtime_metrics.rpc_request_counts.lock() {
        for ((svc, method, code), n) in guard.iter() {
            let _ = writeln!(
                out,
                "scryd_rpc_requests_total{{service=\"{}\",method=\"{}\",code=\"{}\"}} {}",
                prometheus_escape_label(svc),
                prometheus_escape_label(method),
                prometheus_escape_label(code),
                n
            );
        }
    }

    let _ = writeln!(
        out,
        "# HELP scryd_rpc_duration_seconds gRPC handler wall time in seconds."
    );
    let _ = writeln!(out, "# TYPE scryd_rpc_duration_seconds histogram");
    if let Ok(guard) = state.runtime_metrics.rpc_duration_hist.lock() {
        for ((svc, method), hist) in guard.iter() {
            // `RpcDurationHist::buckets[i]` already stores the count of samples with duration <= le[i]
            // (see `RuntimeMetrics::record_rpc`), i.e. Prometheus cumulative bucket values.
            for (i, &le) in RPC_DURATION_BUCKETS_SECS.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "scryd_rpc_duration_seconds_bucket{{service=\"{}\",method=\"{}\",le=\"{}\"}} {}",
                    prometheus_escape_label(svc),
                    prometheus_escape_label(method),
                    le,
                    hist.buckets[i]
                );
            }
            let _ = writeln!(
                out,
                "scryd_rpc_duration_seconds_bucket{{service=\"{}\",method=\"{}\",le=\"+Inf\"}} {}",
                prometheus_escape_label(svc),
                prometheus_escape_label(method),
                hist.count
            );
            let _ = writeln!(
                out,
                "scryd_rpc_duration_seconds_sum{{service=\"{}\",method=\"{}\"}} {}",
                prometheus_escape_label(svc),
                prometheus_escape_label(method),
                (hist.sum_ns as f64) / 1_000_000_000.0
            );
            let _ = writeln!(
                out,
                "scryd_rpc_duration_seconds_count{{service=\"{}\",method=\"{}\"}} {}",
                prometheus_escape_label(svc),
                prometheus_escape_label(method),
                hist.count
            );
        }
    }

    if let Ok(snapshot) = state.index_backend.indexing_metrics_snapshot() {
        let _ = writeln!(
            out,
            "# HELP scryd_indexing_jobs_queued Queued indexing jobs."
        );
        let _ = writeln!(out, "# TYPE scryd_indexing_jobs_queued gauge");
        let _ = writeln!(out, "scryd_indexing_jobs_queued {}", snapshot.queued_jobs);
        let _ = writeln!(
            out,
            "# HELP scryd_indexing_jobs_running Running indexing jobs."
        );
        let _ = writeln!(out, "# TYPE scryd_indexing_jobs_running gauge");
        let _ = writeln!(out, "scryd_indexing_jobs_running {}", snapshot.running_jobs);
        let _ = writeln!(
            out,
            "# HELP scryd_indexing_jobs_completed Completed indexing jobs."
        );
        let _ = writeln!(out, "# TYPE scryd_indexing_jobs_completed counter");
        let _ = writeln!(
            out,
            "scryd_indexing_jobs_completed {}",
            snapshot.completed_jobs
        );
        let _ = writeln!(
            out,
            "# HELP scryd_indexing_jobs_failed Failed indexing jobs."
        );
        let _ = writeln!(out, "# TYPE scryd_indexing_jobs_failed counter");
        let _ = writeln!(out, "scryd_indexing_jobs_failed {}", snapshot.failed_jobs);
        if let Some(v) = snapshot.avg_running_latency_ms {
            let _ = writeln!(out, "scryd_indexing_avg_running_latency_ms {}", v as f64);
        }
        if let Some(v) = snapshot.avg_completion_latency_ms {
            let _ = writeln!(out, "scryd_indexing_avg_completion_latency_ms {}", v as f64);
        }
    }

    out
}

pub(crate) async fn run_metrics_http_server(
    addr: std::net::SocketAddr,
    state: Arc<AppState>,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind metrics endpoint at {addr}"))?;
    info!(addr = %addr, "scryd metrics endpoint enabled");

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => break,
            accepted = listener.accept() => {
                let (mut stream, _) = accepted.context("accept metrics connection")?;
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..n]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");
                if path == "/metrics" {
                    let body = build_metrics_text(&state);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                } else {
                    let response =
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    let _ = stream.write_all(response.as_bytes()).await;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_escape_label_escapes_backslash_and_quote() {
        assert_eq!(prometheus_escape_label(r#"a\b"c"#), r#"a\\b\"c"#);
    }
}
