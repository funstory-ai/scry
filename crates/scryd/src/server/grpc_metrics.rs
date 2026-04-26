//! Per-RPC Prometheus metrics (plan P2-2).

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
    time::Instant,
};

use http::{Request, Response};
use pin_project::pin_project;
use tonic::body::BoxBody;
use tower::Service;
use uuid::Uuid;

use crate::server::state::RuntimeMetrics;

#[derive(Clone)]
pub(crate) struct GrpcRpcMetricsLayer {
    pub(crate) metrics: Arc<RuntimeMetrics>,
}

impl<S> tower::Layer<S> for GrpcRpcMetricsLayer {
    type Service = GrpcRpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcRpcMetricsService {
            inner,
            metrics: Arc::clone(&self.metrics),
        }
    }
}

#[derive(Clone)]
pub(crate) struct GrpcRpcMetricsService<S> {
    inner: S,
    metrics: Arc<RuntimeMetrics>,
}

impl<S, E> Service<Request<BoxBody>> for GrpcRpcMetricsService<S>
where
    S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = E> + Clone + Send + 'static,
    S::Future: Send + 'static,
    E: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = E;
    type Future = GrpcRpcMetricsFuture<S::Future, E>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let started = Instant::now();
        let path = req.uri().path().to_string();
        let (service, method) = parse_grpc_path(&path);
        GrpcRpcMetricsFuture {
            inner: self.inner.call(req),
            started,
            service,
            method,
            metrics: Arc::clone(&self.metrics),
            _err: std::marker::PhantomData::<E>,
        }
    }
}

#[pin_project]
pub(crate) struct GrpcRpcMetricsFuture<F, E> {
    #[pin]
    inner: F,
    started: Instant,
    service: String,
    method: String,
    metrics: Arc<RuntimeMetrics>,
    _err: std::marker::PhantomData<E>,
}

impl<F, E> Future for GrpcRpcMetricsFuture<F, E>
where
    F: Future<Output = Result<Response<BoxBody>, E>>,
{
    type Output = Result<Response<BoxBody>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match ready!(this.inner.poll(cx)) {
            Ok(res) => {
                let code = grpc_status_code_from_response(&res);
                this.metrics.record_rpc(
                    this.service.as_str(),
                    this.method.as_str(),
                    code,
                    this.started.elapsed(),
                );
                Poll::Ready(Ok(res))
            }
            Err(err) => {
                this.metrics.record_rpc(
                    this.service.as_str(),
                    this.method.as_str(),
                    "transport",
                    this.started.elapsed(),
                );
                Poll::Ready(Err(err))
            }
        }
    }
}

pub(crate) fn trace_span_for_grpc_request(req: &http::Request<()>) -> tracing::Span {
    let path = req.uri().path();
    let (service, method) = parse_grpc_path(path);
    static X_REQUEST_ID: http::header::HeaderName =
        http::header::HeaderName::from_static("x-request-id");
    let request_id = req
        .headers()
        .get(&X_REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::now_v7().to_string());
    let auth_kind = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            let lower = s.to_ascii_lowercase();
            if lower.starts_with("bearer ") {
                "bearer"
            } else if lower.starts_with("basic ") {
                "basic"
            } else {
                "other"
            }
        })
        .unwrap_or("none");
    tracing::span!(
        tracing::Level::INFO,
        "rpc",
        service = %service,
        method = %method,
        request_id = %request_id,
        authorization = auth_kind,
    )
}

fn parse_grpc_path(path: &str) -> (String, String) {
    let trimmed = path.trim_start_matches('/');
    if let Some((svc, rest)) = trimmed.rsplit_once('/') {
        if !svc.is_empty() && !rest.is_empty() {
            return (svc.to_string(), rest.to_string());
        }
    }
    ("unknown".to_string(), "unknown".to_string())
}

fn grpc_status_code_from_response(res: &Response<BoxBody>) -> &'static str {
    if let Some(raw) = res.headers().get(tonic::Status::GRPC_STATUS) {
        if let Ok(s) = raw.to_str() {
            return match s {
                "0" => "0",
                "1" => "1",
                "2" => "2",
                "3" => "3",
                "4" => "4",
                "5" => "5",
                "6" => "6",
                "7" => "7",
                "8" => "8",
                "9" => "9",
                "10" => "10",
                "11" => "11",
                "12" => "12",
                "13" => "13",
                "14" => "14",
                "15" => "15",
                "16" => "16",
                _ => "unknown",
            };
        }
    }
    "0"
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Response;

    #[test]
    fn parse_grpc_path_splits_service_and_method() {
        let (s, m) = parse_grpc_path("/scry.v1.Admin/CreateWorkspace");
        assert_eq!(s, "scry.v1.Admin");
        assert_eq!(m, "CreateWorkspace");
    }

    #[test]
    fn grpc_status_reads_grpc_status_header() {
        let mut res = Response::new(BoxBody::default());
        res.headers_mut().insert(
            tonic::Status::GRPC_STATUS,
            http::HeaderValue::from_static("16"),
        );
        assert_eq!(grpc_status_code_from_response(&res), "16");
    }

    #[test]
    fn grpc_status_defaults_to_ok_without_header() {
        let res = Response::new(BoxBody::default());
        assert_eq!(grpc_status_code_from_response(&res), "0");
    }
}
