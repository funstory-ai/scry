use std::{env, time::Duration};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::credits::EmbeddingCharge;

#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    #[error("embedding provider transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("embedding provider rejected request with status={status}: {body}")]
    Upstream { status: StatusCode, body: String },
    #[error("embedding provider response malformed: {0}")]
    ResponseShape(String),
}

#[derive(Debug, Clone)]
pub struct EmbedOutcome<T> {
    pub value: T,
    pub charge: EmbeddingCharge,
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
    fn model_id(&self) -> &str;

    async fn embed_documents(
        &self,
        inputs: &[String],
    ) -> Result<EmbedOutcome<Vec<Vec<f32>>>, EmbeddingError>;
    async fn embed_query(&self, input: &str) -> Result<EmbedOutcome<Vec<f32>>, EmbeddingError>;
}

#[derive(Clone, Debug)]
pub struct MockEmbeddingProvider {
    model_id: String,
}

impl Default for MockEmbeddingProvider {
    fn default() -> Self {
        Self {
            model_id: "mock-embedding-32d".to_string(),
        }
    }
}

#[async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    fn provider_kind(&self) -> &'static str {
        "mock"
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }

    async fn embed_documents(
        &self,
        inputs: &[String],
    ) -> Result<EmbedOutcome<Vec<Vec<f32>>>, EmbeddingError> {
        let total_tokens = crate::credits::estimate_tokens_batch(inputs);
        Ok(EmbedOutcome {
            value: inputs.iter().map(|value| simple_embed(value)).collect(),
            charge: EmbeddingCharge {
                total_tokens,
                credits: 0,
                from_upstream_usage: false,
            },
        })
    }

    async fn embed_query(&self, input: &str) -> Result<EmbedOutcome<Vec<f32>>, EmbeddingError> {
        let total_tokens = crate::credits::estimate_tokens_utf8(input);
        Ok(EmbedOutcome {
            value: simple_embed(input),
            charge: EmbeddingCharge {
                total_tokens,
                credits: 0,
                from_upstream_usage: false,
            },
        })
    }
}

#[derive(Clone, Debug)]
pub struct OpenAiCompatibleEmbeddingProvider {
    client: reqwest::Client,
    endpoint: String,
    api_key: String,
    model: String,
    document_task: Option<String>,
    query_task: Option<String>,
    normalized: Option<bool>,
}

impl OpenAiCompatibleEmbeddingProvider {
    pub fn from_env() -> anyhow::Result<Self> {
        let base_url = env::var("SCRYD_EMBED_BASE_URL")
            .unwrap_or_else(|_| "https://api.openai.com".to_string());
        let api_key = env::var("SCRYD_EMBED_API_KEY")
            .context("SCRYD_EMBED_API_KEY is required for openai-compatible provider")?;
        let model =
            env::var("SCRYD_EMBED_MODEL").unwrap_or_else(|_| "text-embedding-3-small".to_string());
        let timeout_ms = env::var("SCRYD_EMBED_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(15_000);
        let document_task = env::var("SCRYD_EMBED_DOCUMENT_TASK")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let query_task = env::var("SCRYD_EMBED_QUERY_TASK")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let normalized = env::var("SCRYD_EMBED_NORMALIZED")
            .ok()
            .and_then(|value| parse_bool_env(&value));

        let endpoint = format!("{}/v1/embeddings", base_url.trim_end_matches('/'));
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .context("failed to build embedding http client")?;

        Ok(Self {
            client,
            endpoint,
            api_key,
            model,
            document_task,
            query_task,
            normalized,
        })
    }

    async fn embed_with_task(
        &self,
        inputs: &[String],
        task: Option<&str>,
    ) -> Result<(Vec<Vec<f32>>, EmbeddingCharge), EmbeddingError> {
        if inputs.is_empty() {
            return Ok((
                Vec::new(),
                EmbeddingCharge {
                    total_tokens: 0,
                    credits: 0,
                    from_upstream_usage: false,
                },
            ));
        }

        let payload = OpenAiCompatibleEmbeddingsRequest {
            model: self.model.as_str(),
            input: inputs,
            task,
            normalized: self.normalized,
        };
        let response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&payload)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_else(|_| String::new());
            return Err(EmbeddingError::Upstream { status, body });
        }

        let body = response
            .json::<OpenAiCompatibleEmbeddingsResponse>()
            .await?;
        let mut ordered: Vec<Option<Vec<f32>>> = vec![None; inputs.len()];
        for item in body.data {
            if item.index >= ordered.len() {
                return Err(EmbeddingError::ResponseShape(format!(
                    "embedding index {} out of range for {} inputs",
                    item.index,
                    ordered.len()
                )));
            }
            ordered[item.index] = Some(item.embedding);
        }

        let vectors: Vec<Vec<f32>> = ordered
            .into_iter()
            .enumerate()
            .map(|(idx, embedding)| {
                embedding.ok_or_else(|| {
                    EmbeddingError::ResponseShape(format!(
                        "missing embedding at index {idx} in provider response"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;

        let (total_tokens, from_upstream) = match body.usage {
            Some(ref u) => match usage_total_tokens(u) {
                Some(t) => (t, true),
                None => (crate::credits::estimate_tokens_batch(inputs), false),
            },
            None => (crate::credits::estimate_tokens_batch(inputs), false),
        };
        let cfg = crate::credits::EmbeddingCreditConfig::from_env();
        let credits = cfg.credits_for_tokens(total_tokens);

        Ok((
            vectors,
            EmbeddingCharge {
                total_tokens,
                credits,
                from_upstream_usage: from_upstream,
            },
        ))
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAiCompatibleEmbeddingProvider {
    fn provider_kind(&self) -> &'static str {
        "openai-compatible"
    }

    fn model_id(&self) -> &str {
        &self.model
    }

    async fn embed_documents(
        &self,
        inputs: &[String],
    ) -> Result<EmbedOutcome<Vec<Vec<f32>>>, EmbeddingError> {
        let (value, charge) = self
            .embed_with_task(inputs, self.document_task.as_deref())
            .await?;
        Ok(EmbedOutcome { value, charge })
    }

    async fn embed_query(&self, input: &str) -> Result<EmbedOutcome<Vec<f32>>, EmbeddingError> {
        let (mut batch, charge) = self
            .embed_with_task(&[input.to_string()], self.query_task.as_deref())
            .await?;
        if batch.len() != 1 {
            return Err(EmbeddingError::ResponseShape(format!(
                "expected exactly 1 query embedding, got {}",
                batch.len()
            )));
        }
        Ok(EmbedOutcome {
            value: batch.remove(0),
            charge,
        })
    }
}

#[derive(Clone)]
pub struct EmbeddingRuntime {
    pub provider_kind: String,
    pub model_id: String,
    pub provider: std::sync::Arc<dyn EmbeddingProvider>,
}

pub fn provider_from_env() -> anyhow::Result<EmbeddingRuntime> {
    let provider_kind = env::var("SCRYD_EMBED_PROVIDER").unwrap_or_else(|_| "mock".to_string());
    match provider_kind.trim().to_ascii_lowercase().as_str() {
        "mock" => {
            let provider = MockEmbeddingProvider::default();
            Ok(EmbeddingRuntime {
                provider_kind: provider.provider_kind().to_string(),
                model_id: provider.model_id().to_string(),
                provider: std::sync::Arc::new(provider),
            })
        }
        "openai-compatible" | "openai" => {
            let provider = OpenAiCompatibleEmbeddingProvider::from_env()?;
            Ok(EmbeddingRuntime {
                provider_kind: provider.provider_kind().to_string(),
                model_id: provider.model_id().to_string(),
                provider: std::sync::Arc::new(provider),
            })
        }
        other => Err(anyhow!(
            "unsupported SCRYD_EMBED_PROVIDER='{other}', expected one of: mock, openai-compatible"
        )),
    }
}

fn parse_bool_env(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn simple_embed(text: &str) -> Vec<f32> {
    let mut out = vec![0f32; 32];
    for (i, byte) in text.bytes().enumerate() {
        let idx = i % out.len();
        out[idx] += (byte as f32) / 255.0;
    }
    out
}

#[derive(Debug, Serialize)]
struct OpenAiCompatibleEmbeddingsRequest<'a> {
    model: &'a str,
    input: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    task: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    normalized: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatibleEmbeddingsResponse {
    data: Vec<OpenAiCompatibleEmbeddingDatum>,
    #[serde(default)]
    usage: Option<OpenAiCompatibleUsage>,
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatibleUsage {
    #[serde(default)]
    prompt_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
}

fn usage_total_tokens(u: &OpenAiCompatibleUsage) -> Option<u64> {
    u.total_tokens.or(u.prompt_tokens)
}

#[derive(Debug, Deserialize)]
struct OpenAiCompatibleEmbeddingDatum {
    index: usize,
    embedding: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::{Arc, Mutex};

    use tokio::task::JoinHandle;

    use super::{EmbeddingProvider, OpenAiCompatibleEmbeddingProvider};

    fn extract_json_body(raw_http_request: &str) -> String {
        if let Some((_headers, body)) = raw_http_request.split_once("\r\n\r\n") {
            body.to_string()
        } else {
            String::new()
        }
    }

    async fn spawn_mock_embedding_server(
        response_body: &'static str,
        accept_count: usize,
    ) -> (SocketAddr, Arc<Mutex<Vec<String>>>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("mock local addr");
        let received = Arc::new(Mutex::new(Vec::<String>::new()));
        let received_for_task = Arc::clone(&received);

        let handle = tokio::task::spawn_blocking(move || {
            for _ in 0..accept_count {
                let (mut stream, _) = listener.accept().expect("accept mock request");
                let mut buf = [0u8; 16 * 1024];
                let size = stream.read(&mut buf).expect("read request");
                let raw = String::from_utf8_lossy(&buf[..size]).to_string();
                received_for_task
                    .lock()
                    .expect("lock received requests")
                    .push(raw);

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write response");
                stream.flush().expect("flush response");
            }
        });

        (addr, received, handle)
    }

    #[tokio::test]
    async fn openai_compatible_provider_sends_expected_request_shape() {
        let response_body = r#"{"data":[{"index":0,"embedding":[0.1,0.2]}]}"#;
        let (addr, received, handle) = spawn_mock_embedding_server(response_body, 2).await;

        std::env::set_var("SCRYD_EMBED_BASE_URL", format!("http://{addr}"));
        std::env::set_var("SCRYD_EMBED_API_KEY", "test-key");
        std::env::set_var("SCRYD_EMBED_MODEL", "jina-embeddings-v5-text-small");
        std::env::set_var("SCRYD_EMBED_DOCUMENT_TASK", "retrieval.passage");
        std::env::set_var("SCRYD_EMBED_QUERY_TASK", "retrieval.query");
        std::env::set_var("SCRYD_EMBED_NORMALIZED", "true");

        let provider = OpenAiCompatibleEmbeddingProvider::from_env().expect("provider from env");
        let doc_outcome = provider
            .embed_documents(&["doc-a".to_string()])
            .await
            .expect("embed documents");
        assert_eq!(doc_outcome.value.len(), 1);
        assert_eq!(doc_outcome.value[0], vec![0.1, 0.2]);
        assert!(doc_outcome.charge.total_tokens > 0);

        let query_outcome = provider.embed_query("query-a").await.expect("embed query");
        assert_eq!(query_outcome.value, vec![0.1, 0.2]);
        assert!(query_outcome.charge.total_tokens > 0);

        handle.await.expect("mock server task");

        let captured = received.lock().expect("lock captured");
        assert_eq!(captured.len(), 2, "expected one docs and one query request");

        let first = &captured[0];
        assert!(
            first.starts_with("POST /v1/embeddings HTTP/1.1"),
            "docs request must call /v1/embeddings"
        );
        assert!(
            first
                .to_ascii_lowercase()
                .contains("authorization: bearer test-key"),
            "docs request must include bearer auth"
        );
        let first_body = extract_json_body(first);
        assert!(first_body.contains(r#""model":"jina-embeddings-v5-text-small""#));
        assert!(first_body.contains(r#""task":"retrieval.passage""#));
        assert!(first_body.contains(r#""normalized":true"#));
        assert!(first_body.contains(r#""input":["doc-a"]"#));

        let second = &captured[1];
        assert!(
            second.starts_with("POST /v1/embeddings HTTP/1.1"),
            "query request must call /v1/embeddings"
        );
        let second_body = extract_json_body(second);
        assert!(second_body.contains(r#""task":"retrieval.query""#));
        assert!(second_body.contains(r#""normalized":true"#));
        assert!(second_body.contains(r#""input":["query-a"]"#));
    }

    #[tokio::test]
    async fn openai_compatible_provider_bills_credits_from_usage() {
        let response_body = r#"{"data":[{"index":0,"embedding":[0.1]}],"usage":{"total_tokens":1000000,"prompt_tokens":1000000}}"#;
        let (addr, _received, handle) = spawn_mock_embedding_server(response_body, 1).await;

        std::env::set_var("SCRYD_EMBED_BASE_URL", format!("http://{addr}"));
        std::env::set_var("SCRYD_EMBED_API_KEY", "test-key");
        std::env::set_var("SCRYD_EMBED_MODEL", "text-embedding-3-small");
        std::env::set_var("SCRYD_EMBED_CREDITS_PER_MILLION_TOKENS", "2.5");

        let provider = OpenAiCompatibleEmbeddingProvider::from_env().expect("provider from env");
        let outcome = provider
            .embed_documents(&["hello".to_string()])
            .await
            .expect("embed");
        assert_eq!(outcome.value.len(), 1);
        assert_eq!(outcome.charge.total_tokens, 1_000_000);
        assert!(outcome.charge.from_upstream_usage);
        assert_eq!(outcome.charge.credits, 3);

        handle.await.expect("mock server task");
    }
}
