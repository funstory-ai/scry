use crate::server::prelude::*;

use crate::server::config::{env_bool_with_default, env_u64_with_default, AuthConfig};
use crate::server::constants::*;
use crate::server::state::{global_runtime_metrics, RuntimeMetrics};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenKind {
    Workspace,
    Admin,
    Local,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TokenClaims {
    #[serde(default)]
    ws: Option<String>,
    kind: String,
    exp: u64,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    scp: Vec<String>,
}

#[derive(Debug)]
pub(crate) enum AuthRequirement<'a> {
    WorkspaceScoped {
        workspace_id: &'a str,
        required_scope: &'a str,
    },
    AdminOnly {
        required_scope: &'a str,
    },
}

#[derive(Debug, Serialize)]
pub(crate) struct TokenHeader<'a> {
    alg: &'a str,
    typ: &'a str,
}

#[derive(Debug, Serialize)]
pub(crate) struct TokenClaimsForMint {
    ws: Option<String>,
    kind: String,
    exp: u64,
    scope: String,
}

#[derive(Debug)]
pub(crate) struct MintTokenArgs {
    kind: TokenKind,
    workspace_id: Option<String>,
    scope: String,
    ttl_secs: u64,
    secret: Vec<u8>,
}
pub(crate) fn parse_token_kind(value: &str) -> Option<TokenKind> {
    match value {
        "workspace" => Some(TokenKind::Workspace),
        "admin" => Some(TokenKind::Admin),
        "local" => Some(TokenKind::Local),
        _ => None,
    }
}

fn is_default_secret(secret: &[u8]) -> bool {
    secret == DEFAULT_AUTH_SECRET.as_bytes()
}

fn is_dev_mode_enabled() -> anyhow::Result<bool> {
    env_bool_with_default("SCRYD_DEV_MODE", false)
}

pub(crate) fn is_loopback_ip(ip: &std::net::IpAddr) -> bool {
    ip.is_loopback()
}

pub(crate) fn is_loopback_socket_addr(addr: &std::net::SocketAddr) -> bool {
    is_loopback_ip(&addr.ip())
}

fn encode_token(secret: &[u8], claims: &TokenClaimsForMint) -> anyhow::Result<String> {
    let header_json = serde_json::to_vec(&TokenHeader {
        alg: "HS256",
        typ: "JWT",
    })
    .context("encode token header json")?;
    let claims_json = serde_json::to_vec(claims).context("encode token claims json")?;
    let header_b64 = URL_SAFE_NO_PAD.encode(header_json);
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json);
    let payload = format!("{header_b64}.{claims_b64}");
    let mut mac = HmacSha256::new_from_slice(secret).context("invalid hmac secret")?;
    mac.update(payload.as_bytes());
    let signature_b64 = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{payload}.{signature_b64}"))
}

pub(crate) fn parse_mint_token_args(
    args: impl IntoIterator<Item = String>,
) -> anyhow::Result<MintTokenArgs> {
    let mut kind = TokenKind::Workspace;
    let mut workspace_id: Option<String> = None;
    let mut scope = "workspace.access".to_string();
    let mut ttl_secs = env_u64_with_default("SCRYD_AUTH_TOKEN_TTL_SECS", 3600)?;
    let mut secret: Option<Vec<u8>> = None;

    let mut iter = args.into_iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--kind" => {
                let raw = iter
                    .next()
                    .context("missing value for --kind (workspace/admin/local)")?;
                kind = parse_token_kind(raw.trim())
                    .with_context(|| format!("invalid --kind value: {raw}"))?;
            }
            "--workspace-id" => {
                workspace_id = Some(
                    iter.next()
                        .context("missing value for --workspace-id")?
                        .trim()
                        .to_string(),
                );
            }
            "--scope" => {
                scope = iter
                    .next()
                    .context("missing value for --scope")?
                    .trim()
                    .to_string();
            }
            "--ttl-secs" => {
                let raw = iter.next().context("missing value for --ttl-secs")?;
                ttl_secs = raw
                    .parse::<u64>()
                    .with_context(|| format!("invalid --ttl-secs value: {raw}"))?;
            }
            "--secret" => {
                secret = Some(
                    iter.next()
                        .context("missing value for --secret")?
                        .into_bytes(),
                );
            }
            "--help" | "-h" => {
                println!(
                    "Usage: scryd mint-token [--kind workspace|admin|local] [--workspace-id <id>] [--scope <scope words>] [--ttl-secs <n>] [--secret <secret>]"
                );
                std::process::exit(0);
            }
            unknown => {
                anyhow::bail!("unknown mint-token argument: {unknown}");
            }
        }
    }

    if scope.trim().is_empty() {
        anyhow::bail!("--scope cannot be empty");
    }
    if ttl_secs == 0 {
        anyhow::bail!("--ttl-secs must be > 0");
    }
    if kind == TokenKind::Workspace {
        match workspace_id.as_deref() {
            Some(ws) if !ws.trim().is_empty() => {}
            _ => anyhow::bail!("workspace token requires --workspace-id <id>"),
        }
    }
    let secret = match secret {
        Some(secret) => secret,
        None => {
            let env_secret = env::var("SCRYD_AUTH_SECRET").ok();
            let candidate = env_secret
                .as_deref()
                .unwrap_or(DEFAULT_AUTH_SECRET)
                .as_bytes()
                .to_vec();
            if candidate == DEFAULT_AUTH_SECRET.as_bytes() {
                anyhow::bail!(
                    "mint-token requires --secret or SCRYD_AUTH_SECRET set to a non-default value; see {}",
                    HARDENING_DOC_PATH
                );
            }
            candidate
        }
    };
    if secret.is_empty() {
        anyhow::bail!("signing secret cannot be empty");
    }
    Ok(MintTokenArgs {
        kind,
        workspace_id,
        scope,
        ttl_secs,
        secret,
    })
}

pub(crate) fn mint_token_cli(args: impl IntoIterator<Item = String>) -> anyhow::Result<()> {
    let MintTokenArgs {
        kind,
        workspace_id,
        scope,
        ttl_secs,
        secret,
    } = parse_mint_token_args(args)?;
    let exp = unix_now_secs()
        .checked_add(ttl_secs)
        .context("token expiration overflow")?;
    let claims = TokenClaimsForMint {
        ws: if kind == TokenKind::Workspace {
            workspace_id
        } else {
            None
        },
        kind: match kind {
            TokenKind::Workspace => "workspace".to_string(),
            TokenKind::Admin => "admin".to_string(),
            TokenKind::Local => "local".to_string(),
        },
        exp,
        scope,
    };
    let token = encode_token(&secret, &claims)?;
    println!("{token}");
    Ok(())
}
pub(crate) fn auth_from_env() -> anyhow::Result<AuthConfig> {
    let active_secret = env::var("SCRYD_AUTH_SECRET")
        .unwrap_or_else(|_| DEFAULT_AUTH_SECRET.to_string())
        .into_bytes();
    if active_secret.is_empty() {
        anyhow::bail!("SCRYD_AUTH_SECRET cannot be empty");
    }
    if is_default_secret(&active_secret) && !is_dev_mode_enabled()? {
        anyhow::bail!(
            "SCRYD_AUTH_SECRET is set to the default insecure value. Refusing to start without SCRYD_DEV_MODE=1; see {}",
            HARDENING_DOC_PATH
        );
    }
    let previous_secrets_raw = match env::var("SCRYD_AUTH_PREVIOUS_SECRETS") {
        Ok(raw) => raw,
        Err(env::VarError::NotPresent) => String::new(),
        Err(env::VarError::NotUnicode(_)) => {
            anyhow::bail!("SCRYD_AUTH_PREVIOUS_SECRETS must be valid unicode");
        }
    };
    let previous_secrets = previous_secrets_raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.as_bytes().to_vec())
        .collect();
    let allow_local_without_token = env_bool_with_default("SCRYD_ALLOW_LOCAL_NO_AUTH", false)?;
    let enforce_scopes = env_bool_with_default("SCRYD_AUTH_ENFORCE_SCOPES", true)?;
    Ok(AuthConfig {
        active_secret,
        previous_secrets,
        allow_local_without_token,
        enforce_scopes,
    })
}
fn parse_scope_set(claims: &TokenClaims) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    if let Some(scope_text) = &claims.scope {
        for scope in scope_text.split_whitespace() {
            if !scope.is_empty() {
                out.insert(scope.to_string());
            }
        }
    }
    for scope in &claims.scp {
        if !scope.trim().is_empty() {
            out.insert(scope.trim().to_string());
        }
    }
    out
}

fn has_scope(scopes: &std::collections::HashSet<String>, required: &str) -> bool {
    scopes.contains(required) || scopes.contains("*")
}

fn verify_token_with_secret(token: &str, secret: &[u8]) -> anyhow::Result<TokenClaims> {
    let mut parts = token.split('.');
    let header_b64 = parts.next().context("missing token header")?;
    let claims_b64 = parts.next().context("missing token claims")?;
    let sig_b64 = parts.next().context("missing token signature")?;
    if parts.next().is_some() {
        anyhow::bail!("token has too many segments");
    }

    let signed_payload = format!("{header_b64}.{claims_b64}");
    let expected_sig = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .context("invalid base64 signature")?;
    let mut mac = HmacSha256::new_from_slice(secret).context("invalid hmac secret")?;
    mac.update(signed_payload.as_bytes());
    mac.verify_slice(&expected_sig)
        .map_err(|_| anyhow::anyhow!("signature mismatch"))?;

    let claims_json = URL_SAFE_NO_PAD
        .decode(claims_b64)
        .context("invalid base64 claims")?;
    let claims: TokenClaims =
        serde_json::from_slice(&claims_json).context("invalid claims json")?;
    Ok(claims)
}

#[allow(clippy::result_large_err)]
pub(crate) fn verify_auth(
    metadata: &MetadataMap,
    auth: &AuthConfig,
    runtime_metrics: Option<&RuntimeMetrics>,
    requirement: AuthRequirement<'_>,
) -> Result<(), Status> {
    let header = metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .unwrap_or("");
    if header.is_empty() && auth.allow_local_without_token {
        return Ok(());
    }
    let token = header
        .strip_prefix("Bearer ")
        .ok_or_else(|| Status::unauthenticated("missing bearer authorization token"))?;

    let claims = std::iter::once(&auth.active_secret)
        .chain(auth.previous_secrets.iter())
        .find_map(|secret| verify_token_with_secret(token, secret).ok())
        .ok_or_else(|| Status::unauthenticated("invalid token signature"))?;
    let now = unix_now_secs();
    if claims.exp <= now {
        if let Some(metrics) = runtime_metrics {
            metrics
                .auth_unauthenticated_total
                .fetch_add(1, Ordering::Relaxed);
        }
        return Err(Status::unauthenticated("token expired"));
    }

    let kind = parse_token_kind(&claims.kind)
        .ok_or_else(|| Status::unauthenticated("unknown token kind"))?;
    let scopes = parse_scope_set(&claims);
    match requirement {
        AuthRequirement::AdminOnly { required_scope } => {
            if kind == TokenKind::Admin {
                if auth.enforce_scopes && !has_scope(&scopes, required_scope) {
                    if let Some(metrics) = runtime_metrics {
                        metrics.auth_denied_total.fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(Status::permission_denied("required scope missing"));
                }
                if let Some(metrics) = runtime_metrics {
                    metrics.auth_success_total.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            } else {
                if let Some(metrics) = runtime_metrics {
                    metrics.auth_denied_total.fetch_add(1, Ordering::Relaxed);
                }
                Err(Status::permission_denied("admin token required"))
            }
        }
        AuthRequirement::WorkspaceScoped {
            workspace_id,
            required_scope,
        } => {
            if kind == TokenKind::Admin || kind == TokenKind::Local {
                if auth.enforce_scopes && !has_scope(&scopes, required_scope) {
                    if let Some(metrics) = runtime_metrics {
                        metrics.auth_denied_total.fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(Status::permission_denied("required scope missing"));
                }
                if let Some(metrics) = runtime_metrics {
                    metrics.auth_success_total.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
            if kind != TokenKind::Workspace {
                if let Some(metrics) = runtime_metrics {
                    metrics.auth_denied_total.fetch_add(1, Ordering::Relaxed);
                }
                return Err(Status::permission_denied("workspace token required"));
            }
            if auth.enforce_scopes && !has_scope(&scopes, required_scope) {
                if let Some(metrics) = runtime_metrics {
                    metrics.auth_denied_total.fetch_add(1, Ordering::Relaxed);
                }
                return Err(Status::permission_denied("required scope missing"));
            }
            match claims.ws {
                Some(ref token_ws) if token_ws == workspace_id => {
                    if let Some(metrics) = runtime_metrics {
                        metrics.auth_success_total.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(())
                }
                _ => {
                    if let Some(metrics) = runtime_metrics {
                        metrics.auth_denied_total.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(Status::permission_denied("workspace token mismatch"))
                }
            }
        }
    }
}

pub(crate) fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[allow(clippy::result_large_err)]
pub(crate) fn ensure_workspace_auth<T>(
    request: &Request<T>,
    auth: &AuthConfig,
    workspace_id: &str,
) -> Result<(), Status> {
    verify_auth(
        request.metadata(),
        auth,
        global_runtime_metrics(),
        AuthRequirement::WorkspaceScoped {
            workspace_id,
            required_scope: "workspace.access",
        },
    )
}

#[allow(clippy::result_large_err)]
pub(crate) fn ensure_admin_auth_scoped<T>(
    request: &Request<T>,
    auth: &AuthConfig,
    required_scope: &'static str,
) -> Result<(), Status> {
    verify_auth(
        request.metadata(),
        auth,
        global_runtime_metrics(),
        AuthRequirement::AdminOnly { required_scope },
    )
}
