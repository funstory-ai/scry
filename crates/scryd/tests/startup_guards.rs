use std::process::Command;

fn scryd_bin() -> String {
    std::env::var("CARGO_BIN_EXE_scryd").expect("CARGO_BIN_EXE_scryd must be set by cargo test")
}

fn base_command() -> Command {
    let mut cmd = Command::new(scryd_bin());
    cmd.env("SCRYD_AUTH_PREVIOUS_SECRETS", "");
    cmd.env("SCRYD_CATALOG_BACKEND", "sqlite");
    cmd
}

#[test]
fn startup_rejects_default_secret_without_dev_mode() {
    let output = base_command()
        .env("SCRYD_AUTH_SECRET", "dev-insecure-secret-change-me")
        .env("SCRYD_DEV_MODE", "0")
        .env("SCRYD_GRPC_ADDR", "127.0.0.1:50051")
        .output()
        .expect("scryd should execute");
    assert!(
        !output.status.success(),
        "scryd startup should fail with default secret"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("default insecure value"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn startup_rejects_non_loopback_without_tls() {
    let output = base_command()
        .env("SCRYD_AUTH_SECRET", "test-secret-not-default")
        .env("SCRYD_GRPC_ADDR", "0.0.0.0:50051")
        .env_remove("SCRYD_TLS_CERT_PATH")
        .env_remove("SCRYD_TLS_KEY_PATH")
        .output()
        .expect("scryd should execute");
    assert!(
        !output.status.success(),
        "scryd startup should fail when tls missing on non-loopback"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not loopback and TLS is not configured"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn startup_rejects_public_metrics_without_opt_in() {
    let output = base_command()
        .env("SCRYD_AUTH_SECRET", "test-secret-not-default")
        .env("SCRYD_GRPC_ADDR", "127.0.0.1:50051")
        .env("SCRYD_METRICS_ADDR", "0.0.0.0:9090")
        .env("SCRYD_METRICS_ALLOW_PUBLIC", "0")
        .output()
        .expect("scryd should execute");
    assert!(
        !output.status.success(),
        "scryd startup should fail when metrics bind is public by default"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Refusing to expose /metrics publicly"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn mint_token_rejects_default_secret_without_explicit_secret() {
    let output = base_command()
        .arg("mint-token")
        .arg("--kind")
        .arg("admin")
        .env_remove("SCRYD_AUTH_SECRET")
        .output()
        .expect("scryd should execute");
    assert!(
        !output.status.success(),
        "mint-token should fail without explicit non-default secret"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("requires --secret or SCRYD_AUTH_SECRET"),
        "unexpected stderr: {stderr}"
    );
}
