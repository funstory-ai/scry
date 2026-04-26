//! Smoke: `scryd` CLI parses and runs lightweight subcommands (post–P2-4).
use std::process::Command;

fn scryd_bin() -> String {
    std::env::var("CARGO_BIN_EXE_scryd").expect("CARGO_BIN_EXE_scryd must be set by cargo test")
}

#[test]
fn cli_ping_prints_pong() {
    let out = Command::new(scryd_bin())
        .arg("ping")
        .output()
        .expect("spawn scryd ping");
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "pong");
}

#[test]
fn cli_token_mint_help_exits_zero() {
    let out = Command::new(scryd_bin())
        .args(["token", "mint", "--help"])
        .output()
        .expect("spawn scryd token mint --help");
    assert!(out.status.success());
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(help.contains("mint") || help.contains("kind"));
}

#[test]
fn cli_admin_help_exits_zero() {
    let out = Command::new(scryd_bin())
        .args(["admin", "--help"])
        .output()
        .expect("spawn scryd admin --help");
    assert!(out.status.success());
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(help.contains("create-workspace") || help.contains("admin"));
}
