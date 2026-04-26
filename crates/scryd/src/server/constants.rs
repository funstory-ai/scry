pub(crate) const DEFAULT_AUTH_SECRET: &str = "dev-insecure-secret-change-me";
pub(crate) const HARDENING_DOC_PATH: &str = "docs/hardening.md";
pub(crate) const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const DEFAULT_IO_STREAM_CHUNK_BYTES: usize = 1024 * 1024;
/// Max bytes kept in RAM for a single IO handle buffer before spilling to a temp file (P2-3).
pub(crate) const DEFAULT_IO_HANDLE_MEMORY_LIMIT: usize = 32 * 1024 * 1024;
/// HTTP/2 SETTINGS_MAX_FRAME_SIZE upper bound is 2^24-1 bytes (RFC 7540).
pub(crate) const DEFAULT_GRPC_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024 - 1;
pub(crate) const DEFAULT_EVENTS_BUS_CAPACITY: usize = 4096;

/// Upper bounds for `scryd_rpc_duration_seconds` (plan P2-2); implicit +Inf bucket.
pub(crate) const RPC_DURATION_BUCKETS_SECS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
