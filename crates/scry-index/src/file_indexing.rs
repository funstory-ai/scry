//! Precompute digest + FTS chunks for file indexing without requiring `IndexStore`.

use crate::chunking::{chunk_text, headline_only_chunk, headline_only_chunk_bounded, IndexedChunk};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexingPolicyDb {
    Full,
    HeadlineOnly,
    Skipped,
}

impl IndexingPolicyDb {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::HeadlineOnly => "headline_only",
            Self::Skipped => "skipped",
        }
    }

    pub fn headline_only_hint(&self) -> bool {
        matches!(self, Self::HeadlineOnly)
    }
}

/// Default: 64 MiB — beyond this we only index a headline slice for UTF-8 text files.
pub const DEFAULT_SKIP_CHUNKING_OVER_BYTES: u64 = 64 * 1024 * 1024;

/// Bytes taken from head and tail for `headline_only` policy (each side).
pub const DEFAULT_HEADLINE_EACH_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone)]
pub struct PreparedFileIndexing {
    pub digest_hex: String,
    pub indexing_policy: IndexingPolicyDb,
    pub headline_only: bool,
    pub chunks: Vec<IndexedChunk>,
}

/// Index rows for binary / invalid UTF-8: hash only, no FTS chunks.
pub fn prepare_non_utf8_file_indexing(digest_hex: String) -> PreparedFileIndexing {
    PreparedFileIndexing {
        digest_hex,
        indexing_policy: IndexingPolicyDb::Skipped,
        headline_only: false,
        chunks: Vec::new(),
    }
}

/// Headline-only indexing when only bounded head/tail UTF-8 slices are available (large files).
pub fn prepare_file_indexing_headline_only_from_head_tail(
    normalized_path: &str,
    total_len: u64,
    digest_hex: String,
    headline_each_bytes: usize,
    head_utf8: &str,
    tail_utf8: &str,
) -> PreparedFileIndexing {
    let hc = headline_only_chunk_bounded(
        normalized_path,
        total_len as usize,
        head_utf8,
        tail_utf8,
        headline_each_bytes,
    );
    PreparedFileIndexing {
        digest_hex,
        indexing_policy: IndexingPolicyDb::HeadlineOnly,
        headline_only: true,
        chunks: vec![hc],
    }
}

/// Classify + chunk for valid UTF-8; `digest_hex` must be the blake3 of the same byte content.
pub fn prepare_file_indexing_from_utf8(
    normalized_path: &str,
    text: &str,
    content_len: u64,
    digest_hex: String,
    skip_chunking_over_bytes: u64,
    headline_each_bytes: usize,
) -> PreparedFileIndexing {
    if content_len > skip_chunking_over_bytes {
        let hc = headline_only_chunk(normalized_path, text, headline_each_bytes);
        return PreparedFileIndexing {
            digest_hex,
            indexing_policy: IndexingPolicyDb::HeadlineOnly,
            headline_only: true,
            chunks: vec![hc],
        };
    }
    PreparedFileIndexing {
        digest_hex,
        indexing_policy: IndexingPolicyDb::Full,
        headline_only: false,
        chunks: chunk_text(normalized_path, text),
    }
}

/// Hash + classify indexing (full slice in memory).
///
/// For binary (invalid UTF-8), returns `skipped` with no chunks (nodes row still updated).
pub fn prepare_file_indexing(
    normalized_path: &str,
    content: &[u8],
    skip_chunking_over_bytes: u64,
    headline_each_bytes: usize,
) -> PreparedFileIndexing {
    let digest_hex = blake3::hash(content).to_hex().to_string();
    let size_u64 = u64::try_from(content.len()).unwrap_or(u64::MAX);

    let text = match std::str::from_utf8(content) {
        Ok(t) => t,
        Err(_) => return prepare_non_utf8_file_indexing(digest_hex),
    };

    prepare_file_indexing_from_utf8(
        normalized_path,
        text,
        size_u64,
        digest_hex,
        skip_chunking_over_bytes,
        headline_each_bytes,
    )
}
