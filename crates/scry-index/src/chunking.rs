//! Text chunking for FTS (`nodes`/`chunks`), extracted for reuse from streaming indexing paths.

use std::path::Path;

#[derive(Debug, Clone)]
pub struct IndexedChunk {
    pub start_line: u32,
    pub end_line: u32,
    pub content: String,
    pub context_path: String,
}

fn count_lines(content: &str) -> u32 {
    if content.is_empty() {
        1
    } else {
        content.lines().count().max(1) as u32
    }
}

#[derive(Debug, Clone)]
struct ChunkSlice {
    start_line: u32,
    end_line: u32,
    content: String,
    context_path: String,
}

pub fn chunk_text(path: &str, text: &str) -> Vec<IndexedChunk> {
    chunk_content_inner(path, text)
        .into_iter()
        .map(|c| IndexedChunk {
            start_line: c.start_line,
            end_line: c.end_line,
            content: c.content,
            context_path: c.context_path,
        })
        .collect()
}

/// Like [`headline_only_chunk`], but takes only bounded head/tail text slices and the total byte
/// length of the file (used when the full file is not loaded into memory).
pub fn headline_only_chunk_bounded(
    path: &str,
    total_len: usize,
    head: &str,
    tail: &str,
    each_max: usize,
) -> IndexedChunk {
    let each = each_max.max(4096);
    if total_len <= each * 2 {
        let combined = format!("{head}{tail}");
        let (sl, el) = line_numbers_for_offset_range(&combined, 0, combined.len());
        return IndexedChunk {
            start_line: sl,
            end_line: el,
            content: combined,
            context_path: format!("{path}#headline_only"),
        };
    }
    let joined = format!("{head}\n…\n{tail}");
    let (sl, el) = line_numbers_for_offset_range(&joined, 0, joined.len());
    IndexedChunk {
        start_line: sl,
        end_line: el,
        content: joined,
        context_path: format!("{path}#headline_only"),
    }
}

/// Single headline chunk for degraded indexing (large UTF-8 text files).
pub fn headline_only_chunk(path: &str, text: &str, each_max: usize) -> IndexedChunk {
    let each = each_max.max(4096);
    let len = text.len();
    if len <= each * 2 {
        let (sl, el) = line_numbers_for_offset_range(text, 0, len);
        return IndexedChunk {
            start_line: sl,
            end_line: el,
            content: text.to_string(),
            context_path: format!("{path}#headline_only"),
        };
    }
    let head = &text[..each];
    let tail_start = len.saturating_sub(each);
    let tail_start = tail_start.min(text.len());
    let tail_start = floor_char_boundary(text, tail_start);
    let tail = &text[tail_start..];
    let joined = format!("{head}\n…\n{tail}");
    let (sl, el) = line_numbers_for_offset_range(&joined, 0, joined.len());
    IndexedChunk {
        start_line: sl,
        end_line: el,
        content: joined,
        context_path: format!("{path}#headline_only"),
    }
}

fn floor_char_boundary(s: &str, mut i: usize) -> usize {
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn chunk_content_inner(path: &str, text: &str) -> Vec<ChunkSlice> {
    if is_markdown_path(path) {
        chunk_markdown_sections(path, text)
    } else {
        chunk_plain_text_sections(path, text)
    }
}

fn is_markdown_path(path: &str) -> bool {
    matches!(
        Path::new(path).extension().and_then(|ext| ext.to_str()),
        Some("md" | "markdown" | "mdx")
    )
}

/// Upper byte bound for a single Markdown section chunk. Sections larger than this are
/// further split via the plain-text splitter so embedding payloads stay bounded even when
/// a Markdown file has giant sections or no headings at all.
pub(crate) const MARKDOWN_SECTION_MAX_BYTES: usize = 64 * 1024;

fn chunk_markdown_sections(path: &str, text: &str) -> Vec<ChunkSlice> {
    let mut out = Vec::new();
    let mut section_start = 0usize;
    let mut section_title = "document".to_string();

    for (line_start, line) in iter_line_starts_with_text(text) {
        let trimmed = line.trim_start();
        if markdown_heading(trimmed).is_some() {
            if section_start < line_start {
                let section = &text[section_start..line_start];
                if !section.trim().is_empty() {
                    push_markdown_section(
                        path,
                        text,
                        section_start,
                        line_start,
                        &section_title,
                        &mut out,
                    );
                }
            }
            section_start = line_start;
            section_title = trimmed.trim_start_matches('#').trim().to_string();
        }
    }

    if section_start < text.len() {
        let section = &text[section_start..];
        if !section.trim().is_empty() {
            push_markdown_section(
                path,
                text,
                section_start,
                text.len(),
                &section_title,
                &mut out,
            );
        }
    }

    ensure_non_empty_chunks(path, text, out)
}

/// Push a Markdown section into `out`, splitting it via the plain-text splitter when the
/// raw section exceeds [`MARKDOWN_SECTION_MAX_BYTES`].
fn push_markdown_section(
    path: &str,
    text: &str,
    section_start: usize,
    section_end: usize,
    section_title: &str,
    out: &mut Vec<ChunkSlice>,
) {
    let section = &text[section_start..section_end];
    let context_path = format!("{path}#{section_title}");
    if section.len() <= MARKDOWN_SECTION_MAX_BYTES {
        let (start_line, end_line) =
            line_numbers_for_offset_range(text, section_start, section_end);
        out.push(ChunkSlice {
            start_line,
            end_line,
            content: section.to_string(),
            context_path,
        });
        return;
    }
    // Section too large: fall back to plain-text splitting inside this section so no single
    // chunk carries more than `MAX_CHUNK_BYTES` into embedding requests. Line numbers are
    // computed on the outer `text` so they stay globally correct.
    for piece in split_plain_text_in_range(text, section_start, section_end) {
        out.push(ChunkSlice {
            start_line: piece.start_line,
            end_line: piece.end_line,
            content: piece.content,
            context_path: context_path.clone(),
        });
    }
}

fn iter_line_starts_with_text(text: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut start = 0usize;
    for line in text.split_inclusive('\n') {
        out.push((start, line));
        start += line.len();
    }
    if start < text.len() {
        out.push((start, &text[start..]));
    }
    out
}

fn markdown_heading(line: &str) -> Option<(u8, &str)> {
    let hashes = line.chars().take_while(|ch| *ch == '#').count();
    if hashes == 0 || hashes > 6 {
        return None;
    }
    let rest = line[hashes..].trim();
    if rest.is_empty() {
        return None;
    }
    Some((u8::try_from(hashes).ok()?, rest))
}

pub(crate) const PLAIN_TEXT_MAX_CHUNK_BYTES: usize = 2048;
const PLAIN_TEXT_OVERLAP_BYTES: usize = 256;

fn chunk_plain_text_sections(path: &str, text: &str) -> Vec<ChunkSlice> {
    let mut out = Vec::new();
    if text.is_empty() {
        return vec![ChunkSlice {
            start_line: 1,
            end_line: 1,
            content: String::new(),
            context_path: path.to_string(),
        }];
    }

    for piece in split_plain_text_in_range(text, 0, text.len()) {
        out.push(ChunkSlice {
            start_line: piece.start_line,
            end_line: piece.end_line,
            content: piece.content,
            context_path: path.to_string(),
        });
    }

    ensure_non_empty_chunks(path, text, out)
}

/// Split `text[range_start..range_end]` into byte-bounded chunks (≤ `PLAIN_TEXT_MAX_CHUNK_BYTES`),
/// preferring to break on `\n` boundaries and overlapping by `PLAIN_TEXT_OVERLAP_BYTES` for FTS
/// recall. `context_path` is intentionally left empty — callers fill it in so Markdown sections
/// can keep their `#section` suffix while still re-using this splitter.
fn split_plain_text_in_range(text: &str, range_start: usize, range_end: usize) -> Vec<ChunkSlice> {
    let mut out = Vec::new();
    if range_end <= range_start {
        return out;
    }
    let mut start = range_start;
    while start < range_end {
        let tentative_end = (start + PLAIN_TEXT_MAX_CHUNK_BYTES).min(range_end);
        let end = if tentative_end < range_end {
            text[start..tentative_end]
                .rfind('\n')
                .map(|idx| start + idx + 1)
                .filter(|candidate| *candidate > start)
                .unwrap_or(tentative_end)
        } else {
            tentative_end
        };
        let mut end = end;
        while end < range_end && !text.is_char_boundary(end) {
            end += 1;
        }
        let chunk = &text[start..end];
        if !chunk.is_empty() {
            let (start_line, end_line) = line_numbers_for_offset_range(text, start, end);
            out.push(ChunkSlice {
                start_line,
                end_line,
                content: chunk.to_string(),
                context_path: String::new(),
            });
        }
        if end >= range_end {
            break;
        }
        let overlap = PLAIN_TEXT_OVERLAP_BYTES.min(end - start);
        start = end.saturating_sub(overlap);
        while start < range_end && !text.is_char_boundary(start) {
            start += 1;
        }
    }
    out
}

fn ensure_non_empty_chunks(path: &str, text: &str, chunks: Vec<ChunkSlice>) -> Vec<ChunkSlice> {
    if chunks.is_empty() {
        vec![ChunkSlice {
            start_line: 1,
            end_line: count_lines(text),
            content: text.to_string(),
            context_path: path.to_string(),
        }]
    } else {
        chunks
    }
}

fn line_numbers_for_offset_range(text: &str, start: usize, end: usize) -> (u32, u32) {
    let mut start_line = 1u32;
    let mut end_line = 1u32;
    let mut byte_offset = 0usize;

    for line in text.split_inclusive('\n') {
        let line_len = line.len();
        let line_start = byte_offset;
        let line_end = byte_offset + line_len;
        if start >= line_start && start < line_end {
            start_line = end_line;
        }
        if end > line_start && end <= line_end {
            return (start_line, end_line);
        }
        byte_offset += line_len;
        end_line = end_line.saturating_add(1);
    }
    (start_line, count_lines(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_section_over_cap_is_split_by_plain_text() {
        let mut body = String::with_capacity(MARKDOWN_SECTION_MAX_BYTES * 2 + 64);
        body.push_str("# big\n");
        for i in 0..(MARKDOWN_SECTION_MAX_BYTES * 2 / 32) {
            body.push_str(&format!("line {i:08} padding padding\n"));
        }
        let chunks = chunk_text("huge.md", &body);

        assert!(
            chunks.len() > 1,
            "oversized markdown section must be split; got {}",
            chunks.len()
        );
        for (idx, c) in chunks.iter().enumerate() {
            assert!(
                c.content.len() <= PLAIN_TEXT_MAX_CHUNK_BYTES + /* overlap slack */ 512,
                "chunk[{idx}] is {} bytes, exceeds plain-text cap",
                c.content.len()
            );
            assert!(
                c.context_path.starts_with("huge.md#"),
                "chunk[{idx}] must keep markdown section context, got {}",
                c.context_path
            );
        }
    }

    #[test]
    fn markdown_small_section_fits_in_single_chunk() {
        let body = "# small\nhello world\nline 2\nline 3\n";
        let chunks = chunk_text("small.md", body);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].context_path, "small.md#small");
        assert!(chunks[0].content.contains("hello world"));
    }

    #[test]
    fn markdown_no_headings_still_splits_by_plain_text_when_oversized() {
        let line = "abcdefghij".repeat(64); // 640 bytes
        let mut body = String::new();
        for _ in 0..200 {
            // ~128 KiB, no headings => single implicit "document" section
            body.push_str(&line);
            body.push('\n');
        }
        let chunks = chunk_text("no-heading.md", &body);
        assert!(
            chunks.len() > 1,
            "no-heading oversized markdown must still split"
        );
        for c in &chunks {
            assert!(
                c.content.len() <= PLAIN_TEXT_MAX_CHUNK_BYTES + 512,
                "chunk exceeds cap: {}",
                c.content.len()
            );
            assert_eq!(c.context_path, "no-heading.md#document");
        }
    }

    #[test]
    fn plain_text_chunks_respect_cap() {
        let body = "x".repeat(PLAIN_TEXT_MAX_CHUNK_BYTES * 3 + 17);
        let chunks = chunk_text("huge.txt", &body);
        assert!(chunks.len() >= 3);
        for c in &chunks {
            assert!(c.content.len() <= PLAIN_TEXT_MAX_CHUNK_BYTES + 512);
            assert_eq!(c.context_path, "huge.txt");
        }
    }
}
