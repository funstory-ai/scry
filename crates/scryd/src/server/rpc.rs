use crate::server::prelude::*;

use crate::catalog;
use crate::embedding;
pub(crate) fn index_status(err: IndexError) -> Status {
    match err {
        IndexError::WorkspaceNotFound(_)
        | IndexError::NodeNotFound { .. }
        | IndexError::NodeIdNotFound { .. }
        | IndexError::ParentNotFound(_) => Status::not_found(err.to_string()),
        IndexError::WorkspaceAlreadyExists(_) | IndexError::AlreadyExistsAtPath { .. } => {
            Status::already_exists(err.to_string())
        }
        IndexError::InvalidPath(_) | IndexError::InvalidCursor | IndexError::InvalidInput(_) => {
            Status::invalid_argument(err.to_string())
        }
        IndexError::InvalidHandleMode(_) => Status::invalid_argument(err.to_string()),
        IndexError::InvalidIndexingJobStatus(_) => Status::internal(err.to_string()),
        IndexError::NotDirectory(_) | IndexError::DirectoryNotEmpty(_) => {
            Status::failed_precondition(err.to_string())
        }
        IndexError::VersionConflict { .. } => Status::failed_precondition(err.to_string()),
        IndexError::ContentNotInIndex => Status::internal(err.to_string()),
        IndexError::InvalidNodeKind(_) => Status::internal(err.to_string()),
        IndexError::NumericConversion => Status::internal(err.to_string()),
        IndexError::EmbeddingCountMismatch { .. } => Status::internal(err.to_string()),
        IndexError::RedirectLimitExceeded { .. } | IndexError::RedirectLoop { .. } => {
            Status::internal(err.to_string())
        }
        IndexError::Sqlite(_) | IndexError::ConnectionPoolPoisoned(_) => {
            Status::internal(err.to_string())
        }
        IndexError::ConnectionPoolExhausted => Status::resource_exhausted(err.to_string()),
    }
}

pub(crate) fn catalog_status(err: catalog::CatalogError) -> Status {
    match err {
        catalog::CatalogError::WorkspaceNotFound { .. } => Status::not_found(err.to_string()),
        catalog::CatalogError::WorkspaceAlreadyExists { .. } => {
            Status::already_exists(err.to_string())
        }
        catalog::CatalogError::Other(_) => Status::internal(err.to_string()),
    }
}

pub(crate) fn embedding_status(err: embedding::EmbeddingError) -> Status {
    match err {
        embedding::EmbeddingError::Transport(_) | embedding::EmbeddingError::Upstream { .. } => {
            Status::unavailable(err.to_string())
        }
        embedding::EmbeddingError::ResponseShape(_) => Status::internal(err.to_string()),
    }
}

pub(crate) fn internal_status(err: impl std::fmt::Display) -> Status {
    Status::internal(err.to_string())
}

pub(crate) fn to_proto_attrs(attrs: &scry_index::NodeAttrs) -> Attrs {
    Attrs {
        kind: match attrs.kind {
            NodeKind::File => ProtoNodeKind::File as i32,
            NodeKind::Dir => ProtoNodeKind::Dir as i32,
        },
        size: attrs.size,
        mtime_unix_nano: attrs.mtime_unix_nano,
        mode: attrs.mode,
        version: attrs.version,
        content_hash: attrs.content_hash.clone(),
        mime: attrs.mime.clone(),
    }
}

pub(crate) fn to_dir_entry(node: NodeRecord) -> DirEntry {
    DirEntry {
        name: node.name,
        node_id: node.node_id,
        attrs: Some(to_proto_attrs(&node.attrs)),
    }
}

pub(crate) fn from_proto_kind(kind: i32) -> Result<NodeKind, &'static str> {
    match ProtoNodeKind::try_from(kind).unwrap_or(ProtoNodeKind::Unspecified) {
        ProtoNodeKind::File => Ok(NodeKind::File),
        ProtoNodeKind::Dir => Ok(NodeKind::Dir),
        ProtoNodeKind::Unspecified => Err("node kind must be file or dir"),
    }
}

pub(crate) fn to_proto_event_kind(kind: scry_index::EventKind) -> ProtoEventKind {
    match kind {
        scry_index::EventKind::NodeCreated => ProtoEventKind::NodeCreated,
        scry_index::EventKind::NodeModified => ProtoEventKind::NodeModified,
        scry_index::EventKind::NodeDeleted => ProtoEventKind::NodeDeleted,
        scry_index::EventKind::NodeRenamed => ProtoEventKind::NodeRenamed,
        scry_index::EventKind::AttrsChanged => ProtoEventKind::AttrsChanged,
        scry_index::EventKind::IndexUpdated => ProtoEventKind::IndexUpdated,
    }
}

pub(crate) fn normalize_subscribe_filter(
    filter: scry_proto::scry::v1::SubscribeFilter,
) -> scry_proto::scry::v1::SubscribeFilter {
    scry_proto::scry::v1::SubscribeFilter {
        path_prefix: filter
            .path_prefix
            .into_iter()
            .map(|prefix| prefix.trim_start_matches('/').to_string())
            .filter(|prefix| !prefix.is_empty())
            .collect(),
        node_ids: filter
            .node_ids
            .into_iter()
            .filter(|node_id| !node_id.trim().is_empty())
            .collect(),
        kinds: filter.kinds,
        include_index_events: filter.include_index_events,
    }
}

pub(crate) fn should_emit_event(
    event: &scry_index::EventRecord,
    filter: &scry_proto::scry::v1::SubscribeFilter,
) -> bool {
    let proto_kind = to_proto_event_kind(event.kind.clone()) as i32;
    if !filter.include_index_events && proto_kind == ProtoEventKind::IndexUpdated as i32 {
        return false;
    }
    if !filter.kinds.is_empty() && !filter.kinds.contains(&proto_kind) {
        return false;
    }
    if !filter.node_ids.is_empty() && !filter.node_ids.contains(&event.node_id) {
        return false;
    }
    if !filter.path_prefix.is_empty() {
        let matched = filter.path_prefix.iter().any(|prefix| {
            event.path == *prefix || event.path.starts_with(&(prefix.to_string() + "/"))
        });
        if !matched {
            return false;
        }
    }
    true
}

pub(crate) fn to_index_search_mode(mode: i32) -> scry_index::SearchMode {
    match SearchMode::try_from(mode).unwrap_or(SearchMode::Unspecified) {
        SearchMode::Unspecified | SearchMode::Fts => scry_index::SearchMode::Fts,
        SearchMode::Vector => scry_index::SearchMode::Vector,
        SearchMode::Hybrid => scry_index::SearchMode::Hybrid,
    }
}

pub(crate) fn to_index_handle_mode(mode: i32) -> scry_index::HandleMode {
    match ProtoHandleMode::try_from(mode).unwrap_or(ProtoHandleMode::Unspecified) {
        ProtoHandleMode::Unspecified | ProtoHandleMode::Read => scry_index::HandleMode::Read,
        ProtoHandleMode::Write => scry_index::HandleMode::Write,
        ProtoHandleMode::ReadWrite => scry_index::HandleMode::ReadWrite,
    }
}
