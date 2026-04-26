use crate::server::prelude::*;

use crate::server::auth::*;
use crate::server::config::AuthConfig;
use crate::server::rpc::*;
use crate::server::state::AppState;

pub(crate) struct EventsSvc {
    pub(crate) state: Arc<AppState>,
    pub(crate) auth: Arc<AuthConfig>,
}

type SubscribeStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<ProtoEvent, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl Events for EventsSvc {
    type SubscribeStream = SubscribeStream;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        let workspace_id = req.workspace_id;
        if workspace_id.trim().is_empty() {
            return Err(Status::invalid_argument("workspace_id cannot be empty"));
        }
        let since_cursor = if req.since_cursor.trim().is_empty() {
            let subscriber_id = req.subscriber_id.trim();
            if subscriber_id.is_empty() {
                Some(
                    self.state
                        .index_backend
                        .latest_event_cursor(&workspace_id)
                        .map_err(index_status)?
                        .unwrap_or(0),
                )
            } else {
                self.state
                    .index_backend
                    .subscriber_cursor(&workspace_id, subscriber_id)
                    .map_err(index_status)?
                    .or_else(|| {
                        self.state
                            .index_backend
                            .latest_event_cursor(&workspace_id)
                            .ok()
                            .flatten()
                    })
                    .or(Some(0))
            }
        } else {
            Some(
                req.since_cursor
                    .parse::<u64>()
                    .map_err(|_| Status::invalid_argument("since_cursor must be numeric"))?,
            )
        };
        let filter = req.filter.unwrap_or_default();
        let normalized_filter = normalize_subscribe_filter(filter);
        let state = Arc::clone(&self.state);
        let mut notifier = state
            .subscribe_workspace_events(&workspace_id)
            .ok_or_else(|| Status::internal("failed to subscribe workspace event notifier"))?;
        let outbound = async_stream::try_stream! {
            let mut cursor = since_cursor;
            loop {
                let batch = state
                    .index_backend
                    .list_events_since(&workspace_id, cursor, 256)
                    .map_err(index_status)?;
                if batch.is_empty() {
                    match notifier.recv().await {
                        Ok(ev) => {
                            let id = ev.cursor.parse::<u64>().ok();
                            if let (Some(cur), Some(eid)) = (cursor, id) {
                                if eid <= cur {
                                    continue;
                                }
                            }
                            cursor = id;
                            if !should_emit_event(&ev, &normalized_filter) {
                                continue;
                            }
                            let proto_kind = to_proto_event_kind(ev.kind.clone()) as i32;
                            yield ProtoEvent {
                                cursor: ev.cursor,
                                created_at_unix_nano: ev.created_at_unix_nano,
                                kind: proto_kind,
                                workspace_id: ev.workspace_id,
                                node_id: ev.node_id,
                                payload_json: ev.payload_json,
                                path: ev.path,
                            };
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            let catchup = state
                                .index_backend
                                .list_events_since(&workspace_id, cursor, 1024)
                                .map_err(index_status)?;
                            for event in catchup {
                                let id = event.cursor.parse::<u64>().ok();
                                cursor = id;
                                if !should_emit_event(&event, &normalized_filter) {
                                    continue;
                                }
                                let proto_kind = to_proto_event_kind(event.kind.clone()) as i32;
                                yield ProtoEvent {
                                    cursor: event.cursor,
                                    created_at_unix_nano: event.created_at_unix_nano,
                                    kind: proto_kind,
                                    workspace_id: event.workspace_id,
                                    node_id: event.node_id,
                                    payload_json: event.payload_json,
                                    path: event.path,
                                };
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                    continue;
                }
                for event in batch {
                    let id = event.cursor.parse::<u64>().ok();
                    cursor = id;
                    if !should_emit_event(&event, &normalized_filter) {
                        continue;
                    }
                    let proto_kind = to_proto_event_kind(event.kind.clone()) as i32;
                    yield ProtoEvent {
                        cursor: event.cursor,
                        created_at_unix_nano: event.created_at_unix_nano,
                        kind: proto_kind,
                        workspace_id: event.workspace_id,
                        node_id: event.node_id,
                        payload_json: event.payload_json,
                        path: event.path,
                    };
                }
            }
        };
        Ok(Response::new(Box::pin(outbound)))
    }

    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<AcknowledgeResponse>, Status> {
        ensure_workspace_auth(&request, &self.auth, request.get_ref().workspace_id.trim())?;
        let req = request.into_inner();
        if req.workspace_id.trim().is_empty() {
            return Err(Status::invalid_argument("workspace_id cannot be empty"));
        }
        if req.cursor.trim().is_empty() {
            return Err(Status::invalid_argument("cursor cannot be empty"));
        }
        let subscriber_id = if req.subscriber_id.trim().is_empty() {
            "default"
        } else {
            req.subscriber_id.trim()
        };
        self.state
            .index_backend
            .acknowledge_cursor(&req.workspace_id, subscriber_id, &req.cursor)
            .map_err(index_status)?;
        Ok(Response::new(AcknowledgeResponse {}))
    }
}
