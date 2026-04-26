use crate::server::prelude::*;

#[derive(Default)]
pub(crate) struct HealthSvc;

#[tonic::async_trait]
impl Health for HealthSvc {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            message: "pong".to_string(),
        }))
    }
}
