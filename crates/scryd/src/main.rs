#[tokio::main]
async fn main() -> anyhow::Result<()> {
    scryd::run().await
}
