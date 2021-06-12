//! Functions and data structures related to reliability score checks

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use http::Request;
use setup1_shared::reliability::{ContributorMessage, CoordinatorMessage};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

use crate::utils::get_authorization_value;

mod bandwidth;
mod cpu;
mod latency;

/// Builds a request with authorization header to initialize
/// a WebSocket handshake later
fn prepare_request(api_url: &Url, api_path: &str, private_key: &str) -> Result<Request<()>> {
    let mut url = api_url.join(api_path)?;
    url.set_scheme("ws")
        .map_err(|e| anyhow!("Failed to set url scheme to ws: {:?}", e))?;

    let auth_rng = &mut rand::rngs::OsRng;
    let authorization = get_authorization_value(private_key, "GET", api_path, auth_rng)?;

    Request::builder()
        .uri(url.as_str())
        .header(http::header::AUTHORIZATION, authorization)
        .body(())
        .map_err(Into::into)
}

/// Check reliability score before starting
/// to contribute
pub(crate) async fn check(api_base_url: &Url, private_key: &str) -> Result<()> {
    let api_path = "/v1/contributor/reliability";
    let request = prepare_request(api_base_url, api_path, private_key)?;

    let (ws_stream, _response) = connect_async(request)
        .await
        .map_err(|e| anyhow!("Failed to connect to coordinator via WebSocket: {:?}", e))?;
    tracing::trace!("WebSocket handshake has been successfully completed");

    let (mut write, mut read) = ws_stream.split();

    loop {
        let raw_message = if let Some(next) = read.next().await {
            next?
        } else {
            tracing::trace!("Got None in reliability checks, closing the connection");
            break;
        };
        if raw_message.is_close() {
            break;
        }
        if raw_message.is_ping() | raw_message.is_pong() {
            // do nothing for now, but check the standard
            // and tungstenite implementation details
            continue;
        }
        let decoded = CoordinatorMessage::decode(&raw_message.into_data())?;
        tracing::trace!("Got message from coordinator: {}", decoded);

        match decoded {
            CoordinatorMessage::Ping { id } => latency::check(&mut write, id).await?,
            CoordinatorMessage::BandwidthChallenge(data) => bandwidth::check(&mut write, data).await?,
            CoordinatorMessage::CpuChallenge(data) => cpu::check(&mut write, data).await?,
            other => {
                let text = format!(
                    "Wrong message, expected one of Ping | BandwidthChallenge | CpuChallenge, got {}",
                    other,
                );
                tracing::warn!("{}", text);
                let error = ContributorMessage::Error(text);
                let response = Message::binary(error.encode()?);
                write.send(response).await.map_err(|e| anyhow!("{:?}", e))?;
            }
        }
    }
    Ok(())
}
