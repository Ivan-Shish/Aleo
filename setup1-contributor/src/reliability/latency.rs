use std::fmt::Debug;

use anyhow::{anyhow, Result};
use futures::{Sink, SinkExt};
use setup1_shared::reliability::ContributorMessage;
use tokio_tungstenite::tungstenite::protocol::Message;

pub(super) async fn check<W>(write_half: &mut W, request_id: i64) -> Result<()>
where
    W: Sink<Message> + Unpin,
    W::Error: Debug,
{
    let pong = ContributorMessage::Pong { id: request_id };
    let response = Message::binary(pong.encode()?);
    write_half.send(response).await.map_err(|e| anyhow!("{:?}", e))?;
    Ok(())
}
