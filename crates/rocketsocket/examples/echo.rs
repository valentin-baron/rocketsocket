//! An echo bot: the smallest useful thing this framework can do.
//!
//! ```text
//! ROCKETSOCKET_URL=https://chat.example.com \
//! ROCKETSOCKET_USER_ID=<id> ROCKETSOCKET_TOKEN=<personal access token> \
//!   cargo run -p rocketsocket --example echo
//! ```
//!
//! Give the bot account the **`bot` role** first. It is not cosmetic: it grants
//! `api-bypass-rate-limit`, and without it Rocket.Chat's default limiter allows ten
//! requests per minute per route, which a bot exhausts immediately.

use rocketsocket::model::event::StreamEvent;
use rocketsocket::prelude::*;
use rocketsocket::realtime::client::ClientEvent;
use rocketsocket::rest::chat::SendMessage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info,rocketsocket=debug").init();

    let url = std::env::var("ROCKETSOCKET_URL")?;
    let user_id = std::env::var("ROCKETSOCKET_USER_ID")?;
    let token = std::env::var("ROCKETSOCKET_TOKEN")?;

    let (bot, mut events) =
        Bot::connect(&url, Credentials::personal_access_token(user_id.clone(), token)).await?;

    // One subscription covers every room the bot can see.
    bot.watch_all_messages().await?;
    tracing::info!("listening");

    while let Some(event) = events.recv().await {
        let ClientEvent::Stream { event, .. } = event else {
            continue;
        };

        // One typed variant instead of indexing a positional array. `MyMessage` carries the
        // trailing metadata element that per-room subscriptions do not have.
        let StreamEvent::MyMessage { message, .. } = event else {
            continue;
        };

        // Skip our own echoes. Do NOT filter on `message.bot` — that field is deprecated
        // and is never set for bot-role users; it is only populated by integrations.
        if message.u.id.as_str() == user_id {
            continue;
        }
        // An edit re-broadcasts the whole document, as do reactions and pins. Without this
        // the bot echoes its way into a loop on every edit.
        if message.is_edited() || message.is_system() {
            continue;
        }

        let Some(text) = message.msg.strip_prefix("!echo ") else { continue };
        tracing::info!(room = %message.rid, "echoing");

        let reply = SendMessage::new(message.rid.clone()).text(text);
        if let Err(error) = bot.rest().send_message(&reply).await {
            tracing::warn!(%error, "failed to reply");
        }
    }

    Ok(())
}
