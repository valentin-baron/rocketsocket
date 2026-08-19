//! The same echo bot, written with `#[event]`.
//!
//! Compare `echo.rs`, which drives the event stream by hand. Both are supported: the macro
//! layer is sugar, and everything it produces is a value you could have written yourself.
//!
//! ```text
//! ROCKETSOCKET_URL=https://chat.example.com \
//! ROCKETSOCKET_USER_ID=<id> ROCKETSOCKET_TOKEN=<personal access token> \
//!   cargo run -p rocketsocket --example handlers
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use rocketsocket::framework::{Context, Framework, MessageCreate, MessageDeleted, State};
use rocketsocket::prelude::*;
use rocketsocket::rest::chat::SendMessage;

/// Whatever the bot wants to keep. Generic, not a typemap — checked at compile time.
#[derive(Debug, Default)]
struct Data {
    me: String,
    echoed: AtomicU64,
}

type Error = Box<dyn std::error::Error + Send + Sync>;

/// The event is identified by the `MessageCreate` parameter, not by this function's name.
#[rocketsocket::event]
async fn echo(context: Context<Data>, event: MessageCreate) -> Result<(), Error> {
    let message = &event.message;

    // Never filter on `message.bot`: it is deprecated and never set for bot-role users.
    if message.u.id.as_str() == context.data().me {
        return Ok(());
    }
    // Any mutation re-broadcasts the whole document, so without this the bot loops on edits.
    if message.is_edited() || message.is_system() {
        return Ok(());
    }

    let Some(text) = message.msg.strip_prefix("!echo ") else {
        return Ok(());
    };

    context.bot().rest().send_message(&SendMessage::new(message.rid.clone()).text(text)).await?;
    context.data().echoed.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// A second handler for a different event. Declaring only what it needs.
#[rocketsocket::event]
async fn note_deletions(event: MessageDeleted, State(data): State<Data>) -> Result<(), Error> {
    tracing::info!(room = %event.room, message = %event.message, echoed = data.echoed.load(Ordering::Relaxed), "deleted");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt().with_env_filter("info,rocketsocket=debug").init();

    let url = std::env::var("ROCKETSOCKET_URL")?;
    let user_id = std::env::var("ROCKETSOCKET_USER_ID")?;
    let token = std::env::var("ROCKETSOCKET_TOKEN")?;

    let (bot, mut events) =
        Bot::connect(&url, Credentials::personal_access_token(user_id.clone(), token)).await?;
    bot.watch_all_messages().await?;

    let framework = Framework::new(bot, Data { me: user_id, echoed: AtomicU64::new(0) })
        .handler(echo())
        .handler(note_deletions());

    // A bot with no handlers connects, logs in and does nothing — which looks exactly like
    // a quiet server. Fail loudly instead.
    assert!(!framework.is_empty(), "no handlers registered");

    while let Some(event) = events.recv().await {
        framework.dispatch(&event).await;
    }

    Ok(())
}
