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
    echoed: AtomicU64,
}

type Error = Box<dyn std::error::Error + Send + Sync>;

/// The event is identified by the `MessageCreate` parameter, not by this function's name.
///
/// No loop guards in the body: the bot never hears itself, never sees edits or
/// re-broadcasts, and never sees system messages, because those are the defaults. `prefix`
/// does the text match, so the body only has the interesting part.
#[rocketsocket::event(prefix = "!echo ")]
async fn echo(context: Context<Data>, event: MessageCreate) -> Result<(), Error> {
    let message = &event.message;
    let text = message.msg.trim_start_matches("!echo ");

    context.bot().rest().send_message(&SendMessage::new(message.rid.clone()).text(text)).await?;
    context.data().echoed.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

/// A second handler for the *same* event, under a different name — the function name is
/// free because the event is identified by the parameter type.
#[rocketsocket::event(prefix = "!ping ", mentions_me)]
async fn ping(context: Context<Data>, event: MessageCreate) -> Result<(), Error> {
    context
        .bot()
        .rest()
        .send_message(&SendMessage::new(event.message.rid.clone()).text("pong"))
        .await?;
    Ok(())
}

/// A third, for a different event, declaring only what it needs.
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
        Bot::connect(&url, Credentials::personal_access_token(user_id, token)).await?;
    bot.watch_all_messages().await?;

    let framework = Framework::new(bot, Data { echoed: AtomicU64::new(0) })
        .handler(echo())
        .handler(ping())
        .handler(note_deletions());

    // A bot with no handlers connects, logs in and does nothing — which looks exactly like
    // a quiet server. Fail loudly instead.
    assert!(!framework.is_empty(), "no handlers registered");

    while let Some(event) = events.recv().await {
        framework.dispatch(&event).await;
    }

    Ok(())
}
