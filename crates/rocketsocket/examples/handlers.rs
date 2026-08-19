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

/// A role-gated handler. The author must hold the global `admin` role, and the check runs
/// before the body — so the body never has to remember it.
///
/// The lookup behind it is memoised per workspace, not per message: Rocket.Chat's default
/// limiter allows ten requests per minute per route, and a role check per message would
/// exhaust that immediately. It also **fails closed** — if the lookup errors or times out
/// the handler stays quiet rather than running for someone who might not be an admin.
#[rocketsocket::event(prefix = "!shutdown", admin)]
async fn shutdown(context: Context<Data>, event: MessageCreate) -> Result<(), Error> {
    context
        .bot()
        .rest()
        .send_message(&SendMessage::new(event.message.rid.clone()).text("acknowledged"))
        .await?;
    Ok(())
}

/// The same, scoped to the room the message arrived in: the author must be its owner,
/// moderator or leader. A workspace admin who is not one of those does *not* pass — use
/// `any_admin` for "either".
#[rocketsocket::event(prefix = "!purge", room_admin)]
async fn purge(context: Context<Data>, event: MessageCreate) -> Result<(), Error> {
    tracing::info!(room = %event.message.rid, "purge requested by a room admin");
    let _ = context;
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
        .handler(shutdown())
        .handler(purge())
        .handler(note_deletions());

    // Optional, and strictly an improvement: a granted or revoked role then takes effect as
    // soon as the event arrives instead of waiting out the role cache's TTL. `dispatch`
    // applies the events; this only asks for them. Not fatal if it fails — the TTL still
    // bounds how long a stale grant survives.
    if let Err(error) = framework.watch_role_changes().await {
        tracing::warn!(%error, "role changes will be picked up by TTL only");
    }

    // A bot with no handlers connects, logs in and does nothing — which looks exactly like
    // a quiet server. Fail loudly instead.
    assert!(!framework.is_empty(), "no handlers registered");

    while let Some(event) = events.recv().await {
        framework.dispatch(&event).await;
    }

    Ok(())
}
