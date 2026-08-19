//! The single entry point: one credential, both transports.

use rocketsocket_realtime::client::{Client as RealtimeClient, ClientEvents};
use rocketsocket_realtime::connection::{Config, Credential};
use rocketsocket_rest::auth::Credentials;
use rocketsocket_rest::client::Client as RestClient;
use rocketsocket_rest::error::RestError;

/// Why a bot could not start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BotError {
    /// The base URL was not usable.
    #[error("invalid server URL: {0}")]
    Url(String),

    /// Authentication failed.
    #[error(transparent)]
    Rest(#[from] RestError),
}

/// A connected bot: a REST client for acting, a realtime client for receiving.
#[derive(Debug, Clone)]
pub struct Bot {
    rest: RestClient,
    realtime: RealtimeClient,
}

impl Bot {
    /// Authenticates over REST, then opens the websocket with the same token.
    ///
    /// `base_url` is the workspace root (`https://chat.example.com`), not the websocket
    /// endpoint — the DDP URL is derived from it.
    ///
    /// # Errors
    /// Returns [`BotError`] if the URL is unusable or authentication is rejected.
    pub async fn connect(
        base_url: &str,
        credentials: Credentials,
    ) -> Result<(Self, ClientEvents), BotError> {
        let rest = RestClient::new(base_url)?;
        let outcome = rest.login(&credentials).await?;

        // The REST token *is* a DDP resume token: /api/v1/login wraps the DDP login method
        // and both transports read services.resume.loginTokens. Reusing it avoids issuing a
        // second token, which matters because MAX_RESUME_LOGIN_TOKENS defaults to 50 and a
        // bot that re-authenticates on every reconnect evicts its own older tokens.
        let token = outcome.authentication.resume_token().expose().to_owned();

        let config = Config::new(websocket_url(base_url)?, Credential::Resume(token));
        let (realtime, events) = RealtimeClient::spawn(config);

        Ok((Self { rest, realtime }, events))
    }

    /// A `Bot` wired to a loopback address that is never connected to.
    ///
    /// For tests that need a `Context` but never perform IO. Not public API.
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        let rest = RestClient::new("http://127.0.0.1:1").expect("a loopback URL is valid");
        let config = Config::new(
            "ws://127.0.0.1:1/websocket".to_owned(),
            Credential::Resume("test".to_owned()),
        );
        let (realtime, _events) = RealtimeClient::spawn(config);
        Self { rest, realtime }
    }

    /// The bot's own user id, once authenticated.
    ///
    /// Needed by the loop-safety filters: Rocket.Chat has no per-message "is a bot" flag
    /// worth trusting (`IMessage.bot` is deprecated and never set for bot-role users), so
    /// comparing `u._id` is the only reliable way for a bot to recognise its own traffic.
    pub async fn user_id(&self) -> Option<rocketsocket_model::UserId> {
        self.rest.authentication().await.map(|auth| auth.user_id().clone())
    }

    /// The REST client, for anything that changes server state.
    #[must_use]
    pub fn rest(&self) -> &RestClient {
        &self.rest
    }

    /// The realtime client, for subscriptions and DDP reads.
    #[must_use]
    pub fn realtime(&self) -> &RealtimeClient {
        &self.realtime
    }

    /// Subscribes to every message the bot can see.
    ///
    /// One subscription covering every room, which is what a Discord-style bot wants.
    /// Per-room subscriptions deliver `[message]`; this key delivers
    /// `[message, {roomParticipant, roomType, roomName}]` — an extra trailing element the
    /// per-room form does not have.
    ///
    /// # Errors
    /// Returns [`rocketsocket_realtime::CallError`] if the server refuses the subscription.
    pub async fn watch_all_messages(
        &self,
    ) -> Result<(), rocketsocket_realtime::correlate::CallError> {
        self.realtime.subscribe("room-messages", "__my_messages__").await?;
        Ok(())
    }

    /// Shuts the websocket down.
    pub async fn shutdown(&self) {
        self.realtime.shutdown().await;
    }
}

/// Derives the DDP endpoint from a workspace base URL.
///
/// Rocket.Chat serves DDP at exactly `/websocket`. The path must match: the monolith
/// rewrites only that exact path onto SockJS's raw-websocket transport, and the EE
/// `ddp-streamer` treats anything else as SockJS and wraps every frame.
fn websocket_url(base_url: &str) -> Result<String, BotError> {
    let trimmed = base_url.trim_end_matches('/');

    let ws = if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        trimmed.to_owned()
    } else {
        return Err(BotError::Url(format!("expected an http(s) or ws(s) URL, got {base_url:?}")));
    };

    Ok(format!("{ws}/websocket"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_the_websocket_endpoint_from_a_workspace_url() {
        assert_eq!(
            websocket_url("https://chat.example.com").unwrap(),
            "wss://chat.example.com/websocket"
        );
        assert_eq!(
            websocket_url("http://localhost:3000").unwrap(),
            "ws://localhost:3000/websocket"
        );
    }

    #[test]
    fn a_trailing_slash_does_not_produce_a_double_slash() {
        // The path must be exactly "/websocket": the monolith rewrites only that, and
        // ddp-streamer treats anything else as SockJS and wraps every frame.
        assert_eq!(
            websocket_url("https://chat.example.com/").unwrap(),
            "wss://chat.example.com/websocket"
        );
    }

    #[test]
    fn an_already_websocket_url_is_accepted() {
        assert_eq!(
            websocket_url("wss://chat.example.com").unwrap(),
            "wss://chat.example.com/websocket"
        );
    }

    #[test]
    fn a_schemeless_url_is_rejected_rather_than_guessed() {
        assert!(websocket_url("chat.example.com").is_err());
    }
}
