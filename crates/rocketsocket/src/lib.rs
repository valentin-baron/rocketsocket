//! A Rust framework for Rocket.Chat bots.
//!
//! # The shape of the thing
//!
//! Rocket.Chat needs two transports, and this crate uses each for what it is good at:
//!
//! - the **websocket** (DDP) receives — `stream-*` subscriptions have no REST equivalent
//!   and never will, so deletions, typing, presence and read-state are only observable
//!   there;
//! - **REST** acts — Rocket.Chat has deprecated ~116 DDP methods for removal in 9.0, and
//!   the surviving DDP `sendMessage` cannot carry attachments or blocks at all.
//!
//! One credential drives both. `POST /api/v1/login` is a wrapper around the DDP `login`
//! method and both read the same token array, so the token it returns is simultaneously a
//! DDP resume token. That matters beyond convenience: the EE `ddp-streamer` accepts *only*
//! `{resume}` on its websocket login, so this is the only path that works on both the
//! monolith and a microservices deployment.
//!
//! # Getting started
//!
//! Use a **Personal Access Token**. They never expire, they bypass 2FA, and they avoid the
//! 50-token cap a bot would otherwise churn through by re-authenticating on every
//! reconnect. Give the bot account the **`bot` role** — it is not cosmetic: it grants
//! `api-bypass-rate-limit`, without which the default limiter allows 10 requests per
//! minute per route.
//!
//! ```no_run
//! use rocketsocket::{Bot, Credentials};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let (bot, mut events) = Bot::connect(
//!     "https://chat.example.com",
//!     Credentials::personal_access_token("<user id>", "<token>"),
//! )
//! .await?;
//!
//! while let Some(event) = events.recv().await {
//!     println!("{event:?}");
//! }
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

pub use rocketsocket_model as model;

#[cfg(feature = "realtime")]
pub use rocketsocket_realtime as realtime;

#[cfg(feature = "rest")]
pub use rocketsocket_rest as rest;

#[cfg(all(feature = "realtime", feature = "rest"))]
mod bot;

#[cfg(all(feature = "realtime", feature = "rest"))]
pub mod framework;

#[cfg(all(feature = "realtime", feature = "rest"))]
pub use self::bot::{Bot, BotError};

/// Declares an event handler. See [`framework`] for the runtime it expands against.
#[cfg(all(feature = "macros", feature = "realtime", feature = "rest"))]
pub use rocketsocket_macros::event;

#[cfg(feature = "rest")]
pub use rocketsocket_rest::auth::Credentials;

/// The types most bots need.
pub mod prelude {
    pub use rocketsocket_model::entity::{Message, Room, Subscription, User};
    pub use rocketsocket_model::id::{MessageId, RoomId, UserId};

    #[cfg(all(feature = "realtime", feature = "rest"))]
    pub use crate::Bot;
    #[cfg(feature = "rest")]
    pub use crate::Credentials;
    #[cfg(feature = "realtime")]
    pub use rocketsocket_realtime::client::{ClientEvent, ClientEvents};
}
