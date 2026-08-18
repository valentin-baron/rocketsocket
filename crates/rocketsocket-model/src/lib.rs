//! Types for the Rocket.Chat realtime (DDP) and REST APIs.
//!
//! This crate is pure data: `serde` definitions, typed ids, and the DDP wire protocol.
//! It performs no IO and pulls in no async runtime, so a program that only needs to
//! deserialize a Rocket.Chat payload does not compile a WebSocket stack.
//!
//! # Forward compatibility
//!
//! Rocket.Chat adds fields and enum variants in minor releases, and its own published
//! documentation lags the server by years. Every type here is therefore built to survive
//! payloads it has never seen:
//!
//! - no `deny_unknown_fields` — unknown fields are ignored
//! - every wire enum has an `Unknown` variant that round-trips losslessly
//! - every field that a server projection may omit is `Option` with `#[serde(default)]`
//! - unrecognised protocol frames decode into [`protocol::ServerMessage::Unknown`] rather
//!   than failing
//!
//! Deserialization failing is treated as a bug in this crate, not as invalid input.

pub mod datetime;
pub mod entity;
pub mod id;
pub mod protocol;

pub use self::datetime::Timestamp;
pub use self::id::{
    Id, MessageId, RoleId, RoomId, SubscriptionId, UploadId, UserId, marker,
};
