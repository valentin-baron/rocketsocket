//! Rocket.Chat realtime API — DDP over WebSocket.
//!
//! This crate owns the websocket half of a Rocket.Chat client: the handshake, liveness,
//! subscriptions, and the reconnect loop. Actions that mutate the server belong on REST,
//! for reasons recorded in the repository's `PLAN.md` §1.

pub mod backoff;
pub mod connection;
pub mod correlate;
pub mod liveness;
pub mod session;
pub mod subscription;

pub use self::backoff::Backoff;
pub use self::correlate::{CallError, Correlator, Epoch, Resolution};
pub use self::liveness::{Liveness, LivenessPolicy};
pub use self::session::{Action, Fatal, Phase, Session};
pub use self::subscription::{Registry, StreamKey, SubState};
