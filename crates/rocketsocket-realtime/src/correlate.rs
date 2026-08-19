//! Correlating method calls and subscriptions with their replies.
//!
//! DDP multiplexes every call over one socket and matches replies by a client-chosen id,
//! so the client owns the pending-call table. Two rules dominate the design, both learned
//! from failures in the reference implementations:
//!
//! 1. **A reply can only arrive on the connection its request went out on.** So every event
//!    that ends a connection must settle every waiter on it — a caller left awaiting a
//!    reply that can never come is a hang, not an error.
//! 2. **A late reply from a superseded socket must not settle a new call.** Rust's
//!    ownership does not help here: timers and in-flight futures outlive the socket. Every
//!    entry is therefore tagged with a connection epoch.

use std::collections::HashMap;

use tokio::sync::oneshot;

use rocketsocket_model::protocol::DdpError;

/// Monotonic connection generation. Bumped on every (re)connect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Epoch(u64);

impl Epoch {
    /// The next generation.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

/// Why a call will never be answered.
///
/// Not `Eq`: a server error can carry arbitrary JSON `details`, which is only `PartialEq`.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum CallError {
    /// The server answered with an error.
    #[error(transparent)]
    Server(#[from] DdpError),

    /// The connection ended before the frame reached the wire.
    ///
    /// Safe to retry: the server never saw it.
    #[error("connection closed before the call was sent")]
    NotSent,

    /// The frame was written, but the connection ended before a reply arrived.
    ///
    /// **Not** safe to blindly retry — the server may have executed it. This is exactly the
    /// distinction that decides whether re-sending a message posts it twice.
    #[error("connection closed after the call was sent; it may have been executed")]
    Abandoned {
        /// The call id, so a caller can reconcile against server state.
        id: String,
    },
}

/// The pending-call table for one client.
#[derive(Debug)]
pub struct Correlator {
    epoch: Epoch,
    next_id: u64,
    prefix: &'static str,
    pending: HashMap<String, Pending>,
}

#[derive(Debug)]
struct Pending {
    epoch: Epoch,
    sent: bool,
    reply: oneshot::Sender<Result<serde_json::Value, CallError>>,
}

/// What [`Correlator::resolve`] did with a reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Resolution {
    /// Handed to the waiting caller.
    Delivered,
    /// No such call is pending — a duplicate reply, or one for a call from a dead
    /// connection. Log and carry on; never tear the connection down for this.
    Unknown,
    /// The call belonged to a superseded connection, so the reply was dropped.
    StaleEpoch,
    /// The caller stopped waiting before the reply arrived.
    CallerGone,
}

impl Correlator {
    /// A correlator whose ids carry `prefix`.
    #[must_use]
    pub fn new(prefix: &'static str) -> Self {
        Self { epoch: Epoch::default(), next_id: 0, prefix, pending: HashMap::new() }
    }

    /// The current connection generation.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// How many calls are outstanding.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Registers a call, returning its id and the receiver for its reply.
    ///
    /// Ids are namespaced per correlator so a method id can never collide with a
    /// subscription id, which would otherwise let a `nosub` settle a `sub`'s waiter.
    pub fn register(
        &mut self,
    ) -> (String, oneshot::Receiver<Result<serde_json::Value, CallError>>) {
        let id = format!("{}{}", self.prefix, self.next_id);
        self.next_id += 1;

        let (reply, receiver) = oneshot::channel();
        self.pending.insert(id.clone(), Pending { epoch: self.epoch, sent: false, reply });
        (id, receiver)
    }

    /// Marks a registered call as written to the socket.
    ///
    /// Drives the [`CallError::NotSent`] / [`CallError::Abandoned`] distinction, so it must
    /// be called *after* the write succeeds, never before.
    pub fn mark_sent(&mut self, id: &str) {
        if let Some(pending) = self.pending.get_mut(id) {
            pending.sent = true;
        }
    }

    /// Delivers a reply to its caller.
    pub fn resolve(
        &mut self,
        id: &str,
        outcome: Result<serde_json::Value, DdpError>,
    ) -> Resolution {
        let Some(pending) = self.pending.remove(id) else {
            return Resolution::Unknown;
        };

        if pending.epoch != self.epoch {
            return Resolution::StaleEpoch;
        }

        match pending.reply.send(outcome.map_err(CallError::Server)) {
            Ok(()) => Resolution::Delivered,
            Err(_) => Resolution::CallerGone,
        }
    }

    /// Ends the current connection: settles every waiter and starts a new epoch.
    ///
    /// Returns how many calls were abandoned. Nothing may be left pending — a caller
    /// awaiting a reply that can never arrive hangs forever, which is worse than any error
    /// it could be handed instead.
    pub fn disconnect(&mut self) -> usize {
        let abandoned = std::mem::take(&mut self.pending);
        let count = abandoned.len();

        for (id, pending) in abandoned {
            let error = if pending.sent { CallError::Abandoned { id } } else { CallError::NotSent };
            // The receiver may already be gone; that is fine, the caller stopped caring.
            let _ = pending.reply.send(Err(error));
        }

        self.epoch = self.epoch.next();
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_reply_reaches_its_caller() {
        let mut correlator = Correlator::new("m");
        let (id, receiver) = correlator.register();
        correlator.mark_sent(&id);

        assert_eq!(correlator.resolve(&id, Ok(json!({"ok": true}))), Resolution::Delivered);
        assert_eq!(receiver.blocking_recv().unwrap().unwrap(), json!({"ok": true}));
        assert_eq!(correlator.pending(), 0);
    }

    #[test]
    fn ids_are_unique_and_prefixed_so_calls_and_subs_cannot_collide() {
        let mut methods = Correlator::new("m");
        let mut subs = Correlator::new("s");

        let (first, _a) = methods.register();
        let (second, _b) = methods.register();
        let (sub, _c) = subs.register();

        assert_ne!(first, second);
        assert!(first.starts_with('m') && sub.starts_with('s'));
        // A nosub answering a sub must never be able to settle a method's waiter.
        assert_ne!(first, sub);
    }

    #[test]
    fn an_unknown_reply_is_reported_but_never_fatal() {
        // siderite tears the whole connection down here; a stray frame must not cost us the
        // socket.
        let mut correlator = Correlator::new("m");
        assert_eq!(correlator.resolve("nope", Ok(json!(1))), Resolution::Unknown);
    }

    #[test]
    fn a_duplicate_reply_is_unknown_rather_than_a_panic() {
        let mut correlator = Correlator::new("m");
        let (id, _receiver) = correlator.register();
        assert_eq!(correlator.resolve(&id, Ok(json!(1))), Resolution::Delivered);
        assert_eq!(correlator.resolve(&id, Ok(json!(1))), Resolution::Unknown);
    }

    #[test]
    fn disconnect_distinguishes_sent_from_unsent_calls() {
        // This decides whether retrying re-posts a message. It has to be right.
        let mut correlator = Correlator::new("m");
        let (sent_id, sent_rx) = correlator.register();
        let (_unsent_id, unsent_rx) = correlator.register();
        correlator.mark_sent(&sent_id);

        assert_eq!(correlator.disconnect(), 2);

        match sent_rx.blocking_recv().unwrap().unwrap_err() {
            CallError::Abandoned { id } => assert_eq!(id, sent_id),
            other => panic!("expected Abandoned, got {other:?}"),
        }
        assert_eq!(unsent_rx.blocking_recv().unwrap().unwrap_err(), CallError::NotSent);
    }

    #[test]
    fn nothing_is_left_pending_after_a_disconnect() {
        let mut correlator = Correlator::new("m");
        let (_id, _rx) = correlator.register();
        correlator.disconnect();
        assert_eq!(correlator.pending(), 0, "a waiter left pending hangs its caller forever");
    }

    #[test]
    fn a_late_reply_from_a_superseded_socket_cannot_settle_a_new_call() {
        let mut correlator = Correlator::new("m");
        let (id, _dead_rx) = correlator.register();
        correlator.mark_sent(&id);
        correlator.disconnect();

        // The new connection happens to hand out the same id shape; the epoch is what saves
        // us. Re-register so an entry exists under a *new* epoch.
        let (new_id, new_rx) = correlator.register();
        assert_eq!(correlator.resolve(&new_id, Ok(json!("fresh"))), Resolution::Delivered);
        assert_eq!(new_rx.blocking_recv().unwrap().unwrap(), json!("fresh"));
    }

    #[test]
    fn a_stale_entry_is_dropped_rather_than_delivered() {
        let mut correlator = Correlator::new("m");
        let (id, _rx) = correlator.register();

        // Simulate a reconnect that did not go through disconnect() — the entry survives
        // with the old epoch and must not be delivered against the new connection.
        correlator.epoch = correlator.epoch.next();
        assert_eq!(correlator.resolve(&id, Ok(json!(1))), Resolution::StaleEpoch);
    }

    #[test]
    fn a_caller_that_stopped_waiting_is_reported_not_panicked_on() {
        let mut correlator = Correlator::new("m");
        let (id, receiver) = correlator.register();
        drop(receiver);
        assert_eq!(correlator.resolve(&id, Ok(json!(1))), Resolution::CallerGone);
    }

    #[test]
    fn a_server_error_reaches_the_caller_as_such() {
        let mut correlator = Correlator::new("m");
        let (id, receiver) = correlator.register();
        let error: DdpError =
            serde_json::from_str(r#"{"error":"error-not-allowed","reason":"Not allowed"}"#)
                .unwrap();

        assert_eq!(correlator.resolve(&id, Err(error.clone())), Resolution::Delivered);
        assert_eq!(receiver.blocking_recv().unwrap().unwrap_err(), CallError::Server(error));
    }
}
