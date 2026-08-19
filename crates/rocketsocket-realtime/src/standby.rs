//! Waiting inline for a future event.
//!
//! A stream of events is the right primary API, but it makes one very common thing
//! awkward: "post a question, then wait for *that user* to answer in *that room*".
//! Written against a bare stream, every such flow has to hand control back to the main
//! loop and reassemble its state when the reply arrives.
//!
//! [`Standby`] is twilight's answer, and it is small: register a predicate, get a future,
//! and let the main loop keep driving. Nothing here polls or owns the stream — the
//! application calls [`Standby::process`] for each event it receives, which keeps the
//! ownership story honest and means `Standby` works with any dispatch style.

use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::client::ClientEvent;

type Predicate = Box<dyn Fn(&ClientEvent) -> bool + Send>;

struct Waiter {
    predicate: Predicate,
    sender: oneshot::Sender<ClientEvent>,
}

/// Why a wait ended without an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Canceled {
    /// The [`Standby`] was dropped, or the connection ended, before anything matched.
    #[error("no matching event arrived before the stream ended")]
    Dropped,
}

/// Registry of predicates waiting for a matching event.
#[derive(Default)]
pub struct Standby {
    // A std Mutex, deliberately: every critical section is a predicate call and a Vec
    // operation, with no await inside, so an async mutex would only add overhead.
    waiters: Mutex<Vec<Waiter>>,
}

impl std::fmt::Debug for Standby {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pending = self.waiters.lock().map_or(0, |waiters| waiters.len());
        f.debug_struct("Standby").field("pending", &pending).finish()
    }
}

impl Standby {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many waiters are outstanding.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.waiters.lock().map_or(0, |waiters| waiters.len())
    }

    /// Waits for the next event matching `predicate`.
    ///
    /// The returned future resolves when [`process`](Self::process) sees a match, or
    /// errors with [`Canceled`] if this `Standby` is dropped first. It never blocks the
    /// event loop: the predicate runs inside `process`, on whatever task drives it.
    ///
    /// Predicates run in registration order, and **only the first match wins** — an event
    /// is handed to exactly one waiter, so two flows waiting on overlapping predicates do
    /// not both consume it.
    pub fn wait_for(
        &self,
        predicate: impl Fn(&ClientEvent) -> bool + Send + 'static,
    ) -> impl Future<Output = Result<ClientEvent, Canceled>> + 'static {
        let (sender, receiver) = oneshot::channel();

        if let Ok(mut waiters) = self.waiters.lock() {
            waiters.push(Waiter { predicate: Box::new(predicate), sender });
        }

        async move { receiver.await.map_err(|_| Canceled::Dropped) }
    }

    /// Offers an event to the waiters.
    ///
    /// Returns whether it matched one. Call this for every event the application receives;
    /// an event nothing is waiting for costs one predicate call per waiter.
    pub fn process(&self, event: &ClientEvent) -> bool {
        let Ok(mut waiters) = self.waiters.lock() else {
            return false;
        };

        // Reap waiters whose caller went away, so an abandoned wait does not cost a
        // predicate call on every future event.
        waiters.retain(|waiter| !waiter.sender.is_closed());

        let Some(index) = waiters.iter().position(|waiter| (waiter.predicate)(event)) else {
            return false;
        };

        // `remove` rather than `swap_remove`: registration order is part of the contract,
        // and reordering would make which waiter wins depend on unrelated traffic.
        let waiter = waiters.remove(index);
        drop(waiters);

        waiter.sender.send(event.clone()).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::StreamKey;

    fn stream_event(stream: &str, event: &str) -> ClientEvent {
        ClientEvent::Stream {
            key: StreamKey::new(stream, event),
            event: rocketsocket_model::event::StreamEvent::decode(stream, event, &[]),
            args: Vec::new(),
        }
    }

    #[tokio::test]
    async fn a_matching_event_resolves_the_waiter() {
        let standby = Standby::new();
        let waiting = standby.wait_for(
            |event| matches!(event, ClientEvent::Stream { key, .. } if key.event == "GENERAL"),
        );

        assert!(standby.process(&stream_event("room-messages", "GENERAL")));
        let event = waiting.await.expect("should have resolved");
        assert!(matches!(event, ClientEvent::Stream { key, .. } if key.event == "GENERAL"));
        assert_eq!(standby.pending(), 0);
    }

    #[tokio::test]
    async fn a_non_matching_event_leaves_the_waiter_pending() {
        let standby = Standby::new();
        let _waiting = standby.wait_for(
            |event| matches!(event, ClientEvent::Stream { key, .. } if key.event == "GENERAL"),
        );

        assert!(!standby.process(&stream_event("room-messages", "random")));
        assert_eq!(standby.pending(), 1);
    }

    #[tokio::test]
    async fn only_the_first_matching_waiter_consumes_an_event() {
        // Two flows waiting on overlapping predicates must not both consume one event.
        let standby = Standby::new();
        let first = standby.wait_for(|_| true);
        let _second = standby.wait_for(|_| true);

        assert!(standby.process(&stream_event("room-messages", "GENERAL")));
        assert_eq!(standby.pending(), 1, "the second waiter must still be waiting");
        first.await.expect("the first waiter should have won");
    }

    #[tokio::test]
    async fn an_abandoned_waiter_is_reaped_rather_than_scanned_forever() {
        let standby = Standby::new();
        drop(standby.wait_for(|_| false));
        assert_eq!(standby.pending(), 1);

        // The first event after the caller went away clears it.
        standby.process(&stream_event("room-messages", "GENERAL"));
        assert_eq!(standby.pending(), 0);
    }

    #[tokio::test]
    async fn dropping_the_registry_cancels_outstanding_waiters() {
        let standby = Standby::new();
        let waiting = standby.wait_for(|_| false);
        drop(standby);
        assert_eq!(waiting.await.unwrap_err(), Canceled::Dropped);
    }
}
