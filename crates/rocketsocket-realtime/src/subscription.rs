//! The subscription registry and stream-event routing.
//!
//! Two Rocket.Chat quirks shape everything here.
//!
//! **Subscriptions do not survive a reconnect.** `Session::close()` deactivates them
//! without sending `nosub`, and DDP session resume is unimplemented in every Rocket.Chat
//! release, so the server remembers nothing. A client that does not replay its
//! subscriptions after logging back in goes permanently silent while still looking
//! connected — the worst failure mode a bot can have. The registry therefore remembers
//! every subscription as *intent*, separate from whether it is currently live.
//!
//! **Events are demultiplexed by `eventName`, not by subscription id.** Rocket.Chat's
//! `stream-*` publications bypass Meteor's mergebox and push every event as a `changed`
//! frame on a pseudo-collection named after the stream, all sharing the constant document
//! id `"id"`. Routing on the document id would deliver every room's messages to every
//! subscriber; routing on the subscription id is impossible, because the frame does not
//! carry one.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use serde_json::Value;

/// What a subscription is for: a stream name plus the event key within it.
///
/// The key is the second half of Rocket.Chat's addressing scheme — a room id for
/// `stream-room-messages`, `<uid>/<event>` for `stream-notify-user`, and so on.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamKey {
    /// Stream name without the `stream-` prefix, e.g. `room-messages`.
    pub stream: String,
    /// The event key, e.g. `GENERAL` or `__my_messages__`.
    pub event: String,
}

impl StreamKey {
    /// Builds a key.
    pub fn new(stream: impl Into<String>, event: impl Into<String>) -> Self {
        Self { stream: stream.into(), event: event.into() }
    }

    /// The DDP publication name, i.e. the key's stream with the `stream-` prefix.
    #[must_use]
    pub fn publication(&self) -> String {
        format!("stream-{}", self.stream)
    }
}

/// Whether a remembered subscription is currently established on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubState {
    /// A `sub` has been sent; no `ready` or `nosub` yet.
    Pending,
    /// The server acknowledged it with `ready`.
    Ready,
    /// Remembered, but not currently established — before the first send, or after a
    /// disconnect dropped it.
    Inactive,
}

/// One remembered subscription.
#[derive(Debug, Clone)]
struct Entry_ {
    key: StreamKey,
    params: Vec<Value>,
    state: SubState,
}

/// Remembers subscription intent and routes incoming stream events.
#[derive(Debug, Default)]
pub struct Registry {
    /// Keyed by DDP subscription id.
    by_id: HashMap<String, Entry_>,
    /// Reverse index for routing, since events name a stream and event, never an id.
    by_key: HashMap<StreamKey, Vec<String>>,
}

impl Registry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many subscriptions are remembered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether nothing is remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Remembers a subscription under `id`, in state [`SubState::Pending`].
    ///
    /// Ids must be fresh. A `sub` re-using an id the server already holds is **silently
    /// dropped** — no `ready`, no `nosub` — so a caller that recycles ids waits forever on
    /// an acknowledgement that will never come.
    pub fn insert(&mut self, id: impl Into<String>, key: StreamKey, params: Vec<Value>) {
        let id = id.into();
        self.by_key.entry(key.clone()).or_default().push(id.clone());
        self.by_id.insert(id, Entry_ { key, params, state: SubState::Pending });
    }

    /// The state of one subscription.
    #[must_use]
    pub fn state(&self, id: &str) -> Option<SubState> {
        self.by_id.get(id).map(|entry| entry.state)
    }

    /// Marks a subscription acknowledged.
    ///
    /// `ready.subs` is an array and the server does batch it — after `setUserId` reruns
    /// every subscription, several acknowledgements arrive in one frame. Callers must
    /// iterate the array; reading only `subs[0]` (as `Rocket.Chat.js.SDK` does) leaves the
    /// rest wrongly pending.
    pub fn mark_ready(&mut self, id: &str) -> bool {
        match self.by_id.get_mut(id) {
            Some(entry) => {
                entry.state = SubState::Ready;
                true
            }
            None => false,
        }
    }

    /// Forgets a subscription entirely, e.g. after the server refused it with `nosub`.
    ///
    /// Refused subscriptions must be forgotten rather than left to be replayed: a stream
    /// the server will not grant is retried on every reconnect forever otherwise. This is
    /// the same fix the JS SDK landed for its own resubscribe loop.
    pub fn remove(&mut self, id: &str) -> bool {
        let Some(entry) = self.by_id.remove(id) else {
            return false;
        };
        if let Entry::Occupied(mut occupied) = self.by_key.entry(entry.key) {
            occupied.get_mut().retain(|candidate| candidate != id);
            if occupied.get().is_empty() {
                occupied.remove();
            }
        }
        true
    }

    /// Marks every subscription inactive, because the connection ended.
    ///
    /// Intent is deliberately kept: this is what a reconnect replays.
    pub fn connection_lost(&mut self) {
        for entry in self.by_id.values_mut() {
            entry.state = SubState::Inactive;
        }
    }

    /// Everything to re-send after logging back in, as `(id, key, params)`.
    ///
    /// The returned id is the *old* one, and is only useful for calling
    /// [`rekey`](Self::rekey) once the replay has been issued. The replay itself must
    /// allocate a **fresh** id: a `sub` re-using an id the client still has outstanding is
    /// silently dropped by the server — no `ready`, no `nosub` — so recycling ids would
    /// leave the caller waiting on an acknowledgement that never comes.
    #[must_use]
    pub fn to_replay(&self) -> Vec<(String, StreamKey, Vec<Value>)> {
        let mut replay: Vec<_> = self
            .by_id
            .iter()
            .map(|(id, entry)| (id.clone(), entry.key.clone(), entry.params.clone()))
            .collect();
        // Deterministic order keeps reconnect behaviour reproducible in tests and logs.
        replay.sort_by(|a, b| a.0.cmp(&b.0));
        replay
    }

    /// Marks every remembered subscription as sent again, after a replay.
    pub fn mark_replayed(&mut self) {
        for entry in self.by_id.values_mut() {
            entry.state = SubState::Pending;
        }
    }

    /// Moves a remembered subscription onto the id its replay was issued under.
    ///
    /// Returns whether `old` was known. The entry keeps its stream key and params, so
    /// routing is unaffected; only the wire id changes.
    pub fn rekey(&mut self, old: &str, new: impl Into<String>) -> bool {
        let Some(mut entry) = self.by_id.remove(old) else {
            return false;
        };
        let new = new.into();

        if let Entry::Occupied(mut occupied) = self.by_key.entry(entry.key.clone()) {
            for id in occupied.get_mut() {
                if id == old {
                    id.clone_from(&new);
                }
            }
        }

        entry.state = SubState::Pending;
        self.by_id.insert(new, entry);
        true
    }

    /// The subscription ids interested in an incoming event.
    ///
    /// Routing is on `(collection, fields.eventName)` because that is all the frame
    /// carries. Two subscriptions to the same stream and key both match, and Rocket.Chat
    /// does not de-duplicate them.
    #[must_use]
    pub fn subscribers(&self, key: &StreamKey) -> &[String] {
        self.by_key.get(key).map_or(&[], Vec::as_slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(stream: &str, event: &str) -> StreamKey {
        StreamKey::new(stream, event)
    }

    #[test]
    fn a_key_renders_its_publication_name() {
        assert_eq!(key("room-messages", "GENERAL").publication(), "stream-room-messages");
        assert_eq!(key("notify-user", "u1/message").publication(), "stream-notify-user");
    }

    #[test]
    fn a_subscription_walks_pending_to_ready() {
        let mut registry = Registry::new();
        registry.insert("s0", key("room-messages", "GENERAL"), vec![json!("GENERAL")]);

        assert_eq!(registry.state("s0"), Some(SubState::Pending));
        assert!(registry.mark_ready("s0"));
        assert_eq!(registry.state("s0"), Some(SubState::Ready));
    }

    #[test]
    fn marking_an_unknown_id_ready_is_reported_not_fatal() {
        let mut registry = Registry::new();
        assert!(!registry.mark_ready("nope"));
    }

    #[test]
    fn events_route_by_stream_and_event_name() {
        let mut registry = Registry::new();
        registry.insert("s0", key("room-messages", "GENERAL"), vec![]);
        registry.insert("s1", key("room-messages", "random"), vec![]);

        assert_eq!(registry.subscribers(&key("room-messages", "GENERAL")), ["s0"]);
        assert_eq!(registry.subscribers(&key("room-messages", "random")), ["s1"]);
        assert!(registry.subscribers(&key("room-messages", "other")).is_empty());
    }

    #[test]
    fn two_subscriptions_to_the_same_key_both_receive() {
        // Every subscription to one stream shares a collection and the document id "id",
        // so the frame cannot distinguish them and the server does not de-duplicate.
        let mut registry = Registry::new();
        registry.insert("s0", key("notify-room", "GENERAL/deleteMessage"), vec![]);
        registry.insert("s1", key("notify-room", "GENERAL/deleteMessage"), vec![]);

        assert_eq!(
            registry.subscribers(&key("notify-room", "GENERAL/deleteMessage")),
            ["s0", "s1"]
        );
    }

    #[test]
    fn a_disconnect_keeps_intent_but_drops_liveness() {
        let mut registry = Registry::new();
        registry.insert("s0", key("room-messages", "__my_messages__"), vec![json!("x")]);
        registry.mark_ready("s0");

        registry.connection_lost();

        assert_eq!(registry.state("s0"), Some(SubState::Inactive));
        assert_eq!(registry.len(), 1, "intent must survive so the reconnect can replay it");
    }

    #[test]
    fn replay_reproduces_every_subscription_with_its_original_id_and_params() {
        let mut registry = Registry::new();
        registry.insert(
            "s0",
            key("room-messages", "GENERAL"),
            vec![json!("GENERAL"), json!(false)],
        );
        registry.insert("s1", key("notify-user", "u1/message"), vec![json!("u1/message")]);
        registry.mark_ready("s0");
        registry.connection_lost();

        let replay = registry.to_replay();
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].0, "s0");
        assert_eq!(replay[0].1, key("room-messages", "GENERAL"));
        assert_eq!(replay[0].2, vec![json!("GENERAL"), json!(false)]);
        assert_eq!(replay[1].0, "s1");

        registry.mark_replayed();
        assert_eq!(registry.state("s0"), Some(SubState::Pending));
        assert_eq!(registry.state("s1"), Some(SubState::Pending));
    }

    #[test]
    fn a_refused_subscription_is_forgotten_so_it_is_not_retried_forever() {
        let mut registry = Registry::new();
        registry.insert("s0", key("room-messages", "secret"), vec![]);

        assert!(registry.remove("s0"));
        assert_eq!(registry.state("s0"), None);
        assert!(registry.to_replay().is_empty(), "a stream the server refuses must not replay");
        assert!(registry.subscribers(&key("room-messages", "secret")).is_empty());
    }

    #[test]
    fn removing_one_of_two_subscribers_leaves_the_other_routable() {
        let mut registry = Registry::new();
        registry.insert("s0", key("notify-room", "GENERAL/typing"), vec![]);
        registry.insert("s1", key("notify-room", "GENERAL/typing"), vec![]);

        registry.remove("s0");
        assert_eq!(registry.subscribers(&key("notify-room", "GENERAL/typing")), ["s1"]);
    }

    #[test]
    fn removing_an_unknown_id_is_reported_not_fatal() {
        let mut registry = Registry::new();
        assert!(!registry.remove("nope"));
    }

    #[test]
    fn an_empty_registry_replays_nothing() {
        let registry = Registry::new();
        assert!(registry.is_empty());
        assert!(registry.to_replay().is_empty());
    }

    #[test]
    fn rekey_moves_a_subscription_onto_its_replay_id_without_disturbing_routing() {
        // A replay must use a fresh id -- the server silently drops a `sub` re-using an
        // outstanding one -- so the registry has to follow the id, not fix it.
        let mut registry = Registry::new();
        registry.insert("s0", key("room-messages", "GENERAL"), vec![json!("GENERAL")]);
        registry.mark_ready("s0");
        registry.connection_lost();

        assert!(registry.rekey("s0", "s7"));

        assert_eq!(registry.state("s0"), None);
        assert_eq!(registry.state("s7"), Some(SubState::Pending));
        assert_eq!(registry.subscribers(&key("room-messages", "GENERAL")), ["s7"]);
        assert_eq!(registry.to_replay()[0].2, vec![json!("GENERAL")], "params must survive");
    }

    #[test]
    fn rekey_only_moves_the_entry_it_names() {
        let mut registry = Registry::new();
        registry.insert("s0", key("notify-room", "GENERAL/typing"), vec![]);
        registry.insert("s1", key("notify-room", "GENERAL/typing"), vec![]);

        registry.rekey("s0", "s9");

        let mut subscribers = registry.subscribers(&key("notify-room", "GENERAL/typing")).to_vec();
        subscribers.sort();
        assert_eq!(subscribers, ["s1", "s9"]);
    }

    #[test]
    fn rekeying_an_unknown_id_is_reported_not_fatal() {
        let mut registry = Registry::new();
        assert!(!registry.rekey("nope", "s1"));
    }
}
