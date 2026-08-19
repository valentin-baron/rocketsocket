//! A realtime client that keeps its subscriptions alive across reconnects.
//!
//! [`Connection`] deliberately does not replay subscriptions — it does not know what you
//! subscribed to. This module supplies the missing half: it owns a [`Registry`] of
//! subscription intent, honours [`Event::Resubscribe`], and hands the application a stream
//! of events with stream frames already routed.
//!
//! Without this, a bot survives a Rocket.Chat restart in the sense that its socket
//! reconnects and its state says `Ready` — while receiving nothing at all, forever.

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, warn};

use rocketsocket_model::event::StreamEvent;
use rocketsocket_model::protocol::ServerMessage;

use crate::connection::{Config, Connection, Event, Events};
use crate::correlate::{CallError, Epoch};
use crate::subscription::{Registry, StreamKey};

/// An event delivered to the application.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ClientEvent {
    /// A Rocket.Chat stream event.
    ///
    /// [`event`](Self::Stream::event) is the decoded form, which is what most code should
    /// match on. Decoding never fails: an unmodelled stream, an unexpected arity or a
    /// payload that does not fit falls through to [`StreamEvent::Unknown`] with the raw
    /// arguments intact, so a server that grows an event cannot break a running bot.
    ///
    /// [`args`](Self::Stream::args) is kept alongside it because the typed layer covers 15
    /// of the 80 declared `(stream, event)` pairs; everything else is only reachable
    /// positionally. It is also the escape hatch when this crate's model lags the server.
    Stream {
        /// Which stream and event key this came from.
        key: StreamKey,
        /// The decoded event.
        event: StreamEvent,
        /// The raw positional payload from `fields.args`, exactly as received.
        args: Vec<Value>,
    },

    /// Every subscription has been re-issued after a reconnect.
    Resubscribed {
        /// The generation the replay ran against.
        epoch: Epoch,
        /// How many subscriptions were restored.
        restored: usize,
    },

    /// A lifecycle or transport event, passed through from the connection.
    Connection(Event),
}

/// A realtime client with durable subscriptions.
#[derive(Debug, Clone)]
pub struct Client {
    connection: Connection,
    registry: Arc<Mutex<Registry>>,
}

/// The application's event stream.
#[derive(Debug)]
pub struct ClientEvents {
    rx: mpsc::Receiver<ClientEvent>,
}

impl ClientEvents {
    /// The next event, or `None` once the connection has terminally failed or shut down.
    pub async fn recv(&mut self) -> Option<ClientEvent> {
        self.rx.recv().await
    }
}

impl Client {
    /// Starts a client and its supervisor.
    ///
    /// The supervisor task consumes the connection's events, replays subscriptions when
    /// asked, and forwards everything to the returned stream.
    #[must_use]
    pub fn spawn(config: Config) -> (Self, ClientEvents) {
        let capacity = config.event_capacity.max(1);
        let (connection, events) = Connection::spawn(config);
        let registry = Arc::new(Mutex::new(Registry::new()));
        let (tx, rx) = mpsc::channel(capacity);

        let client = Self { connection: connection.clone(), registry: Arc::clone(&registry) };
        tokio::spawn(supervise(connection, registry, events, tx));

        (client, ClientEvents { rx })
    }

    /// Calls a DDP method.
    ///
    /// Reserve this for the reads that have no REST equivalent — the `updatedSince` delta
    /// family (`rooms/get`, `subscriptions/get`, `permissions/get`) and the cursor-paginated
    /// `messages/get`. Anything that mutates the server belongs on REST: Rocket.Chat has
    /// deprecated ~116 DDP methods for removal in 9.0, and the surviving `sendMessage`
    /// cannot carry attachments or blocks at all.
    ///
    /// # Errors
    /// Returns [`CallError`] if the server rejects the call or the connection ends first.
    pub async fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, CallError> {
        self.connection.call(method, params).await
    }

    /// Subscribes to a Rocket.Chat stream, and remembers it across reconnects.
    ///
    /// `stream` omits the `stream-` prefix (`room-messages`, `notify-user`, …) and `event`
    /// is the key within it (a room id, `__my_messages__`, `<uid>/message`, …).
    ///
    /// # Errors
    /// Returns [`CallError`] if the server refuses the subscription or the connection ends.
    pub async fn subscribe(
        &self,
        stream: impl Into<String>,
        event: impl Into<String>,
    ) -> Result<StreamKey, CallError> {
        let key = StreamKey::new(stream, event);
        // Rocket.Chat's own client sends the modern object form; the legacy trailing
        // `false` is equivalent. `args` inside it is authorisation data for `allowRead`,
        // not a filter, and only Omnichannel uses it.
        let params = vec![
            Value::String(key.event.clone()),
            serde_json::json!({ "useCollection": false, "args": [] }),
        ];

        let id = self.connection.subscribe(&key.publication(), params.clone()).await?;

        // Recorded only once the server acknowledged it. Recording earlier would replay a
        // subscription the server refused, on every reconnect, forever.
        self.registry.lock().await.insert(id, key.clone(), params);
        Ok(key)
    }

    /// How many subscriptions are currently remembered.
    pub async fn subscription_count(&self) -> usize {
        self.registry.lock().await.len()
    }

    /// Shuts the connection down.
    pub async fn shutdown(&self) {
        self.connection.shutdown().await;
    }
}

/// Consumes connection events, replays subscriptions, and forwards to the application.
async fn supervise(
    connection: Connection,
    registry: Arc<Mutex<Registry>>,
    mut events: Events,
    tx: mpsc::Sender<ClientEvent>,
) {
    while let Some(event) = events.recv().await {
        match event {
            Event::Disconnected { .. } => {
                registry.lock().await.connection_lost();
                if tx.send(ClientEvent::Connection(event)).await.is_err() {
                    return;
                }
            }

            Event::Resubscribe { epoch } => {
                let restored = replay(&connection, &registry, epoch).await;
                if tx.send(ClientEvent::Resubscribed { epoch, restored }).await.is_err() {
                    return;
                }
            }

            Event::Frame(ref frame) => {
                if let Some(stream) = frame.as_stream_event() {
                    let key = StreamKey::new(stream.stream, stream.event_name);
                    let decoded =
                        StreamEvent::decode(stream.stream, stream.event_name, stream.args);
                    let event =
                        ClientEvent::Stream { key, event: decoded, args: stream.args.to_vec() };
                    if tx.send(event).await.is_err() {
                        return;
                    }
                } else if tx.send(ClientEvent::Connection(event)).await.is_err() {
                    return;
                }
            }

            other => {
                let terminal = matches!(other, Event::Fatal(_));
                if tx.send(ClientEvent::Connection(other)).await.is_err() {
                    return;
                }
                if terminal {
                    return;
                }
            }
        }
    }
}

/// Re-issues every remembered subscription under a fresh id.
async fn replay(connection: &Connection, registry: &Arc<Mutex<Registry>>, epoch: Epoch) -> usize {
    let pending = registry.lock().await.to_replay();
    let mut restored = 0;

    for (old_id, key, params) in pending {
        // A fresh id is mandatory: `sub` is idempotent by id, and one the server already
        // knows is dropped without `ready` or `nosub`, hanging the caller.
        match connection.subscribe(&key.publication(), params).await {
            Ok(new_id) => {
                registry.lock().await.rekey(&old_id, new_id);
                restored += 1;
            }
            Err(error) => {
                // A refused subscription is forgotten rather than retried on every
                // reconnect for the lifetime of the process.
                warn!(?key, %error, "dropping a subscription the server would not restore");
                registry.lock().await.remove(&old_id);
            }
        }
    }

    debug!(?epoch, restored, "replayed subscriptions");
    restored
}

/// Convenience: the frame is a stream event for this key.
#[must_use]
pub fn stream_key_of(frame: &ServerMessage) -> Option<StreamKey> {
    frame.as_stream_event().map(|event| StreamKey::new(event.stream, event.event_name))
}
