//! Subscriptions must survive a reconnect.
//!
//! Rocket.Chat tears every subscription down when a socket dies, without sending `nosub`,
//! and no release implements DDP session resume. A client that does not replay therefore
//! reconnects, reports `Ready`, and receives nothing ever again — while looking perfectly
//! healthy. That failure is invisible in unit tests of either half, so it is pinned here
//! against a scripted server.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use rocketsocket_realtime::client::{Client, ClientEvent};
use rocketsocket_realtime::connection::{Config, Credential, Event};

/// Generous, because it only bounds a failure: every wait is on an event, never a duration.
const BOUND: Duration = Duration::from_secs(10);

/// Waits for the replay that follows a reconnect.
///
/// `Resubscribe` is emitted after *every* successful login, including the first — where
/// the registry is still empty and `restored` is legitimately 0. Tests that care about a
/// reconnect must therefore step past the disconnect first, or they assert against the
/// initial login and see nothing restored.
async fn replay_after_reconnect(events: &mut rocketsocket_realtime::client::ClientEvents) -> usize {
    let mut seen_disconnect = false;
    loop {
        match tokio::time::timeout(BOUND, events.recv()).await.expect("timed out") {
            Some(ClientEvent::Connection(Event::Disconnected { .. })) => seen_disconnect = true,
            Some(ClientEvent::Resubscribed { restored, .. }) if seen_disconnect => {
                return restored;
            }
            Some(_) => continue,
            None => panic!("event stream ended before the replay"),
        }
    }
}

struct Peer(WebSocketStream<TcpStream>);

impl Peer {
    async fn recv(&mut self) -> Value {
        loop {
            let message = tokio::time::timeout(BOUND, self.0.next())
                .await
                .expect("peer timed out")
                .expect("stream ended")
                .expect("websocket error");
            if let Message::Text(text) = message {
                return serde_json::from_str(&text).expect("server sent invalid JSON");
            }
        }
    }

    async fn send(&mut self, value: Value) {
        self.0.send(Message::text(value.to_string())).await.expect("send failed");
    }

    /// Drives `connect` → `connected` → `login` → `result`.
    async fn handshake(&mut self) {
        let connect = self.recv().await;
        assert_eq!(connect["msg"], "connect");
        self.send(json!({"msg": "connected", "session": "test-session"})).await;

        let login = self.recv().await;
        assert_eq!(login["msg"], "method");
        assert_eq!(login["method"], "login");
        let id = login["id"].as_str().expect("login id").to_owned();
        self.send(json!({"msg": "result", "id": id, "result": {"id": "u1", "token": "t"}})).await;
    }

    /// Reads one `sub` frame and acknowledges it, returning its id.
    async fn accept_sub(&mut self) -> (String, String) {
        let frame = self.recv().await;
        assert_eq!(frame["msg"], "sub", "expected a subscription, got {frame}");
        let id = frame["id"].as_str().expect("sub id").to_owned();
        let name = frame["name"].as_str().expect("sub name").to_owned();
        self.send(json!({"msg": "ready", "subs": [id]})).await;
        (id, name)
    }
}

async fn accept(listener: &TcpListener) -> Peer {
    let (stream, _) = tokio::time::timeout(BOUND, listener.accept())
        .await
        .expect("no connection arrived")
        .expect("accept failed");
    Peer(tokio_tungstenite::accept_async(stream).await.expect("upgrade failed"))
}

fn config(port: u16) -> Config {
    let mut config = Config::new(
        format!("ws://127.0.0.1:{port}/websocket"),
        Credential::Resume("token".to_owned()),
    );
    config.backoff_base = Duration::from_millis(1);
    config.backoff_cap = Duration::from_millis(5);
    config
}

#[tokio::test]
async fn a_subscription_is_replayed_under_a_fresh_id_after_a_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();

    let (client, mut events) = Client::spawn(config(port));

    // First connection: subscribe to the catch-all every bot wants.
    let mut peer = accept(&listener).await;
    peer.handshake().await;

    let subscriber = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe("room-messages", "__my_messages__").await }
    });

    let (first_id, name) = peer.accept_sub().await;
    assert_eq!(name, "stream-room-messages");
    subscriber.await.expect("subscribe task").expect("subscribe failed");
    assert_eq!(client.subscription_count().await, 1);

    // The server goes away, as it does on every deploy.
    drop(peer);

    // Second connection: the replay must arrive unprompted.
    let mut peer = accept(&listener).await;
    peer.handshake().await;
    let (second_id, name) = peer.accept_sub().await;

    assert_eq!(name, "stream-room-messages", "the replay must target the same stream");
    assert_ne!(
        second_id, first_id,
        "a replay re-using a live id is silently dropped by the server, hanging the caller"
    );

    // And the client says so.
    assert_eq!(replay_after_reconnect(&mut events).await, 1);
    assert_eq!(client.subscription_count().await, 1, "the replay must not duplicate the entry");
}

#[tokio::test]
async fn a_stream_event_reaches_the_application_after_a_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();

    let (client, mut events) = Client::spawn(config(port));

    let mut peer = accept(&listener).await;
    peer.handshake().await;
    let subscriber = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe("room-messages", "GENERAL").await }
    });
    peer.accept_sub().await;
    subscriber.await.expect("task").expect("subscribe");

    drop(peer);

    let mut peer = accept(&listener).await;
    peer.handshake().await;
    peer.accept_sub().await;

    // The whole point: a message sent on the *second* connection still gets through.
    peer.send(json!({
        "msg": "changed",
        "collection": "stream-room-messages",
        "id": "id",
        "fields": {
            "eventName": "GENERAL",
            "args": [{
                "_id": "m1", "rid": "GENERAL", "msg": "still here",
                "ts": {"$date": 1755529012345_i64},
                "u": {"_id": "u2", "username": "alice"},
                "_updatedAt": {"$date": 1755529012345_i64}
            }]
        }
    }))
    .await;

    loop {
        match tokio::time::timeout(BOUND, events.recv()).await.expect("timed out") {
            Some(ClientEvent::Stream { key, args, event }) => {
                assert_eq!(key.stream, "room-messages");
                assert_eq!(key.event, "GENERAL");
                assert_eq!(args[0]["msg"], "still here");
                // The typed layer must decode it too, not just carry the raw args.
                match event {
                    rocketsocket_model::event::StreamEvent::RoomMessage { room, message } => {
                        assert_eq!(room.as_str(), "GENERAL");
                        assert_eq!(message.msg, "still here");
                    }
                    other => panic!("expected a typed RoomMessage, got {other:?}"),
                }
                return;
            }
            Some(_) => continue,
            None => panic!("event stream ended before the message arrived"),
        }
    }
}

#[tokio::test]
async fn a_subscription_the_server_refuses_on_replay_is_forgotten() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();

    let (client, mut events) = Client::spawn(config(port));

    let mut peer = accept(&listener).await;
    peer.handshake().await;
    let subscriber = tokio::spawn({
        let client = client.clone();
        async move { client.subscribe("room-messages", "secret").await }
    });
    peer.accept_sub().await;
    subscriber.await.expect("task").expect("subscribe");

    drop(peer);

    // On the second connection the server refuses it — permissions changed while we were
    // away, say. Retrying it on every reconnect for the life of the process is the bug.
    let mut peer = accept(&listener).await;
    peer.handshake().await;
    let frame = peer.recv().await;
    assert_eq!(frame["msg"], "sub");
    let id = frame["id"].as_str().expect("sub id").to_owned();
    peer.send(json!({
        "msg": "nosub",
        "id": id,
        "error": {"error": "not-allowed", "reason": "Not allowed", "errorType": "Meteor.Error"}
    }))
    .await;

    assert_eq!(
        replay_after_reconnect(&mut events).await,
        0,
        "a refused subscription must not count as restored"
    );

    assert_eq!(
        client.subscription_count().await,
        0,
        "a refused subscription must be forgotten, not retried on every future reconnect"
    );
}

#[tokio::test]
async fn a_terminal_failure_ends_the_application_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();

    let (_client, mut events) = Client::spawn(config(port));

    let mut peer = accept(&listener).await;
    let connect = peer.recv().await;
    assert_eq!(connect["msg"], "connect");
    // Version negotiation failure: reconnecting would propose the same version and fail
    // identically, so the stream must end rather than loop.
    peer.send(json!({"msg": "failed", "version": "2"})).await;

    let mut saw_fatal = false;
    loop {
        match tokio::time::timeout(BOUND, events.recv()).await.expect("timed out") {
            Some(ClientEvent::Connection(Event::Fatal(_))) => saw_fatal = true,
            Some(_) => continue,
            None => break,
        }
    }
    assert!(saw_fatal, "the stream ended without reporting why");
}
