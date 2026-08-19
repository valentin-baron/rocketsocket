//! Tests against a real Rocket.Chat.
//!
//! Every other test in this workspace runs against a scripted peer that matches *my
//! reading* of the server source. That is exactly the wrong instrument for finding places
//! where the reading is wrong, which is the largest remaining risk in this crate. These
//! tests close that gap.
//!
//! They are `#[ignore]`d, because they need a server. Bring one up with
//! `docker-compose.test.yml`, then:
//!
//! ```text
//! ROCKETSOCKET_URL=http://localhost:3000 \
//! ROCKETSOCKET_USER_ID=<id> ROCKETSOCKET_TOKEN=<personal access token> \
//!   cargo test -p rocketsocket-realtime -- --ignored
//! ```

use std::time::Duration;

use rocketsocket_realtime::client::{Client, ClientEvent};
use rocketsocket_realtime::connection::{Config, Credential, Event};

/// Server details from the environment, or `None` when the harness is not configured.
fn live() -> Option<(String, String)> {
    let url = std::env::var("ROCKETSOCKET_URL").ok()?;
    let token = std::env::var("ROCKETSOCKET_TOKEN").ok()?;
    Some((url, token))
}

/// Turns an `http(s)://host` base URL into the DDP endpoint.
fn websocket_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    let base = base.strip_prefix("https://").map_or_else(
        || base.strip_prefix("http://").map_or(base.to_owned(), |rest| format!("ws://{rest}")),
        |rest| format!("wss://{rest}"),
    );
    format!("{base}/websocket")
}

#[tokio::test]
#[ignore = "needs a live Rocket.Chat; see docker-compose.test.yml"]
async fn a_real_server_completes_the_handshake_and_login() {
    let Some((url, token)) = live() else {
        eprintln!("ROCKETSOCKET_URL/ROCKETSOCKET_TOKEN unset; skipping");
        return;
    };

    let config = Config::new(websocket_url(&url), Credential::Resume(token));
    let (client, mut events) = Client::spawn(config);

    let ready = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match events.recv().await {
                Some(ClientEvent::Connection(Event::Ready { epoch })) => return Some(epoch),
                Some(ClientEvent::Connection(Event::Fatal(fatal))) => {
                    panic!("login failed against the live server: {fatal}")
                }
                Some(_) => continue,
                None => return None,
            }
        }
    })
    .await
    .expect("timed out waiting for Ready");

    assert!(ready.is_some(), "the event stream ended before login succeeded");
    client.shutdown().await;
}

#[tokio::test]
#[ignore = "needs a live Rocket.Chat; see docker-compose.test.yml"]
async fn a_real_server_accepts_the_catch_all_subscription_and_sends_messages() {
    let Some((url, token)) = live() else {
        eprintln!("ROCKETSOCKET_URL/ROCKETSOCKET_TOKEN unset; skipping");
        return;
    };

    let config = Config::new(websocket_url(&url), Credential::Resume(token));
    let (client, mut events) = Client::spawn(config);

    // Wait for login before subscribing: subscribing first triggers setUserId, which reruns
    // every subscription and produces a diff storm.
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(event) = events.recv().await {
            if matches!(event, ClientEvent::Connection(Event::Ready { .. })) {
                return;
            }
        }
        panic!("stream ended before Ready");
    })
    .await
    .expect("timed out waiting for Ready");

    // The single subscription a Discord-style bot wants: every message it can see.
    client
        .subscribe("room-messages", "__my_messages__")
        .await
        .expect("the live server refused __my_messages__");

    // Post through REST so the round trip crosses both transports, then read it back off
    // the websocket. This is the assertion that would catch a wrong stream name, a wrong
    // params shape, or an entity field this crate models incorrectly.
    eprintln!("subscribed; post a message in any room the bot can see within 30s");

    let seen = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(event) = events.recv().await {
            if let ClientEvent::Stream { key, args, .. } = event
                && key.event == "__my_messages__"
            {
                assert!(!args.is_empty(), "a message event must carry a payload");
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);

    assert!(seen, "no message arrived on __my_messages__");
    client.shutdown().await;
}
