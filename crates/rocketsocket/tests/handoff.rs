//! The credential handoff between transports.
//!
//! `Bot::connect` rests on one claim: the token `POST /api/v1/login` returns is
//! simultaneously a DDP resume token, because that endpoint wraps the DDP `login` method
//! and both transports read `services.resume.loginTokens`. Everything else in the crate
//! assumes it — it is why there is one credential rather than two, and why the client works
//! against the EE `ddp-streamer`, whose websocket login accepts only `{resume}`.
//!
//! A claim that load-bearing should not rest on reading the server source alone. This
//! stands up a scripted Rocket.Chat that answers the REST login and then *asserts the
//! websocket login carries the same token back*.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;

use rocketsocket::{Bot, Credentials};

const TOKEN: &str = "a-token-minted-by-rest";
const USER_ID: &str = "bot-user-id";
const BOUND: Duration = Duration::from_secs(10);

/// A minimal Rocket.Chat: HTTP `/api/v1/login`, then DDP on `/websocket`.
///
/// Hand-written rather than using a framework, because the point is to assert the exact
/// bytes each transport sends.
async fn serve(listener: TcpListener, observed: oneshot::Sender<Value>) {
    let mut observed = Some(observed);

    loop {
        let Ok((stream, _)) = listener.accept().await else { return };

        // `peek`, not `read`: consuming the bytes here would leave the websocket
        // handshake with no request to parse, and it would hang forever.
        let mut buffer = vec![0_u8; 4096];
        let read = match stream.peek(&mut buffer).await {
            Ok(0) | Err(_) => continue,
            Ok(read) => read,
        };
        let request = String::from_utf8_lossy(&buffer[..read]).to_string();

        if request.to_ascii_lowercase().contains("upgrade: websocket") {
            let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await else {
                continue;
            };

            // connect -> connected
            let _ = socket.next().await;
            let _ = socket
                .send(Message::text(json!({"msg":"connected","session":"s"}).to_string()))
                .await;

            // The frame under test: the DDP login.
            if let Some(Ok(Message::Text(text))) = socket.next().await
                && let Ok(frame) = serde_json::from_str::<Value>(&text)
                && let Some(sender) = observed.take()
            {
                let _ = sender.send(frame);
            }
            return;
        }

        // REST login. The response shape is Rocket.Chat's: data.authToken + data.userId.
        let body = json!({
            "status": "success",
            "data": {
                "userId": USER_ID,
                "authToken": TOKEN,
                "me": { "_id": USER_ID, "username": "bot" }
            }
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        loop {
            if stream.writable().await.is_err() {
                break;
            }
            match stream.try_write(response.as_bytes()) {
                Ok(_) => break,
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(_) => break,
            }
        }
    }
}

#[tokio::test]
async fn the_rest_token_is_reused_as_the_ddp_resume_token() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(serve(listener, tx));

    let (_bot, _events) =
        Bot::connect(&format!("http://127.0.0.1:{port}"), Credentials::password("bot", "hunter2"))
            .await
            .expect("connect should succeed against the scripted server");

    let login = tokio::time::timeout(BOUND, rx).await.expect("timed out").expect("no login frame");

    assert_eq!(login["msg"], "method");
    assert_eq!(login["method"], "login");

    // The assertion the whole two-transport design rests on.
    let params = login["params"].as_array().expect("login params");
    let resume = params
        .first()
        .and_then(|first| first.get("resume"))
        .and_then(Value::as_str)
        .expect("the DDP login must use the resume form");

    assert_eq!(
        resume, TOKEN,
        "the websocket must reuse the token REST minted, not authenticate separately"
    );

    // And it must be the resume form specifically: the EE ddp-streamer's login handler
    // destructures only `{resume}`, so a password payload there is silently a 403.
    assert!(
        params[0].get("password").is_none() && params[0].get("user").is_none(),
        "a password login over DDP fails against microservices deployments"
    );
}
