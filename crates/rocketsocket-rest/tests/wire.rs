//! Wire-level tests against a hand-written HTTP server on loopback.
//!
//! There is no Rocket.Chat to test against, and mocking `reqwest` would only test the mock.
//! A [`tokio::net::TcpListener`] speaking enough HTTP/1.1 to answer canned responses tests
//! the parts that actually break: the bytes this crate puts on the wire, and its behaviour
//! when the bytes coming back are not what a well-behaved JSON API would send.
//!
//! Only [`tokio::net`] is used — the inherent `try_read` / `try_write` methods rather than
//! the `AsyncReadExt` traits — so the test suite needs no feature beyond the ones the crate
//! already declares.

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use rocketsocket_rest::{
    ApiErrorKind, Authentication, Client, ConfirmUpload, Credentials, FileUpload, PostMessage,
    RestError, SendMessage,
};

// -------------------------------------------------------------------------------------
// A minimal HTTP/1.1 server
// -------------------------------------------------------------------------------------

/// One request as the server saw it.
#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Recorded {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("request body is JSON")
    }

    fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// A canned response.
#[derive(Debug, Clone)]
struct Canned {
    status: u16,
    reason: &'static str,
    content_type: &'static str,
    headers: Vec<(String, String)>,
    body: String,
}

impl Canned {
    fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            reason: "OK",
            content_type: "application/json",
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    fn text(status: u16, body: &str) -> Self {
        Self {
            status,
            reason: "Unauthorized",
            content_type: "text/plain; charset=utf-8",
            headers: Vec::new(),
            body: body.to_owned(),
        }
    }

    fn header(mut self, name: &str, value: impl ToString) -> Self {
        self.headers.push((name.to_owned(), value.to_string()));
        self
    }

    fn encode(&self) -> Vec<u8> {
        let mut head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            self.status,
            self.reason,
            self.content_type,
            self.body.len()
        );
        for (name, value) in &self.headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        head.push_str(&self.body);
        head.into_bytes()
    }
}

/// A server that answers `responses.len()` requests and then stops.
struct MockServer {
    addr: SocketAddr,
    handle: JoinHandle<Vec<Recorded>>,
}

impl MockServer {
    async fn start(responses: Vec<Canned>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");

        let handle = tokio::spawn(async move {
            let mut recorded = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().await.expect("accept");
                recorded.push(read_request(&mut stream).await);
                write_all(&mut stream, &response.encode()).await;
            }
            recorded
        });

        Self { addr, handle }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn requests(self) -> Vec<Recorded> {
        self.handle.await.expect("server task")
    }
}

async fn read_request(stream: &mut TcpStream) -> Recorded {
    let mut buf = Vec::new();
    loop {
        stream.readable().await.expect("readable");
        let mut chunk = [0_u8; 4096];
        match stream.try_read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                buf.extend_from_slice(&chunk[..read]);
                if request_complete(&buf) {
                    break;
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => continue,
            Err(error) => panic!("read failed: {error}"),
        }
    }
    parse_request(&buf)
}

async fn write_all(stream: &mut TcpStream, bytes: &[u8]) {
    let mut written = 0;
    while written < bytes.len() {
        stream.writable().await.expect("writable");
        match stream.try_write(&bytes[written..]) {
            Ok(count) => written += count,
            Err(error) if error.kind() == ErrorKind::WouldBlock => continue,
            Err(error) => panic!("write failed: {error}"),
        }
    }
}

fn header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n").map(|at| at + 4)
}

fn request_complete(buf: &[u8]) -> bool {
    let Some(start) = header_end(buf) else { return false };
    let head = String::from_utf8_lossy(&buf[..start]).to_lowercase();

    if head.contains("transfer-encoding: chunked") {
        return buf.ends_with(b"0\r\n\r\n");
    }

    let length = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);

    buf.len() >= start + length
}

fn parse_request(buf: &[u8]) -> Recorded {
    let start = header_end(buf).expect("complete request head");
    let head = String::from_utf8_lossy(&buf[..start]).into_owned();
    let mut lines = head.lines();

    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let path = parts.next().unwrap_or_default().to_owned();

    let headers = lines
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect::<Vec<_>>();

    let chunked = headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("transfer-encoding") && value.eq_ignore_ascii_case("chunked")
    });

    let body = if chunked { dechunk(&buf[start..]) } else { buf[start..].to_vec() };

    Recorded { method, path, headers, body }
}

/// Decode a chunked body. reqwest streams multipart bodies whose length it cannot compute.
fn dechunk(mut rest: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    while let Some(line_end) = rest.windows(2).position(|window| window == b"\r\n") {
        let size = usize::from_str_radix(
            String::from_utf8_lossy(&rest[..line_end]).split(';').next().unwrap_or("0").trim(),
            16,
        )
        .unwrap_or(0);

        rest = &rest[line_end + 2..];
        if size == 0 || rest.len() < size {
            break;
        }
        body.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2.min(rest.len() - size)..];
    }
    body
}

// -------------------------------------------------------------------------------------
// Fixtures
// -------------------------------------------------------------------------------------

const USER_ID: &str = "aobEdbYhXfu5hkeqG";
const TOKEN: &str = "cQ6Hvj0hR6WLhOn6RSKRQBSNCzDQwLrJhtNPnDLtWDs";

fn user_document() -> Value {
    json!({
        "_id": USER_ID,
        "username": "bot",
        "name": "Bot",
        "roles": ["bot", "user"],
        "active": true,
        "status": "online",
    })
}

fn message_document() -> Value {
    json!({
        "_id": "7aDSXtjMA3KPLxLjt",
        "_updatedAt": "2026-08-19T10:00:00.000Z",
        "rid": "GENERAL",
        "msg": "hello",
        "ts": "2026-08-19T10:00:00.000Z",
        "u": { "_id": USER_ID, "username": "bot" },
    })
}

fn authenticated_client(server: &MockServer) -> Client {
    Client::builder(&server.url())
        .expect("valid base URL")
        .authentication(Authentication::new(USER_ID, TOKEN))
        .build()
        .expect("client")
}

// -------------------------------------------------------------------------------------
// Authentication
// -------------------------------------------------------------------------------------

#[tokio::test]
async fn login_extracts_the_token_and_the_user() {
    let server = MockServer::start(vec![Canned::json(
        200,
        json!({
            "status": "success",
            "data": { "userId": USER_ID, "authToken": TOKEN, "me": user_document() },
            "success": true,
        }),
    )])
    .await;

    let client = Client::new(&server.url()).expect("client");
    let outcome = client
        .login(&Credentials::password("bot@example.com", "hunter2"))
        .await
        .expect("login succeeds");

    // The token is exposed, because the realtime crate needs it as a DDP resume token.
    assert_eq!(outcome.authentication.token().expose(), TOKEN);
    assert_eq!(outcome.authentication.resume_token().expose(), TOKEN);
    assert_eq!(outcome.authentication.user_id().as_ref(), USER_ID);

    // `login` already answered with the user, so `/me` afterwards would be a wasted trip.
    assert_eq!(outcome.me.username.as_deref(), Some("bot"));

    // It is stored on the client, so later calls are authenticated.
    let stored = client.authentication().await.expect("credentials stored");
    assert_eq!(stored.token().expose(), TOKEN);

    let requests = server.requests().await;
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/api/v1/login");
    assert_eq!(requests[0].json(), json!({ "user": "bot@example.com", "password": "hunter2" }));
    // Login carries no credentials of its own.
    assert!(requests[0].header("x-auth-token").is_none());
}

#[tokio::test]
async fn a_personal_access_token_is_validated_with_one_me_call() {
    let server = MockServer::start(vec![Canned::json(200, {
        let mut me = user_document();
        me["success"] = json!(true);
        me
    })])
    .await;

    let client = Client::new(&server.url()).expect("client");
    let outcome = client
        .login(&Credentials::personal_access_token(USER_ID, TOKEN))
        .await
        .expect("PAT accepted");

    assert_eq!(outcome.authentication.token().expose(), TOKEN);

    let requests = server.requests().await;
    assert_eq!(requests.len(), 1, "a PAT needs no login round trip");
    assert_eq!(requests[0].path, "/api/v1/me");
    assert_eq!(requests[0].header("x-auth-token"), Some(TOKEN));
    assert_eq!(requests[0].header("x-user-id"), Some(USER_ID));
}

#[tokio::test]
async fn a_rejected_personal_access_token_does_not_stay_installed() {
    let server = MockServer::start(vec![Canned::text(401, "Unauthorized")]).await;

    let client = Client::new(&server.url()).expect("client");
    let error = client
        .login(&Credentials::personal_access_token(USER_ID, "wrong"))
        .await
        .expect_err("PAT rejected");

    assert!(error.api().is_some_and(|api| api.is_unauthorized()));
    assert!(client.authentication().await.is_none(), "bad credentials must not linger");

    let _ = server.requests().await;
}

#[tokio::test]
async fn two_factor_headers_ride_along_on_one_clone() {
    let server = MockServer::start(vec![
        Canned::json(200, json!({ "_id": USER_ID, "success": true })),
        Canned::json(200, json!({ "_id": USER_ID, "success": true })),
    ])
    .await;

    let client = authenticated_client(&server);
    let with_code = client.with_two_factor(rocketsocket_rest::TwoFactor::totp("123456"));

    with_code.me().await.expect("with code");
    client.me().await.expect("without code");

    let requests = server.requests().await;
    assert_eq!(requests[0].header("x-2fa-code"), Some("123456"));
    assert_eq!(requests[0].header("x-2fa-method"), Some("totp"));
    assert!(requests[1].header("x-2fa-code").is_none(), "the original client is untouched");
}

#[tokio::test]
async fn a_call_without_credentials_fails_before_any_request() {
    let client = Client::new("https://chat.example.invalid").expect("client");
    let error = client.me().await.expect_err("no credentials");
    assert!(matches!(error, RestError::NotAuthenticated));
}

// -------------------------------------------------------------------------------------
// Errors
// -------------------------------------------------------------------------------------

#[tokio::test]
async fn a_400_failure_envelope_is_split_into_code_and_message() {
    let server = MockServer::start(vec![Canned::json(
        400,
        json!({
            "success": false,
            "error": "Not allowed",
            "errorType": "error-not-allowed",
            "details": { "method": "sendMessage" },
        }),
    )])
    .await;

    let client = authenticated_client(&server);
    let error = client
        .send_message(&SendMessage::new("GENERAL").text("hello"))
        .await
        .expect_err("server refuses");

    let api = error.api().expect("an API error");
    assert_eq!(api.status().as_u16(), 400);
    assert_eq!(api.kind(), ApiErrorKind::BadRequest);
    assert_eq!(api.code(), Some("error-not-allowed"), "errorType is the machine code on REST");
    assert_eq!(api.message(), Some("Not allowed"), "error is the prose on REST");
    assert!(api.has_code("error-not-allowed"));
    assert_eq!(api.details().and_then(|d| d.get("method")), Some(&json!("sendMessage")));

    let _ = server.requests().await;
}

#[tokio::test]
async fn a_401_with_a_plain_text_body_still_yields_an_api_error() {
    // `authenticationMiddleware` answers `res.status(401).send('Unauthorized')` — no JSON at
    // all. A deserializer that insists on JSON reports a parse bug instead of "log in".
    let server = MockServer::start(vec![Canned::text(401, "Unauthorized")]).await;

    let client = authenticated_client(&server);
    let error = client.me().await.expect_err("unauthorized");

    let api = error.api().expect("an API error, not a decode failure");
    assert_eq!(api.kind(), ApiErrorKind::Unauthorized);
    assert!(api.is_unauthorized());
    assert!(!api.is_forbidden());
    assert_eq!(api.code(), None);
    assert_eq!(api.message(), Some("Unauthorized"));
    assert_eq!(api.raw_body(), Some("Unauthorized"));

    let _ = server.requests().await;
}

#[tokio::test]
async fn a_403_is_forbidden_even_though_the_body_says_unauthorized() {
    // Until server 9.0, `API.v1.forbidden()` sends the *string* "unauthorized".
    let server = MockServer::start(vec![Canned::json(
        403,
        json!({ "success": false, "error": "unauthorized" }),
    )])
    .await;

    let client = authenticated_client(&server);
    let error = client.me().await.expect_err("forbidden");
    let api = error.api().expect("an API error");

    assert!(api.is_forbidden(), "discriminate on status");
    assert!(!api.is_unauthorized(), "not on the misleading string");
    assert_eq!(api.message(), Some("unauthorized"));

    let _ = server.requests().await;
}

#[tokio::test]
async fn a_429_reports_the_rate_limit_and_a_correct_retry_after() {
    let reset = now_millis() + 30_000;
    let server = MockServer::start(vec![
        Canned::json(429, json!({ "success": false, "error": "Too many requests" }))
            .header("X-RateLimit-Limit", 10)
            .header("X-RateLimit-Remaining", 0)
            .header("X-RateLimit-Reset", reset),
    ])
    .await;

    let client = authenticated_client(&server);
    let error = client
        .send_message(&SendMessage::new("GENERAL").text("hello"))
        .await
        .expect_err("throttled");

    assert!(error.is_too_many_requests());
    let api = error.api().expect("an API error");
    let limits = api.rate_limit().expect("rate limit headers");
    assert_eq!(limits.limit(), Some(10));
    assert_eq!(limits.remaining(), Some(0));

    // Milliseconds since the epoch, not seconds remaining: ~30 s, not ~1.8 million years.
    let wait = error.retry_after().expect("a retry delay");
    assert!(
        wait > Duration::from_secs(25) && wait <= Duration::from_secs(31),
        "retry_after was {wait:?}"
    );

    let _ = server.requests().await;
}

#[tokio::test]
async fn a_success_body_of_the_wrong_shape_is_a_decode_error() {
    let server = MockServer::start(vec![Canned::json(200, json!({ "success": true }))]).await;

    let client = authenticated_client(&server);
    let error = client
        .send_message(&SendMessage::new("GENERAL").text("hello"))
        .await
        .expect_err("no message member");

    assert!(matches!(error, RestError::Decode { .. }), "got {error:?}");

    let _ = server.requests().await;
}

// -------------------------------------------------------------------------------------
// Sending
// -------------------------------------------------------------------------------------

#[tokio::test]
async fn send_message_nests_the_message_and_carries_blocks() {
    let server = MockServer::start(vec![Canned::json(
        200,
        json!({ "message": message_document(), "success": true }),
    )])
    .await;

    let client = authenticated_client(&server);
    let blocks = vec![json!({
        "type": "section",
        "text": { "type": "mrkdwn", "text": "*build 412* is live" },
    })];

    let message = client
        .send_message(
            &SendMessage::new("GENERAL")
                .text("hello")
                .client_id("7aDSXtjMA3KPLxLjt")
                .thread("parent-message-id")
                .thread_show(true)
                .blocks(blocks.clone())
                .preview_urls(vec![]),
        )
        .await
        .expect("sent");

    assert_eq!(message.msg, "hello");

    let requests = server.requests().await;
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/api/v1/chat.sendMessage");
    assert_eq!(requests[0].header("x-auth-token"), Some(TOKEN));
    assert_eq!(requests[0].header("x-user-id"), Some(USER_ID));

    assert_eq!(
        requests[0].json(),
        json!({
            "message": {
                "rid": "GENERAL",
                "_id": "7aDSXtjMA3KPLxLjt",
                "msg": "hello",
                "tmid": "parent-message-id",
                "tshow": true,
                "blocks": blocks,
            },
            "previewUrls": [],
        }),
        "blocks belong inside `message`; previewUrls is its sibling",
    );

    // No client timestamp: the server rejects skew beyond 60 s and silently rewrites 10-60 s.
    assert!(requests[0].json()["message"].get("ts").is_none());
}

#[tokio::test]
async fn post_message_addresses_channels_by_name() {
    let server = MockServer::start(vec![Canned::json(
        200,
        json!({ "ts": 1_755_600_000_000_i64, "channel": "#general", "message": message_document(), "success": true }),
    )])
    .await;

    let client = authenticated_client(&server);
    let posted = client
        .post_message(&PostMessage::to_channel("#general").text("hello").parse_urls(false))
        .await
        .expect("posted");

    assert_eq!(posted.channel, "#general");
    assert_eq!(posted.ts, 1_755_600_000_000_i64);

    let requests = server.requests().await;
    assert_eq!(requests[0].path, "/api/v1/chat.postMessage");
    assert_eq!(
        requests[0].json(),
        json!({ "channel": ["#general"], "text": "hello", "parseUrls": false }),
        "the schema's two branches are exclusive: never both channel and roomId",
    );
}

// -------------------------------------------------------------------------------------
// Upload
// -------------------------------------------------------------------------------------

#[tokio::test]
async fn upload_runs_both_steps_of_the_transaction() {
    let server = MockServer::start(vec![
        Canned::json(
            200,
            json!({ "file": { "_id": "upload-id", "url": "/file-upload/upload-id/report.pdf" }, "success": true }),
        ),
        Canned::json(200, json!({ "message": message_document(), "success": true })),
    ])
    .await;

    let client = authenticated_client(&server);
    let file =
        FileUpload::new("report.pdf", b"%PDF-1.7 fake".to_vec()).content_type("application/pdf");

    let message = client
        .upload_file("GENERAL", file, ConfirmUpload::new().text("last night's run"))
        .await
        .expect("uploaded and posted");

    assert_eq!(message.rid.as_ref(), "GENERAL");

    let requests = server.requests().await;
    assert_eq!(requests.len(), 2, "one step alone posts nothing");

    assert_eq!(requests[0].path, "/api/v1/rooms.media/GENERAL");
    let multipart = requests[0].body_text();
    assert!(multipart.contains("name=\"file\""), "the field name is hard-coded `file`");
    assert!(multipart.contains("filename=\"report.pdf\""));
    assert!(multipart.contains("application/pdf"));
    assert!(multipart.contains("%PDF-1.7 fake"));

    assert_eq!(requests[1].path, "/api/v1/rooms.mediaConfirm/GENERAL/upload-id");
    assert_eq!(
        requests[1].json(),
        json!({ "msg": "last night's run" }),
        "only members the server's check() whitelist accepts",
    );
}

#[tokio::test]
async fn a_failed_confirmation_hands_back_the_orphaned_upload() {
    let server = MockServer::start(vec![
        Canned::json(
            200,
            json!({ "file": { "_id": "upload-id", "url": "/file-upload/upload-id/report.pdf" }, "success": true }),
        ),
        Canned::json(
            400,
            json!({ "success": false, "error": "Invalid file", "errorType": "invalid-file" }),
        ),
    ])
    .await;

    let client = authenticated_client(&server);
    let error = client
        .upload_file(
            "GENERAL",
            FileUpload::new("report.pdf", b"bytes".to_vec()),
            ConfirmUpload::new(),
        )
        .await
        .expect_err("confirmation refused");

    // The bytes are on the server, nothing was posted, and the id is the only way back.
    let orphan = error.orphaned_file().expect("the upload id must survive the failure");
    assert_eq!(orphan.id.as_ref(), "upload-id");
    assert!(error.source_error().api().is_some_and(|api| api.has_code("invalid-file")));
    assert!(error.to_string().contains("orphaned"), "the hazard is named in the message");

    let requests = server.requests().await;
    assert_eq!(requests.len(), 2);
}

fn now_millis() -> i64 {
    i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH).expect("after the epoch").as_millis(),
    )
    .expect("fits")
}
