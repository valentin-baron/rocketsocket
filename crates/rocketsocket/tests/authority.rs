//! The role filters, against a scripted Rocket.Chat.
//!
//! `#[event(admin)]`, `#[event(room_admin)]` and `#[event(any_admin)]` are the only filters
//! that need the server, which makes them the only ones that can fail *open* if written
//! carelessly — a 500, a timeout or an unparsed body admitting an unprivileged author to an
//! admin-only handler leaves no trace anywhere. So every failure path here asserts a
//! rejection, and the happy paths assert that the answer is fetched once rather than once
//! per message.
//!
//! The server is hand-written on a `tokio::net::TcpListener`, as in `handoff.rs`: the point
//! is to assert the exact requests the framework issues — path, query and *count* — which a
//! mock of the client would not.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use rocketsocket::filter::{Authority, Filtered, Filters};
use rocketsocket::framework::{Framework, Handler};
use rocketsocket::model::event::StreamEvent;
use rocketsocket::model::{RoomId, UserId};
use rocketsocket::realtime::client::ClientEvent;
use rocketsocket::realtime::correlate::Epoch;
use rocketsocket::realtime::subscription::StreamKey;
use rocketsocket::rest::{Authentication, Client as RestClient};
use rocketsocket::roles::{RoleCacheConfig, RoleDirectory};
use rocketsocket::{Bot, Credentials};

const PUBLIC_ROLES: &str = "/api/v1/roles.getUsersInPublicRoles";
const ROOM_ROLES: &str = "/api/v1/rooms.roles";
const BOUND: Duration = Duration::from_secs(10);

// -------------------------------------------------------------------------------------
// A scripted Rocket.Chat
// -------------------------------------------------------------------------------------

/// One request, as the scripted server saw it.
#[derive(Debug, Clone)]
struct Request {
    /// Path without the query string.
    path: String,
    /// Query string, without the `?`.
    query: String,
    /// How many requests for this same path arrived *before* this one.
    index: usize,
}

/// What the scripted server should answer.
#[derive(Debug, Clone)]
enum Reply {
    /// A status and a body.
    Body(u16, String),
    /// Accept the request and never answer. The shape of a wedged deployment, and the only
    /// way to exercise the lookup timeout.
    Hang,
    /// Read the request and close the socket without a response.
    Drop,
}

impl Reply {
    fn ok(body: impl Into<String>) -> Self {
        Self::Body(200, body.into())
    }
}

#[derive(Clone)]
struct Server {
    base: String,
    seen: Arc<Mutex<Vec<Request>>>,
}

impl Server {
    /// Starts a server that answers every request through `answer`.
    fn spawn(answer: impl Fn(&Request) -> Reply + Send + Sync + 'static) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let port = listener.local_addr().expect("addr").port();
        let listener = TcpListener::from_std(listener).expect("adopt");

        let seen: Arc<Mutex<Vec<Request>>> = Arc::new(Mutex::new(Vec::new()));
        let server = Self { base: format!("http://127.0.0.1:{port}"), seen: Arc::clone(&seen) };

        let answer: Arc<dyn Fn(&Request) -> Reply + Send + Sync> = Arc::new(answer);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                tokio::spawn(handle(stream, Arc::clone(&answer), Arc::clone(&seen)));
            }
        });

        server
    }

    /// How many requests arrived for `path`.
    fn hits(&self, path: &str) -> usize {
        self.seen.lock().expect("lock").iter().filter(|request| request.path == path).count()
    }

    /// Every request for `path`, in order.
    fn requests(&self, path: &str) -> Vec<Request> {
        self.seen
            .lock()
            .expect("lock")
            .iter()
            .filter(|request| request.path == path)
            .cloned()
            .collect()
    }
}

async fn handle(
    mut stream: tokio::net::TcpStream,
    answer: Arc<dyn Fn(&Request) -> Reply + Send + Sync>,
    seen: Arc<Mutex<Vec<Request>>>,
) {
    // These are all GETs, so the headers are the whole request.
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => {
                buffer.extend_from_slice(&chunk[..read]);
                if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
        }
    }

    let text = String::from_utf8_lossy(&buffer).into_owned();
    let target = text.split_whitespace().nth(1).unwrap_or("/").to_owned();
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path.to_owned(), query.to_owned()),
        None => (target, String::new()),
    };

    let request = {
        let mut seen = seen.lock().expect("lock");
        let index = seen.iter().filter(|request| request.path == path).count();
        let request = Request { path, query, index };
        seen.push(request.clone());
        request
    };

    match answer(&request) {
        Reply::Hang => {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        Reply::Drop => {}
        Reply::Body(status, body) => {
            let head = format!(
                "HTTP/1.1 {status} SCRIPTED\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(body.as_bytes()).await;
            let _ = stream.flush().await;
        }
    }
}

// -------------------------------------------------------------------------------------
// Payloads, in the shapes the v8.8 server actually produces
// -------------------------------------------------------------------------------------

/// `roles.getUsersInPublicRoles`. Reports every *public* role, not only `admin` — a stock
/// workspace seeds `livechat-agent` and `livechat-manager` with descriptions too, so the
/// filtering has to happen client side.
fn public_roles(admins: &[&str]) -> String {
    let mut users: Vec<Value> = admins
        .iter()
        .map(|id| json!({"_id": id, "username": id, "roles": ["admin", "user"]}))
        .collect();
    users.push(json!({"_id": "agent", "username": "agent", "roles": ["livechat-agent"]}));
    json!({"success": true, "users": users}).to_string()
}

/// `rooms.roles`: one entry per user holding a subscription-scoped role in the room.
fn room_roles(rid: &str, holders: &[(&str, &str)]) -> String {
    let roles: Vec<Value> = holders
        .iter()
        .map(|(user, role)| json!({"rid": rid, "u": {"_id": user, "username": user}, "roles": [role]}))
        .collect();
    json!({"success": true, "roles": roles}).to_string()
}

/// `/api/v1/me`, which is all `Bot::connect` needs from a Personal Access Token.
fn me() -> String {
    json!({"success": true, "_id": "bot", "username": "bot"}).to_string()
}

/// `API.v1.failure` — note that the status is the discriminator, not the string.
fn failure(reason: &str) -> String {
    json!({"success": false, "error": reason}).to_string()
}

// -------------------------------------------------------------------------------------
// Fixtures
// -------------------------------------------------------------------------------------

fn directory(server: &Server, config: RoleCacheConfig) -> RoleDirectory {
    let rest = RestClient::builder(&server.base)
        .expect("a loopback URL is valid")
        .authentication(Authentication::new("bot", "token"))
        .build()
        .expect("build");
    RoleDirectory::new(rest, config)
}

/// Short enough that a test never waits on it, long enough that nothing expires mid-test.
fn config() -> RoleCacheConfig {
    RoleCacheConfig::DEFAULT.timeout(Duration::from_secs(5))
}

async fn bot(server: &Server) -> Bot {
    let (bot, _events) =
        Bot::connect(&server.base, Credentials::personal_access_token("bot", "token"))
            .await
            .expect("the scripted server answers /api/v1/me");
    bot
}

fn message(author: &str, room: &str) -> ClientEvent {
    let args = vec![json!({
        "_id": "m1", "rid": room, "msg": "!promote",
        "ts": {"$date": 1_755_529_012_345_i64},
        "u": {"_id": author, "username": author},
        "_updatedAt": {"$date": 1_755_529_012_345_i64}
    })];
    ClientEvent::Stream {
        key: StreamKey::new("room-messages", room),
        event: StreamEvent::decode("room-messages", room, &args),
        args,
    }
}

fn roles_change(action: &str, role: &str, user: &str, scope: Option<&str>) -> ClientEvent {
    let mut payload = json!({"type": action, "_id": role, "u": {"_id": user, "username": user}});
    if let Some(scope) = scope {
        payload["scope"] = json!(scope);
    }
    let args = vec![payload];
    ClientEvent::Stream {
        key: StreamKey::new("notify-logged", "roles-change"),
        event: StreamEvent::decode("notify-logged", "roles-change", &args),
        args,
    }
}

/// `Handler` stores a plain fn pointer, so a counting handler needs a `static` counter.
///
/// The event check stands in for what `#[event]` generates: a real handler declares
/// `MessageCreate`, and its extractor answers `Extract::Skip` for anything else — so a
/// `roles-change` or a reconnect passing through `dispatch` does not run it.
macro_rules! counting_handler {
    ($name:literal, $counter:ident, $authority:expr) => {
        Handler::<()>::new($name, Filters::DEFAULT.authority($authority), |event, _context| {
            if matches!(event, ClientEvent::Stream { event: StreamEvent::RoomMessage { .. }, .. }) {
                $counter.fetch_add(1, Ordering::SeqCst);
            }
            Box::pin(async move { Ok(()) })
        })
    };
}

// -------------------------------------------------------------------------------------
// The happy paths, end to end through the framework
// -------------------------------------------------------------------------------------

static ADMIN_RAN: AtomicU32 = AtomicU32::new(0);

#[tokio::test]
async fn an_admin_author_reaches_an_admin_handler() {
    let server = Server::spawn(|request| match request.path.as_str() {
        PUBLIC_ROLES => Reply::ok(public_roles(&["boss"])),
        "/api/v1/me" => Reply::ok(me()),
        _ => Reply::Body(404, failure("unknown-endpoint")),
    });

    let framework = Framework::new(bot(&server).await, ())
        .role_cache(config())
        .handler(counting_handler!("admin", ADMIN_RAN, Authority::Admin));

    framework.dispatch(&message("boss", "GENERAL")).await;
    assert_eq!(ADMIN_RAN.load(Ordering::SeqCst), 1, "a server admin passes `admin`");

    // The whole point of `roles.getUsersInPublicRoles` over `users.info`: the answer is
    // per workspace, so it is fetched once however many people talk to the handler.
    assert_eq!(server.hits(PUBLIC_ROLES), 1);
}

static NON_ADMIN_RAN: AtomicU32 = AtomicU32::new(0);

#[tokio::test]
async fn a_non_admin_author_does_not_reach_an_admin_handler() {
    let server = Server::spawn(|request| match request.path.as_str() {
        PUBLIC_ROLES => Reply::ok(public_roles(&["boss"])),
        "/api/v1/me" => Reply::ok(me()),
        _ => Reply::Body(404, failure("unknown-endpoint")),
    });

    let framework = Framework::new(bot(&server).await, ())
        .role_cache(config())
        .handler(counting_handler!("admin", NON_ADMIN_RAN, Authority::Admin));

    framework.dispatch(&message("nobody", "GENERAL")).await;
    assert_eq!(NON_ADMIN_RAN.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_room_owner_passes_room_admin_in_that_room_only() {
    let server = Server::spawn(|request| match request.path.as_str() {
        ROOM_ROLES if request.query == "rid=GENERAL" => {
            Reply::ok(room_roles("GENERAL", &[("alice", "owner"), ("dave", "archivist")]))
        }
        ROOM_ROLES => Reply::ok(room_roles("other", &[("bob", "moderator")])),
        _ => Reply::Body(404, failure("unknown-endpoint")),
    });
    let directory = directory(&server, config());

    let alice = UserId::new("alice");
    assert!(directory.is_room_admin(&alice, &RoomId::new("GENERAL")).await);

    // The same user, a different room. A room-scoped grant that leaked across rooms would
    // be the worst kind of bug here: invisible, and wrong in the permissive direction.
    assert!(!directory.is_room_admin(&alice, &RoomId::new("other")).await);

    // A subscription role the workspace defined itself is reported by `getRoomRoles` too,
    // and is not owner/moderator/leader.
    assert!(!directory.is_room_admin(&UserId::new("dave"), &RoomId::new("GENERAL")).await);

    // `rooms.roles` validates its query with `additionalProperties: false`, so `rid` has to
    // be there and nothing else may be.
    let queries: Vec<_> =
        server.requests(ROOM_ROLES).into_iter().map(|request| request.query).collect();
    assert_eq!(queries, vec!["rid=GENERAL".to_owned(), "rid=other".to_owned()]);
}

#[tokio::test]
async fn any_admin_passes_by_either_route() {
    let server = Server::spawn(|request| match request.path.as_str() {
        PUBLIC_ROLES => Reply::ok(public_roles(&["boss"])),
        ROOM_ROLES => Reply::ok(room_roles("GENERAL", &[("alice", "leader")])),
        _ => Reply::Body(404, failure("unknown-endpoint")),
    });
    let directory = directory(&server, config());
    let room = RoomId::new("GENERAL");

    // Via the global role.
    assert_eq!(
        directory.verdict(Authority::AdminOrRoomAdmin, &UserId::new("boss"), &room).await,
        None
    );
    assert_eq!(
        server.hits(ROOM_ROLES),
        0,
        "a workspace admin must short-circuit before the per-room lookup"
    );

    // Via the room role.
    assert_eq!(
        directory.verdict(Authority::AdminOrRoomAdmin, &UserId::new("alice"), &room).await,
        None
    );
    assert_eq!(server.hits(ROOM_ROLES), 1);

    // Neither.
    assert_eq!(
        directory.verdict(Authority::AdminOrRoomAdmin, &UserId::new("mallory"), &room).await,
        Some(Filtered::Authority)
    );
}

// -------------------------------------------------------------------------------------
// Every way of not knowing rejects
// -------------------------------------------------------------------------------------

/// Runs one lookup of each kind against a server that answers `reply`, and asserts both
/// reject. Parameterised because the interesting thing is that *every* failure shape lands
/// in the same place.
async fn assert_rejects(reply: impl Fn(&Request) -> Reply + Send + Sync + 'static) {
    let server = Server::spawn(reply);
    let directory = directory(&server, config().timeout(Duration::from_millis(250)));
    let room = RoomId::new("GENERAL");

    assert_eq!(
        directory.verdict(Authority::Admin, &UserId::new("boss"), &room).await,
        Some(Filtered::Authority),
        "an unresolvable global role must reject"
    );
    assert_eq!(
        directory.verdict(Authority::RoomAdmin, &UserId::new("alice"), &room).await,
        Some(Filtered::Authority),
        "an unresolvable room role must reject"
    );
    assert_eq!(
        directory.verdict(Authority::AdminOrRoomAdmin, &UserId::new("boss"), &room).await,
        Some(Filtered::Authority),
        "`any_admin` must not admit just because one of its two routes is broken"
    );
}

#[tokio::test]
async fn a_server_error_rejects() {
    assert_rejects(|_| Reply::Body(500, failure("internal-error"))).await;
}

#[tokio::test]
async fn a_timeout_rejects() {
    // A server that accepts the connection and then says nothing. Without the lookup
    // budget this would hang the dispatch loop rather than merely closing the handler.
    let started = std::time::Instant::now();
    tokio::time::timeout(BOUND, assert_rejects(|_| Reply::Hang))
        .await
        .expect("a hung lookup must not hang dispatch");
    assert!(started.elapsed() < BOUND);
}

#[tokio::test]
async fn a_success_false_envelope_rejects() {
    // 200 with `success: false` is not supposed to happen — `API.v1.failure` pairs it with
    // a non-2xx — but a reverse proxy that rewrites status codes makes it happen anyway,
    // and `{"success": false}` carries no `users` key, which must not read as "no admins".
    assert_rejects(|_| Reply::ok(failure("error-invalid-user"))).await;
}

#[tokio::test]
async fn an_empty_answer_envelope_rejects() {
    // The same trap without the explicit failure: a body with the right shape and no
    // `success` marker is a truncated or proxied response, not an empty workspace.
    assert_rejects(|_| Reply::ok(r#"{"users":[],"roles":[]}"#)).await;
}

#[tokio::test]
async fn an_unparseable_body_rejects() {
    assert_rejects(|_| Reply::ok("<html>gateway error</html>")).await;
}

#[tokio::test]
async fn an_unauthorised_lookup_rejects() {
    // A dead or wrong token. The handler going quiet is the correct visible symptom.
    assert_rejects(|_| Reply::Body(401, failure("unauthorized"))).await;
}

#[tokio::test]
async fn a_dropped_connection_rejects() {
    // No reply at all: the scripted server closes the socket after reading the request.
    let server = Server::spawn(|_| Reply::Drop);
    let directory = directory(&server, config());
    assert_eq!(
        directory.verdict(Authority::Admin, &UserId::new("boss"), &RoomId::new("GENERAL")).await,
        Some(Filtered::Authority)
    );
}

#[tokio::test]
async fn a_room_the_bot_cannot_see_rejects() {
    // `executeGetRoomRoles` reports both an unknown room and an inaccessible one, and
    // reports the latter as `error-invalid-user` — so neither is distinguishable from a
    // permission problem, and both must close the handler.
    let server = Server::spawn(|_| Reply::Body(400, failure("error-invalid-room")));
    let directory = directory(&server, config());
    assert!(!directory.is_room_admin(&UserId::new("alice"), &RoomId::new("secret")).await);
}

#[tokio::test]
async fn an_unauthenticated_client_rejects_without_calling_anything() {
    let server = Server::spawn(|_| Reply::ok(public_roles(&["boss"])));
    let rest = RestClient::new(&server.base).expect("a loopback URL is valid");
    let directory = RoleDirectory::new(rest, config());

    assert!(!directory.is_admin(&UserId::new("boss")).await);
    assert_eq!(server.hits(PUBLIC_ROLES), 0, "there is nothing to authenticate the call with");
}

// -------------------------------------------------------------------------------------
// Memoisation
// -------------------------------------------------------------------------------------

#[tokio::test]
async fn the_answer_is_memoised_rather_than_fetched_per_message() {
    // The requirement the whole design exists for: Rocket.Chat's default limiter allows ten
    // requests per 60 s per route per IP, so a lookup per message dies in under a second.
    let server = Server::spawn(|request| match request.path.as_str() {
        PUBLIC_ROLES => Reply::ok(public_roles(&["boss"])),
        ROOM_ROLES => Reply::ok(room_roles("GENERAL", &[("alice", "owner")])),
        _ => Reply::Body(404, failure("unknown-endpoint")),
    });
    let directory = directory(&server, config());
    let room = RoomId::new("GENERAL");

    for _ in 0..50 {
        assert!(directory.is_admin(&UserId::new("boss")).await);
        assert!(directory.is_room_admin(&UserId::new("alice"), &room).await);
    }

    assert_eq!(server.hits(PUBLIC_ROLES), 1, "the global snapshot is fetched once");
    assert_eq!(server.hits(ROOM_ROLES), 1, "and one snapshot per room");
}

#[tokio::test]
async fn concurrent_lookups_for_one_key_do_not_stampede() {
    // Sixteen messages arriving together must not become sixteen requests. The scripted
    // server is deliberately slow so every caller is in flight before the first answer.
    let server = Server::spawn(|request| match request.path.as_str() {
        PUBLIC_ROLES => Reply::ok(public_roles(&["boss"])),
        _ => Reply::Body(404, failure("unknown-endpoint")),
    });
    let directory = Arc::new(directory(&server, config()));

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let directory = Arc::clone(&directory);
        tasks.spawn(async move { directory.is_admin(&UserId::new("boss")).await });
    }
    while let Some(result) = tasks.join_next().await {
        assert!(result.expect("task"), "every waiter sees the leader's answer");
    }

    assert_eq!(server.hits(PUBLIC_ROLES), 1);
}

#[tokio::test]
async fn a_broken_server_is_not_polled_once_per_message() {
    // Failures are cached too. Not caching them would mean a 500 turns into a request per
    // message — the exact behaviour the cache exists to prevent — while the handler stayed
    // closed anyway.
    let server = Server::spawn(|_| Reply::Body(500, failure("internal-error")));
    let directory = directory(&server, config().failure_ttl(Duration::from_secs(60)));

    for _ in 0..20 {
        assert!(!directory.is_admin(&UserId::new("boss")).await);
    }
    assert_eq!(server.hits(PUBLIC_ROLES), 1);
}

#[tokio::test]
async fn a_failure_expires_sooner_than_a_success_so_an_outage_is_not_a_lockout() {
    let server = Server::spawn(|request| {
        if request.index == 0 {
            Reply::Body(503, failure("service-unavailable"))
        } else {
            Reply::ok(public_roles(&["boss"]))
        }
    });
    let directory = directory(&server, config().failure_ttl(Duration::from_millis(50)));
    let boss = UserId::new("boss");

    assert!(!directory.is_admin(&boss).await, "fails closed while the server is down");

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(directory.is_admin(&boss).await, "and recovers on its own once it is back");
    assert_eq!(server.hits(PUBLIC_ROLES), 2);
}

// -------------------------------------------------------------------------------------
// Revocation
// -------------------------------------------------------------------------------------

#[tokio::test]
async fn a_demotion_takes_effect_when_the_ttl_expires() {
    // The backstop. `roles-change` is gated on the `UI_DisplayRoles` setting server-side,
    // and losing a subscription emits nothing at all, so a bound that does not depend on
    // any event is what actually limits how long a revoked role keeps passing.
    let server = Server::spawn(|request| {
        if request.index == 0 {
            Reply::ok(room_roles("GENERAL", &[("alice", "moderator")]))
        } else {
            Reply::ok(room_roles("GENERAL", &[]))
        }
    });
    let directory = directory(&server, config().ttl(Duration::from_millis(50)));
    let alice = UserId::new("alice");
    let room = RoomId::new("GENERAL");

    assert!(directory.is_room_admin(&alice, &room).await);

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!directory.is_room_admin(&alice, &room).await, "the grant expired with the TTL");
}

static REVOKED_RAN: AtomicU32 = AtomicU32::new(0);

#[tokio::test]
async fn a_roles_change_event_revokes_a_grant_before_the_ttl() {
    // The live signal, wired through `dispatch` — the framework applies it without the
    // application doing anything beyond subscribing.
    let server = Server::spawn(|request| match request.path.as_str() {
        "/api/v1/me" => Reply::ok(me()),
        ROOM_ROLES if request.index == 0 => {
            Reply::ok(room_roles("GENERAL", &[("alice", "moderator")]))
        }
        ROOM_ROLES => Reply::ok(room_roles("GENERAL", &[])),
        _ => Reply::Body(404, failure("unknown-endpoint")),
    });

    // An hour-long TTL, so nothing here can pass because of expiry.
    let framework = Framework::new(bot(&server).await, ())
        .role_cache(config().ttl(Duration::from_secs(3600)))
        .handler(counting_handler!("moderators", REVOKED_RAN, Authority::RoomAdmin));

    framework.dispatch(&message("alice", "GENERAL")).await;
    assert_eq!(REVOKED_RAN.load(Ordering::SeqCst), 1);

    framework.dispatch(&roles_change("removed", "moderator", "alice", Some("GENERAL"))).await;

    framework.dispatch(&message("alice", "GENERAL")).await;
    assert_eq!(
        REVOKED_RAN.load(Ordering::SeqCst),
        1,
        "the demotion took effect immediately, without waiting out the TTL"
    );
    assert_eq!(server.hits(ROOM_ROLES), 2, "and cost exactly one refetch");
}

#[tokio::test]
async fn a_role_change_in_another_room_does_not_disturb_this_one() {
    let server = Server::spawn(|_| Reply::ok(room_roles("GENERAL", &[("alice", "owner")])));
    let directory = directory(&server, config());
    let alice = UserId::new("alice");

    assert!(directory.is_room_admin(&alice, &RoomId::new("GENERAL")).await);
    directory.invalidate_room(&RoomId::new("elsewhere"));
    assert!(directory.is_room_admin(&alice, &RoomId::new("GENERAL")).await);

    assert_eq!(server.hits(ROOM_ROLES), 1, "an unrelated room's change is not a refetch");
}

static RECONNECT_RAN: AtomicU32 = AtomicU32::new(0);

#[tokio::test]
async fn a_reconnect_drops_every_grant() {
    // Nothing is replayed for the gap a reconnect leaves, so a role revoked while the
    // socket was down would otherwise keep passing until the TTL ran out.
    let server = Server::spawn(|request| match request.path.as_str() {
        "/api/v1/me" => Reply::ok(me()),
        PUBLIC_ROLES if request.index == 0 => Reply::ok(public_roles(&["boss"])),
        PUBLIC_ROLES => Reply::ok(public_roles(&[])),
        _ => Reply::Body(404, failure("unknown-endpoint")),
    });

    let framework = Framework::new(bot(&server).await, ())
        .role_cache(config().ttl(Duration::from_secs(3600)))
        .handler(counting_handler!("admin", RECONNECT_RAN, Authority::Admin));

    framework.dispatch(&message("boss", "GENERAL")).await;
    assert_eq!(RECONNECT_RAN.load(Ordering::SeqCst), 1);

    framework.dispatch(&ClientEvent::Resubscribed { epoch: Epoch::default(), restored: 3 }).await;

    framework.dispatch(&message("boss", "GENERAL")).await;
    assert_eq!(RECONNECT_RAN.load(Ordering::SeqCst), 1, "the grant was re-checked, not assumed");
    assert_eq!(server.hits(PUBLIC_ROLES), 2);
}

// -------------------------------------------------------------------------------------
// The default costs nothing
// -------------------------------------------------------------------------------------

static PLAIN_RAN: AtomicU32 = AtomicU32::new(0);

#[tokio::test]
async fn a_handler_without_a_role_filter_never_asks_the_server() {
    let server = Server::spawn(|request| match request.path.as_str() {
        "/api/v1/me" => Reply::ok(me()),
        _ => Reply::Body(404, failure("unknown-endpoint")),
    });

    let framework = Framework::new(bot(&server).await, ()).handler(counting_handler!(
        "plain",
        PLAIN_RAN,
        Authority::Anyone
    ));

    framework.dispatch(&message("anyone", "GENERAL")).await;
    assert_eq!(PLAIN_RAN.load(Ordering::SeqCst), 1);
    assert_eq!(server.hits(PUBLIC_ROLES), 0);
    assert_eq!(server.hits(ROOM_ROLES), 0);
}
