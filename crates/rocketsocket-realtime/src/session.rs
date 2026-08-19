//! The connection lifecycle, as a synchronous state machine.
//!
//! Deliberately free of IO, timers and tasks: it consumes decoded [`ServerMessage`]s and
//! returns [`Action`]s for a runner to execute. Serenity does the same with its gateway
//! shard, and the payoff is the same — the protocol logic is exhaustively testable without
//! a socket, a runtime, or a live Rocket.Chat.

use rocketsocket_model::protocol::{ClientMessage, DdpError, ServerMessage};

/// Where a connection is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Phase {
    /// No socket, or the socket is gone.
    Disconnected,
    /// `connect` has been sent; waiting for `connected`.
    Handshaking,
    /// Logged out but connected. A `login` call is outstanding or about to be made.
    Authenticating,
    /// Logged in. Subscriptions may be issued.
    Ready,
    /// Terminally failed. Retrying cannot help; the runner must stop.
    FatallyClosed,
}

/// Why a connection can never succeed as configured.
///
/// Distinguishing these from transient failures is the whole point: retrying any of them
/// is a hot loop against the server, which is the single most common way a bot becomes a
/// nuisance to the workspace it runs in.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Fatal {
    /// The server rejected DDP version negotiation.
    ///
    /// The monolith replies `failed` and closes; reconnecting to propose the same version
    /// again would fail identically.
    #[error("server does not support DDP version {version}, it offered {offered}")]
    VersionMismatch {
        /// The version we proposed.
        version: String,
        /// The version the server countered with.
        offered: String,
    },

    /// The stored resume token is dead — expired, revoked, or logged out server-side.
    ///
    /// Reconnecting with the same token produces the same 403 forever. The caller must
    /// obtain a fresh credential.
    #[error("session expired or revoked: {0}")]
    SessionExpired(String),

    /// Authentication failed for some other reason (bad password, unknown user, 2FA).
    #[error("login rejected: {0}")]
    LoginRejected(String),
}

/// Something a runner should do.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Action {
    /// Write this frame to the socket.
    Send(ClientMessage),
    /// Authenticate now. The runner owns the credential and allocates the call id, then
    /// reports it back via [`Session::expect_login`].
    Login,
    /// Login succeeded; re-issue every remembered subscription.
    ///
    /// The server keeps no subscription state across connections — `Session::close()` tears
    /// them down without sending `nosub`, and DDP session resume is unimplemented in every
    /// Rocket.Chat release — so this is not optional bookkeeping, it is how the bot keeps
    /// receiving anything at all.
    Resubscribe,
    /// Stop. Reconnecting cannot fix this.
    Fatal(Fatal),
    /// The peer sent a frame that violates the protocol. Log it; do not act on it.
    ///
    /// Emitted for a frame whose `msg` this crate models but whose payload did not fit, so
    /// it landed in the catch-all. Left visible rather than swallowed because it usually
    /// means an in-flight call will never be answered.
    ProtocolViolation {
        /// The offending `msg` tag, when the frame had one.
        msg: Option<String>,
    },
}

/// The connection lifecycle state machine.
#[derive(Debug, Clone)]
pub struct Session {
    phase: Phase,
    proposed_version: String,
    session_id: Option<String>,
    login_call: Option<String>,
}

impl Session {
    /// A new, disconnected session proposing the given DDP version.
    #[must_use]
    pub fn new(proposed_version: impl Into<String>) -> Self {
        Self {
            phase: Phase::Disconnected,
            proposed_version: proposed_version.into(),
            session_id: None,
            login_call: None,
        }
    }

    /// The current phase.
    #[must_use]
    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// The server-assigned session id, once handshaking has completed.
    ///
    /// Opaque and observability-only: it is 17 `Random.id()` characters on the monolith and
    /// a UUID on the EE `ddp-streamer`, and it can never be used to resume — both servers
    /// ignore `connect.session`.
    #[must_use]
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Whether the connection is terminally failed.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(self.phase, Phase::FatallyClosed)
    }

    /// Begins a connection, returning the opening frame.
    ///
    /// Note the absence of a `session` member: both server implementations ignore it today,
    /// and Meteor's unreleased resume support pairs it with a `receivedCount` we do not
    /// track — sending a lone `session` to such a server would be worse than sending none.
    pub fn connect(&mut self) -> Action {
        self.phase = Phase::Handshaking;
        self.session_id = None;
        self.login_call = None;
        Action::Send(ClientMessage::connect())
    }

    /// Records the call id of the `login` the runner just sent.
    pub fn expect_login(&mut self, call_id: impl Into<String>) {
        self.login_call = Some(call_id.into());
    }

    /// Notes that the socket is gone, so the next [`connect`](Self::connect) starts clean.
    ///
    /// Keeps a terminal phase terminal: a fatal failure that also closed the socket must not
    /// be downgraded to a retryable disconnect.
    pub fn disconnected(&mut self) {
        if self.phase != Phase::FatallyClosed {
            self.phase = Phase::Disconnected;
        }
        self.session_id = None;
        self.login_call = None;
    }

    /// Feeds one decoded frame in, returning what to do about it.
    ///
    /// Stream events and method results are *not* handled here — they belong to the
    /// subscription registry and the call correlator. This only drives the lifecycle.
    pub fn handle(&mut self, message: &ServerMessage) -> Vec<Action> {
        // A frame naming a tag we model but failing to decode as it would otherwise be
        // silently absorbed by the catch-all, stranding whatever it was answering.
        if message.is_malformed() {
            let msg = match message {
                ServerMessage::Unknown(unknown) => unknown.msg.clone(),
                _ => None,
            };
            return vec![Action::ProtocolViolation { msg }];
        }

        match message {
            // The server pings only when the connection has been idle, and any inbound
            // frame resets its timer — so a busy bot may never see one. When it does, the
            // id must be echoed if present, or the server closes the socket on timeout.
            ServerMessage::Ping { id } => {
                vec![Action::Send(ClientMessage::Pong { id: id.clone() })]
            }

            ServerMessage::Connected { session } => {
                self.session_id = Some(session.clone());
                self.phase = Phase::Authenticating;
                vec![Action::Login]
            }

            ServerMessage::Failed { version } => {
                self.phase = Phase::FatallyClosed;
                vec![Action::Fatal(Fatal::VersionMismatch {
                    version: self.proposed_version.clone(),
                    offered: version.clone(),
                })]
            }

            ServerMessage::Result { id, error, .. } if self.login_call.as_deref() == Some(id) => {
                self.login_call = None;
                match error {
                    None => {
                        self.phase = Phase::Ready;
                        vec![Action::Resubscribe]
                    }
                    Some(error) => {
                        self.phase = Phase::FatallyClosed;
                        vec![Action::Fatal(classify_login_failure(error))]
                    }
                }
            }

            _ => Vec::new(),
        }
    }
}

/// Decides whether a failed login is worth ever retrying.
fn classify_login_failure(error: &DdpError) -> Fatal {
    let describe = || {
        error
            .reason
            .clone()
            .or_else(|| error.message.clone())
            .unwrap_or_else(|| error.error.to_string())
    };

    if error.is_expired_session() {
        Fatal::SessionExpired(describe())
    } else {
        Fatal::LoginRejected(describe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(json: &str) -> ServerMessage {
        serde_json::from_str(json).expect("frame must decode")
    }

    fn session() -> Session {
        let mut session = Session::new("1");
        assert!(matches!(session.connect(), Action::Send(ClientMessage::Connect { .. })));
        session
    }

    #[test]
    fn the_happy_path_walks_disconnected_to_ready() {
        let mut session = Session::new("1");
        assert_eq!(session.phase(), Phase::Disconnected);

        session.connect();
        assert_eq!(session.phase(), Phase::Handshaking);

        let actions =
            session.handle(&decode(r#"{"msg":"connected","session":"6f8Kf4iAaLQ2sJyN9"}"#));
        assert_eq!(actions, vec![Action::Login]);
        assert_eq!(session.phase(), Phase::Authenticating);
        assert_eq!(session.session_id(), Some("6f8Kf4iAaLQ2sJyN9"));

        session.expect_login("m1");
        let actions = session
            .handle(&decode(r#"{"msg":"result","id":"m1","result":{"id":"u1","token":"tok"}}"#));
        assert_eq!(actions, vec![Action::Resubscribe]);
        assert_eq!(session.phase(), Phase::Ready);
    }

    #[test]
    fn both_greeting_frames_are_ignored_rather_than_disturbing_the_handshake() {
        // Meteor <= 2.2 greeted with a frame that has no `msg` at all; the EE ddp-streamer
        // still sends one tagged `server_id`. Neither is an error and neither is a protocol
        // violation.
        let mut session = session();
        for greeting in [r#"{"server_id":"0"}"#, r#"{"msg":"server_id","server_id":"0"}"#] {
            let actions = session.handle(&decode(greeting));
            assert!(actions.is_empty(), "{greeting} should be ignored");
            assert_eq!(session.phase(), Phase::Handshaking);
        }
    }

    #[test]
    fn a_ping_is_answered_and_echoes_the_id() {
        let mut session = session();
        assert_eq!(
            session.handle(&decode(r#"{"msg":"ping"}"#)),
            vec![Action::Send(ClientMessage::Pong { id: None })]
        );
        assert_eq!(
            session.handle(&decode(r#"{"msg":"ping","id":"h7"}"#)),
            vec![Action::Send(ClientMessage::Pong { id: Some("h7".to_owned()) })]
        );
    }

    #[test]
    fn a_ping_is_answered_even_before_login() {
        // Liveness must not depend on lifecycle: a server whose ping goes unanswered closes
        // the socket, and the handshake would never complete.
        let mut session = Session::new("1");
        assert_eq!(session.phase(), Phase::Disconnected);
        assert_eq!(
            session.handle(&decode(r#"{"msg":"ping"}"#)),
            vec![Action::Send(ClientMessage::Pong { id: None })]
        );
    }

    #[test]
    fn version_negotiation_failure_is_terminal() {
        // The server closes the socket after `failed`; proposing the same version again
        // fails identically, so retrying is a hot loop.
        let mut session = session();
        let actions = session.handle(&decode(r#"{"msg":"failed","version":"1"}"#));
        assert_eq!(
            actions,
            vec![Action::Fatal(Fatal::VersionMismatch {
                version: "1".to_owned(),
                offered: "1".to_owned()
            })]
        );
        assert!(session.is_fatal());
    }

    #[test]
    fn a_dead_resume_token_is_terminal_and_named_as_such() {
        // Both wordings, and note ddp-streamer drops the trailing full stop, which is why
        // the model matches on a substring.
        for reason in [
            "You've been logged out by the server. Please log in again.",
            "You've been logged out by the server. Please log in again",
            "Your session has expired. Please log in again.",
        ] {
            let mut session = session();
            session.handle(&decode(r#"{"msg":"connected","session":"s"}"#));
            session.expect_login("m1");

            let frame = serde_json::json!({
                "msg": "result",
                "id": "m1",
                "error": { "error": 403, "reason": reason, "errorType": "Meteor.Error" }
            })
            .to_string();

            let actions = session.handle(&decode(&frame));
            assert!(
                matches!(actions.as_slice(), [Action::Fatal(Fatal::SessionExpired(_))]),
                "{reason} should be classified as an expired session, got {actions:?}"
            );
            assert!(session.is_fatal());
        }
    }

    #[test]
    fn an_ordinary_login_rejection_is_terminal_but_distinguishable() {
        let mut session = session();
        session.handle(&decode(r#"{"msg":"connected","session":"s"}"#));
        session.expect_login("m1");

        let actions = session.handle(&decode(
            r#"{"msg":"result","id":"m1","error":{"error":403,"reason":"Incorrect password"}}"#,
        ));
        assert_eq!(
            actions,
            vec![Action::Fatal(Fatal::LoginRejected("Incorrect password".to_owned()))]
        );
    }

    #[test]
    fn a_result_for_someone_elses_call_does_not_move_the_lifecycle() {
        let mut session = session();
        session.handle(&decode(r#"{"msg":"connected","session":"s"}"#));
        session.expect_login("m1");

        let actions = session.handle(&decode(r#"{"msg":"result","id":"m2","result":true}"#));
        assert!(actions.is_empty());
        assert_eq!(session.phase(), Phase::Authenticating);
    }

    #[test]
    fn a_malformed_frame_is_surfaced_instead_of_being_swallowed() {
        // Absorbed by the untagged catch-all, so nothing else would ever report it, and the
        // call it was answering would hang forever.
        let mut session = session();
        let actions = session.handle(&decode(r#"{"msg":"result","id":"1","error":{"m":"x"}}"#));
        assert_eq!(actions, vec![Action::ProtocolViolation { msg: Some("result".to_owned()) }]);
        // Reporting it must not derail the lifecycle.
        assert_eq!(session.phase(), Phase::Handshaking);
    }

    #[test]
    fn a_genuinely_unknown_frame_is_not_reported_as_a_violation() {
        let mut session = session();
        assert!(session.handle(&decode(r#"{"msg":"someFutureFrame","x":1}"#)).is_empty());
    }

    #[test]
    fn reconnecting_clears_the_old_session_and_replays_subscriptions() {
        let mut session = session();
        session.handle(&decode(r#"{"msg":"connected","session":"first"}"#));
        session.expect_login("m1");
        session.handle(&decode(r#"{"msg":"result","id":"m1","result":{}}"#));
        assert_eq!(session.phase(), Phase::Ready);

        session.disconnected();
        assert_eq!(session.phase(), Phase::Disconnected);
        assert_eq!(session.session_id(), None);

        session.connect();
        session.handle(&decode(r#"{"msg":"connected","session":"second"}"#));
        assert_eq!(session.session_id(), Some("second"));
        session.expect_login("m2");

        // The server remembers no subscriptions across connections, so every reconnect must
        // replay them or the bot goes silent while looking healthy.
        let actions = session.handle(&decode(r#"{"msg":"result","id":"m2","result":{}}"#));
        assert_eq!(actions, vec![Action::Resubscribe]);
    }

    #[test]
    fn a_stale_login_result_after_reconnect_is_ignored() {
        // The correlator tags calls with a connection epoch, but the lifecycle must not
        // depend on that: a late result from the previous socket must not authenticate the
        // new one.
        let mut session = session();
        session.handle(&decode(r#"{"msg":"connected","session":"first"}"#));
        session.expect_login("m1");
        session.disconnected();
        session.connect();
        session.handle(&decode(r#"{"msg":"connected","session":"second"}"#));

        let actions = session.handle(&decode(r#"{"msg":"result","id":"m1","result":{}}"#));
        assert!(actions.is_empty(), "a login from the dead socket must not make us Ready");
        assert_eq!(session.phase(), Phase::Authenticating);
    }

    #[test]
    fn a_fatal_phase_survives_a_subsequent_disconnect() {
        let mut session = session();
        session.handle(&decode(r#"{"msg":"failed","version":"2"}"#));
        assert!(session.is_fatal());
        session.disconnected();
        assert!(session.is_fatal(), "a terminal failure must not decay into a retryable one");
    }
}
