//! The async runner: one task owning the socket.
//!
//! [`Session`] decides *what* the protocol requires; this module does it. One task owns the
//! **unsplit** [`WebSocketStream`] — no `.split()`, no `BiLock` — because a single owner is
//! what makes reconnect tractable: there is exactly one place that knows the socket is gone
//! and exactly one place that replaces it. Siderite's runner has the same shape.
//!
//! The design constraints that are not negotiable, and why:
//!
//! 1. **Protocol frames are written on a separate lane.** `connect`, `login`, `ping` and
//!    `pong` go to a priority queue that is drained ahead of application traffic, so a
//!    saturated outbound queue can never delay a `pong`. The server answers client pings
//!    out of band for precisely this reason; a client that queued its own pongs behind a
//!    slow method would be killed by its own head-of-line blocking.
//! 2. **Every disconnect settles every waiter** through [`Correlator::disconnect`], which
//!    also bumps the epoch. Nothing is left pending: a caller awaiting a reply that can
//!    never arrive is a hang, which is worse than any error it could be handed instead.
//! 3. **[`Fatal`] ends the event stream.** The runner returns, the broadcast sender drops,
//!    and the consumer's `while let Some(event)` loop ends — twilight's `FatallyClosed`
//!    mapping to `Poll::Ready(None)`. Retrying a dead resume token forever is the classic
//!    way a bot becomes a nuisance to the workspace it runs in.
//! 4. **Reconnect drives the whole chain** — connect, `connected`, `login`, resubscribe —
//!    because no Rocket.Chat release implements DDP session resume. See
//!    [`Event::Resubscribe`] for the contract the subscription registry relies on.

use core::fmt;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::collections::VecDeque;
use std::sync::Arc;

use futures_util::stream::Stream;
use futures_util::{SinkExt as _, StreamExt as _};
use rocketsocket_model::protocol::{ClientMessage, ServerMessage};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Instant as Deadline, Sleep};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, trace, warn};

use crate::backoff::Backoff;
use crate::correlate::{CallError, Correlator, Epoch, Resolution};
use crate::liveness::{Liveness, LivenessPolicy};
use crate::session::{Action, Fatal, Phase, Session};

/// The socket type produced by [`connect_async`].
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

// ---------------------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------------------

/// How to authenticate the DDP session.
///
/// Not `Debug`-derived on purpose: a token that leaks into a log line is a credential leak.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Credential {
    /// A resume token — a Personal Access Token, or the `authToken` from `POST
    /// /api/v1/login`.
    ///
    /// The only portable form. The EE `ddp-streamer` destructures nothing else
    /// (`ee/apps/ddp-streamer/src/configureServer.ts:68` reads `{ resume }` and no other
    /// key), so password login simply does not exist on microservice deployments. It also
    /// sidesteps 2FA — logins of `type: 'resume'` are exempt from `onValidateLogin`.
    Resume(String),

    /// A username and a plaintext password.
    ///
    /// **Monolith only**, and second best even there: it re-enters the full login handler
    /// chain on every reconnect, mints a fresh entry in `services.resume.loginTokens` each
    /// time, and so churns through the `MAX_RESUME_LOGIN_TOKENS` cap (default 50). Prefer
    /// exchanging it once over REST and using [`Credential::Resume`] thereafter.
    Password {
        /// Username (not an email address).
        user: String,
        /// The password, sent as-is; use `wss://`.
        password: String,
    },
}

impl Credential {
    /// The `params` array for the `login` method.
    fn params(&self) -> Vec<Value> {
        match self {
            Self::Resume(token) => vec![json!({ "resume": token })],
            Self::Password { user, password } => {
                vec![json!({ "user": { "username": user }, "password": password })]
            }
        }
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resume(_) => f.write_str("Credential::Resume(<redacted>)"),
            Self::Password { user, .. } => {
                f.debug_struct("Credential::Password").field("user", user).finish_non_exhaustive()
            }
        }
    }
}

/// Everything the runner needs to keep a connection alive.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// The websocket endpoint, e.g. `wss://chat.example.com/websocket`.
    pub url: String,
    /// How to log in, once the handshake completes.
    pub credential: Credential,
    /// Budget for the TCP connect plus the HTTP upgrade.
    pub connect_timeout: Duration,
    /// Budget for `connect` → `connected` → `login` → `result`, measured from the moment
    /// the socket opens. A server that accepts the upgrade and then says nothing is the
    /// most common shape of a wedged deployment, and it must not hold the runner forever.
    pub handshake_timeout: Duration,
    /// Idle time before the runner sends a DDP `ping`.
    pub ping_after: Duration,
    /// Idle time before the runner declares the connection dead and reconnects.
    pub dead_after: Duration,
    /// Depth of the command queue between the handles and the runner. When it fills,
    /// [`Connection::call`] applies backpressure to its caller rather than growing.
    pub command_capacity: usize,
    /// How many application frames may sit unwritten before commands stop being accepted.
    /// Protocol frames are never subject to this.
    pub outbox_capacity: usize,
    /// Depth of the event broadcast ring. A consumer slower than this sees
    /// [`Event::Lagged`] rather than silently losing frames.
    pub event_capacity: usize,
    /// First reconnect window; see [`Backoff`].
    pub backoff_base: Duration,
    /// Ceiling on the reconnect window.
    pub backoff_cap: Duration,
}

impl Config {
    /// A configuration with defaults chosen to sit inside both servers' timeout budgets.
    #[must_use]
    pub fn new(url: impl Into<String>, credential: Credential) -> Self {
        Self {
            url: url.into(),
            credential,
            connect_timeout: Duration::from_secs(15),
            handshake_timeout: Duration::from_secs(20),
            ping_after: LivenessPolicy::DEFAULT_PING_AFTER,
            dead_after: LivenessPolicy::DEFAULT_DEAD_AFTER,
            command_capacity: 64,
            outbox_capacity: 256,
            event_capacity: 1024,
            backoff_base: Backoff::DEFAULT_BASE,
            backoff_cap: Backoff::DEFAULT_CAP,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Public surface
// ---------------------------------------------------------------------------------------

/// Where the connection is, for observers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionState {
    /// Waiting out a reconnect delay.
    Disconnected,
    /// Opening a socket.
    Connecting,
    /// Socket open, `connect` sent, waiting for `connected`.
    Handshaking,
    /// `login` outstanding.
    Authenticating,
    /// Logged in; subscriptions and calls flow.
    Ready,
    /// Shut down at the caller's request, or because every handle was dropped.
    Closed,
    /// Terminally failed. The event stream has ended.
    Fatal,
}

/// Something the runner wants the application to know.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Event {
    /// A connection attempt is starting. `attempt` counts consecutive failures.
    Connecting {
        /// Consecutive failed attempts before this one.
        attempt: u32,
    },

    /// The handshake completed; the session id is observability-only.
    Connected {
        /// The server-assigned session id. Opaque, and useless for resuming.
        session: String,
    },

    /// Login succeeded. Calls and subscriptions will now be written.
    Ready {
        /// The connection generation this readiness belongs to.
        epoch: Epoch,
    },

    /// **Replay every subscription now.**
    ///
    /// The contract: on receiving this, the subscription registry must re-issue
    /// [`Connection::subscribe`] for every subscription it wants live, with **fresh ids**
    /// (`sub` is idempotent by id and is silently dropped if the id is already known — no
    /// `ready`, no `nosub`, the future hangs). The runner deliberately does not replay
    /// anything itself: it does not know what you subscribed to, and inventing a registry
    /// here would duplicate the one in `subscription.rs`.
    ///
    /// This is not optional bookkeeping. `Session::close()` on both servers tears every
    /// subscription down *without* sending `nosub`, and no Rocket.Chat release implements
    /// DDP session resume, so a client that skips this stays connected and goes silent —
    /// the worst possible failure mode for a bot.
    ///
    /// Emitted after [`Event::Ready`] for the same epoch, and after every reconnect.
    Resubscribe {
        /// The connection generation to resubscribe on. A registry that is still replaying
        /// when a newer epoch arrives should abandon the older replay.
        epoch: Epoch,
    },

    /// A frame the runner did not consume itself: `added` / `changed` / `removed` /
    /// `addedBefore` / `movedBefore`, and server-level `error` reports.
    ///
    /// Stream events are the `changed` frames for which
    /// [`ServerMessage::as_stream_event`] returns `Some`; dispatch on
    /// `(collection, eventName)`, never on the document id.
    Frame(ServerMessage),

    /// The socket is gone. A reconnect follows unless the stream also ends.
    Disconnected {
        /// Human-readable cause, for logs.
        reason: String,
    },

    /// Terminal. This is the last event; the stream ends immediately after it.
    Fatal(Fatal),

    /// The consumer fell behind and the ring overwrote events.
    ///
    /// Surfaced rather than swallowed: a bot that silently missed four hundred messages is
    /// far worse than one that says so.
    Lagged {
        /// How many events were lost.
        missed: u64,
    },
}

/// A cheap, cloneable handle to a running connection.
#[derive(Debug, Clone)]
pub struct Connection {
    commands: mpsc::Sender<Command>,
    /// A parked receiver, kept only so [`Connection::events`] can subscribe later. It is
    /// deliberately *not* a `Sender`: a sender held here would keep the broadcast channel
    /// open after the runner exits, and [`Fatal`] would never end the stream.
    events: Arc<broadcast::Receiver<Event>>,
    state: watch::Receiver<ConnectionState>,
}

/// A consumer's view of the event stream.
///
/// [`Events::recv`] returns `None` exactly once, when the runner has stopped for good —
/// terminal failure, an explicit shutdown, or every handle dropped.
#[derive(Debug)]
pub struct Events {
    receiver: broadcast::Receiver<Event>,
}

impl Events {
    /// The next event, or `None` once the connection has stopped for good.
    pub async fn recv(&mut self) -> Option<Event> {
        match self.receiver.recv().await {
            Ok(event) => Some(event),
            Err(broadcast::error::RecvError::Lagged(missed)) => Some(Event::Lagged { missed }),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }

    /// The same events as a [`Stream`], for `while let Some(event) = stream.next().await`.
    pub fn into_stream(self) -> impl Stream<Item = Event> {
        futures_util::stream::unfold(self, |mut events| async move {
            events.recv().await.map(|event| (event, events))
        })
    }
}

impl Connection {
    /// Spawns the runner and returns a handle plus the first consumer.
    ///
    /// The consumer is subscribed *before* the runner starts, so no event is missed. Must
    /// be called from within a Tokio runtime.
    #[must_use]
    pub fn spawn(config: Config) -> (Self, Events) {
        let (commands, command_rx) = mpsc::channel(config.command_capacity.max(1));
        let (events, receiver) = broadcast::channel(config.event_capacity.max(1));
        let (state_tx, state) = watch::channel(ConnectionState::Disconnected);

        let handle = Self { commands, events: Arc::new(events.subscribe()), state: state.clone() };

        let runner = Runner::new(config, command_rx, events, state_tx);
        tokio::spawn(runner.run());

        (handle, Events { receiver })
    }

    /// An additional independent consumer, starting from the current tail.
    #[must_use]
    pub fn events(&self) -> Events {
        Events { receiver: self.events.resubscribe() }
    }

    /// The connection state, as a watch channel.
    #[must_use]
    pub fn state(&self) -> watch::Receiver<ConnectionState> {
        self.state.clone()
    }

    /// Calls a DDP method and waits for its `result`.
    ///
    /// A call issued while the connection is down is queued, not failed, and goes out once
    /// the session is `Ready` again — up to `command_capacity`, past which this awaits.
    ///
    /// # Errors
    /// [`CallError::Server`] if the server answered with an error, [`CallError::NotSent`]
    /// if the frame never reached the wire (safe to retry), or [`CallError::Abandoned`] if
    /// it did and the connection died before the reply (**not** safe to blindly retry).
    pub async fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, CallError> {
        let (ack, pending) = oneshot::channel();
        let command = Command::Call { method: method.to_owned(), params, ack };
        self.await_reply(command, pending).await
    }

    /// Issues a `sub` and waits for `ready` or `nosub`, returning the subscription id.
    ///
    /// The id is what an eventual `unsub` must carry; the runner allocates a fresh one per
    /// call, which is mandatory — `sub` with an id the server already knows is silently
    /// dropped and its future would never settle.
    ///
    /// # Errors
    /// As [`Connection::call`].
    pub async fn subscribe(&self, name: &str, params: Vec<Value>) -> Result<String, CallError> {
        let (ack, pending) = oneshot::channel();
        let command = Command::Subscribe { name: name.to_owned(), params, ack };
        let (id, reply) = match pending_of(&self.commands, command, pending).await {
            Ok(pending) => (pending.id, pending.reply),
            Err(error) => return Err(error),
        };
        match reply.await {
            Ok(Ok(_)) => Ok(id),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(CallError::NotSent),
        }
    }

    /// Cancels a subscription. Fire and forget: `unsub` for an id the EE `ddp-streamer`
    /// does not know is answered with silence, so waiting for `nosub` can hang.
    ///
    /// Must not be issued for a subscription whose [`Connection::subscribe`] has not
    /// settled yet — `sub` and `unsub` share an id space and the `nosub` would settle the
    /// pending `sub`. Serialising that per id belongs to the subscription registry.
    pub async fn unsubscribe(&self, id: impl Into<String>) {
        let _ = self.commands.send(Command::Unsubscribe { id: id.into() }).await;
    }

    /// Stops the runner and ends the event stream.
    pub async fn shutdown(&self) {
        let _ = self.commands.send(Command::Shutdown).await;
    }

    async fn await_reply(
        &self,
        command: Command,
        pending: oneshot::Receiver<Pending>,
    ) -> Result<Value, CallError> {
        let pending = pending_of(&self.commands, command, pending).await?;
        match pending.reply.await {
            Ok(outcome) => outcome,
            // The correlator settles every entry on disconnect, so a dropped sender can
            // only mean the runner died between registering and settling.
            Err(_) => Err(CallError::NotSent),
        }
    }
}

/// Hands a command to the runner and waits for it to register the call.
async fn pending_of(
    commands: &mpsc::Sender<Command>,
    command: Command,
    pending: oneshot::Receiver<Pending>,
) -> Result<Pending, CallError> {
    if commands.send(command).await.is_err() {
        return Err(CallError::NotSent);
    }
    pending.await.map_err(|_| CallError::NotSent)
}

// ---------------------------------------------------------------------------------------
// Runner internals
// ---------------------------------------------------------------------------------------

/// A registered but unsettled call.
#[derive(Debug)]
struct Pending {
    id: String,
    reply: oneshot::Receiver<Result<Value, CallError>>,
}

#[derive(Debug)]
enum Command {
    Call { method: String, params: Vec<Value>, ack: oneshot::Sender<Pending> },
    Subscribe { name: String, params: Vec<Value>, ack: oneshot::Sender<Pending> },
    Unsubscribe { id: String },
    Shutdown,
}

/// Which correlator to mark once a frame has actually been flushed.
#[derive(Debug)]
enum Mark {
    Method(String),
    Sub(String),
}

/// A serialised frame waiting for the sink.
#[derive(Debug)]
struct Outbound {
    frame: Message,
    mark: Option<Mark>,
}

/// One turn of the socket loop.
#[derive(Debug)]
enum Step {
    Frame(Message),
    Eof,
    Failed(WsError),
    Command(Command),
    HandlesDropped,
    Deadline,
}

/// Why the socket loop returned.
#[derive(Debug)]
enum Outcome {
    Closed(String),
    Shutdown,
    Fatal(Fatal),
}

struct Runner {
    config: Config,
    session: Session,
    methods: Correlator,
    subs: Correlator,
    liveness: LivenessPolicy,
    backoff: Backoff,

    commands: mpsc::Receiver<Command>,
    events: broadcast::Sender<Event>,
    state: watch::Sender<ConnectionState>,

    socket: Option<Socket>,
    /// Protocol frames: `connect`, `login`, `ping`, `pong`. Drained first, always.
    priority: VecDeque<Outbound>,
    /// Application frames: `method`, `sub`, `unsub`. Written only once `Ready`.
    app: VecDeque<Outbound>,
    /// Frames handed to the sink but not yet flushed; marked sent only once they are.
    unflushed: Vec<Mark>,
    needs_flush: bool,

    timer: Pin<Box<Sleep>>,
    handshake_deadline: Option<Deadline>,
    /// Kept alive so the login `result` resolves to a live waiter instead of logging a
    /// spurious "caller gone".
    login_reply: Option<oneshot::Receiver<Result<Value, CallError>>>,
    fatal: Option<Fatal>,
    shutdown: bool,
    /// Alternates read-first and command-first polling so neither can starve the other.
    turn: bool,
}

/// The monotonic clock the timers use, as `std::time::Instant` for [`LivenessPolicy`].
///
/// Deliberately routed through Tokio's clock so `tokio::time::pause()` moves it, which is
/// what makes the timing tests deterministic.
fn now() -> std::time::Instant {
    Deadline::now().into_std()
}

impl Runner {
    fn new(
        config: Config,
        commands: mpsc::Receiver<Command>,
        events: broadcast::Sender<Event>,
        state: watch::Sender<ConnectionState>,
    ) -> Self {
        let liveness = LivenessPolicy::with_thresholds(config.ping_after, config.dead_after, now());
        let backoff = Backoff::with_limits(config.backoff_base, config.backoff_cap);
        Self {
            session: Session::new(ClientMessage::DDP_VERSION),
            methods: Correlator::new("m"),
            subs: Correlator::new("s"),
            liveness,
            backoff,
            config,
            commands,
            events,
            state,
            socket: None,
            priority: VecDeque::new(),
            app: VecDeque::new(),
            unflushed: Vec::new(),
            needs_flush: false,
            timer: Box::pin(tokio::time::sleep(Duration::from_secs(0))),
            handshake_deadline: None,
            login_reply: None,
            fatal: None,
            shutdown: false,
            turn: false,
        }
    }

    async fn run(mut self) {
        while !self.shutdown {
            let delay = self.backoff.next_delay();
            if !delay.is_zero() {
                self.set_state(ConnectionState::Disconnected);
                self.idle(Deadline::now() + delay).await;
                if self.shutdown {
                    break;
                }
            }

            self.set_state(ConnectionState::Connecting);
            self.emit(Event::Connecting { attempt: self.backoff.attempt().saturating_sub(1) });

            let socket = match self.open().await {
                Ok(socket) => socket,
                Err(reason) => {
                    self.disconnected(&reason);
                    continue;
                }
            };

            match self.drive(socket).await {
                Outcome::Closed(reason) => {
                    self.socket = None;
                    self.disconnected(&reason);
                }
                Outcome::Shutdown => {
                    self.close_politely().await;
                    self.settle();
                    self.session.disconnected();
                    self.set_state(ConnectionState::Closed);
                    break;
                }
                Outcome::Fatal(fatal) => {
                    self.close_politely().await;
                    self.settle();
                    self.set_state(ConnectionState::Fatal);
                    // The last event anyone sees: dropping `self` drops the broadcast
                    // sender, which ends the consumer's loop.
                    self.emit(Event::Fatal(fatal));
                    break;
                }
            }
        }

        if !matches!(*self.state.borrow(), ConnectionState::Fatal | ConnectionState::Closed) {
            self.set_state(ConnectionState::Closed);
        }
        self.settle();
    }

    /// Waits out a reconnect delay, still servicing commands so callers are not blocked
    /// for the whole window and so a dropped handle is noticed promptly.
    async fn idle(&mut self, until: Deadline) {
        let sleep = tokio::time::sleep_until(until);
        tokio::pin!(sleep);
        loop {
            let capacity = self.config.outbox_capacity;
            tokio::select! {
                () = &mut sleep => return,
                command = self.commands.recv(), if self.app.len() < capacity => match command {
                    Some(command) => {
                        self.handle_command(command);
                        if self.shutdown {
                            return;
                        }
                    }
                    None => {
                        self.shutdown = true;
                        return;
                    }
                },
            }
        }
    }

    async fn open(&mut self) -> Result<Socket, String> {
        let attempt = connect_async(self.config.url.as_str());
        match tokio::time::timeout(self.config.connect_timeout, attempt).await {
            Ok(Ok((socket, _response))) => Ok(socket),
            Ok(Err(error)) => Err(format!("websocket handshake failed: {error}")),
            Err(_) => Err("websocket handshake timed out".to_owned()),
        }
    }

    /// Runs one socket until it dies, fails terminally, or the caller shuts down.
    async fn drive(&mut self, socket: Socket) -> Outcome {
        self.socket = Some(socket);
        self.liveness =
            LivenessPolicy::with_thresholds(self.config.ping_after, self.config.dead_after, now());
        self.handshake_deadline = Some(Deadline::now() + self.config.handshake_timeout);

        let action = self.session.connect();
        self.apply(action);
        self.sync_state();
        self.arm_timer();

        loop {
            let step = poll_fn(|cx| self.poll_step(cx)).await;

            let outcome = match step {
                Step::Frame(frame) => self.on_frame(frame),
                Step::Eof => Some(Outcome::Closed("the server closed the socket".to_owned())),
                Step::Failed(error) => Some(Outcome::Closed(format!("socket error: {error}"))),
                Step::Command(command) => {
                    self.handle_command(command);
                    None
                }
                Step::HandlesDropped => Some(Outcome::Shutdown),
                Step::Deadline => self.on_deadline(),
            };

            self.sync_state();

            if let Some(fatal) = self.fatal.take() {
                return Outcome::Fatal(fatal);
            }
            if self.shutdown {
                return Outcome::Shutdown;
            }
            if let Some(outcome) = outcome {
                return outcome;
            }

            self.arm_timer();
        }
    }

    /// Polls writes, reads, commands and the timer against a single `&mut` socket.
    ///
    /// Hand-rolled rather than `select!` because every socket arm would need its own
    /// mutable borrow of the stream, which `select!` cannot give out simultaneously. Each
    /// source is polled to `Pending` or reported, so no wakeup is lost.
    fn poll_step(&mut self, cx: &mut Context<'_>) -> Poll<Step> {
        let socket = self.socket.as_mut().expect("drive() owns a socket for its lifetime");
        let writable = self.session.phase() == Phase::Ready;

        // 1. Writes. Protocol frames unconditionally, application frames only once the
        //    session is Ready — which is also what keeps `login` ahead of everything else.
        loop {
            let application_pending = writable && !self.app.is_empty();
            if self.priority.is_empty() && !application_pending {
                break;
            }
            match socket.poll_ready_unpin(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Step::Failed(error)),
                Poll::Pending => break,
            }
            let outbound = match self.priority.pop_front() {
                Some(outbound) => outbound,
                None => self.app.pop_front().expect("checked non-empty above"),
            };
            if let Err(error) = socket.start_send_unpin(outbound.frame) {
                return Poll::Ready(Step::Failed(error));
            }
            if let Some(mark) = outbound.mark {
                self.unflushed.push(mark);
            }
            self.needs_flush = true;
        }

        if self.needs_flush {
            match socket.poll_flush_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    self.needs_flush = false;
                    // Marked only now: a frame sitting in the sink's buffer when the
                    // socket dies never reached the server, and must settle as `NotSent`.
                    for mark in self.unflushed.drain(..) {
                        match mark {
                            Mark::Method(id) => self.methods.mark_sent(&id),
                            Mark::Sub(id) => self.subs.mark_sent(&id),
                        }
                    }
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Step::Failed(error)),
                Poll::Pending => {}
            }
        }

        // 2. Reads and commands, alternating so a busy socket cannot starve callers.
        let accept_commands = self.app.len() < self.config.outbox_capacity;
        self.turn = !self.turn;
        if self.turn {
            if let Poll::Ready(step) = poll_read(socket, cx) {
                return Poll::Ready(step);
            }
            if accept_commands && let Poll::Ready(step) = poll_commands(&mut self.commands, cx) {
                return Poll::Ready(step);
            }
        } else {
            if accept_commands && let Poll::Ready(step) = poll_commands(&mut self.commands, cx) {
                return Poll::Ready(step);
            }
            if let Poll::Ready(step) = poll_read(socket, cx) {
                return Poll::Ready(step);
            }
        }

        // 3. Timers last: liveness and the handshake budget never preempt real traffic.
        if self.timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Step::Deadline);
        }

        Poll::Pending
    }

    fn on_frame(&mut self, frame: Message) -> Option<Outcome> {
        // Every inbound frame is evidence of life, whatever it carries — which mirrors
        // both servers, who reset their own heartbeat on any inbound message.
        self.liveness.saw_traffic(now());

        match frame {
            Message::Text(text) => {
                self.on_text(text.as_str());
                None
            }
            Message::Close(frame) => {
                let reason = frame.map_or_else(
                    || "the server sent a close frame".to_owned(),
                    |frame| format!("the server closed with {}: {}", frame.code, frame.reason),
                );
                Some(Outcome::Closed(reason))
            }
            // Tungstenite answers websocket-level control frames itself.
            Message::Ping(_) | Message::Pong(_) => None,
            Message::Binary(payload) => {
                warn!(bytes = payload.len(), "ignoring a binary frame: DDP is text-only");
                None
            }
            Message::Frame(_) => None,
        }
    }

    fn on_text(&mut self, text: &str) {
        let message: ServerMessage = match serde_json::from_str(text) {
            Ok(message) => message,
            Err(error) => {
                // Almost anything shaped like a JSON object decodes into the catch-all, so
                // reaching here means the frame was not even that.
                warn!(%error, frame = %truncate(text), "undecodable frame");
                return;
            }
        };

        for action in self.session.handle(&message) {
            self.apply(action);
        }
        self.correlate(&message);

        match &message {
            ServerMessage::Connected { session } => {
                self.emit(Event::Connected { session: session.clone() });
            }
            ServerMessage::Added { .. }
            | ServerMessage::Changed { .. }
            | ServerMessage::Removed { .. }
            | ServerMessage::AddedBefore { .. }
            | ServerMessage::MovedBefore { .. } => self.emit(Event::Frame(message)),
            ServerMessage::Error { reason, offending_message } => {
                warn!(%reason, offending = ?offending_message, "the server rejected a frame");
                self.emit(Event::Frame(message));
            }
            _ => {}
        }
    }

    fn apply(&mut self, action: Action) {
        match action {
            Action::Send(frame) => self.push_priority(&frame, None),

            Action::Login => {
                let (id, reply) = self.methods.register();
                self.session.expect_login(&id);
                self.login_reply = Some(reply);
                let params = self.config.credential.params();
                let frame = ClientMessage::method(&id, "login", params);
                // On the protocol lane: login must precede every application frame, and
                // subscribing before `setUserId` reruns every subscription server-side.
                self.push_priority(&frame, Some(Mark::Method(id)));
            }

            Action::Resubscribe => {
                let epoch = self.methods.epoch();
                self.handshake_deadline = None;
                self.backoff.reset();
                self.login_reply = None;
                self.sync_state();
                self.emit(Event::Ready { epoch });
                self.emit(Event::Resubscribe { epoch });
            }

            Action::Fatal(fatal) => {
                warn!(%fatal, "the connection failed terminally; not retrying");
                self.fatal = Some(fatal);
            }

            // Loudly, and never swallowed: a frame that landed in the catch-all despite
            // naming a tag we model usually means an in-flight call will never be answered.
            Action::ProtocolViolation { msg } => {
                warn!(msg = ?msg, "protocol violation: a modelled frame failed to decode");
            }
        }
    }

    fn correlate(&mut self, message: &ServerMessage) {
        match message {
            ServerMessage::Result { id, result, error } => {
                let outcome = match error {
                    Some(error) => Err(error.clone()),
                    // `ddp-streamer` omits `result` for a falsy return value, so absence
                    // means null rather than "no answer".
                    None => Ok(result.clone().unwrap_or(Value::Null)),
                };
                let resolution = self.methods.resolve(id, outcome);
                note(id, "result", resolution);
            }
            // An array, and it can batch several ids — reading `subs[0]` loses the rest.
            ServerMessage::Ready { subs } => {
                for id in subs {
                    let resolution = self.subs.resolve(id, Ok(Value::Null));
                    note(id, "ready", resolution);
                }
            }
            ServerMessage::Nosub { id, error } => {
                let outcome = match error {
                    Some(error) => Err(error.clone()),
                    None => Ok(Value::Null),
                };
                let resolution = self.subs.resolve(id, outcome);
                note(id, "nosub", resolution);
            }
            _ => {}
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Call { method, params, ack } => {
                let (id, reply) = self.methods.register();
                if ack.send(Pending { id: id.clone(), reply }).is_err() {
                    // The caller vanished between sending and registering; forget the entry
                    // rather than leaving it to be abandoned at the next disconnect.
                    self.methods.cancel(&id);
                    return;
                }
                let frame = ClientMessage::method(&id, method, params);
                self.push_app(&frame, Some(Mark::Method(id)));
            }

            Command::Subscribe { name, params, ack } => {
                let (id, reply) = self.subs.register();
                if ack.send(Pending { id: id.clone(), reply }).is_err() {
                    self.subs.cancel(&id);
                    return;
                }
                let frame = ClientMessage::sub(&id, name, params);
                self.push_app(&frame, Some(Mark::Sub(id)));
            }

            Command::Unsubscribe { id } => {
                self.push_app(&ClientMessage::Unsub { id }, None);
            }

            Command::Shutdown => self.shutdown = true,
        }
    }

    fn on_deadline(&mut self) -> Option<Outcome> {
        let now = now();

        if self.session.phase() == Phase::Ready {
            self.handshake_deadline = None;
        }
        if let Some(deadline) = self.handshake_deadline
            && Deadline::now() >= deadline
        {
            return Some(Outcome::Closed(format!(
                "the handshake did not complete within {:?}",
                self.config.handshake_timeout
            )));
        }

        match self.liveness.poll(now) {
            Liveness::Healthy => None,
            Liveness::SendPing => {
                self.push_priority(&ClientMessage::Ping { id: None }, None);
                None
            }
            Liveness::Dead => Some(Outcome::Closed(format!(
                "no traffic for {:?}; assuming the connection is dead",
                self.config.dead_after
            ))),
        }
    }

    fn arm_timer(&mut self) {
        let liveness = Deadline::from_std(self.liveness.next_deadline(now()));
        let deadline = match self.handshake_deadline {
            Some(handshake) => liveness.min(handshake),
            None => liveness,
        };
        self.timer.as_mut().reset(deadline);
    }

    fn push_priority(&mut self, frame: &ClientMessage, mark: Option<Mark>) {
        let frame = encode(frame);
        self.priority.push_back(Outbound { frame, mark });
    }

    fn push_app(&mut self, frame: &ClientMessage, mark: Option<Mark>) {
        let frame = encode(frame);
        self.app.push_back(Outbound { frame, mark });
    }

    /// Best-effort close handshake, bounded so a wedged peer cannot hold the runner.
    async fn close_politely(&mut self) {
        if let Some(mut socket) = self.socket.take() {
            let _ = tokio::time::timeout(Duration::from_secs(1), socket.close(None)).await;
        }
    }

    /// Ends the current connection: drops unwritten frames and settles every waiter.
    fn settle(&mut self) {
        self.priority.clear();
        self.app.clear();
        self.unflushed.clear();
        self.needs_flush = false;
        self.login_reply = None;
        self.handshake_deadline = None;

        let abandoned = self.methods.disconnect() + self.subs.disconnect();
        if abandoned > 0 {
            debug!(abandoned, "settled every waiter on the closed connection");
        }
    }

    fn disconnected(&mut self, reason: &str) {
        debug!(%reason, "connection lost");
        self.session.disconnected();
        self.settle();
        self.set_state(ConnectionState::Disconnected);
        self.emit(Event::Disconnected { reason: reason.to_owned() });
    }

    fn sync_state(&mut self) {
        let state = match self.session.phase() {
            Phase::Disconnected => ConnectionState::Disconnected,
            Phase::Handshaking => ConnectionState::Handshaking,
            Phase::Authenticating => ConnectionState::Authenticating,
            Phase::Ready => ConnectionState::Ready,
            Phase::FatallyClosed => ConnectionState::Fatal,
        };
        self.set_state(state);
    }

    fn set_state(&mut self, state: ConnectionState) {
        self.state.send_if_modified(|current| {
            if *current == state {
                false
            } else {
                *current = state;
                true
            }
        });
    }

    fn emit(&self, event: Event) {
        // An error only means nobody is listening right now, which is not our problem.
        let _ = self.events.send(event);
    }
}

fn poll_read(socket: &mut Socket, cx: &mut Context<'_>) -> Poll<Step> {
    match socket.poll_next_unpin(cx) {
        Poll::Ready(Some(Ok(frame))) => Poll::Ready(Step::Frame(frame)),
        Poll::Ready(Some(Err(error))) => Poll::Ready(Step::Failed(error)),
        Poll::Ready(None) => Poll::Ready(Step::Eof),
        Poll::Pending => Poll::Pending,
    }
}

fn poll_commands(commands: &mut mpsc::Receiver<Command>, cx: &mut Context<'_>) -> Poll<Step> {
    match commands.poll_recv(cx) {
        Poll::Ready(Some(command)) => Poll::Ready(Step::Command(command)),
        Poll::Ready(None) => Poll::Ready(Step::HandlesDropped),
        Poll::Pending => Poll::Pending,
    }
}

/// Serialises a client frame.
///
/// Infallible in practice: every field is a `String` or a `serde_json::Value`, and `Value`
/// cannot hold the non-finite floats or non-string keys that `serde_json` rejects.
fn encode(frame: &ClientMessage) -> Message {
    Message::text(serde_json::to_string(frame).expect("a ClientMessage always serialises"))
}

/// Never tear a connection down over an unrecognised id — a stray frame turning into a
/// full disconnect is siderite's bug, and it is self-inflicted.
fn note(id: &str, kind: &str, resolution: Resolution) {
    match resolution {
        Resolution::Delivered => trace!(id, kind, "reply delivered"),
        Resolution::Unknown => debug!(id, kind, "reply for an unknown id; ignoring"),
        Resolution::StaleEpoch => debug!(id, kind, "reply from a superseded connection"),
        Resolution::CallerGone => trace!(id, kind, "reply arrived after the caller gave up"),
    }
}

fn truncate(text: &str) -> String {
    const LIMIT: usize = 256;
    if text.len() <= LIMIT {
        return text.to_owned();
    }
    let mut end = LIMIT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests {
    use core::fmt::Write as _;
    use std::net::SocketAddr;
    use std::sync::Mutex;

    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::accept_async;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event as LogEvent, Level, Metadata, Subscriber};

    use super::*;

    /// Every wait in these tests is bounded, so a regression fails instead of wedging the
    /// suite. Generous, because it is only ever paid by a broken build.
    const PATIENCE: Duration = Duration::from_secs(10);

    /// Timers are milliseconds rather than the production seconds, and the clock is real.
    ///
    /// `tokio::time::pause()` was the obvious choice and is the wrong one here: with the
    /// clock paused the runtime auto-advances to the next timer whenever it is about to
    /// park, and a socket round trip over loopback *does* park. The clock then jumps a
    /// whole handshake budget mid-handshake and every test tears its connection down. Real
    /// time with millisecond thresholds keeps the suite deterministic — every wait is on an
    /// event, never on a duration — and the whole module still runs in well under a second.
    const HANDSHAKE_BUDGET: Duration = Duration::from_millis(150);

    // -----------------------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------------------

    /// An in-process DDP server whose behaviour each test scripts by hand.
    struct Harness {
        addr: SocketAddr,
        sockets: mpsc::Receiver<WebSocketStream<TcpStream>>,
    }

    impl Harness {
        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local_addr");
            let (tx, sockets) = mpsc::channel(8);

            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        if let Ok(socket) = accept_async(stream).await {
                            let _ = tx.send(socket).await;
                        }
                    });
                }
            });

            Self { addr, sockets }
        }

        /// Thresholds far enough out that only a test that asks for one ever fires.
        fn config(&self) -> Config {
            let mut config = Config::new(
                format!("ws://{}/websocket", self.addr),
                Credential::Resume("tok".into()),
            );
            config.connect_timeout = Duration::from_secs(5);
            config.handshake_timeout = Duration::from_secs(5);
            config.ping_after = Duration::from_secs(5);
            config.dead_after = Duration::from_secs(10);
            config.backoff_base = Duration::from_millis(1);
            config.backoff_cap = Duration::from_millis(5);
            config
        }

        fn spawn(&self) -> (Connection, Events) {
            Connection::spawn(self.config())
        }

        fn spawn_with(&self, tune: impl FnOnce(&mut Config)) -> (Connection, Events) {
            let mut config = self.config();
            tune(&mut config);
            Connection::spawn(config)
        }

        async fn accept(&mut self) -> Peer {
            let socket = tokio::time::timeout(PATIENCE, self.sockets.recv())
                .await
                .expect("no client connected")
                .expect("accept loop died");
            Peer { socket }
        }

        /// Asserts the client is not retrying — the property that separates a terminal
        /// failure from a transient one.
        async fn expect_no_reconnect(&mut self) {
            settle().await;
            assert!(self.sockets.try_recv().is_err(), "the client reconnected after a Fatal");
        }
    }

    /// One accepted server-side connection.
    struct Peer {
        socket: WebSocketStream<TcpStream>,
    }

    impl Peer {
        async fn recv(&mut self) -> Value {
            let frame = tokio::time::timeout(PATIENCE, self.socket.next())
                .await
                .expect("the client sent nothing")
                .expect("the client closed the socket")
                .expect("websocket error");
            match frame {
                Message::Text(text) => serde_json::from_str(text.as_str()).expect("valid JSON"),
                other => panic!("expected a text frame, got {other:?}"),
            }
        }

        /// The next frame tagged `msg`, skipping anything else.
        async fn expect(&mut self, msg: &str) -> Value {
            loop {
                let frame = self.recv().await;
                if frame["msg"] == msg {
                    return frame;
                }
            }
        }

        async fn send(&mut self, value: Value) {
            self.socket.send(Message::text(value.to_string())).await.expect("send");
        }

        /// `connect` → `connected` → `login` → `result`. Returns the login call id.
        async fn handshake(&mut self) -> String {
            let connect = self.expect("connect").await;
            assert_eq!(connect["version"], "1", "the client must propose DDP 1");

            self.send(json!({ "msg": "connected", "session": "session-1" })).await;

            let login = self.expect("method").await;
            assert_eq!(login["method"], "login");
            assert_eq!(
                login["params"],
                json!([{ "resume": "tok" }]),
                "resume is the only login form the EE ddp-streamer accepts"
            );

            let id = login["id"].as_str().expect("login id").to_owned();
            self.send(json!({ "msg": "result", "id": id, "result": { "id": "u1" } })).await;
            id
        }
    }

    /// Hands the scheduler enough turns for every runnable task to reach its next await.
    ///
    /// Used only where the thing being asserted is an *absence* — a frame that must not be
    /// written — which no event can announce. No sleeping: on the single-threaded test
    /// runtime a yield round-robins every ready task, and the chains involved here are a
    /// few channel hops long.
    async fn settle() {
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
    }

    async fn next_event(events: &mut Events) -> Option<Event> {
        tokio::time::timeout(PATIENCE, events.recv()).await.expect("the event stream stalled")
    }

    /// The next event matching `wanted`, panicking if the stream ends first.
    async fn wait_for(events: &mut Events, wanted: impl Fn(&Event) -> bool) -> Event {
        loop {
            match next_event(events).await {
                Some(event) if wanted(&event) => return event,
                Some(_) => {}
                None => panic!("the event stream ended while waiting"),
            }
        }
    }

    async fn wait_ready(events: &mut Events) -> Epoch {
        match wait_for(events, |event| matches!(event, Event::Ready { .. })).await {
            Event::Ready { epoch } => epoch,
            other => unreachable!("{other:?}"),
        }
    }

    // -----------------------------------------------------------------------------------
    // A tracing subscriber, so "logged loudly" is an assertion and not a hope.
    // -----------------------------------------------------------------------------------

    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<(Level, String)>>>);

    impl Logs {
        fn contains(&self, level: Level, needle: &str) -> bool {
            self.0
                .lock()
                .expect("log mutex")
                .iter()
                .any(|(seen, line)| *seen == level && line.contains(needle))
        }
    }

    impl Subscriber for Logs {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}

        fn event(&self, event: &LogEvent<'_>) {
            let mut line = String::new();
            event.record(&mut Rendered(&mut line));
            self.0.lock().expect("log mutex").push((*event.metadata().level(), line));
        }
    }

    struct Rendered<'a>(&'a mut String);

    impl Visit for Rendered<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }

    // -----------------------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------------------

    #[tokio::test]
    async fn the_happy_path_reaches_ready() {
        let mut harness = Harness::start().await;
        let (connection, mut events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.handshake().await;

        assert!(matches!(
            wait_for(&mut events, |event| matches!(event, Event::Connected { .. })).await,
            Event::Connected { session } if session == "session-1"
        ));
        let epoch = wait_ready(&mut events).await;

        // The resubscribe hook fires on the same epoch, every time login succeeds.
        assert!(matches!(
            wait_for(&mut events, |event| matches!(event, Event::Resubscribe { .. })).await,
            Event::Resubscribe { epoch: seen } if seen == epoch
        ));
        assert_eq!(*connection.state().borrow(), ConnectionState::Ready);
    }

    #[tokio::test]
    async fn a_server_that_never_answers_the_handshake_is_dropped_and_retried() {
        let mut harness = Harness::start().await;
        let (_connection, mut events) =
            harness.spawn_with(|config| config.handshake_timeout = HANDSHAKE_BUDGET);

        // Accept the upgrade, then say nothing at all — the shape of a wedged deployment.
        let mut peer = harness.accept().await;
        peer.expect("connect").await;

        let disconnected =
            wait_for(&mut events, |event| matches!(event, Event::Disconnected { .. })).await;
        assert!(
            matches!(&disconnected, Event::Disconnected { reason } if reason.contains("handshake")),
            "{disconnected:?}"
        );

        // Transient, so it must be retried — and this time answered.
        let mut second = harness.accept().await;
        second.handshake().await;
        wait_ready(&mut events).await;
    }

    #[tokio::test]
    async fn a_version_mismatch_ends_the_stream_without_retrying() {
        let mut harness = Harness::start().await;
        let (connection, mut events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.expect("connect").await;
        peer.send(json!({ "msg": "failed", "version": "2" })).await;

        let fatal = wait_for(&mut events, |event| matches!(event, Event::Fatal(_))).await;
        assert!(
            matches!(&fatal, Event::Fatal(Fatal::VersionMismatch { offered, .. }) if offered == "2"),
            "{fatal:?}"
        );

        // Twilight's FatallyClosed → Poll::Ready(None): the user's loop simply ends.
        assert!(next_event(&mut events).await.is_none(), "a Fatal must end the stream");
        assert_eq!(*connection.state().borrow(), ConnectionState::Fatal);
        harness.expect_no_reconnect().await;
    }

    #[tokio::test]
    async fn an_expired_resume_token_ends_the_stream_without_retrying() {
        let mut harness = Harness::start().await;
        let (_connection, mut events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.expect("connect").await;
        peer.send(json!({ "msg": "connected", "session": "session-1" })).await;

        let login = peer.expect("method").await;
        let id = login["id"].as_str().expect("login id").to_owned();
        peer.send(json!({
            "msg": "result",
            "id": id,
            "error": {
                "error": 403,
                "reason": "You've been logged out by the server. Please log in again.",
                "errorType": "Meteor.Error",
            },
        }))
        .await;

        let fatal = wait_for(&mut events, |event| matches!(event, Event::Fatal(_))).await;
        assert!(matches!(&fatal, Event::Fatal(Fatal::SessionExpired(_))), "{fatal:?}");

        assert!(next_event(&mut events).await.is_none());
        // The same token would be rejected identically forever; retrying is the hot loop.
        harness.expect_no_reconnect().await;
    }

    #[tokio::test]
    async fn an_in_flight_call_is_abandoned_not_reported_as_unsent() {
        let mut harness = Harness::start().await;
        let (connection, mut events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.handshake().await;
        wait_ready(&mut events).await;

        let caller = tokio::spawn({
            let connection = connection.clone();
            async move { connection.call("getRoomIdByNameOrId", vec![json!("GENERAL")]).await }
        });

        // Reading it proves it reached the wire, which is exactly what makes the outcome
        // "may have executed" rather than "safe to retry".
        let frame = peer.expect("method").await;
        assert_eq!(frame["method"], "getRoomIdByNameOrId");
        let id = frame["id"].as_str().expect("call id").to_owned();

        drop(peer);

        let outcome = caller.await.expect("caller task");
        assert!(
            matches!(&outcome, Err(CallError::Abandoned { id: seen }) if *seen == id),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_call_that_never_reached_the_wire_is_reported_as_not_sent() {
        let mut harness = Harness::start().await;
        let (connection, _events) = harness.spawn();

        // Connected but not authenticated: application frames are held back, so the call
        // is registered without ever being written.
        let mut peer = harness.accept().await;
        peer.expect("connect").await;
        peer.send(json!({ "msg": "connected", "session": "session-1" })).await;
        peer.expect("method").await;

        let caller = tokio::spawn({
            let connection = connection.clone();
            async move { connection.call("chat.sendMessage", vec![json!({})]).await }
        });
        settle().await;

        drop(peer);

        let outcome = caller.await.expect("caller task");
        assert!(matches!(outcome, Err(CallError::NotSent)), "{outcome:?}");
    }

    #[tokio::test]
    async fn a_server_ping_is_answered_with_a_pong_echoing_the_id() {
        let mut harness = Harness::start().await;
        let (_connection, mut events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.handshake().await;
        wait_ready(&mut events).await;

        peer.send(json!({ "msg": "ping", "id": "h7" })).await;
        let pong = peer.expect("pong").await;
        assert_eq!(pong["id"], "h7", "an unechoed id makes the server time the socket out");
    }

    #[tokio::test]
    async fn a_ping_is_answered_even_while_application_traffic_is_held_back() {
        let mut harness = Harness::start().await;
        let (connection, _events) = harness.spawn();

        // Authenticating: the application lane is blocked behind login.
        let mut peer = harness.accept().await;
        peer.expect("connect").await;
        peer.send(json!({ "msg": "connected", "session": "session-1" })).await;
        peer.expect("method").await;

        for index in 0..8 {
            let connection = connection.clone();
            tokio::spawn(async move { connection.call("slow", vec![json!(index)]).await });
        }
        settle().await;

        // The pong must still get out: liveness cannot queue behind application traffic.
        peer.send(json!({ "msg": "ping", "id": "h9" })).await;
        let pong = peer.expect("pong").await;
        assert_eq!(pong["id"], "h9");
    }

    #[tokio::test]
    async fn an_unexpected_close_is_followed_by_a_reconnect_on_a_new_epoch() {
        let mut harness = Harness::start().await;
        let (_connection, mut events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.handshake().await;
        let first = wait_ready(&mut events).await;

        drop(peer);
        wait_for(&mut events, |event| matches!(event, Event::Disconnected { .. })).await;

        // The full chain again — the server remembers nothing, including subscriptions.
        let mut second = harness.accept().await;
        second.handshake().await;
        let epoch = wait_ready(&mut events).await;
        assert!(epoch > first, "a reconnect must start a new epoch: {first:?} -> {epoch:?}");

        assert!(matches!(
            wait_for(&mut events, |event| matches!(event, Event::Resubscribe { .. })).await,
            Event::Resubscribe { epoch: seen } if seen == epoch
        ));
    }

    #[tokio::test]
    async fn a_stream_event_reaches_a_consumer() {
        let mut harness = Harness::start().await;
        let (connection, mut events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.handshake().await;
        wait_ready(&mut events).await;

        let subscriber = tokio::spawn({
            let connection = connection.clone();
            async move {
                connection.subscribe("stream-room-messages", vec![json!("__my_messages__")]).await
            }
        });

        let sub = peer.expect("sub").await;
        assert_eq!(sub["name"], "stream-room-messages");
        let id = sub["id"].as_str().expect("sub id").to_owned();
        peer.send(json!({ "msg": "ready", "subs": [id] })).await;
        assert_eq!(subscriber.await.expect("subscriber task").expect("subscribed"), id);

        peer.send(json!({
            "msg": "changed",
            "collection": "stream-room-messages",
            "id": "id",
            "fields": { "eventName": "__my_messages__", "args": [{ "_id": "abc", "msg": "hi" }] },
        }))
        .await;

        let event = wait_for(&mut events, |event| matches!(event, Event::Frame(_))).await;
        let Event::Frame(frame) = event else { unreachable!() };
        let stream = frame.as_stream_event().expect("a stream event");
        assert_eq!(stream.stream, "room-messages");
        assert_eq!(stream.event_name, "__my_messages__");
        assert_eq!(stream.arg(0).and_then(|arg| arg["msg"].as_str()), Some("hi"));
    }

    #[tokio::test]
    async fn a_malformed_frame_is_logged_loudly_and_does_not_derail_the_connection() {
        let logs = Logs::default();
        let _guard = tracing::subscriber::set_default(logs.clone());

        let mut harness = Harness::start().await;
        let (_connection, mut events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.handshake().await;
        wait_ready(&mut events).await;

        // Names a tag we model but does not fit it, so it lands in the catch-all. Left
        // visible because it usually means an in-flight call will never be answered.
        peer.send(json!({ "msg": "result", "id": "m9", "error": { "nonsense": true } })).await;
        // Not even JSON we can decode at all.
        peer.socket.send(Message::text("<html>502</html>")).await.expect("send");

        // The connection carries on regardless.
        peer.send(json!({ "msg": "ping", "id": "h1" })).await;
        assert_eq!(peer.expect("pong").await["id"], "h1");

        assert!(
            logs.contains(Level::WARN, "protocol violation"),
            "a ProtocolViolation must be logged at warn, not swallowed"
        );
        assert!(logs.contains(Level::WARN, "result"), "the offending msg must be in the record");
        assert!(logs.contains(Level::WARN, "undecodable frame"));
    }

    #[tokio::test]
    async fn dropping_every_handle_shuts_the_runner_down_and_ends_the_stream() {
        let mut harness = Harness::start().await;
        let (connection, mut events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.handshake().await;
        wait_ready(&mut events).await;

        drop(connection);

        // Drains whatever was already buffered, then must reach the end of the stream.
        while let Some(event) = next_event(&mut events).await {
            assert!(!matches!(event, Event::Fatal(_)), "an orderly shutdown is not a failure");
        }
    }

    #[tokio::test]
    async fn a_silent_connection_is_probed_and_then_declared_dead() {
        let mut harness = Harness::start().await;
        let (_connection, mut events) = harness.spawn_with(|config| {
            config.ping_after = Duration::from_millis(80);
            config.dead_after = Duration::from_millis(160);
        });

        let mut peer = harness.accept().await;
        peer.handshake().await;
        wait_ready(&mut events).await;

        // Silence buys a probe...
        let ping = peer.expect("ping").await;
        assert_eq!(ping["msg"], "ping");

        // ...and leaving it unanswered costs the connection.
        let disconnected =
            wait_for(&mut events, |event| matches!(event, Event::Disconnected { .. })).await;
        assert!(
            matches!(&disconnected, Event::Disconnected { reason } if reason.contains("dead")),
            "{disconnected:?}"
        );
    }

    #[tokio::test]
    async fn a_slow_consumer_is_told_it_lagged_instead_of_losing_events_silently() {
        let mut harness = Harness::start().await;
        let (_connection, mut events) = harness.spawn_with(|config| config.event_capacity = 2);

        let mut peer = harness.accept().await;
        peer.handshake().await;

        // Flood a consumer that is not reading. The ring overwrites; what must not happen
        // is the socket reader stalling behind it, because that would stop the pongs and
        // the server would close the connection out from under us.
        for index in 0..64 {
            peer.send(json!({
                "msg": "changed",
                "collection": "stream-room-messages",
                "id": "id",
                "fields": { "eventName": "GENERAL", "args": [{ "_id": index }] },
            }))
            .await;
        }

        // Proof the reader was never stalled.
        peer.send(json!({ "msg": "ping", "id": "h1" })).await;
        assert_eq!(peer.expect("pong").await["id"], "h1");

        let first = next_event(&mut events).await.expect("an event");
        assert!(
            matches!(first, Event::Lagged { missed } if missed > 0),
            "a lagging consumer must be told, not quietly shortchanged: {first:?}"
        );
    }

    #[tokio::test]
    async fn the_event_stream_adapter_ends_when_the_connection_does() {
        let mut harness = Harness::start().await;
        let (connection, events) = harness.spawn();

        let mut peer = harness.accept().await;
        peer.handshake().await;

        let stream = events.into_stream();
        futures_util::pin_mut!(stream);

        let ready = loop {
            let event = tokio::time::timeout(PATIENCE, stream.next())
                .await
                .expect("the stream stalled")
                .expect("the stream ended early");
            if matches!(event, Event::Ready { .. }) {
                break event;
            }
        };
        assert!(matches!(ready, Event::Ready { .. }));

        connection.shutdown().await;
        while tokio::time::timeout(PATIENCE, stream.next()).await.expect("stalled").is_some() {}
    }

    #[test]
    fn a_credential_never_prints_its_secret() {
        let resume = format!("{:?}", Credential::Resume("super-secret".into()));
        assert!(!resume.contains("super-secret"), "{resume}");

        let password = format!(
            "{:?}",
            Credential::Password { user: "bot".into(), password: "hunter2".into() }
        );
        assert!(!password.contains("hunter2"), "{password}");
        assert!(password.contains("bot"));
    }
}
