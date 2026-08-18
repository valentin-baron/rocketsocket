//! The DDP wire protocol, as Rocket.Chat speaks it.
//!
//! DDP is Meteor's JSON-over-WebSocket protocol: each WebSocket text frame carries exactly
//! one JSON object whose `msg` member names the frame type. This module is pure plumbing —
//! it models frames, not the documents they carry. Anything inside `fields`, `params`,
//! `result` or `args` stays a [`serde_json::Value`] here and is decoded by the caller.
//!
//! # Which server?
//!
//! Rocket.Chat has two DDP implementations and they do not agree:
//!
//! - the monolith (`meteor` + `livedata_server.js`), which is Meteor's own DDP server, and
//! - the enterprise `ddp-streamer` micro-service, a hand-written re-implementation.
//!
//! Every deviation the two have from each other, and from Meteor's published DDP spec, is
//! documented on the type that absorbs it. The rules of thumb:
//!
//! - **Never fail on an unrecognised frame.** [`ServerMessage`] has an
//!   [`Unknown`](ServerMessage::Unknown) variant that swallows any frame, *including one
//!   with no `msg` member at all*, and preserves the original payload.
//! - **Never assume an optional member is present.** Meteor's `stringifyDDP` deletes
//!   members that would serialize empty, and `ddp-streamer` omits members for any *falsy*
//!   value.
//!
//! # Borrowing
//!
//! All frame types own their data, because `#[serde(tag = "msg")]` buffers the whole frame
//! into serde's internal `Content` before it can dispatch on the tag.
//!
//! A plain `&'de str` field does survive that buffer when the input is itself borrowed
//! (`from_str` / `from_slice`), but a `&'de RawValue` does **not**: the content buffer has
//! no representation for `RawValue`'s private newtype, so it fails at runtime with
//! `invalid type: newtype struct, expected any valid JSON value`. See
//! `borrowed_raw_value_cannot_work_inside_an_internally_tagged_enum` in this module's
//! tests. Since a frame that arrives from a reader (or from a `Message::Text` that the
//! caller wants to drop) cannot borrow at all, everything here is owned; do not "optimise"
//! these structs into borrowing ones.

use core::fmt;
use core::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Prefix every Rocket.Chat streaming publication uses for its pseudo-collection.
pub const STREAM_PREFIX: &str = "stream-";

/// The constant document id every `stream-*` event frame carries.
///
/// Rocket.Chat's streams are not real collections, so there is no real document: the server
/// hard-codes the id `"id"`. It is *not* a discriminator — see [`ServerMessage::as_stream_event`].
pub const STREAM_DOCUMENT_ID: &str = "id";

/// Empty argument list, returned when a stream frame carries no `args`.
const NO_ARGS: &[Value] = &[];

// ---------------------------------------------------------------------------
// Client -> server
// ---------------------------------------------------------------------------

/// A frame sent by the client.
///
/// The server validates these in `livedata_server.js` before doing anything else, and a
/// violation closes or ignores the frame rather than reporting a useful error:
///
/// - `id` must be a **string** — `stringifyDDP` throws on a non-string id, and the server's
///   own `Meteor._debug` path then drops the reply;
/// - `method` / `name` must be strings;
/// - `params`, if present, must be an **array**.
///
/// The types below make all three unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "msg", rename_all = "camelCase")]
pub enum ClientMessage {
    /// Opens the DDP session. Must be the first frame on a new socket.
    ///
    /// Meteor's spec also defines an optional `session` member for resuming a previous
    /// session. We never send it: Rocket.Chat's server ignores it outright, and the
    /// unreleased Meteor resume support interprets a session id the client did not obtain
    /// from a `connected` frame as a *new* session request, so sending one can only ever
    /// change behaviour for the worse.
    Connect {
        /// DDP version the client proposes, e.g. `"1"`.
        version: String,
        /// Versions the client can fall back to, best first.
        support: Vec<String>,
    },

    /// Invokes a server method. The reply is a [`ServerMessage::Result`] with the same `id`.
    ///
    /// `randomSeed` is deliberately not modelled. Meteor's client sends it to seed
    /// client-side id generation for latency compensation, which we do not do — and the
    /// server-side `check()` in `livedata_server.js` requires it to be a **`String`**,
    /// while the official TypeScript client types it as `Record<string, unknown>`. That
    /// type is a bug: sending the object it describes makes the server reject the frame.
    /// Since we never send the member, the discrepancy cannot bite us.
    Method {
        /// Client-chosen correlation id. Must be a string.
        id: String,
        /// Method name, e.g. `"login"` or `"sendMessage"`.
        method: String,
        /// Positional arguments. Omitted from the wire when empty.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        params: Vec<Value>,
    },

    /// Subscribes to a publication. The server replies `ready` or `nosub`.
    Sub {
        /// Client-chosen subscription id. Must be a string.
        id: String,
        /// Publication name, e.g. `"stream-room-messages"`.
        name: String,
        /// Positional arguments. Omitted from the wire when empty.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        params: Vec<Value>,
    },

    /// Cancels a subscription. The server replies with `nosub` carrying the same id.
    Unsub {
        /// The id passed to the original [`Sub`](ClientMessage::Sub).
        id: String,
    },

    /// Heartbeat. The peer must answer with a `pong` echoing `id`.
    Ping {
        /// Optional correlation id; omitted from the wire when `None`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },

    /// Reply to a [`ServerMessage::Ping`], echoing its `id` verbatim.
    Pong {
        /// The id from the ping being answered; omitted when the ping carried none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },
}

impl ClientMessage {
    /// The only DDP version Rocket.Chat has ever accepted.
    pub const DDP_VERSION: &'static str = "1";

    /// Builds the standard opening handshake, `{"msg":"connect","version":"1","support":["1"]}`.
    #[must_use]
    pub fn connect() -> Self {
        Self::Connect {
            version: Self::DDP_VERSION.to_owned(),
            support: vec![Self::DDP_VERSION.to_owned()],
        }
    }

    /// Builds a method call.
    #[must_use]
    pub fn method(id: impl Into<String>, method: impl Into<String>, params: Vec<Value>) -> Self {
        Self::Method { id: id.into(), method: method.into(), params }
    }

    /// Builds a subscription request.
    #[must_use]
    pub fn sub(id: impl Into<String>, name: impl Into<String>, params: Vec<Value>) -> Self {
        Self::Sub { id: id.into(), name: name.into(), params }
    }

    /// The correlation id this frame carries, if any.
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        match self {
            Self::Method { id, .. } | Self::Sub { id, .. } | Self::Unsub { id } => Some(id),
            Self::Ping { id } | Self::Pong { id } => id.as_deref(),
            Self::Connect { .. } => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Server -> client
// ---------------------------------------------------------------------------

/// A frame received from the server.
///
/// Marked `#[non_exhaustive]`: Rocket.Chat and Meteor both add frame types, and anything not
/// listed here already decodes into [`Unknown`](ServerMessage::Unknown), so gaining a variant
/// is not a breaking change for callers who keep a wildcard arm.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "msg", rename_all = "camelCase", rename_all_fields = "camelCase")]
#[non_exhaustive]
pub enum ServerMessage {
    /// The session is open. `session` is the server-assigned session id.
    Connected {
        /// Server-assigned session id.
        session: String,
    },

    /// Version negotiation failed; `version` is the version the server suggests instead.
    Failed {
        /// A version the server does support.
        version: String,
    },

    /// Heartbeat from the server; answer with [`ClientMessage::Pong`] echoing `id`.
    Ping {
        /// Correlation id to echo back. Absent for an id-less ping.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },

    /// Reply to a [`ClientMessage::Ping`].
    Pong {
        /// The id of the ping being answered, if it carried one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },

    /// Terminal reply to a [`ClientMessage::Method`].
    ///
    /// Exactly one of `result` / `error` is meaningful, but **both can be absent**:
    ///
    /// - the monolith omits `result` when the method returned `undefined`;
    /// - `ddp-streamer` omits it whenever the value is *falsy* — a truthiness bug — so a
    ///   method that returned `false`, `0` or `""` is indistinguishable on the wire from one
    ///   that returned nothing at all.
    ///
    /// An explicit JSON `null` also decodes to `None`; do not read meaning into the
    /// distinction, because the server does not preserve one.
    Result {
        /// The id from the originating method call.
        id: String,
        /// The method's return value, when the server chose to send one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
        /// The error the method threw.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<DdpError>,
    },

    /// The listed methods' data-side effects have all been sent.
    ///
    /// Batched: one frame can settle many outstanding calls.
    Updated {
        /// Ids of the methods whose writes are now visible.
        methods: Vec<String>,
    },

    /// A subscription ended: either the client unsubscribed, or the server refused it.
    ///
    /// The presence of `error` is what separates "your `unsub` was processed" from
    /// "your `sub` was rejected"; both arrive as this frame.
    Nosub {
        /// The subscription id.
        id: String,
        /// Why the subscription was refused, if it was.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<DdpError>,
    },

    /// The listed subscriptions have sent their initial document set.
    ///
    /// Batched: the server coalesces every subscription that became ready in the same tick,
    /// so a single frame can carry ids from unrelated `sub` calls.
    Ready {
        /// Ids of the now-ready subscriptions.
        subs: Vec<String>,
    },

    /// A document entered a collection the client is subscribed to.
    Added {
        /// Collection name.
        collection: String,
        /// Document id.
        id: String,
        /// Initial field values; absent when the document has no fields beyond `_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fields: Option<Map<String, Value>>,
    },

    /// A document changed.
    ///
    /// **Both** `fields` and `cleared` are optional, and `fields` is genuinely absent in
    /// practice: `stringifyDDP` moves every key whose value is `undefined` from `fields`
    /// into `cleared` and then *deletes* `fields` if what remains is empty. A frame like
    /// `{"msg":"changed","collection":"c","id":"i","cleared":["a"]}` is well-formed.
    ///
    /// This is also the frame every Rocket.Chat stream event arrives on — see
    /// [`as_stream_event`](ServerMessage::as_stream_event).
    Changed {
        /// Collection name, or the `stream-*` pseudo-collection for a stream event.
        collection: String,
        /// Document id — `"id"` for stream events, see [`STREAM_DOCUMENT_ID`].
        id: String,
        /// Updated field values.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fields: Option<Map<String, Value>>,
        /// Names of fields that became `undefined`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cleared: Option<Vec<String>>,
    },

    /// A document left a collection.
    Removed {
        /// Collection name.
        collection: String,
        /// Document id.
        id: String,
    },

    /// Ordered-collection insert.
    ///
    /// Meteor's own DDP specification says the ordered frames "are not currently used by
    /// Meteor", and Rocket.Chat never emits one. Modelled so that a server which starts
    /// sending them does not degrade into [`Unknown`](ServerMessage::Unknown).
    AddedBefore {
        /// Collection name.
        collection: String,
        /// Document id.
        id: String,
        /// Initial field values.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fields: Option<Map<String, Value>>,
        /// Id of the document to insert before; `None` means "append".
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before: Option<String>,
    },

    /// Ordered-collection move. As unused as [`AddedBefore`](ServerMessage::AddedBefore).
    MovedBefore {
        /// Collection name.
        collection: String,
        /// Document id.
        id: String,
        /// Id of the document to move before; `None` means "move to the end".
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before: Option<String>,
    },

    /// A protocol-level error: the server could not even parse or route the client's frame.
    ///
    /// Note this frame carries **no `id`**, so it cannot be correlated with the call that
    /// caused it — `offendingMessage` is the only clue, and it too is optional (the server
    /// omits it when the input was not valid JSON at all).
    ///
    /// The official JavaScript client has no case for this frame and drops it silently,
    /// which turns a malformed request into a call that simply never resolves. We surface it.
    Error {
        /// Human-readable description, e.g. `"Bad request"`.
        reason: String,
        /// The frame the server objected to, echoed back when it was parseable JSON.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offending_message: Option<Value>,
    },

    /// Any frame this crate does not recognise, preserved verbatim.
    ///
    /// This variant must stay **last**: serde tries untagged variants in declaration order
    /// after the tagged ones fail to match, so anything below it would be unreachable.
    ///
    /// It exists because `#[serde(other)]` cannot carry a payload — the workaround (an
    /// `#[serde(untagged)]` catch-all variant holding a flattened map) is the one serenity
    /// uses for Discord's gateway. It catches three real cases:
    ///
    /// 1. **No `msg` member at all.** Meteor ≤ 2.2 greeted every new socket with
    ///    `{"server_id":"0"}` before the `connect` handshake.
    /// 2. **`msg: "server_id"`.** The enterprise `ddp-streamer` still opens with
    ///    `{"msg":"server_id","server_id":"0"}`.
    /// 3. Any frame type added by a future server release.
    #[serde(untagged)]
    Unknown(UnknownMessage),
}

/// An unrecognised DDP frame, kept whole.
///
/// `msg` is `Option` because the frame may not have had one; `rest` holds every other
/// member, so the original JSON can be re-serialized without loss.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct UnknownMessage {
    /// The frame's `msg` value, when it had one and it was a string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg: Option<String>,
    /// Every other member of the frame.
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

impl<'de> Deserialize<'de> for UnknownMessage {
    /// Hand-written rather than derived so that *nothing* is lost or rejected.
    ///
    /// A derived `msg: Option<String>` would make the whole frame fail to decode when `msg`
    /// is present but is not a string — which, since this is the catch-all variant, would
    /// mean [`ServerMessage`] failing outright on a frame it is supposed to absorb. Here a
    /// non-string `msg` simply stays in [`rest`](Self::rest) and round-trips.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut rest = Map::<String, Value>::deserialize(deserializer)?;
        let msg = match rest.get("msg") {
            Some(Value::String(_)) => match rest.remove("msg") {
                Some(Value::String(msg)) => Some(msg),
                _ => unreachable!("just matched a string"),
            },
            _ => None,
        };
        Ok(Self { msg, rest })
    }
}

impl ServerMessage {
    /// The correlation id this frame carries, if the frame type has one.
    ///
    /// Returns `None` for [`Error`](ServerMessage::Error), which has no id by design, and
    /// for the collection frames, whose `id` is a *document* id and must never be used for
    /// call correlation.
    #[must_use]
    pub fn correlation_id(&self) -> Option<&str> {
        match self {
            Self::Result { id, .. } | Self::Nosub { id, .. } => Some(id),
            Self::Ping { id } | Self::Pong { id } => id.as_deref(),
            _ => None,
        }
    }

    /// Interprets this frame as a Rocket.Chat stream event, if it is one.
    ///
    /// Rocket.Chat's `stream-*` publications do not use real collections. Every event is a
    /// `changed` frame on a pseudo-collection named after the stream, carrying the constant
    /// document id `"id"` and a payload of `{"eventName": ..., "args": [...]}`:
    ///
    /// ```json
    /// {"msg":"changed","collection":"stream-room-messages","id":"id",
    ///  "fields":{"eventName":"GENERAL","args":[{"_id":"..."}]}}
    /// ```
    ///
    /// **Dispatch on `(collection, eventName)` — never on the document id.** Two
    /// subscriptions to the same stream (say `stream-notify-user` for `subscriptions-changed`
    /// and for `rooms-changed`) share *both* the collection and that constant id; only
    /// `eventName` separates them. `stream-user-presence` ignores the convention entirely
    /// and puts the subscribed uid in both `id` and `eventName`, which this accessor handles
    /// for free precisely because it never looks at `id`.
    #[must_use]
    pub fn as_stream_event(&self) -> Option<StreamEvent<'_>> {
        let Self::Changed { collection, fields, .. } = self else {
            return None;
        };
        let stream = collection.strip_prefix(STREAM_PREFIX)?;
        let fields = fields.as_ref()?;
        let event_name = fields.get("eventName")?.as_str()?;
        let args = fields.get("args").and_then(Value::as_array).map_or(NO_ARGS, Vec::as_slice);
        Some(StreamEvent { stream, event_name, args })
    }
}

// ---------------------------------------------------------------------------
// Stream events
// ---------------------------------------------------------------------------

/// The `fields` payload of a Rocket.Chat stream event.
///
/// Useful when a stream frame is deserialized on its own; [`ServerMessage::as_stream_event`]
/// is the zero-copy path for a frame that has already been decoded.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamFields {
    /// The routing key within the stream: a room id, a uid, an event name, …
    pub event_name: String,
    /// Positional event arguments. See [`StreamEvent::args`] for why this is not a tuple.
    #[serde(default)]
    pub args: Vec<Value>,
}

/// A borrowed view of a stream event, produced by [`ServerMessage::as_stream_event`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamEvent<'a> {
    /// Stream name with the `stream-` prefix removed, e.g. `"room-messages"`.
    pub stream: &'a str,
    /// The `eventName` field: what the subscription was keyed on.
    pub event_name: &'a str,
    /// The positional arguments.
    ///
    /// This is a `Vec`/slice and not a fixed-arity tuple on purpose: **arity is not stable
    /// across server versions**. `user-status` on `stream-notify-logged` grew from 3 to 6 to
    /// 8 elements, and `stream-notify-user`'s `__my_messages__` event appends a trailing
    /// element that the per-room `stream-room-messages` form does not have. A tuple type
    /// would turn every server upgrade into a deserialization failure.
    ///
    /// Elements can also legitimately be `null`: EJSON drops an `undefined` *object value*
    /// but encodes an `undefined` *array element* as `null`, so a positional argument list
    /// can contain holes in the middle.
    pub args: &'a [Value],
}

impl<'a> StreamEvent<'a> {
    /// The full pseudo-collection name, e.g. `"stream-room-messages"`.
    #[must_use]
    pub fn collection(&self) -> String {
        format!("{STREAM_PREFIX}{}", self.stream)
    }

    /// Number of positional arguments present, including `null` holes.
    #[must_use]
    pub fn arity(&self) -> usize {
        self.args.len()
    }

    /// The argument at `index`, including an explicit `null`.
    #[must_use]
    pub fn arg_raw(&self, index: usize) -> Option<&'a Value> {
        self.args.get(index)
    }

    /// The argument at `index`, treating a `null` hole as absent.
    ///
    /// EJSON encodes an `undefined` array element as `null`, so a `null` here means "the
    /// server had nothing for this position", which is what callers want to skip.
    #[must_use]
    pub fn arg(&self, index: usize) -> Option<&'a Value> {
        match self.args.get(index) {
            Some(Value::Null) | None => None,
            Some(value) => Some(value),
        }
    }

    /// The argument at `index` as a string, or `None` if absent, null, or not a string.
    #[must_use]
    pub fn arg_str(&self, index: usize) -> Option<&'a str> {
        self.arg(index)?.as_str()
    }

    /// The argument at `index` as an integer, or `None` if absent or not integral.
    #[must_use]
    pub fn arg_i64(&self, index: usize) -> Option<i64> {
        self.arg(index)?.as_i64()
    }

    /// The argument at `index` as a boolean, or `None` if absent or not a boolean.
    #[must_use]
    pub fn arg_bool(&self, index: usize) -> Option<bool> {
        self.arg(index)?.as_bool()
    }

    /// Deserializes the argument at `index` into `T`.
    ///
    /// Lenient by design: `None` covers "not present", "null hole" and "unexpected shape"
    /// alike, because a positional slot's meaning has changed between server versions more
    /// than once. Use [`arg_raw`](Self::arg_raw) when the distinction matters.
    #[must_use]
    pub fn arg_as<T: DeserializeOwned>(&self, index: usize) -> Option<T> {
        serde_json::from_value(self.arg(index)?.clone()).ok()
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A Meteor error, EJSON-serialized into a `result` or `nosub` frame.
///
/// ```json
/// {"isClientSafe":true,"error":"error-not-allowed","reason":"Not allowed",
///  "message":"Not allowed [error-not-allowed]","errorType":"Meteor.Error",
///  "details":{"method":"sendMessage"}}
/// ```
///
/// Only `error` is guaranteed: the server sanitizes anything that is not client-safe down to
/// `{"isClientSafe":true,"error":500,"reason":"Internal server error", ...}`, and several
/// Rocket.Chat call sites throw with a code but no reason.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DdpError {
    /// The error code — a string for domain errors, a bare number for HTTP-ish ones.
    pub error: ErrorCode,
    /// Short human-readable reason, e.g. `"Not allowed"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Reason with the code appended, e.g. `"Not allowed [error-not-allowed]"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Usually `"Meteor.Error"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    /// Whether the server considered this error safe to expose. Sanitized errors set it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_client_safe: Option<bool>,
    /// Structured extras. Shape is per-error; `too-many-requests` puts `timeToReset` here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    /// Any member Rocket.Chat adds later, kept so the error round-trips.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Substring of the reason Meteor sends when the resume token was invalidated server-side.
///
/// Matched as a substring, and deliberately excluding the leading `You've`: the apostrophe
/// has shipped both as ASCII `'` and as U+2019 depending on the source file, and the
/// sentence has been prefixed by callers.
const REASON_LOGGED_OUT: &str = "been logged out by the server";

/// Substring of the reason Meteor sends when the resume token simply expired.
const REASON_SESSION_EXPIRED: &str = "session has expired";

/// Rocket.Chat's rate-limiter error code.
const CODE_TOO_MANY_REQUESTS: &str = "too-many-requests";

impl DdpError {
    /// True if this is the rate limiter refusing the call.
    ///
    /// Pair with [`retry_after`](Self::retry_after): the server tells you how long to wait,
    /// and a client that ignores it gets its IP throttled harder.
    #[must_use]
    pub fn is_too_many_requests(&self) -> bool {
        self.error.is(CODE_TOO_MANY_REQUESTS)
    }

    /// How long the server wants the client to wait before retrying.
    ///
    /// Read from `details.timeToReset`, which Rocket.Chat's DDP rate limiter sets **in
    /// milliseconds**. A negative or non-numeric value yields `Duration::ZERO` / `None`
    /// rather than an error.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        let value = self.details.as_ref()?.get("timeToReset")?;
        let millis = match value.as_u64() {
            Some(millis) => millis,
            None => {
                let seconds = value.as_f64()?;
                if !seconds.is_finite() || seconds <= 0.0 { 0 } else { seconds as u64 }
            }
        };
        Some(Duration::from_millis(millis))
    }

    /// True if this error means the resume token is dead and retrying it is pointless.
    ///
    /// Meteor rejects a stale `resume` login with error `403` and one of two reasons:
    /// `"You've been logged out by the server. Please log in again."` (the token was
    /// invalidated) or `"Your session has expired. Please log in again."` (it aged out).
    ///
    /// The match is on a **substring**, not equality, for two reasons: the apostrophe in
    /// `You've` differs between server builds, and Rocket.Chat wraps the sentence with extra
    /// context in some code paths. Getting this wrong is not cosmetic — a client that keeps
    /// retrying a token producing these reasons hot-loops against the login endpoint until
    /// the rate limiter cuts it off.
    #[must_use]
    pub fn is_expired_session(&self) -> bool {
        let matches =
            |text: &str| text.contains(REASON_LOGGED_OUT) || text.contains(REASON_SESSION_EXPIRED);
        self.reason.as_deref().is_some_and(matches) || self.message.as_deref().is_some_and(matches)
    }

    /// The most useful human-readable text available, falling back to the code.
    #[must_use]
    pub fn describe(&self) -> String {
        self.message
            .clone()
            .or_else(|| self.reason.clone())
            .unwrap_or_else(|| self.error.to_string())
    }
}

impl fmt::Display for DdpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.message, &self.reason) {
            (Some(message), _) => f.write_str(message),
            (None, Some(reason)) => write!(f, "{reason} [{}]", self.error),
            (None, None) => write!(f, "{}", self.error),
        }
    }
}

impl std::error::Error for DdpError {}

/// A Meteor error code.
///
/// Untagged because the wire type is genuinely either: Rocket.Chat's domain errors are
/// strings (`"error-not-allowed"`, `"too-many-requests"`), while authentication failures and
/// sanitized internal errors arrive as **bare numbers** — `403` for a bad or expired login,
/// `500` for a server fault. Modelling this as `String` alone silently fails to match every
/// auth error; modelling it as a number alone fails to match every domain error.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ErrorCode {
    /// A numeric code, e.g. `403`.
    Number(i64),
    /// A symbolic code, e.g. `"error-not-allowed"`.
    String(String),
}

impl ErrorCode {
    /// The code as a string slice, or `None` when it is numeric.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(code) => Some(code),
            Self::Number(_) => None,
        }
    }

    /// The code as a number, or `None` when it is symbolic.
    #[must_use]
    pub fn as_number(&self) -> Option<i64> {
        match self {
            Self::Number(code) => Some(*code),
            Self::String(_) => None,
        }
    }

    /// Compares against a textual code, accepting `"403"` for the numeric form.
    #[must_use]
    pub fn is(&self, code: &str) -> bool {
        match self {
            Self::String(value) => value == code,
            Self::Number(value) => code.parse::<i64>().is_ok_and(|parsed| parsed == *value),
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(code) => write!(f, "{code}"),
            Self::String(code) => f.write_str(code),
        }
    }
}

impl From<i64> for ErrorCode {
    fn from(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<String> for ErrorCode {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ErrorCode {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(json: &str) -> ServerMessage {
        serde_json::from_str(json).expect("every DDP frame must decode")
    }

    // -- handshake ---------------------------------------------------------

    #[test]
    fn connect_serializes_without_a_session_member() {
        let json = serde_json::to_value(ClientMessage::connect()).unwrap();
        assert_eq!(json, json!({"msg": "connect", "version": "1", "support": ["1"]}));
        assert!(json.get("session").is_none());
    }

    #[test]
    fn handshake_pair_round_trips() {
        let connected = parse(r#"{"msg":"connected","session":"2CLZHwFBM6qGyPB7A"}"#);
        assert_eq!(connected, ServerMessage::Connected { session: "2CLZHwFBM6qGyPB7A".into() });

        let failed = parse(r#"{"msg":"failed","version":"1"}"#);
        assert_eq!(failed, ServerMessage::Failed { version: "1".into() });
    }

    // -- client message serialization -------------------------------------

    #[test]
    fn empty_params_are_omitted() {
        let method = ClientMessage::method("42", "getServerInfo", vec![]);
        assert_eq!(
            serde_json::to_value(&method).unwrap(),
            json!({"msg": "method", "id": "42", "method": "getServerInfo"})
        );

        let sub = ClientMessage::sub("1", "activeUsers", vec![]);
        assert_eq!(
            serde_json::to_value(&sub).unwrap(),
            json!({"msg": "sub", "id": "1", "name": "activeUsers"})
        );
    }

    #[test]
    fn present_params_are_kept_as_an_array() {
        let method = ClientMessage::method("3", "login", vec![json!({"resume": "tok"})]);
        let value = serde_json::to_value(&method).unwrap();
        assert_eq!(
            value,
            json!({"msg": "method", "id": "3", "method": "login", "params": [{"resume": "tok"}]})
        );
        // The server's `check()` rejects a non-array `params`.
        assert!(value["params"].is_array());
    }

    #[test]
    fn id_less_ping_and_pong_omit_the_id() {
        assert_eq!(
            serde_json::to_string(&ClientMessage::Pong { id: None }).unwrap(),
            r#"{"msg":"pong"}"#
        );
        assert_eq!(
            serde_json::to_string(&ClientMessage::Ping { id: Some("7".into()) }).unwrap(),
            r#"{"msg":"ping","id":"7"}"#
        );
    }

    #[test]
    fn client_messages_round_trip() {
        for message in [
            ClientMessage::connect(),
            ClientMessage::method("1", "sendMessage", vec![json!({"rid": "GENERAL"})]),
            ClientMessage::sub("2", "stream-room-messages", vec![json!("GENERAL"), json!(false)]),
            ClientMessage::Unsub { id: "2".into() },
            ClientMessage::Ping { id: None },
            ClientMessage::Pong { id: Some("x".into()) },
        ] {
            let json = serde_json::to_string(&message).unwrap();
            assert_eq!(serde_json::from_str::<ClientMessage>(&json).unwrap(), message);
        }
    }

    #[test]
    fn client_correlation_ids() {
        assert_eq!(ClientMessage::connect().id(), None);
        assert_eq!(ClientMessage::method("9", "m", vec![]).id(), Some("9"));
        assert_eq!(ClientMessage::Ping { id: None }.id(), None);
    }

    // -- server frames -----------------------------------------------------

    #[test]
    fn ready_batches_several_subscriptions() {
        let frame = parse(r#"{"msg":"ready","subs":["1","2","3"]}"#);
        let ServerMessage::Ready { subs } = frame else { panic!("expected ready") };
        assert_eq!(subs, ["1", "2", "3"]);
    }

    #[test]
    fn updated_batches_several_methods() {
        let frame = parse(r#"{"msg":"updated","methods":["4","5"]}"#);
        assert_eq!(frame, ServerMessage::Updated { methods: vec!["4".into(), "5".into()] });
    }

    #[test]
    fn nosub_without_error_is_an_unsubscribe_acknowledgement() {
        let frame = parse(r#"{"msg":"nosub","id":"2"}"#);
        let ServerMessage::Nosub { id, error } = frame else { panic!("expected nosub") };
        assert_eq!(id, "2");
        assert!(error.is_none());
    }

    #[test]
    fn nosub_with_error_is_a_rejected_subscription() {
        let frame = parse(
            r#"{"msg":"nosub","id":"2","error":{"isClientSafe":true,"error":"error-not-allowed",
                "reason":"Not allowed","message":"Not allowed [error-not-allowed]",
                "errorType":"Meteor.Error"}}"#,
        );
        let ServerMessage::Nosub { error: Some(error), .. } = frame else {
            panic!("expected nosub with error")
        };
        assert!(error.error.is("error-not-allowed"));
        assert_eq!(error.reason.as_deref(), Some("Not allowed"));
        assert_eq!(error.error_type.as_deref(), Some("Meteor.Error"));
        assert_eq!(error.is_client_safe, Some(true));
        assert_eq!(error.to_string(), "Not allowed [error-not-allowed]");
    }

    #[test]
    fn changed_may_carry_cleared_and_no_fields_at_all() {
        // stringifyDDP deletes `fields` once every key has moved into `cleared`.
        let frame = parse(r#"{"msg":"changed","collection":"c","id":"i","cleared":["a"]}"#);
        let ServerMessage::Changed { collection, id, fields, cleared } = frame else {
            panic!("expected changed")
        };
        assert_eq!(collection, "c");
        assert_eq!(id, "i");
        assert!(fields.is_none());
        assert_eq!(cleared.unwrap(), ["a"]);
    }

    #[test]
    fn added_and_removed_decode() {
        let added = parse(
            r#"{"msg":"added","collection":"users","id":"rocket.cat","fields":{"username":"rocket.cat"}}"#,
        );
        let ServerMessage::Added { collection, id, fields } = added else {
            panic!("expected added")
        };
        assert_eq!(collection, "users");
        assert_eq!(id, "rocket.cat");
        assert_eq!(fields.unwrap()["username"], json!("rocket.cat"));

        // A document with no fields beyond `_id` arrives without `fields`.
        let bare = parse(r#"{"msg":"added","collection":"users","id":"x"}"#);
        assert_eq!(
            bare,
            ServerMessage::Added { collection: "users".into(), id: "x".into(), fields: None }
        );

        let removed = parse(r#"{"msg":"removed","collection":"users","id":"x"}"#);
        assert_eq!(removed, ServerMessage::Removed { collection: "users".into(), id: "x".into() });
    }

    #[test]
    fn ordered_frames_parse_even_though_no_server_emits_them() {
        let added_before = parse(
            r#"{"msg":"addedBefore","collection":"c","id":"b","fields":{"n":1},"before":"a"}"#,
        );
        assert_eq!(
            added_before,
            ServerMessage::AddedBefore {
                collection: "c".into(),
                id: "b".into(),
                fields: Some(serde_json::from_str(r#"{"n":1}"#).unwrap()),
                before: Some("a".into()),
            }
        );

        // `before: null` means "at the end" and must not be confused with an absent member.
        let moved_before =
            parse(r#"{"msg":"movedBefore","collection":"c","id":"b","before":null}"#);
        assert_eq!(
            moved_before,
            ServerMessage::MovedBefore { collection: "c".into(), id: "b".into(), before: None }
        );
    }

    #[test]
    fn protocol_error_frame_has_no_id() {
        let frame = parse(
            r#"{"msg":"error","reason":"Bad request","offendingMessage":{"msg":"method","id":1}}"#,
        );
        let ServerMessage::Error { reason, offending_message } = &frame else {
            panic!("expected error")
        };
        assert_eq!(reason, "Bad request");
        assert_eq!(offending_message.as_ref().unwrap()["id"], json!(1));
        // There is no id to correlate this with; a client must not wait for one.
        assert_eq!(frame.correlation_id(), None);

        // Unparseable input produces the frame without `offendingMessage`.
        let bare = parse(r#"{"msg":"error","reason":"Malformed DDP"}"#);
        assert_eq!(
            bare,
            ServerMessage::Error { reason: "Malformed DDP".into(), offending_message: None }
        );
    }

    #[test]
    fn server_ping_and_pong_decode_with_and_without_an_id() {
        assert_eq!(parse(r#"{"msg":"ping"}"#), ServerMessage::Ping { id: None });
        assert_eq!(
            parse(r#"{"msg":"pong","id":"9"}"#),
            ServerMessage::Pong { id: Some("9".into()) }
        );
        assert_eq!(parse(r#"{"msg":"ping","id":"9"}"#).correlation_id(), Some("9"));
    }

    // -- result / error ----------------------------------------------------

    #[test]
    fn result_without_a_result_member() {
        // The monolith omits `result` for `undefined`; ddp-streamer omits it for anything
        // falsy, so `false` and "no value" are the same frame.
        let frame = parse(r#"{"msg":"result","id":"1"}"#);
        assert_eq!(frame, ServerMessage::Result { id: "1".into(), result: None, error: None });
        assert_eq!(frame.correlation_id(), Some("1"));
    }

    #[test]
    fn result_with_a_value() {
        let frame = parse(r#"{"msg":"result","id":"1","result":{"id":"u1","token":"t"}}"#);
        let ServerMessage::Result { result: Some(result), .. } = frame else {
            panic!("expected a result")
        };
        assert_eq!(result["token"], json!("t"));
    }

    #[test]
    fn numeric_error_codes_decode() {
        // A failed login arrives with the bare number 403, not a string.
        let frame = parse(
            r#"{"msg":"result","id":"1","error":{"isClientSafe":true,"error":403,
                "reason":"User not found","message":"User not found [403]",
                "errorType":"Meteor.Error"}}"#,
        );
        let ServerMessage::Result { error: Some(error), .. } = frame else {
            panic!("expected an error")
        };
        assert_eq!(error.error, ErrorCode::Number(403));
        assert_eq!(error.error.as_number(), Some(403));
        assert_eq!(error.error.as_str(), None);
        assert!(error.error.is("403"));
        assert!(!error.error.is("error-not-allowed"));
        assert_eq!(error.error.to_string(), "403");
    }

    #[test]
    fn string_error_codes_decode() {
        let error: DdpError = serde_json::from_str(
            r#"{"isClientSafe":true,"error":"error-not-allowed","reason":"Not allowed",
                "message":"Not allowed [error-not-allowed]","errorType":"Meteor.Error",
                "details":{"method":"sendMessage"}}"#,
        )
        .unwrap();
        assert_eq!(error.error, ErrorCode::String("error-not-allowed".into()));
        assert_eq!(error.error.as_str(), Some("error-not-allowed"));
        assert_eq!(error.error.as_number(), None);
        assert_eq!(error.details.as_ref().unwrap()["method"], json!("sendMessage"));
    }

    #[test]
    fn error_display_falls_back_when_message_is_missing() {
        let error: DdpError =
            serde_json::from_str(r#"{"error":"error-invalid-room","reason":"Invalid room"}"#)
                .unwrap();
        assert_eq!(error.to_string(), "Invalid room [error-invalid-room]");
        assert_eq!(error.describe(), "Invalid room");

        let bare: DdpError = serde_json::from_str(r#"{"error":500}"#).unwrap();
        assert_eq!(bare.to_string(), "500");
        assert_eq!(bare.describe(), "500");

        // It is a real std error.
        let _: &dyn std::error::Error = &bare;
    }

    #[test]
    fn unknown_error_members_survive_a_round_trip() {
        let error: DdpError =
            serde_json::from_str(r#"{"error":"x","futureMember":{"a":1}}"#).unwrap();
        assert_eq!(error.extra["futureMember"], json!({"a": 1}));
        assert_eq!(
            serde_json::to_value(&error).unwrap(),
            json!({"error": "x", "futureMember": {"a": 1}})
        );
    }

    #[test]
    fn retry_after_reads_time_to_reset_in_milliseconds() {
        let error: DdpError = serde_json::from_str(
            r#"{"isClientSafe":true,"error":"too-many-requests",
                "reason":"Error, too many requests. Please slow down. You must wait 10 seconds before trying again.",
                "details":{"timeToReset":10000,"seconds":10},
                "errorType":"Meteor.Error"}"#,
        )
        .unwrap();
        assert!(error.is_too_many_requests());
        assert_eq!(error.retry_after(), Some(Duration::from_millis(10_000)));

        // No details at all -> no advice.
        let other: DdpError = serde_json::from_str(r#"{"error":"too-many-requests"}"#).unwrap();
        assert!(other.is_too_many_requests());
        assert_eq!(other.retry_after(), None);

        // Details without the member, and a nonsensical value, both stay non-fatal.
        let empty: DdpError =
            serde_json::from_str(r#"{"error":"too-many-requests","details":{}}"#).unwrap();
        assert_eq!(empty.retry_after(), None);
        let negative: DdpError =
            serde_json::from_str(r#"{"error":"x","details":{"timeToReset":-5}}"#).unwrap();
        assert_eq!(negative.retry_after(), Some(Duration::ZERO));
    }

    #[test]
    fn expired_session_reasons_are_recognised() {
        let logged_out: DdpError = serde_json::from_str(
            r#"{"isClientSafe":true,"error":403,
                "reason":"You've been logged out by the server. Please log in again.",
                "message":"You've been logged out by the server. Please log in again. [403]",
                "errorType":"Meteor.Error"}"#,
        )
        .unwrap();
        assert!(logged_out.is_expired_session());

        let expired: DdpError = serde_json::from_str(
            r#"{"isClientSafe":true,"error":403,
                "reason":"Your session has expired. Please log in again.",
                "errorType":"Meteor.Error"}"#,
        )
        .unwrap();
        assert!(expired.is_expired_session());

        // The typographic apostrophe variant must match too — that is why the check is a
        // substring and skips the `You've` prefix.
        let curly: DdpError = serde_json::from_str(
            "{\"error\":403,\"reason\":\"You\u{2019}ve been logged out by the server. Please log in again.\"}",
        )
        .unwrap();
        assert!(curly.is_expired_session());

        // A retryable failure must not be mistaken for a dead token.
        let wrong_password: DdpError = serde_json::from_str(
            r#"{"isClientSafe":true,"error":403,"reason":"Incorrect password"}"#,
        )
        .unwrap();
        assert!(!wrong_password.is_expired_session());
        assert!(!wrong_password.is_too_many_requests());
    }

    // -- unknown frames ----------------------------------------------------

    #[test]
    fn a_frame_with_no_msg_member_decodes_to_unknown() {
        // Meteor <= 2.2 greeted every socket with this before `connect`.
        let frame = parse(r#"{"server_id":"0"}"#);
        let ServerMessage::Unknown(unknown) = &frame else {
            panic!("expected unknown, got {frame:?}")
        };
        assert_eq!(unknown.msg, None);
        assert_eq!(unknown.rest["server_id"], json!("0"));
        assert_eq!(serde_json::to_value(&frame).unwrap(), json!({"server_id": "0"}));
    }

    #[test]
    fn the_ddp_streamer_server_id_greeting_decodes_to_unknown() {
        let frame = parse(r#"{"msg":"server_id","server_id":"0"}"#);
        let ServerMessage::Unknown(unknown) = &frame else {
            panic!("expected unknown, got {frame:?}")
        };
        assert_eq!(unknown.msg.as_deref(), Some("server_id"));
        assert_eq!(unknown.rest["server_id"], json!("0"));
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({"msg": "server_id", "server_id": "0"})
        );
    }

    #[test]
    fn an_unrecognised_msg_keeps_its_payload() {
        let frame = parse(r#"{"msg":"someFutureFrame","a":1,"b":[true,null]}"#);
        let ServerMessage::Unknown(unknown) = &frame else { panic!("expected unknown") };
        assert_eq!(unknown.msg.as_deref(), Some("someFutureFrame"));
        assert_eq!(unknown.rest["a"], json!(1));
        assert_eq!(unknown.rest["b"], json!([true, null]));
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({"msg": "someFutureFrame", "a": 1, "b": [true, null]})
        );
        assert_eq!(frame.correlation_id(), None);
    }

    #[test]
    fn an_empty_object_decodes_to_unknown() {
        assert_eq!(parse("{}"), ServerMessage::Unknown(UnknownMessage::default()));
    }

    #[test]
    fn a_non_string_msg_decodes_to_unknown_rather_than_failing() {
        // Not something a server sends, but the catch-all variant must never be the thing
        // that makes decoding fail. The odd value stays in `rest` and round-trips.
        for frame_json in [r#"{"msg":7,"x":1}"#, r#"{"msg":null,"x":1}"#, r#"{"msg":{"a":1}}"#] {
            let frame = parse(frame_json);
            let ServerMessage::Unknown(unknown) = &frame else {
                panic!("expected unknown for {frame_json}")
            };
            assert_eq!(unknown.msg, None);
            assert!(unknown.rest.contains_key("msg"));
            assert_eq!(
                serde_json::to_value(&frame).unwrap(),
                serde_json::from_str::<Value>(frame_json).unwrap()
            );
        }
    }

    // -- stream events -----------------------------------------------------

    #[test]
    fn a_stream_event_is_a_changed_frame_on_a_fake_collection() {
        let frame = parse(
            r#"{"msg":"changed","collection":"stream-room-messages","id":"id",
                "fields":{"eventName":"GENERAL","args":[{"_id":"m1","msg":"hi"}]}}"#,
        );
        let event = frame.as_stream_event().expect("a stream event");
        assert_eq!(event.stream, "room-messages");
        assert_eq!(event.collection(), "stream-room-messages");
        assert_eq!(event.event_name, "GENERAL");
        assert_eq!(event.arity(), 1);
        assert_eq!(event.arg(0).unwrap()["msg"], json!("hi"));

        // The document id is a constant and carries no information.
        let ServerMessage::Changed { id, .. } = &frame else { panic!("expected changed") };
        assert_eq!(id, STREAM_DOCUMENT_ID);
    }

    #[test]
    fn two_subscriptions_to_one_stream_are_told_apart_by_event_name_only() {
        let subscriptions = parse(
            r#"{"msg":"changed","collection":"stream-notify-user","id":"id",
                "fields":{"eventName":"uid1/subscriptions-changed","args":["updated",{"rid":"r"}]}}"#,
        );
        let rooms = parse(
            r#"{"msg":"changed","collection":"stream-notify-user","id":"id",
                "fields":{"eventName":"uid1/rooms-changed","args":["updated",{"_id":"r"}]}}"#,
        );

        let a = subscriptions.as_stream_event().unwrap();
        let b = rooms.as_stream_event().unwrap();
        // Same collection, same document id: only the event name separates them.
        assert_eq!(a.stream, b.stream);
        assert_ne!(a.event_name, b.event_name);
        assert_eq!(a.arg_str(0), Some("updated"));
    }

    #[test]
    fn user_presence_uses_the_uid_as_both_id_and_event_name() {
        // stream-user-presence ignores the `"id"` convention entirely. Because dispatch is
        // keyed on (collection, eventName) and never on the document id, this needs no
        // special case.
        let frame = parse(
            r#"{"msg":"changed","collection":"stream-user-presence","id":"YHz2Xn9aEqSDkrgLM",
                "fields":{"eventName":"YHz2Xn9aEqSDkrgLM",
                          "args":[["YHz2Xn9aEqSDkrgLM","rocket.cat",1,"Away for lunch"]]}}"#,
        );
        let event = frame.as_stream_event().expect("a stream event");
        assert_eq!(event.stream, "user-presence");
        assert_eq!(event.event_name, "YHz2Xn9aEqSDkrgLM");
        let ServerMessage::Changed { id, .. } = &frame else { panic!("expected changed") };
        assert_eq!(id, event.event_name);
        assert_ne!(id, STREAM_DOCUMENT_ID);

        let inner = event.arg(0).unwrap().as_array().unwrap();
        assert_eq!(inner[1], json!("rocket.cat"));
    }

    #[test]
    fn stream_args_are_positional_and_variable_arity() {
        // `user-status` shipped as 3, then 6, then 8 elements. All must decode.
        for args in [
            json!([["uid", "user", 1, "text"]]),
            json!([["uid", "user", 1, "text", "name", 0]]),
            json!([["uid", "user", 1, "text", "name", 0, null, {"extra": true}]]),
        ] {
            let frame = ServerMessage::Changed {
                collection: "stream-notify-logged".into(),
                id: STREAM_DOCUMENT_ID.into(),
                fields: Some(
                    json!({"eventName": "user-status", "args": args}).as_object().cloned().unwrap(),
                ),
                cleared: None,
            };
            let event = frame.as_stream_event().unwrap();
            assert_eq!(event.event_name, "user-status");
            assert!(event.arg(0).unwrap().is_array());
        }
    }

    #[test]
    fn null_holes_in_args_are_treated_as_absent_but_still_reachable() {
        // EJSON writes an `undefined` array element as null.
        let frame = parse(
            r#"{"msg":"changed","collection":"stream-notify-room","id":"id",
                "fields":{"eventName":"GENERAL/typing","args":["rocket.cat",null,true]}}"#,
        );
        let event = frame.as_stream_event().unwrap();
        assert_eq!(event.arity(), 3);
        assert_eq!(event.arg_str(0), Some("rocket.cat"));
        assert_eq!(event.arg(1), None);
        assert_eq!(event.arg_raw(1), Some(&Value::Null));
        assert_eq!(event.arg_bool(2), Some(true));
        assert_eq!(event.arg(9), None);
        assert_eq!(event.arg_raw(9), None);
        assert_eq!(event.arg_i64(0), None);
        assert_eq!(event.arg_as::<String>(0).as_deref(), Some("rocket.cat"));
        assert_eq!(event.arg_as::<i64>(0), None);
    }

    #[test]
    fn a_stream_frame_without_args_is_still_an_event() {
        let frame = parse(
            r#"{"msg":"changed","collection":"stream-notify-all","id":"id",
                "fields":{"eventName":"public-settings-changed"}}"#,
        );
        let event = frame.as_stream_event().unwrap();
        assert_eq!(event.arity(), 0);
        assert!(event.args.is_empty());
    }

    #[test]
    fn non_stream_frames_are_not_stream_events() {
        // A real collection.
        assert!(
            parse(r#"{"msg":"changed","collection":"users","id":"u","fields":{"a":1}}"#)
                .as_stream_event()
                .is_none()
        );
        // A stream collection but no eventName.
        assert!(
            parse(r#"{"msg":"changed","collection":"stream-x","id":"id","fields":{"args":[]}}"#)
                .as_stream_event()
                .is_none()
        );
        // A stream collection with no fields at all.
        assert!(
            parse(r#"{"msg":"changed","collection":"stream-x","id":"id","cleared":["a"]}"#)
                .as_stream_event()
                .is_none()
        );
        // A different frame type entirely.
        assert!(parse(r#"{"msg":"ready","subs":["1"]}"#).as_stream_event().is_none());
    }

    #[test]
    fn stream_fields_deserialize_on_their_own() {
        let fields: StreamFields =
            serde_json::from_str(r#"{"eventName":"GENERAL","args":[1,"two"]}"#).unwrap();
        assert_eq!(fields.event_name, "GENERAL");
        assert_eq!(fields.args, vec![json!(1), json!("two")]);

        // `args` is absent on some notify streams.
        let bare: StreamFields = serde_json::from_str(r#"{"eventName":"x"}"#).unwrap();
        assert!(bare.args.is_empty());
    }

    // -- representation constraints ---------------------------------------

    #[test]
    fn borrowed_raw_value_cannot_work_inside_an_internally_tagged_enum() {
        // `#[serde(tag = "msg")]` buffers the frame through serde's `Content`, which has no
        // representation for `RawValue`'s private newtype. This is why no field here is a
        // `&RawValue`, however tempting it is for `fields` / `result`.
        #[derive(Debug, Deserialize)]
        #[serde(tag = "msg", rename_all = "camelCase")]
        #[allow(dead_code, reason = "the point of the test is that this never decodes")]
        enum Raw<'a> {
            Result {
                id: String,
                #[serde(borrow)]
                result: &'a serde_json::value::RawValue,
            },
        }

        let err = serde_json::from_str::<Raw<'_>>(r#"{"msg":"result","id":"1","result":{"a":1}}"#)
            .unwrap_err();
        assert!(
            err.to_string().contains("newtype struct"),
            "expected the RawValue buffering failure, got: {err}"
        );

        // The owned form decodes fine.
        assert!(matches!(
            parse(r#"{"msg":"result","id":"1","result":{"a":1}}"#),
            ServerMessage::Result { result: Some(_), .. }
        ));
    }

    #[test]
    fn every_known_frame_round_trips_through_json() {
        let frames = [
            r#"{"msg":"connected","session":"s"}"#,
            r#"{"msg":"failed","version":"1"}"#,
            r#"{"msg":"ping"}"#,
            r#"{"msg":"pong","id":"1"}"#,
            r#"{"msg":"result","id":"1","result":[1,2]}"#,
            r#"{"msg":"result","id":"1","error":{"error":403,"reason":"nope"}}"#,
            r#"{"msg":"updated","methods":["1"]}"#,
            r#"{"msg":"nosub","id":"1"}"#,
            r#"{"msg":"ready","subs":["1","2"]}"#,
            r#"{"msg":"added","collection":"c","id":"i","fields":{"a":1}}"#,
            r#"{"msg":"changed","collection":"c","id":"i","cleared":["a"]}"#,
            r#"{"msg":"removed","collection":"c","id":"i"}"#,
            r#"{"msg":"addedBefore","collection":"c","id":"i","before":"j"}"#,
            r#"{"msg":"movedBefore","collection":"c","id":"i","before":"j"}"#,
            r#"{"msg":"error","reason":"Bad request"}"#,
            r#"{"msg":"server_id","server_id":"0"}"#,
            r#"{"server_id":"0"}"#,
        ];

        for frame in frames {
            let decoded = parse(frame);
            let reencoded = serde_json::to_string(&decoded).unwrap();
            assert_eq!(parse(&reencoded), decoded, "round trip changed {frame}");
            assert_eq!(
                serde_json::from_str::<Value>(&reencoded).unwrap(),
                serde_json::from_str::<Value>(frame).unwrap(),
                "re-encoding {frame} lost or invented members"
            );
        }
    }
}
