//! Sending messages.
//!
//! Two endpoints send a message, and they are not interchangeable.
//!
//! | | [`SendMessage`] → `chat.sendMessage` | [`PostMessage`] → `chat.postMessage` |
//! |---|---|---|
//! | Target | exactly one room, by id | one *or many*, by id **or** `#name` / `@name` |
//! | `blocks` (UI Kit) | **yes — only here** | no, rejected by the schema |
//! | Client-chosen `_id` | yes | no |
//! | Threading | `tmid` + `tshow` | `tmid`, and only with `roomId` |
//! | `groupable` | left unset — clients group adjacent messages | forced `false` |
//! | Joins the room for you | no | yes, for a `#channel` target |
//! | Deprecated | no | no, but it is the webhook compatibility path |
//!
//! [`SendMessage`] is the primary path. Reach for [`PostMessage`] only when you need what
//! only it does: fan-out to several rooms, or addressing a room by name without having
//! resolved its id.
//!
//! # Not over DDP
//!
//! The DDP `sendMessage` method is deprecated for removal in 9.0, and its `check()`
//! whitelist rejects `attachments`, `blocks`, `alias`, `avatar` and `emoji` outright — a
//! call carrying any of them fails with `Match.Error` rather than sending a plainer message.
//! There is no reason to send over the socket.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use rocketsocket_model::entity::Message;
use rocketsocket_model::{MessageId, RoomId};

/// A `chat.sendMessage` request: one message, one room, full message shape.
///
/// ```no_run
/// # use rocketsocket_rest::SendMessage;
/// # use rocketsocket_model::RoomId;
/// # fn example(rid: RoomId) -> SendMessage {
/// SendMessage::new(rid).text("deploy finished").blocks(vec![serde_json::json!({
///     "type": "section",
///     "text": { "type": "mrkdwn", "text": "*build 412* is live" },
/// })])
/// # }
/// ```
///
/// # No timestamp field
///
/// There is deliberately no way to set `ts`. `executeSendMessage` compares a client-supplied
/// timestamp against server time: more than 60 s of skew is rejected outright with
/// `error-message-ts-out-of-sync`, and 10–60 s is *silently* overwritten with server time.
/// Omitting it is the only behaviour that is correct on a machine whose clock drifts.
///
/// # Permissions
///
/// [`alias`](Self::alias) and [`avatar`](Self::avatar) require the `message-impersonate`
/// permission, which the built-in `bot` role grants; without it the send fails outright
/// rather than dropping the fields. [`emoji`](Self::emoji) does **not**: `validateMessage`
/// gates on `message.alias || message.avatar` only.
#[derive(Debug, Clone, Serialize)]
pub struct SendMessage {
    message: OutgoingMessage,
    /// Sibling of `message`, not part of it.
    #[serde(rename = "previewUrls", skip_serializing_if = "Option::is_none")]
    preview_urls: Option<Vec<String>>,
}

/// The `message` member of a `chat.sendMessage` body.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutgoingMessage {
    rid: RoomId,
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    id: Option<MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tmid: Option<MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tshow: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    emoji: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avatar: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attachments: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blocks: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    custom_fields: Option<Value>,
}

impl SendMessage {
    /// An empty message for `rid`.
    ///
    /// A message with neither text, attachments nor blocks is rejected by the server, so
    /// call at least one of [`text`](Self::text), [`attachments`](Self::attachments) or
    /// [`blocks`](Self::blocks).
    #[must_use]
    pub fn new(rid: impl Into<RoomId>) -> Self {
        Self {
            message: OutgoingMessage {
                rid: rid.into(),
                id: None,
                msg: None,
                tmid: None,
                tshow: None,
                alias: None,
                emoji: None,
                avatar: None,
                attachments: None,
                blocks: None,
                custom_fields: None,
            },
            preview_urls: None,
        }
    }

    /// The message body, in Rocket.Chat markdown.
    #[must_use]
    pub fn text(mut self, msg: impl Into<String>) -> Self {
        self.message.msg = Some(msg.into());
        self
    }

    /// A client-chosen message id.
    ///
    /// `chat.sendMessage` is the only send endpoint that accepts one, and it is the only
    /// defence against a timeout that may or may not have posted: `sendMessage` looks the id
    /// up before inserting and, if it already exists, **returns without inserting**. Reusing
    /// the id on a retry therefore cannot post the message twice.
    ///
    /// It does not make the retry return the original message, though — that early return
    /// yields nothing for the handler to serialize, so the duplicate attempt answers with an
    /// error rather than the stored message. Treat a failed retry of a reused id as "already
    /// delivered", not as "not delivered".
    #[must_use]
    pub fn client_id(mut self, id: impl Into<MessageId>) -> Self {
        self.message.id = Some(id.into());
        self
    }

    /// Reply inside the thread rooted at `tmid`.
    ///
    /// Threads must be enabled workspace-wide (`Threads_enabled`) or the send fails with
    /// `error-not-allowed`. Threads do not nest: if `tmid` is itself a thread reply, the
    /// server rewrites it to that thread's root.
    #[must_use]
    pub fn thread(mut self, tmid: impl Into<MessageId>) -> Self {
        self.message.tmid = Some(tmid.into());
        self
    }

    /// Also show this thread reply in the main channel (`tshow`).
    ///
    /// Only valid together with [`thread`](Self::thread); on its own the server answers
    /// `invalid-params`, "tshow provided but missing tmid".
    #[must_use]
    pub fn thread_show(mut self, show: bool) -> Self {
        self.message.tshow = Some(show);
        self
    }

    /// Display name to post under. Requires `message-impersonate`.
    #[must_use]
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.message.alias = Some(alias.into());
        self
    }

    /// Emoji to use as the avatar, e.g. `":robot:"`.
    ///
    /// Unlike [`alias`](Self::alias) and [`avatar`](Self::avatar) this needs no permission.
    #[must_use]
    pub fn emoji(mut self, emoji: impl Into<String>) -> Self {
        self.message.emoji = Some(emoji.into());
        self
    }

    /// Image URL to use as the avatar. Requires `message-impersonate`.
    #[must_use]
    pub fn avatar(mut self, avatar: impl Into<String>) -> Self {
        self.message.avatar = Some(avatar.into());
        self
    }

    /// Message attachments, as raw JSON.
    ///
    /// Raw because the attachment union is large, versioned, and only partially documented;
    /// see the note on `Message::attachments` in `rocketsocket-model`. Attachment *action*
    /// buttons live here, and are — awkwardly — the only interactivity a plain bot can
    /// actually receive.
    #[must_use]
    pub fn attachments(mut self, attachments: Vec<Value>) -> Self {
        self.message.attachments = Some(attachments);
        self
    }

    /// UI Kit blocks, as raw JSON.
    ///
    /// **This endpoint is the only way to send them.** `chat.postMessage`'s schema has
    /// `additionalProperties: false` and no `blocks` member, and DDP `sendMessage` rejects
    /// them in its `check()` whitelist.
    ///
    /// Blocks are write-only for a plain bot: button clicks are routed to
    /// `POST /api/apps/ui.interaction/:appId` and answered by the Apps-Engine, so an
    /// unregistered app id 404s and the click never reaches you. Use blocks for rich
    /// display; use attachment actions or slash commands for interaction.
    #[must_use]
    pub fn blocks(mut self, blocks: Vec<Value>) -> Self {
        self.message.blocks = Some(blocks);
        self
    }

    /// Arbitrary custom fields stored on the message.
    #[must_use]
    pub fn custom_fields(mut self, fields: Value) -> Self {
        self.message.custom_fields = Some(fields);
        self
    }

    /// Restrict link previews to this list of URLs.
    ///
    /// An empty list disables previews for the message; omitting the field leaves the
    /// workspace default in place.
    #[must_use]
    pub fn preview_urls(mut self, urls: Vec<String>) -> Self {
        self.preview_urls = Some(urls);
        self
    }

    /// The room this message is addressed to.
    #[must_use]
    pub fn room_id(&self) -> &RoomId {
        &self.message.rid
    }
}

/// Where a [`PostMessage`] goes.
///
/// The two forms are mutually exclusive: the endpoint's schema is a `oneOf` of two objects,
/// each with `additionalProperties: false`, so a body carrying both `roomId` and `channel`
/// matches neither branch and is rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PostTarget {
    /// Room ids. Fastest — no name resolution, no joining.
    RoomIds(Vec<String>),

    /// Names, resolved by the server's webhook logic:
    ///
    /// - `#name` — a channel, **joining it if the sender is not a member**;
    /// - `@name` — the direct-message room with that user, created on demand;
    /// - anything else — tried as an id or name first, then as a direct message, and
    ///   `invalid-channel` if neither matches.
    ///
    /// The auto-join is the part to be careful with: posting to `#some-channel` silently
    /// adds the bot to it and produces a visible "has joined" system message.
    Channels(Vec<String>),
}

/// A `chat.postMessage` request: the webhook-compatible send path.
///
/// Use it for what [`SendMessage`] cannot do — several rooms in one call, or a room named
/// rather than identified. Everything else is a reason to prefer [`SendMessage`].
///
/// # Defaults that differ from a normal message
///
/// - **`groupable` is forced to `false`** (`processWebhookMessage`: `groupable:
///   messageObj.groupable !== undefined ? … : false`), so consecutive posts are never
///   collapsed into one visual block. The endpoint schema rejects a `groupable` member, so
///   this cannot be overridden here — only an incoming-webhook integration can.
/// - **`parseUrls` defaults to `!attachments`**: link previews are on for a plain text post
///   and off as soon as the message carries attachments.
/// - `blocks` is not accepted at all.
///
/// # The response describes one room
///
/// Even for a multi-room post the handler returns `processWebhookMessage(...)[0]` — the
/// first target's message only. Treat [`PostedMessage`] as a receipt, not as a complete
/// result set, and use [`SendMessage`] per room when you need each message id.
#[derive(Debug, Clone)]
pub struct PostMessage {
    target: PostTarget,
    text: Option<String>,
    alias: Option<String>,
    emoji: Option<String>,
    avatar: Option<String>,
    attachments: Option<Vec<Value>>,
    custom_fields: Option<Value>,
    parse_urls: Option<bool>,
    tmid: Option<MessageId>,
}

impl PostMessage {
    /// Post to one room by id.
    #[must_use]
    pub fn to_room(rid: impl Into<RoomId>) -> Self {
        Self::new(PostTarget::RoomIds(vec![rid.into().as_ref().to_owned()]))
    }

    /// Post the same message to several rooms by id.
    #[must_use]
    pub fn to_rooms(rids: impl IntoIterator<Item = RoomId>) -> Self {
        Self::new(PostTarget::RoomIds(
            rids.into_iter().map(|rid| rid.as_ref().to_owned()).collect(),
        ))
    }

    /// Post to one target by name — `#channel`, `@user`, or a bare name.
    ///
    /// See [`PostTarget::Channels`] for the resolution rules and the auto-join.
    #[must_use]
    pub fn to_channel(channel: impl Into<String>) -> Self {
        Self::new(PostTarget::Channels(vec![channel.into()]))
    }

    /// Post the same message to several named targets.
    #[must_use]
    pub fn to_channels(channels: impl IntoIterator<Item = String>) -> Self {
        Self::new(PostTarget::Channels(channels.into_iter().collect()))
    }

    /// Post to an explicit target.
    #[must_use]
    pub fn new(target: PostTarget) -> Self {
        Self {
            target,
            text: None,
            alias: None,
            emoji: None,
            avatar: None,
            attachments: None,
            custom_fields: None,
            parse_urls: None,
            tmid: None,
        }
    }

    /// The message body.
    #[must_use]
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    /// Display name to post under. Requires `message-impersonate`.
    #[must_use]
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Emoji avatar. Needs no permission, unlike [`alias`](Self::alias).
    #[must_use]
    pub fn emoji(mut self, emoji: impl Into<String>) -> Self {
        self.emoji = Some(emoji.into());
        self
    }

    /// Image-URL avatar. Requires `message-impersonate`.
    #[must_use]
    pub fn avatar(mut self, avatar: impl Into<String>) -> Self {
        self.avatar = Some(avatar.into());
        self
    }

    /// Attachments, as raw JSON. Setting them also turns link previews off by default.
    #[must_use]
    pub fn attachments(mut self, attachments: Vec<Value>) -> Self {
        self.attachments = Some(attachments);
        self
    }

    /// Custom fields stored on the message.
    #[must_use]
    pub fn custom_fields(mut self, fields: Value) -> Self {
        self.custom_fields = Some(fields);
        self
    }

    /// Override link-preview parsing.
    #[must_use]
    pub fn parse_urls(mut self, parse: bool) -> Self {
        self.parse_urls = Some(parse);
        self
    }

    /// Reply in a thread.
    ///
    /// Only valid with a [`PostTarget::RoomIds`] target — `tmid` is not a member of the
    /// `channel` branch of the schema, and that branch forbids extra properties, so a
    /// threaded post addressed by name is rejected with a 400.
    #[must_use]
    pub fn thread(mut self, tmid: impl Into<MessageId>) -> Self {
        self.tmid = Some(tmid.into());
        self
    }

    /// Where this message is addressed.
    #[must_use]
    pub fn target(&self) -> &PostTarget {
        &self.target
    }
}

impl Serialize for PostMessage {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;

        // Only the members the target's schema branch allows are emitted.
        let mut len = 1;
        len += usize::from(self.text.is_some());
        len += usize::from(self.alias.is_some());
        len += usize::from(self.emoji.is_some());
        len += usize::from(self.avatar.is_some());
        len += usize::from(self.attachments.is_some());
        len += usize::from(self.custom_fields.is_some());
        len += usize::from(self.parse_urls.is_some());
        len += usize::from(self.tmid.is_some());

        let mut state = serializer.serialize_struct("PostMessage", len)?;
        match &self.target {
            PostTarget::RoomIds(ids) => state.serialize_field("roomId", ids)?,
            PostTarget::Channels(names) => state.serialize_field("channel", names)?,
        }
        if let Some(text) = &self.text {
            state.serialize_field("text", text)?;
        }
        if let Some(alias) = &self.alias {
            state.serialize_field("alias", alias)?;
        }
        if let Some(emoji) = &self.emoji {
            state.serialize_field("emoji", emoji)?;
        }
        if let Some(avatar) = &self.avatar {
            state.serialize_field("avatar", avatar)?;
        }
        if let Some(attachments) = &self.attachments {
            state.serialize_field("attachments", attachments)?;
        }
        if let Some(fields) = &self.custom_fields {
            state.serialize_field("customFields", fields)?;
        }
        if let Some(parse) = &self.parse_urls {
            state.serialize_field("parseUrls", parse)?;
        }
        if let Some(tmid) = &self.tmid {
            state.serialize_field("tmid", tmid)?;
        }
        state.end()
    }
}

/// What `chat.postMessage` answers with.
#[derive(Debug, Clone, Deserialize)]
pub struct PostedMessage {
    /// The target string the server resolved, echoed back — an id or a `#name`.
    pub channel: String,
    /// The message as stored, for the **first** target only.
    pub message: Message,
    /// `Date.now()` at the moment the response was built, in milliseconds.
    ///
    /// Not the message's timestamp: read `message.ts` for that.
    pub ts: i64,
}

/// The `{message: …}` envelope shared by `chat.sendMessage` and `rooms.mediaConfirm`.
#[derive(Debug, Deserialize)]
pub(crate) struct MessageEnvelope {
    pub(crate) message: Message,
}
