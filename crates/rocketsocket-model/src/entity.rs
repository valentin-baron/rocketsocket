//! Rocket.Chat domain entities: messages, rooms, subscriptions and users.
//!
//! # What these types are modelled from
//!
//! The shapes here follow the server's own `packages/core-typings` interfaces (`IMessage`,
//! `IRoom`, `ISubscription`, `IUser`) as of Rocket.Chat 8.x, *corrected* against
//! `apps/meteor/lib/publishFields.ts` — the Mongo projections actually applied before a
//! document is pushed onto a DDP stream.
//!
//! Those two sources disagree, and the projections win. `IRoom.msgs` and `IRoom.usersCount`
//! are declared non-optional in TypeScript; `IUser.roles`, `IUser.type` and `IUser.active`
//! likewise. All of them are routinely absent from stream payloads, because the projection
//! that produced the document never asked for them. On top of that, a DDP `changed` message
//! carries only the fields that changed, so *any* field can be missing from an update frame.
//!
//! The rule this module follows is therefore blunt:
//!
//! - a field is non-`Option` only if the server cannot construct the document without it
//! - everything else is `Option<T>` with `#[serde(default)]`
//! - no `deny_unknown_fields`, anywhere
//! - every wire enum carries an `Unknown` variant that round-trips byte-identically
//!
//! A deserialization failure on a payload a real server produced is a bug in this crate.
//!
//! # Traps worth knowing about
//!
//! - [`Message::starred`] is a *list of user ids*, not a boolean. [`Message::pinned`] is a
//!   boolean. They do not work the same way.
//! - [`Message::bot`] is deprecated and is not set for messages from users holding the `bot`
//!   role. See its documentation.
//! - [`Room::sys_mes`] is `bool | MessageType[]`, and [`Room::role_priorities_created`] is
//!   `bool | number`. Both are modelled as untagged enums. Only the array arm of `sysMes`
//!   means anything to the server — see [`SysMes`].
//! - [`PresenceStatus`] arrives as an integer on the presence stream, and `0` is ambiguous:
//!   it means both "offline" and "disabled".
//! - `IUser.services` is deliberately not modelled. See [`User`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::datetime::Timestamp;
use crate::id::{MessageId, RoleId, RoomId, SubscriptionId, UploadId, UserId};

/// Defines a string-valued wire enum that can never fail to decode.
///
/// The generated type has one variant per known value plus `Unknown(String)`, and converts
/// through `String` in both directions, so an unrecognised value survives a decode/encode
/// round trip byte-for-byte. Rocket.Chat has added values to every one of these enums in
/// minor releases; failing on an unknown one would break clients against newer servers.
macro_rules! wire_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $( $(#[$vmeta:meta])* $variant:ident = $wire:literal ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(from = "String", into = "String")]
        #[non_exhaustive]
        pub enum $name {
            $( $(#[$vmeta])* $variant, )*
            /// A value this crate does not know about.
            ///
            /// Round-trips unchanged, so forwarding a payload never corrupts it.
            Unknown(String),
        }

        impl $name {
            /// The value as it appears on the wire.
            #[must_use]
            pub fn as_str(&self) -> &str {
                match self {
                    $( Self::$variant => $wire, )*
                    Self::Unknown(other) => other.as_str(),
                }
            }

            /// Whether this value was not recognised by this version of the crate.
            #[must_use]
            pub fn is_unknown(&self) -> bool {
                matches!(self, Self::Unknown(_))
            }
        }

        impl core::fmt::Display for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                match value {
                    $( $wire => Self::$variant, )*
                    other => Self::Unknown(other.to_owned()),
                }
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                match value.as_str() {
                    $( $wire => Self::$variant, )*
                    // Reuse the allocation rather than copying it.
                    _ => Self::Unknown(value),
                }
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                match value {
                    $( $name::$variant => $wire.to_owned(), )*
                    $name::Unknown(other) => other,
                }
            }
        }
    };
}

// ---------------------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------------------

wire_enum! {
    /// The `t` discriminator of a system message.
    ///
    /// A message *without* a `t` is an ordinary user message; a message *with* one is
    /// rendered by the client from the type and the surrounding fields rather than from
    /// [`Message::msg`], which for many of these types holds a bare parameter (a username,
    /// a topic, a role name) rather than prose.
    ///
    /// The variant list mirrors `MessageTypes` in `packages/core-typings/src/IMessage/IMessage.ts`.
    /// It has grown in every major release — Rocket.Chat 8 added `message_pinned_e2e` and
    /// `abac-removed-user-from-room` — and databases carry types that were dropped from the
    /// list years ago (`jitsi_call_started`, `user_joined_otr`, `message_snippeted`). All of
    /// those decode to [`MessageType::Unknown`].
    pub enum MessageType {
        /// End-to-end encrypted message; the ciphertext is in `msg` or `content`.
        E2e = "e2e",
        /// User joined the channel.
        Uj = "uj",
        /// User invited another user.
        Ui = "ui",
        /// User invited another user, who has not yet accepted.
        Uir = "uir",
        /// User left the channel.
        Ul = "ul",
        /// A user was removed from the room by someone else.
        Ru = "ru",
        /// A user was added to the room by someone else.
        Au = "au",
        /// A user was muted or unmuted (legacy combined type).
        MuteUnmute = "mute_unmute",
        /// Room name changed.
        R = "r",
        /// User joined the conversation (direct/discussion variant of `uj`).
        Ut = "ut",
        /// Welcome message.
        Wm = "wm",
        /// Message removed. Combined with `editedAt`, this is a deletion tombstone —
        /// see [`Message::is_deleted_tombstone`].
        Rm = "rm",
        /// A room role was granted to a subscription.
        SubscriptionRoleAdded = "subscription-role-added",
        /// A room role was revoked from a subscription.
        SubscriptionRoleRemoved = "subscription-role-removed",
        /// Room archived.
        RoomArchived = "room-archived",
        /// Room unarchived.
        RoomUnarchived = "room-unarchived",
        /// Room switched between public and private.
        RoomChangedPrivacy = "room_changed_privacy",
        /// Room description changed.
        RoomChangedDescription = "room_changed_description",
        /// Room announcement changed.
        RoomChangedAnnouncement = "room_changed_announcement",
        /// Room avatar changed.
        RoomChangedAvatar = "room_changed_avatar",
        /// Room topic changed.
        RoomChangedTopic = "room_changed_topic",
        /// End-to-end encryption enabled for the room.
        RoomE2eEnabled = "room_e2e_enabled",
        /// End-to-end encryption disabled for the room.
        RoomE2eDisabled = "room_e2e_disabled",
        /// A user was muted.
        UserMuted = "user-muted",
        /// A user was unmuted.
        UserUnmuted = "user-unmuted",
        /// A user was banned.
        UserBanned = "user-banned",
        /// A user was unbanned.
        UserUnbanned = "user-unbanned",
        /// Read-only was lifted from the room.
        RoomRemovedReadOnly = "room-removed-read-only",
        /// The room was set read-only.
        RoomSetReadOnly = "room-set-read-only",
        /// Reacting was allowed in a read-only room.
        RoomAllowedReacting = "room-allowed-reacting",
        /// Reacting was disallowed in a read-only room.
        RoomDisallowedReacting = "room-disallowed-reacting",
        /// A slash command was run.
        Command = "command",
        /// A video conference was started; the call state lives in a separate collection.
        Videoconf = "videoconf",
        /// A message was pinned; the pinned message is carried in `attachments`.
        MessagePinned = "message_pinned",
        /// A message was pinned in an end-to-end encrypted room.
        MessagePinnedE2e = "message_pinned_e2e",
        /// A user was made moderator.
        NewModerator = "new-moderator",
        /// A user's moderator role was removed.
        ModeratorRemoved = "moderator-removed",
        /// A user was made owner.
        NewOwner = "new-owner",
        /// A user's owner role was removed.
        OwnerRemoved = "owner-removed",
        /// A user was made leader.
        NewLeader = "new-leader",
        /// A user's leader role was removed.
        LeaderRemoved = "leader-removed",
        /// A discussion was created from this message; see [`Message::drid`].
        DiscussionCreated = "discussion-created",
        /// A user lost access to an attribute-based-access-control room.
        AbacRemovedUserFromRoom = "abac-removed-user-from-room",

        // --- team types ---
        /// A user was removed from the team.
        RemovedUserFromTeam = "removed-user-from-team",
        /// A user was added to the team.
        AddedUserToTeam = "added-user-to-team",
        /// A user left the team.
        Ult = "ult",
        /// A channel was converted into a team.
        UserConvertedToTeam = "user-converted-to-team",
        /// A team was converted back into a channel.
        UserConvertedToChannel = "user-converted-to-channel",
        /// A room was removed from the team but kept.
        UserRemovedRoomFromTeam = "user-removed-room-from-team",
        /// A room belonging to the team was deleted.
        UserDeletedRoomFromTeam = "user-deleted-room-from-team",
        /// An existing room was added to the team.
        UserAddedRoomToTeam = "user-added-room-to-team",
        /// A user joined the team.
        Ujt = "ujt",

        // --- livechat / omnichannel types ---
        /// Visitor page navigation history.
        LivechatNavigationHistory = "livechat_navigation_history",
        /// The conversation was transferred to another agent or department.
        LivechatTransferHistory = "livechat_transfer_history",
        /// A transcript was requested or sent.
        LivechatTranscriptHistory = "livechat_transcript_history",
        /// A video call was placed from the livechat widget.
        LivechatVideoCall = "livechat_video_call",
        /// The transfer fell back to a default department or agent.
        LivechatTransferHistoryFallback = "livechat_transfer_history_fallback",
        /// The livechat conversation was closed.
        LivechatClose = "livechat-close",
        /// The livechat conversation was started.
        LivechatStarted = "livechat-started",
        /// The conversation priority changed; see the message's `priorityData`.
        OmnichannelPriorityChangeHistory = "omnichannel_priority_change_history",
        /// The conversation SLA changed; see the message's `slaData`.
        OmnichannelSlaChangeHistory = "omnichannel_sla_change_history",
        /// The conversation was placed on hold.
        OmnichannelPlacedChatOnHold = "omnichannel_placed_chat_on_hold",
        /// An on-hold conversation was resumed.
        OmnichannelOnHoldChatResumed = "omnichannel_on_hold_chat_resumed",
    }
}

wire_enum! {
    /// The kind of a room, the `t` field of [`Room`] and [`Subscription`].
    pub enum RoomType {
        /// Public channel.
        Channel = "c",
        /// Private group.
        Private = "p",
        /// Direct message room. May hold more than two participants.
        Direct = "d",
        /// Omnichannel / livechat conversation.
        Omnichannel = "l",
    }
}

impl RoomType {
    /// A direct message room (`d`).
    ///
    /// Note that a direct room is not necessarily a *pair*: Rocket.Chat supports multi-user
    /// DMs, distinguished by `uids.len() > 2` on the room.
    #[must_use]
    pub fn is_direct(&self) -> bool {
        matches!(self, Self::Direct)
    }

    /// A public channel (`c`). Anyone on the server may read and join it.
    #[must_use]
    pub fn is_public(&self) -> bool {
        matches!(self, Self::Channel)
    }

    /// A private group (`p`). Membership is required to see it.
    #[must_use]
    pub fn is_private(&self) -> bool {
        matches!(self, Self::Private)
    }

    /// An omnichannel (livechat) conversation (`l`).
    #[must_use]
    pub fn is_omnichannel(&self) -> bool {
        matches!(self, Self::Omnichannel)
    }

    /// A channel or a private group — a room with a name that users subscribe to,
    /// as opposed to a DM or a livechat conversation.
    #[must_use]
    pub fn is_group(&self) -> bool {
        matches!(self, Self::Channel | Self::Private)
    }
}

wire_enum! {
    /// A user's presence.
    ///
    /// Three separate fields on [`User`] use this type and they mean different things; see
    /// [`User::status`], [`User::status_default`] and [`User::status_connection`].
    pub enum UserStatus {
        /// Connected and active.
        Online = "online",
        /// Connected but idle, or manually set to away.
        Away = "away",
        /// Manually set to do-not-disturb.
        Busy = "busy",
        /// Not connected.
        Offline = "offline",
        /// The account is deactivated. This is an account state, not a connection state,
        /// and the presence stream cannot express it — see [`PresenceStatus`].
        Disabled = "disabled",
    }
}

wire_enum! {
    /// Where a user's status came from.
    pub enum PresenceSource {
        /// Derived by the server from connection activity.
        Internal = "internal",
        /// Pushed by an external integration.
        External = "external",
        /// Chosen by the user.
        Manual = "manual",
    }
}

wire_enum! {
    /// End-to-end encryption state of a message.
    pub enum E2eStatus {
        /// The ciphertext has not been decrypted yet.
        Pending = "pending",
        /// The message has been decrypted.
        Done = "done",
    }
}

wire_enum! {
    /// What a [`MessageMention`] points at.
    ///
    /// Absent for the `all` and `here` pseudo-mentions, which have no type at all.
    pub enum MentionType {
        /// A user mention.
        User = "user",
        /// A team mention.
        Team = "team",
    }
}

wire_enum! {
    /// Notification volume for a subscription.
    pub enum NotificationPreference {
        /// Notify on every message.
        All = "all",
        /// Notify only on mentions.
        Mentions = "mentions",
        /// Never notify.
        Nothing = "nothing",
    }
}

wire_enum! {
    /// Which layer a notification preference was resolved from.
    pub enum PreferenceOrigin {
        /// The per-room subscription setting.
        Subscription = "subscription",
        /// The user's global preference.
        User = "user",
    }
}

wire_enum! {
    /// When a subscription should raise its unread badge.
    pub enum UnreadAlert {
        /// Follow the user's global preference.
        Default = "default",
        /// Any message.
        All = "all",
        /// Mentions only.
        Mentions = "mentions",
        /// Never.
        Nothing = "nothing",
    }
}

wire_enum! {
    /// Membership state of a subscription that is not a plain active membership.
    pub enum SubscriptionStatus {
        /// The user was invited and has not accepted yet.
        Invited = "INVITED",
        /// The user was banned from the room.
        Banned = "BANNED",
    }
}

/// Presence as it appears on the `stream-notify-logged` `user-status` event and on the
/// `/presence` payloads: a small integer rather than a string.
///
/// The server maps [`UserStatus`] onto this with
/// `{offline: 0, online: 1, away: 2, busy: 3, disabled: 0}` — note the collision. **Code `0`
/// is ambiguous**: it means either "offline" or "the account is deactivated", and the
/// distinction is not recoverable from the presence stream. Use the user document's
/// `status` field if you need to tell them apart.
///
/// A decoded value re-encodes to the exact integer it came from, including for
/// [`PresenceStatus::Unknown`]. (A hand-built `Unknown(0..=3)` is the one value that does
/// not survive a round trip, because those codes decode to their named variants.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(from = "u8", into = "u8")]
#[non_exhaustive]
pub enum PresenceStatus {
    /// `0` — offline, or a disabled account.
    Offline,
    /// `1` — online.
    Online,
    /// `2` — away.
    Away,
    /// `3` — busy.
    Busy,
    /// A code this crate does not know about. Round-trips unchanged.
    Unknown(u8),
}

impl PresenceStatus {
    /// The numeric wire form.
    #[must_use]
    pub fn code(self) -> u8 {
        u8::from(self)
    }

    /// The equivalent [`UserStatus`], where one exists.
    ///
    /// `0` maps to [`UserStatus::Offline`]; [`UserStatus::Disabled`] is unreachable through
    /// this conversion because the server flattens it into `0`.
    #[must_use]
    pub fn as_user_status(self) -> Option<UserStatus> {
        match self {
            Self::Offline => Some(UserStatus::Offline),
            Self::Online => Some(UserStatus::Online),
            Self::Away => Some(UserStatus::Away),
            Self::Busy => Some(UserStatus::Busy),
            Self::Unknown(_) => None,
        }
    }

    /// Whether this code was not recognised by this version of the crate.
    #[must_use]
    pub fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown(_))
    }
}

impl From<u8> for PresenceStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Offline,
            1 => Self::Online,
            2 => Self::Away,
            3 => Self::Busy,
            other => Self::Unknown(other),
        }
    }
}

impl From<PresenceStatus> for u8 {
    fn from(value: PresenceStatus) -> Self {
        match value {
            PresenceStatus::Offline => 0,
            PresenceStatus::Online => 1,
            PresenceStatus::Away => 2,
            PresenceStatus::Busy => 3,
            PresenceStatus::Unknown(other) => other,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Shared sub-documents
// ---------------------------------------------------------------------------------------

/// The embedded user stub Rocket.Chat denormalises into messages, rooms and subscriptions.
///
/// `IMessage.u` is typed as `Required<Pick<IUser, '_id' | 'username'>> & Pick<IUser, 'name'>`,
/// so in theory `username` is always there. In practice `IUser.username` is optional at the
/// source, imports and app-authored messages have produced stubs without it, and DDP
/// `changed` frames can deliver a partial `u`. Only `_id` is required here.
///
/// `name` may be **absent or explicitly `null`**, and which one you get depends on the code
/// path that built the stub, not on anything meaningful. The message factories copy it
/// unconditionally (`u: {_id, username, name: user.name}` in both `prepareMessageObject` and
/// `Messages.createWithTypeRoomIdMessageUserAndUnread`), and the workspace runs the Mongo
/// driver with `ignoreUndefined: false`, so a user with no real name is persisted as
/// `name: null`. Other factories guard the key (`...(user.name && { name: user.name })`) and
/// leave it out entirely. Both decode to `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRef {
    /// The user's id.
    #[serde(rename = "_id")]
    pub id: UserId,
    /// The login name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The display name. Absent or `null` when the user has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl UserRef {
    /// The name to show, honouring the server's `UI_Use_Real_Name` setting.
    ///
    /// Falls back to whichever of the two is present, since a stub projected for a stream
    /// may carry only one.
    #[must_use]
    pub fn display_name(&self, use_real_name: bool) -> Option<&str> {
        let (first, second) =
            if use_real_name { (&self.name, &self.username) } else { (&self.username, &self.name) };
        first.as_deref().or(second.as_deref())
    }
}

/// One entry of [`Message::mentions`].
///
/// The `all` and `here` pseudo-mentions carry no [`MessageMention::mention_type`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageMention {
    /// Id of the mentioned user or team. For `all`/`here` this is the literal keyword.
    #[serde(rename = "_id")]
    pub id: String,
    /// What was mentioned. Absent for `all` and `here`.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub mention_type: Option<MentionType>,
    /// Display name of the mentioned user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Login name of the mentioned user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Friendly room name, set when the mention resolves to a team's main channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fname: Option<String>,
}

/// One entry of [`Message::channels`]: a `#channel` reference resolved at send time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelMention {
    /// Id of the referenced room.
    #[serde(rename = "_id")]
    pub id: RoomId,
    /// Name of the referenced room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// A URL found in a message body, together with whatever preview metadata was scraped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageUrl {
    /// The URL as it appeared in the message.
    pub url: String,
    /// The source text the URL was extracted from, when it differs from `url`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Scraped OpenGraph / oEmbed metadata.
    ///
    /// Typed as `Record<string, string>` on the server, but values are stored verbatim from
    /// third-party pages, so they are kept as [`Value`] rather than risking a decode failure
    /// on a numeric or nested entry.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub meta: BTreeMap<String, Value>,
    /// HTTP headers observed when fetching the preview.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<UrlHeaders>,
    /// Set when the server was told not to generate a preview for this URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_parse: Option<bool>,
    /// Node's `url.parse` output for the URL. Shape is Node's, not Rocket.Chat's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parsed_url: Option<Value>,
}

/// Headers recorded while fetching a [`MessageUrl`] preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UrlHeaders {
    /// `Content-Length`, as the string the server stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_length: Option<String>,
    /// `Content-Type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// One emoji's worth of reactions, the value side of [`Message::reactions`].
///
/// The key of that map is the emoji **including its colons**, e.g. `":thumbsup:"`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reaction {
    /// Usernames of the reacting users. Usernames, not ids — renaming a user rewrites this.
    #[serde(default)]
    pub usernames: Vec<String>,
    /// Display names of the reacting users, populated only when the server is configured to
    /// show real names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,
    /// Matrix federation bookkeeping: username to remote event id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_reaction_event_ids: Option<BTreeMap<String, String>>,
}

impl Reaction {
    /// How many users reacted with this emoji.
    #[must_use]
    pub fn count(&self) -> usize {
        self.usernames.len()
    }

    /// Whether `username` is among the reacting users.
    #[must_use]
    pub fn contains(&self, username: &str) -> bool {
        self.usernames.iter().any(|u| u == username)
    }
}

/// One entry of [`Message::starred`].
///
/// Starring is per-user and private, so this is a list of the users who starred the message —
/// **not** a boolean.
///
/// How much of that list you see depends on where the payload came from. REST responses and
/// the `loadHistory` method run it through `normalizeMessagesForUser`
/// (`apps/meteor/server/lib/utils/lib/normalizeMessagesForUser.ts`), which strips every entry
/// but the recipient's own — which is why the field is so easy to mistake for a flag. A
/// `stream-room-messages` frame gets no such treatment: `watch.messages` broadcasts the raw
/// document, so the list there names **every** user who starred the message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Star {
    /// Id of the user who starred the message.
    #[serde(rename = "_id")]
    pub user_id: UserId,
}

/// An uploaded file referenced by a message (`FileProp` on the server).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageFile {
    /// Upload id; the download URL is derived from it.
    #[serde(rename = "_id")]
    pub id: UploadId,
    /// Original file name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// MIME type.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Format hint, e.g. `png`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// Coarse grouping (`image`, `audio`, ...). Absent on files uploaded by older servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_group: Option<String>,
}

// ---------------------------------------------------------------------------------------
// Message
// ---------------------------------------------------------------------------------------

/// A Rocket.Chat message.
///
/// Only six fields are guaranteed: [`Message::id`], [`Message::updated_at`],
/// [`Message::rid`], [`Message::msg`], [`Message::ts`] and [`Message::u`]. Everything else
/// depends on the message and on the projection that produced the payload.
///
/// Note that [`Message::msg`] is *not* the rendered text for a system message: for
/// `room_changed_topic` it is the new topic, for `uj` it is empty, and so on. Check
/// [`Message::is_system`] before treating it as prose.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    /// Message id.
    #[serde(rename = "_id")]
    pub id: MessageId,
    /// Last write to the document. Bumped by edits, reactions, stars, pins and read
    /// receipts, so it is the field to sync against — not [`Message::ts`].
    #[serde(rename = "_updatedAt")]
    pub updated_at: Timestamp,
    /// Room the message belongs to.
    pub rid: RoomId,
    /// Message body. For system messages this holds a parameter rather than prose.
    pub msg: String,
    /// When the message was sent.
    pub ts: Timestamp,
    /// Author stub.
    pub u: UserRef,

    /// System message discriminator. `None` for an ordinary user message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t: Option<MessageType>,

    /// When the message was last edited.
    ///
    /// Also set on deletion when the server is configured to keep tombstones — see
    /// [`Message::is_deleted_tombstone`].
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub edited_at: Option<Timestamp>,
    /// Who performed the edit. Not necessarily the author.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_by: Option<UserRef>,

    /// Message attachments.
    ///
    /// TODO: model the attachment union. `MessageAttachment` is a large discriminated union
    /// (quote / file-by-kind / action / default attachment, each with its own optional
    /// fields, plus nested `attachments`), and getting it wrong would mean rejecting valid
    /// payloads. Until it is modelled properly the raw JSON is preserved so nothing is lost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<Value>>,

    /// Users and teams mentioned in the body, resolved at send time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mentions: Option<Vec<MessageMention>>,
    /// Rooms referenced with `#name`, resolved at send time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<ChannelMention>>,

    /// Parsed message AST produced by `@rocket.chat/message-parser`.
    ///
    /// Kept as raw JSON: the AST is deeply recursive, versioned independently of the server,
    /// and only needed by clients that render markdown themselves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub md: Option<Value>,
    /// UI Kit blocks, for messages posted by apps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocks: Option<Value>,

    /// URLs extracted from the body, with preview metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub urls: Option<Vec<MessageUrl>>,

    /// Reactions, keyed by the emoji **with colons** (`":tada:"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reactions: Option<BTreeMap<String, Reaction>>,

    /// Single attached file.
    ///
    /// Deprecated on the server in favour of [`Message::files`], but still written for
    /// single-file uploads and still present on every historical message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<MessageFile>,
    /// Attached files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<MessageFile>>,

    /// Id of the thread's parent message. Present exactly on thread replies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmid: Option<MessageId>,
    /// Whether a thread reply should also appear in the main channel view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tshow: Option<bool>,
    /// Number of replies. Present on the thread's parent message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcount: Option<i64>,
    /// Timestamp of the last reply. Present on the thread's parent message.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub tlm: Option<Timestamp>,
    /// Users following the thread, on the parent message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replies: Option<Vec<UserId>>,

    /// Id of the discussion room created from this message.
    ///
    /// This is a *room* id even though it lives on a message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drid: Option<RoomId>,
    /// Number of messages in the linked discussion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dcount: Option<i64>,
    /// Timestamp of the last message in the linked discussion.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub dlm: Option<Timestamp>,

    /// Whether this message may be visually grouped with the previous one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groupable: Option<bool>,
    /// Whether the server should scrape URLs in this message. Set by apps and by the REST
    /// `chat.postMessage` endpoint; not part of `core-typings`' `IMessage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parse_urls: Option<bool>,
    /// Legacy per-message unread marker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread: Option<bool>,

    /// Whether the message is pinned in its room. This one really is a boolean.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned: Option<bool>,
    /// When the message was pinned.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub pinned_at: Option<Timestamp>,
    /// Who pinned the message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_by: Option<UserRef>,

    /// Users who starred the message — a **list**, not a boolean. See [`Star`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starred: Option<Vec<Star>>,

    /// Slash-command output visible only to the sender.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private: Option<bool>,
    /// Optimistic client-side placeholder that has not been acknowledged yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temp: Option<bool>,
    /// Hidden from the room history (used by some system flows and by imports).
    #[serde(rename = "_hidden", default, skip_serializing_if = "Option::is_none")]
    pub hidden: Option<bool>,
    /// Set on messages created by an importer rather than sent live.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported: Option<bool>,

    /// Decryption state for end-to-end encrypted messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e2e: Option<E2eStatus>,

    /// Display name override, used by integrations and bots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    /// Avatar URL override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    /// Emoji used as the avatar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emoji: Option<String>,
    /// Role label rendered next to the author.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// Integration provenance.
    ///
    /// **Deprecated, and not what it looks like.** This is set only by the outgoing/incoming
    /// integrations subsystem, which stores its own identifier here (`{"i": "js.SDK"}` and
    /// similar). It is *not* set for messages authored by a user holding the `bot` role, so
    /// `bot.is_some()` is not a test for "was this written by a bot".
    ///
    /// To recognise a bot, compare [`Message::u`]'s `_id` against the ids of the app or bot
    /// users you care about, or check the author's roles from the user document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bot: Option<Value>,

    /// Livechat visitor token. Present only on messages sent by an unauthenticated visitor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,

    /// Free-form custom fields, shape defined by the workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_fields: Option<Value>,
}

impl Message {
    /// Whether this is a system message rather than something a user typed.
    ///
    /// True for any message carrying a `t`, including one whose type this crate does not
    /// recognise — an unknown `t` is still a system message and rendering `msg` as prose
    /// would be wrong.
    ///
    /// This is deliberately wider than the server's own `isSystemMessage`, which additionally
    /// requires `t` to be in the current `MessageTypes` list and therefore answers `false` for
    /// the retired types (`jitsi_call_started`, `otr`, `voip-call-*`, …) that production
    /// databases are still full of.
    #[must_use]
    pub fn is_system(&self) -> bool {
        self.t.is_some()
    }

    /// Whether the message has been edited.
    #[must_use]
    pub fn is_edited(&self) -> bool {
        self.edited_at.is_some()
    }

    /// Whether this is a deletion tombstone.
    ///
    /// The setting that produces one is **`Message_ShowDeletedStatus`**, not
    /// `Message_KeepHistory`: `deleteMessage` calls `Messages.setAsDeletedByIdAndUser`, which
    /// rewrites the document in place with `t = "rm"`, an empty `msg`, an `editedAt` and an
    /// `editedBy`. A thread parent (`tcount > 0`) is tombstoned unconditionally, whatever the
    /// settings say. `Message_KeepHistory` on its own does something different — it sets
    /// `_hidden: true` and leaves the body intact, so [`Message::hidden`] is the flag to check
    /// for that case.
    ///
    /// A tombstone arrives on the stream as an ordinary update and must not be rendered.
    #[must_use]
    pub fn is_deleted_tombstone(&self) -> bool {
        self.is_edited() && self.t.as_ref() == Some(&MessageType::Rm)
    }

    /// Whether this message is a reply inside a thread.
    #[must_use]
    pub fn is_thread_reply(&self) -> bool {
        self.tmid.is_some()
    }

    /// Whether this message is the root of a thread.
    ///
    /// Mirrors the server's `isThreadMainMessage`, which tests for the *presence* of both
    /// `tcount` and `tlm` rather than for a positive count. The distinction is real:
    /// `Messages.decreaseReplyCountById` only `$inc`s `tcount`, it never unsets it, so a
    /// thread whose replies have all been deleted keeps `tcount: 0` alongside its `tlm` and is
    /// still a thread as far as the server and the thread list are concerned.
    #[must_use]
    pub fn is_thread_main(&self) -> bool {
        self.tcount.is_some() && self.tlm.is_some()
    }

    /// Whether a discussion was created from this message.
    #[must_use]
    pub fn is_discussion_parent(&self) -> bool {
        self.drid.is_some()
    }

    /// Whether `user` starred this message.
    ///
    /// Safe on any payload, but note what the list contains: on a REST payload the server has
    /// already filtered `starred` down to the recipient, while on a `stream-room-messages`
    /// frame it holds every user who starred the message. See [`Star`].
    #[must_use]
    pub fn is_starred_by(&self, user: &UserId) -> bool {
        self.starred.as_ref().is_some_and(|stars| stars.iter().any(|star| star.user_id == *user))
    }

    /// Whether the message is pinned, treating an absent field as "not pinned".
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        self.pinned.unwrap_or(false)
    }

    /// The reactions for one emoji, which must include its colons (`":tada:"`).
    #[must_use]
    pub fn reaction(&self, emoji: &str) -> Option<&Reaction> {
        self.reactions.as_ref()?.get(emoji)
    }
}

// ---------------------------------------------------------------------------------------
// Room
// ---------------------------------------------------------------------------------------

/// The `sysMes` field of a [`Room`], which is two different things wearing one name.
///
/// - an array lists the system message types to **hide** in this room, leaving the rest
///   visible;
/// - a boolean configures *nothing*.
///
/// The boolean arm is the trap. Both the server (`getHiddenSystemMessages`) and the client
/// (`useMessages`) do the same thing with this field:
///
/// ```text
/// Array.isArray(room.sysMes) ? room.sysMes : <workspace Hide_System_Messages>
/// ```
///
/// so `sysMes: false` does **not** hide everything and `sysMes: true` does not show
/// everything — a non-array value, exactly like an absent one, simply defers to the
/// workspace-wide `Hide_System_Messages` list, which this crate cannot see.
///
/// A current server never writes a boolean: `Rooms.setSystemMessagesById` stores a non-empty
/// array and `$unset`s the field otherwise. The booleans in the wild come from the Apps
/// engine, which maps its `displaySystemMessages` flag straight onto `sysMes`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum SysMes {
    /// Blanket on/off switch.
    Enabled(bool),
    /// The system message types hidden in this room.
    Hidden(Vec<MessageType>),
}

impl SysMes {
    /// The hidden types, when the room configures them explicitly.
    #[must_use]
    pub fn hidden_types(&self) -> Option<&[MessageType]> {
        match self {
            Self::Hidden(types) => Some(types),
            Self::Enabled(_) => None,
        }
    }

    /// Whether `t` is hidden **by this room's own configuration**.
    ///
    /// Only [`SysMes::Hidden`] can answer `true`. Both boolean arms answer `false`, because
    /// the server ignores them and falls back to the workspace-wide `Hide_System_Messages`
    /// list — which this crate cannot see, so a `false` here means "this room hides nothing
    /// extra", not "this type is visible".
    #[must_use]
    pub fn hides(&self, t: &MessageType) -> bool {
        match self {
            Self::Enabled(_) => false,
            Self::Hidden(types) => types.contains(t),
        }
    }
}

/// The `rolePrioritiesCreated` field of a [`Room`].
///
/// Started life as a boolean migration flag and became a version counter. Older documents
/// still hold `true`/`false`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum RolePrioritiesCreated {
    /// The current form: a version number.
    Version(i64),
    /// The deprecated form: a plain flag.
    Flag(bool),
}

/// The `announcementDetails` sub-document of a [`Room`]. May arrive as `null`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AnnouncementDetails {
    /// CSS style applied to the announcement banner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style: Option<String>,
}

/// A Rocket.Chat room: channel, private group, direct message or omnichannel conversation.
///
/// Only [`Room::id`], [`Room::updated_at`] and [`Room::t`] are guaranteed. In particular
/// [`Room::msgs`] and [`Room::users_count`] are declared non-optional by `core-typings` but
/// are absent from several stream projections, and [`Room::u`] does not exist at all on
/// direct message rooms (`IDirectMessageRoom` omits it) or on omnichannel rooms (the server's
/// own room factory carries a `TODO: Solve 'u' missing issue` where it should be).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Room {
    /// Room id. `GENERAL` for the default channel; a random id otherwise.
    #[serde(rename = "_id")]
    pub id: RoomId,
    /// Last write to the document.
    #[serde(rename = "_updatedAt")]
    pub updated_at: Timestamp,
    /// Room kind.
    pub t: RoomType,

    /// URL-safe room name. Absent on direct message rooms and on omnichannel rooms, which
    /// carry only an `fname` taken from the contact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Display name, which may differ from `name` when `UI_Allow_room_names_with_special_chars`
    /// is on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fname: Option<String>,

    /// Total message count.
    ///
    /// Non-optional in `core-typings`, but dropped by projections that do not ask for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msgs: Option<i64>,
    /// Number of subscribed users. Same caveat as [`Room::msgs`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub users_count: Option<i64>,

    /// The room's owner stub. Absent on direct message and omnichannel rooms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub u: Option<UserRef>,
    /// Participant ids. Set on direct message rooms; `len() > 2` means a multi-user DM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uids: Option<Vec<UserId>>,
    /// Participant usernames, for direct message rooms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usernames: Option<Vec<String>>,

    /// Whether the room is a default channel new users are auto-joined to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<bool>,
    /// Whether only privileged users may post.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broadcast: Option<bool>,
    /// Whether the room is featured in the directory. Only ever `true` when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub featured: Option<bool>,
    /// Room topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Room description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Room announcement banner text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub announcement: Option<String>,
    /// Styling for the announcement banner. Explicitly `null` when cleared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub announcement_details: Option<AnnouncementDetails>,
    /// Whether a join code is required to enter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_code_required: Option<bool>,
    /// Whether end-to-end encryption is on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypted: Option<bool>,
    /// Id of the E2E key currently used by the room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e2e_key_id: Option<String>,
    /// Users still waiting to be handed the room's E2E key.
    #[serde(rename = "usersWaitingForE2EKeys", default, skip_serializing_if = "Option::is_none")]
    pub users_waiting_for_e2e_keys: Option<Vec<Value>>,
    /// Read-only room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ro: Option<bool>,
    /// Whether reacting is allowed despite the room being read-only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub react_when_read_only: Option<bool>,
    /// System message visibility. See [`SysMes`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sys_mes: Option<SysMes>,
    /// Whether the channel is listed in search and directory (`cl`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cl: Option<bool>,
    /// Whether the room is archived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
    /// Muted usernames.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub muted: Option<Vec<String>>,
    /// Explicitly unmuted usernames, used in read-only rooms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unmuted: Option<Vec<String>>,

    /// Denormalised copy of the most recent message.
    ///
    /// Boxed to keep [`Room`] small; [`Message`] is by far the largest type here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message: Option<Box<Message>>,
    /// Timestamp of the last message.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub lm: Option<Timestamp>,
    /// When the room was created.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub ts: Option<Timestamp>,
    /// When a WebRTC call started in this room.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub web_rtc_call_start_time: Option<Timestamp>,

    /// Parent room id, set on discussions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prid: Option<RoomId>,
    /// Avatar cache-busting tag. Note the capitalisation on the wire: `avatarETag`.
    #[serde(rename = "avatarETag", default, skip_serializing_if = "Option::is_none")]
    pub avatar_etag: Option<String>,

    /// Whether this room is a team's main channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_main: Option<bool>,
    /// Id of the team this room belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    /// Whether team members auto-join this room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_default: Option<bool>,

    /// Per-user view state merged in by some endpoints rather than stored on the room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open: Option<bool>,
    /// Per-user unread count, merged in the same way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread: Option<i64>,
    /// Per-user alert flag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alert: Option<bool>,
    /// Per-user favourite flag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub favorite: Option<bool>,
    /// Whether the unread badge is suppressed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hide_unread_status: Option<bool>,
    /// Whether the mention badge is suppressed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hide_mention_status: Option<bool>,
    /// Whether auto-translation is enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_translate: Option<bool>,
    /// Target language for auto-translation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_translate_language: Option<String>,

    /// Message retention policy override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<Value>,
    /// Attribute-based access control definitions. Non-empty means ABAC governs the room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abac_attributes: Option<Vec<Value>>,
    /// Role-priority migration state. See [`RolePrioritiesCreated`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_priorities_created: Option<RolePrioritiesCreated>,
    /// Free-form custom fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_fields: Option<Value>,

    /// Whether the room is federated (deprecated flag).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federated: Option<bool>,
    /// Matrix federation metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation: Option<Value>,

    // --- omnichannel ---
    /// The visitor, on an omnichannel room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub v: Option<Value>,
    /// Where the conversation originated (widget, email, SMS, app, API).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Value>,
    /// The agent currently serving the conversation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_by: Option<Value>,
    /// Department handling the conversation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub department_id: Option<String>,
    /// Whether the conversation is on hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_hold: Option<bool>,
    /// Whether the conversation is waiting on the visitor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_response: Option<Value>,
    /// When the conversation entered the queue.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<Timestamp>,
    /// Conversation tags.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    /// Custom fields collected from the visitor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub livechat_data: Option<Value>,
    /// Conversation metrics (response times, durations).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Value>,
    /// Pending email transcript request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_request: Option<Value>,
    /// Priority id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority_id: Option<String>,
    /// Priority sort weight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority_weight: Option<i64>,
    /// SLA id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sla_id: Option<String>,
    /// SLA due time, in minutes.
    ///
    /// Copied verbatim from the SLA policy's `dueTimeInMinutes`, which is **not constrained to
    /// an integer** anywhere: the REST schema types it as a bare `number` and the admin form
    /// only validates `> 0`, so `1.5` is an accepted policy and reaches this field — and this
    /// field *is* in the `roomFields` projection. Hence `f64`, not an integer type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_waiting_time_queue: Option<f64>,
    /// Linked contact record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact_id: Option<String>,
    /// Whether the visitor's identity was verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified: Option<bool>,
    /// SMS integration metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sms: Option<Value>,
    /// Join code / omnichannel code. Typed `unknown` on the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<Value>,
    /// Human-readable label for the conversation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Sentiment analysis output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sentiment: Option<Value>,
}

impl Room {
    /// Whether this is a direct message room.
    #[must_use]
    pub fn is_direct(&self) -> bool {
        self.t.is_direct()
    }

    /// Whether this is a multi-user direct message room (more than two participants).
    #[must_use]
    pub fn is_multi_user_direct(&self) -> bool {
        self.is_direct() && self.uids.as_ref().is_some_and(|uids| uids.len() > 2)
    }

    /// Whether this room is a team's main channel.
    #[must_use]
    pub fn is_team(&self) -> bool {
        self.team_main.unwrap_or(false)
    }

    /// Whether this room is a discussion of another room.
    #[must_use]
    pub fn is_discussion(&self) -> bool {
        self.prid.is_some()
    }

    /// The name to show for the room, preferring the display name.
    #[must_use]
    pub fn display_name(&self) -> Option<&str> {
        self.fname.as_deref().or(self.name.as_deref())
    }
}

// ---------------------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------------------

/// A superseded end-to-end encryption key for a room, kept so old history stays readable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OldRoomKey {
    /// Id of the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e2e_key_id: Option<String>,
    /// When the key was rotated out.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub ts: Option<Timestamp>,
    /// The key itself, encrypted with the user's key pair. Note the wire name: `E2EKey`.
    #[serde(rename = "E2EKey", default, skip_serializing_if = "Option::is_none")]
    pub e2e_key: Option<String>,
}

/// A user's membership of a room, and every per-user setting attached to it.
///
/// This is the document that drives the sidebar: unread counts, mention badges, mute
/// settings, drafts and the E2E key are all here rather than on the [`Room`].
///
/// The mandatory set is `_id`, `_updatedAt`, `rid`, `u`, `t`, `ts`, `open`, `unread`,
/// `userMentions` and `groupMentions` — all of them projected by `subscriptionFields` in
/// `publishFields.ts` and all of them written by every subscription factory.
///
/// [`Subscription::name`] is *not* in that set despite being `name: string` in
/// `core-typings`; see its documentation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    /// Subscription document id. Distinct from both the room id and the user id.
    #[serde(rename = "_id")]
    pub id: SubscriptionId,
    /// Last write to the document.
    #[serde(rename = "_updatedAt")]
    pub updated_at: Timestamp,
    /// The subscribed room.
    pub rid: RoomId,
    /// The subscribing user.
    pub u: UserRef,
    /// Kind of the subscribed room, denormalised from [`Room::t`].
    pub t: RoomType,
    /// When the subscription was created.
    pub ts: Timestamp,
    /// Room name as this user sees it. For a DM this is the other participant's username.
    ///
    /// Declared `name: string` by `core-typings`, but the server can and does persist it as
    /// **`null`**. `Subscriptions.createWithRoomAndUser` copies `name: room.name`
    /// unconditionally, the workspace runs the Mongo driver with `ignoreUndefined: false` (so
    /// an absent `room.name` is stored as an explicit null rather than dropped), and an
    /// omnichannel room has no `name` at all. Joining a livechat room — `GET
    /// /v1/livechat/room.join`, which routes to `addUserToRoom` → `Room.createUserSubscription`
    /// → `createWithRoomAndUser` without supplying a `name` for a non-`d` room — produces
    /// exactly that document, and `subscriptionFields` projects `name`, so it reaches the
    /// stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Whether the room is open in this user's sidebar.
    pub open: bool,
    /// Unread message count.
    pub unread: i64,
    /// Number of unread direct mentions of this user.
    pub user_mentions: i64,
    /// Number of unread `@all` / `@here` mentions.
    pub group_mentions: i64,

    /// Display name of the room, denormalised from [`Room::fname`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fname: Option<String>,
    /// Whether the sidebar entry should be highlighted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alert: Option<bool>,
    /// Last time this user read the room ("last seen").
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub ls: Option<Timestamp>,
    /// Last time a message was received in the room ("last received").
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub lr: Option<Timestamp>,
    /// Whether the room is favourited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub f: Option<bool>,

    /// Suppresses the unread badge.
    ///
    /// Typed as the literal `true` on the server, so the field is written only when set;
    /// absent means "not suppressed". Prefer [`Subscription::hides_unread_status`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hide_unread_status: Option<bool>,
    /// Suppresses the mention badge. Same `true`-only encoding as
    /// [`Subscription::hide_unread_status`]; prefer [`Subscription::hides_mention_status`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hide_mention_status: Option<bool>,

    /// Whether the subscribed room is a team's main channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_main: Option<bool>,
    /// Id of the team the room belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    /// Whether the room is broadcast-only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broadcast: Option<bool>,
    /// Parent room id, when the subscription is to a discussion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prid: Option<RoomId>,

    /// Ids of threads with unread replies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunread: Option<Vec<MessageId>>,
    /// Threads with unread `@all` / `@here` mentions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunread_group: Option<Vec<MessageId>>,
    /// Threads with unread direct mentions of this user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunread_user: Option<Vec<MessageId>>,

    /// Room-scoped roles held by this user (`owner`, `moderator`, `leader`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roles: Option<Vec<RoleId>>,

    /// Whether the omnichannel conversation is on hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_hold: Option<bool>,
    /// Whether the room is end-to-end encrypted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypted: Option<bool>,
    /// The room's E2E key, encrypted for this user. Wire name: `E2EKey`.
    #[serde(rename = "E2EKey", default, skip_serializing_if = "Option::is_none")]
    pub e2e_key: Option<String>,
    /// A key another member offered while this user's key was still pending.
    /// Wire name: `E2ESuggestedKey`.
    #[serde(rename = "E2ESuggestedKey", default, skip_serializing_if = "Option::is_none")]
    pub e2e_suggested_key: Option<String>,
    /// Superseded room keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_room_keys: Option<Vec<OldRoomKey>>,
    /// Superseded keys offered by other members.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_old_room_keys: Option<Vec<OldRoomKey>>,

    /// When the unread badge should appear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unread_alert: Option<UnreadAlert>,
    /// Desktop notification volume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desktop_notifications: Option<NotificationPreference>,
    /// Mobile push notification volume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mobile_push_notifications: Option<NotificationPreference>,
    /// Email notification volume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email_notifications: Option<NotificationPreference>,
    /// Which layer the desktop preference came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desktop_pref_origin: Option<PreferenceOrigin>,
    /// Which layer the mobile preference came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mobile_pref_origin: Option<PreferenceOrigin>,
    /// Which layer the email preference came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email_pref_origin: Option<PreferenceOrigin>,
    /// Notification sound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_notification_value: Option<String>,
    /// Whether all notifications for the room are off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_notifications: Option<bool>,
    /// Whether `@all` / `@here` are ignored in this room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mute_group_mentions: Option<bool>,
    /// Extra words that count as a highlight for this user in this room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_highlights: Option<Vec<String>>,
    /// Users whose messages this user has chosen to hide.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignored: Option<Vec<UserId>>,

    /// Whether the room is archived for this user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
    /// Whether auto-translation is on for this user in this room.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_translate: Option<bool>,
    /// Target language for auto-translation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_translate_language: Option<String>,

    /// Whether this user blocked the other party of a DM. Typed `unknown` on the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<Value>,
    /// Whether this user was blocked by the other party. Typed `unknown` on the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocker: Option<Value>,

    /// Unsent message body, synced across this user's devices.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draft: Option<String>,
    /// Unsent thread replies, keyed by thread id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_drafts: Option<BTreeMap<String, String>>,

    /// Membership state, when it is not a plain active membership.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SubscriptionStatus>,
    /// Who invited this user, for an `INVITED` subscription.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inviter: Option<UserRef>,

    /// The omnichannel visitor, on a livechat subscription.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub v: Option<Value>,
    /// Omnichannel department. Typed `unknown` on the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub department: Option<Value>,
    /// Join code / omnichannel code. Typed `unknown` on the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<Value>,

    /// When ABAC access was last re-evaluated for this membership.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub abac_last_time_checked: Option<Timestamp>,
    /// Free-form custom fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_fields: Option<Value>,
}

impl Subscription {
    /// Whether the unread badge is suppressed for this room.
    ///
    /// The server writes the field only when it is `true`, so absent means `false`.
    #[must_use]
    pub fn hides_unread_status(&self) -> bool {
        self.hide_unread_status.unwrap_or(false)
    }

    /// Whether the mention badge is suppressed for this room. Absent means `false`.
    #[must_use]
    pub fn hides_mention_status(&self) -> bool {
        self.hide_mention_status.unwrap_or(false)
    }

    /// Whether this user is favouriting the room.
    #[must_use]
    pub fn is_favorite(&self) -> bool {
        self.f.unwrap_or(false)
    }

    /// Direct plus group mentions.
    #[must_use]
    pub fn total_mentions(&self) -> i64 {
        self.user_mentions.saturating_add(self.group_mentions)
    }

    /// Whether the room has anything worth showing a badge for.
    #[must_use]
    pub fn has_unread(&self) -> bool {
        self.unread > 0 || self.total_mentions() > 0
    }

    /// Whether this user holds `role` in the subscribed room.
    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.as_ref().is_some_and(|roles| roles.iter().any(|r| r == role))
    }

    /// The name to show, preferring the display name.
    ///
    /// `None` only when neither field survived — an omnichannel subscription created by
    /// joining a nameless livechat room has a null [`Subscription::name`] and no `fname`.
    ///
    /// Note that this is *not* what the server renders for a direct message: `direct.ts`'s
    /// `roomName` returns `fname` only when `UI_Use_Real_Name` is on and falls back to `name`
    /// otherwise, so on a workspace configured to show usernames this method prefers the wrong
    /// one. It has no access to that setting; a caller that cares must pick the field itself.
    #[must_use]
    pub fn display_name(&self) -> Option<&str> {
        self.fname.as_deref().or(self.name.as_deref())
    }
}

// ---------------------------------------------------------------------------------------
// User
// ---------------------------------------------------------------------------------------

/// An email address on a [`User`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserEmail {
    /// The address.
    pub address: String,
    /// Whether the address has been verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified: Option<bool>,
}

/// A Rocket.Chat user.
///
/// # Three status fields, three meanings
///
/// - [`User::status`] — the **effective** status other people see. Derived by the presence
///   service from the connection state and the user's choice; this is what you render.
/// - [`User::status_default`] — the status the user **chose**. Survives disconnects, and is
///   what `status` reverts to when the user comes back online.
/// - [`User::status_connection`] — the raw **socket-derived** state for the current session.
///   Set from connection activity only, so it never reports `busy`.
///
/// # `services` is not modelled, on purpose
///
/// `IUser.services` holds password hashes, login and resume tokens, OAuth access and refresh
/// tokens, TOTP secrets and email verification tokens. The server never projects it onto a
/// stream, and a Rust type for it would be an invitation to log or serialize credentials by
/// accident. It is deliberately omitted; unknown fields are ignored, so a payload that
/// somehow carries it still decodes.
///
/// # What is guaranteed
///
/// Only [`User::id`]. `core-typings` declares `_updatedAt`, `createdAt`, `roles`, `type` and
/// `active` non-optional, but real payloads drop them: `Users:NameChanged` carries only
/// `{_id, name, username}`, and REST projections are narrower still.
///
/// # This type does not model a `userData` diff
///
/// The `stream-notify-user` `userData` event does not carry a user document on an update. It
/// carries `{type: "updated", id, diff, unset}`, where `diff` is a `Partial<IUser>` with **no
/// `_id` of its own** — the id lives in the sibling `id` member. The presence listener emits
/// exactly that shape (`diff: {status, statusText?, statusSource?, statusExpiresAt?}`).
/// Decoding such a `diff` as a [`User`] fails on the missing `_id`, and that is correct: only
/// the `inserted` variant's `data` member is a whole user document. Merge a diff into a cached
/// [`User`] field by field instead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct User {
    /// User id.
    #[serde(rename = "_id")]
    pub id: UserId,

    /// Last write to the document.
    ///
    /// Optional despite being non-optional in `core-typings`: narrow projections such as the
    /// publication user cache (`{_id: 1, roles: 1}`) and the `Users:NameChanged` payload omit
    /// it entirely.
    #[serde(
        rename = "_updatedAt",
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<Timestamp>,
    /// When the account was created.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<Timestamp>,

    /// Login name. Absent for some app and visitor records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Nickname.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nickname: Option<String>,
    /// Profile bio.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bio: Option<String>,

    /// Global roles. Non-optional on the server, dropped by most projections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roles: Option<Vec<RoleId>>,
    /// Account kind: `user`, `bot`, `app` or `visitor`. Non-optional on the server, dropped
    /// by most projections.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub user_type: Option<String>,
    /// Whether the account is active. Non-optional on the server, dropped by most
    /// projections. A deactivated account reports [`UserStatus::Disabled`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    /// Why the account is inactive (`deactivated`, `pending_approval`, `idle_too_long`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inactive_reason: Option<String>,

    /// The effective status shown to other users.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<UserStatus>,
    /// The status the user chose, which [`User::status`] reverts to on reconnect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_default: Option<UserStatus>,
    /// The raw socket-derived status of the current session. Never reports `busy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_connection: Option<String>,
    /// Free-text status message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_text: Option<String>,
    /// Where the current status came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_source: Option<PresenceSource>,
    /// When a temporary status reverts to [`User::status_default`].
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub status_expires_at: Option<Timestamp>,
    /// Id of the custom status in use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_id: Option<String>,
    /// When the user last logged in.
    #[serde(default, with = "crate::datetime::option", skip_serializing_if = "Option::is_none")]
    pub last_login: Option<Timestamp>,

    /// Registered email addresses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emails: Option<Vec<UserEmail>>,
    /// Where the avatar came from (`upload`, `url`, an OAuth provider name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_origin: Option<String>,
    /// Avatar cache-busting tag. Note the capitalisation on the wire: `avatarETag`.
    #[serde(rename = "avatarETag", default, skip_serializing_if = "Option::is_none")]
    pub avatar_etag: Option<String>,
    /// External avatar URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_url: Option<String>,

    /// UTC offset in hours. Fractional for half-hour and quarter-hour zones (`5.5`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utc_offset: Option<f64>,
    /// Preferred UI language.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Phone number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phone: Option<String>,
    /// Reason given when requesting an account.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// The user's own E2E key pair. The private key is encrypted with a key derived from the
    /// user's password, and is sent only to that user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e2e: Option<Value>,
    /// UI preferences and profile settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<Value>,
    /// Free-form custom fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_fields: Option<Value>,
    /// Admin banners queued for this user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub banners: Option<Value>,
    /// Room the user lands in after login.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_room: Option<String>,
    /// Whether the account is backed by LDAP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ldap: Option<bool>,
    /// Whether the user must change their password on next login.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_password_change: Option<bool>,
    /// Why the password change is required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_password_change_reason: Option<String>,
    /// Highest role priority per room, used to sort the members list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room_role_priorities: Option<BTreeMap<String, i64>>,
    /// Rooms the user belongs to. Written by the server for permission checks; the leading
    /// double underscore is part of the wire name.
    #[serde(rename = "__rooms", default, skip_serializing_if = "Option::is_none")]
    pub rooms: Option<Vec<RoomId>>,
    /// Ids this user carried in the system it was imported from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_ids: Option<Vec<String>>,
    /// Token from the invite the user signed up with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invite_token: Option<String>,
    /// FreeSWITCH extension for voice calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_switch_extension: Option<String>,
    /// Whether the viewer is allowed to see this user's full record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub can_view_all_info: Option<bool>,
    /// Identity provider that created the account.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    /// Attribute-based access control attributes held by the user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abac_attributes: Option<Vec<Value>>,
    /// Whether the account is federated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federated: Option<bool>,
    /// Matrix federation metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation: Option<Value>,
}

impl User {
    /// The name to show, honouring the server's `UI_Use_Real_Name` setting.
    ///
    /// Slightly more forgiving than the server's `getUserDisplayName`, which is
    /// `useRealName ? name || username : username` — it has no fallback when
    /// `UI_Use_Real_Name` is off, so it yields nothing for the app and visitor records that
    /// carry a `name` but no `username`. This returns the `name` in that case.
    #[must_use]
    pub fn display_name(&self, use_real_name: bool) -> Option<&str> {
        let (first, second) =
            if use_real_name { (&self.name, &self.username) } else { (&self.username, &self.name) };
        first.as_deref().or(second.as_deref())
    }

    /// Whether this user holds `role` globally.
    ///
    /// Returns `false` when `roles` was projected away, which is not the same as the user not
    /// holding the role — check `roles.is_some()` first if the distinction matters.
    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.as_ref().is_some_and(|roles| roles.iter().any(|r| r == role))
    }

    /// Whether the user is reachable right now: online, away or busy.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        matches!(self.status, Some(UserStatus::Online | UserStatus::Away | UserStatus::Busy))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A message exactly as it arrives in a `stream-room-messages` frame.
    const CAPTURED_MESSAGE: &str = r#"{"_id":"7aDSXtjMA3KPLxLjt","rid":"GENERAL","msg":"hello @john.doe",
 "ts":{"$date":1755529012345},
 "u":{"_id":"aobEdbYhXfu5hkeqG","username":"alice","name":"Alice A."},
 "_updatedAt":{"$date":1755529012390},"urls":[],
 "mentions":[{"_id":"rbAXPnMktTFbNpwtJ","username":"john.doe","name":"John Doe","type":"user"}],
 "channels":[]}"#;

    fn message(json: &str) -> Message {
        serde_json::from_str(json)
            .unwrap_or_else(|e| panic!("failed to decode message: {e}\n{json}"))
    }

    fn room(json: &str) -> Room {
        serde_json::from_str(json).unwrap_or_else(|e| panic!("failed to decode room: {e}\n{json}"))
    }

    fn subscription(json: &str) -> Subscription {
        serde_json::from_str(json)
            .unwrap_or_else(|e| panic!("failed to decode subscription: {e}\n{json}"))
    }

    fn user(json: &str) -> User {
        serde_json::from_str(json).unwrap_or_else(|e| panic!("failed to decode user: {e}\n{json}"))
    }

    // -- Message -------------------------------------------------------------------------

    #[test]
    fn decodes_a_captured_stream_message() {
        let m = message(CAPTURED_MESSAGE);

        assert_eq!(m.id, "7aDSXtjMA3KPLxLjt");
        assert_eq!(m.rid, "GENERAL");
        assert_eq!(m.msg, "hello @john.doe");
        assert_eq!(m.ts.unix_millis(), 1_755_529_012_345);
        assert_eq!(m.updated_at.unix_millis(), 1_755_529_012_390);
        assert_eq!(m.u.id, "aobEdbYhXfu5hkeqG");
        assert_eq!(m.u.username.as_deref(), Some("alice"));
        assert_eq!(m.u.name.as_deref(), Some("Alice A."));
        assert_eq!(m.urls.as_deref(), Some(&[][..]));
        assert_eq!(m.channels.as_deref(), Some(&[][..]));

        let mentions = m.mentions.as_ref().unwrap();
        assert_eq!(mentions.len(), 1);
        assert_eq!(mentions[0].id, "rbAXPnMktTFbNpwtJ");
        assert_eq!(mentions[0].mention_type, Some(MentionType::User));
        assert_eq!(mentions[0].username.as_deref(), Some("john.doe"));

        assert!(!m.is_system());
        assert!(!m.is_edited());
        assert!(!m.is_thread_reply());
    }

    #[test]
    fn decodes_a_message_carrying_only_the_mandatory_fields() {
        // The narrowest projection any stream applies.
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"GENERAL","msg":"",
                "ts":{"$date":1},"u":{"_id":"u1","username":"bob"}}"#,
        );

        assert_eq!(m.msg, "");
        assert_eq!(m.u.name, None);
        assert_eq!(m.t, None);
        assert_eq!(m.mentions, None);
        assert_eq!(m.attachments, None);
        assert_eq!(m.reactions, None);
        assert_eq!(m.starred, None);
    }

    #[test]
    fn accepts_an_explicitly_null_author_name() {
        // The server unsets `name` rather than deleting the key.
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"hi","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob","name":null}}"#,
        );
        assert_eq!(m.u.name, None);
    }

    #[test]
    fn accepts_an_author_stub_without_a_username() {
        // `IMessage.u` types `username` as required, but imports and app-authored messages
        // have produced stubs without it, and a `changed` frame can deliver a partial `u`.
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"hi","ts":{"$date":1},
                "u":{"_id":"rocket.cat"}}"#,
        );
        assert_eq!(m.u.username, None);
    }

    #[test]
    fn decodes_an_edited_message() {
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":20},"rid":"r","msg":"fixed","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},
                "editedAt":{"$date":20},"editedBy":{"_id":"u1","username":"bob"}}"#,
        );

        assert!(m.is_edited());
        assert_eq!(m.edited_at.unwrap().unix_millis(), 20);
        assert_eq!(m.edited_by.as_ref().unwrap().id, "u1");
        assert!(!m.is_deleted_tombstone());
    }

    #[test]
    fn recognises_a_deletion_tombstone() {
        // With Message_KeepHistory on, a delete rewrites the message in place.
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":30},"rid":"r","msg":"","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},"t":"rm",
                "editedAt":{"$date":30},"editedBy":{"_id":"u1","username":"bob"}}"#,
        );

        assert!(m.is_system());
        assert!(m.is_edited());
        assert!(m.is_deleted_tombstone());
        assert_eq!(m.t, Some(MessageType::Rm));
    }

    #[test]
    fn a_thread_root_whose_replies_were_all_deleted_is_still_a_thread() {
        // `Messages.decreaseReplyCountById` only `$inc`s tcount; it never unsets it, so the
        // last reply going away leaves tcount: 0 next to a live tlm. The server's
        // `isThreadMainMessage` tests presence, not the count.
        let m = message(
            r#"{"_id":"parent1","_updatedAt":{"$date":9},"rid":"r","msg":"q","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},"tcount":0,"tlm":{"$date":9}}"#,
        );
        assert_eq!(m.tcount, Some(0));
        assert!(m.is_thread_main());

        // tcount without tlm is not a thread root, matching the server predicate.
        let partial = message(
            r#"{"_id":"p2","_updatedAt":{"$date":9},"rid":"r","msg":"q","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},"tcount":4}"#,
        );
        assert!(!partial.is_thread_main());
    }

    #[test]
    fn an_unedited_rm_message_is_not_a_tombstone() {
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},"t":"rm"}"#,
        );
        assert!(!m.is_deleted_tombstone());
    }

    #[test]
    fn an_unknown_system_message_type_decodes_and_round_trips() {
        // `jitsi_call_started` was dropped from MessageTypes years ago but still sits in
        // production databases, and every release adds new types.
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},"t":"jitsi_call_started"}"#,
        );

        assert!(m.is_system());
        let t = m.t.clone().unwrap();
        assert!(t.is_unknown());
        assert_eq!(t.as_str(), "jitsi_call_started");
        assert_eq!(serde_json::to_value(&t).unwrap(), "jitsi_call_started");
    }

    #[test]
    fn unknown_message_type_round_trips_byte_identically() {
        for wire in ["totally_new_type", "uj2", "", "user-did-a-thing"] {
            let json = serde_json::to_string(wire).unwrap();
            let decoded: MessageType = serde_json::from_str(&json).unwrap();
            assert!(decoded.is_unknown(), "{wire} should be unknown");
            assert_eq!(serde_json::to_string(&decoded).unwrap(), json);
        }
    }

    #[test]
    fn every_known_message_type_round_trips() {
        // Guards against a typo in one of the ~60 wire literals: a mistyped literal would
        // decode to Unknown rather than to its variant.
        for wire in [
            "e2e",
            "uj",
            "ui",
            "uir",
            "ul",
            "ru",
            "au",
            "mute_unmute",
            "r",
            "ut",
            "wm",
            "rm",
            "subscription-role-added",
            "subscription-role-removed",
            "room-archived",
            "room-unarchived",
            "room_changed_privacy",
            "room_changed_description",
            "room_changed_announcement",
            "room_changed_avatar",
            "room_changed_topic",
            "room_e2e_enabled",
            "room_e2e_disabled",
            "user-muted",
            "user-unmuted",
            "user-banned",
            "user-unbanned",
            "room-removed-read-only",
            "room-set-read-only",
            "room-allowed-reacting",
            "room-disallowed-reacting",
            "command",
            "videoconf",
            "message_pinned",
            "message_pinned_e2e",
            "new-moderator",
            "moderator-removed",
            "new-owner",
            "owner-removed",
            "new-leader",
            "leader-removed",
            "discussion-created",
            "abac-removed-user-from-room",
            "removed-user-from-team",
            "added-user-to-team",
            "ult",
            "user-converted-to-team",
            "user-converted-to-channel",
            "user-removed-room-from-team",
            "user-deleted-room-from-team",
            "user-added-room-to-team",
            "ujt",
            "livechat_navigation_history",
            "livechat_transfer_history",
            "livechat_transcript_history",
            "livechat_video_call",
            "livechat_transfer_history_fallback",
            "livechat-close",
            "livechat-started",
            "omnichannel_priority_change_history",
            "omnichannel_sla_change_history",
            "omnichannel_placed_chat_on_hold",
            "omnichannel_on_hold_chat_resumed",
        ] {
            let t = MessageType::from(wire);
            assert!(!t.is_unknown(), "{wire} decoded as Unknown");
            assert_eq!(t.as_str(), wire);
            assert_eq!(String::from(t), wire);
        }
    }

    #[test]
    fn starred_is_a_list_of_users_not_a_boolean() {
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"hi","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},
                "starred":[{"_id":"aobEdbYhXfu5hkeqG"}],"pinned":true}"#,
        );

        assert_eq!(
            m.starred.as_deref(),
            Some(&[Star { user_id: UserId::new("aobEdbYhXfu5hkeqG") }][..])
        );
        assert!(m.is_starred_by(&UserId::new("aobEdbYhXfu5hkeqG")));
        assert!(!m.is_starred_by(&UserId::new("someone.else")));
        // `pinned`, unlike `starred`, really is a boolean.
        assert!(m.is_pinned());
    }

    #[test]
    fn an_empty_starred_array_means_nobody_starred_it() {
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"hi","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},"starred":[]}"#,
        );
        assert_eq!(m.starred.as_deref(), Some(&[][..]));
        assert!(!m.is_starred_by(&UserId::new("u1")));
    }

    #[test]
    fn decodes_reactions_keyed_by_emoji_with_colons() {
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"hi","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},
                "reactions":{":thumbsup:":{"usernames":["alice","bob"]},
                             ":tada:":{"usernames":["carol"],"names":["Carol C."]}}}"#,
        );

        let thumbs = m.reaction(":thumbsup:").unwrap();
        assert_eq!(thumbs.count(), 2);
        assert!(thumbs.contains("alice"));
        assert_eq!(thumbs.names, None);

        let tada = m.reaction(":tada:").unwrap();
        assert_eq!(tada.names.as_deref(), Some(&["Carol C.".to_owned()][..]));

        // The key really does include the colons.
        assert!(m.reaction("thumbsup").is_none());
    }

    #[test]
    fn decodes_thread_and_discussion_fields() {
        let reply = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"re","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},"tmid":"parent1","tshow":true}"#,
        );
        assert!(reply.is_thread_reply());
        assert_eq!(reply.tshow, Some(true));
        assert!(!reply.is_thread_main());

        let parent = message(
            r#"{"_id":"parent1","_updatedAt":{"$date":1},"rid":"r","msg":"q","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},"tcount":3,"tlm":{"$date":99},
                "replies":["u1","u2"]}"#,
        );
        assert!(parent.is_thread_main());
        assert!(!parent.is_thread_reply());
        assert_eq!(parent.tlm.unwrap().unix_millis(), 99);
        assert_eq!(parent.replies.as_ref().unwrap().len(), 2);

        let discussion = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"topic","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},"t":"discussion-created",
                "drid":"disc1","dcount":2,"dlm":{"$date":5}}"#,
        );
        assert!(discussion.is_discussion_parent());
        assert_eq!(discussion.drid.as_ref().unwrap(), "disc1");
        assert_eq!(discussion.dcount, Some(2));
        assert_eq!(discussion.dlm.unwrap().unix_millis(), 5);
    }

    #[test]
    fn decodes_files_urls_and_opaque_blobs() {
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},
                "file":{"_id":"f1","name":"a.png","type":"image/png","format":"png","size":12},
                "files":[{"_id":"f1","name":"a.png","type":"image/png","typeGroup":"image"}],
                "attachments":[{"title":"a.png","image_url":"/file-upload/f1/a.png"}],
                "urls":[{"url":"https://example.com","meta":{"title":"Example","pageCount":3},
                         "headers":{"contentType":"text/html"}}],
                "md":[{"type":"PARAGRAPH","value":[]}],
                "blocks":[{"type":"section"}],
                "customFields":{"ticket":42},
                "bot":{"i":"js.SDK"},
                "_hidden":false,"imported":true,"e2e":"done"}"#,
        );

        assert_eq!(m.file.as_ref().unwrap().id, "f1");
        assert_eq!(m.file.as_ref().unwrap().content_type.as_deref(), Some("image/png"));
        assert_eq!(m.files.as_ref().unwrap()[0].type_group.as_deref(), Some("image"));
        assert_eq!(m.attachments.as_ref().unwrap().len(), 1);

        let url = &m.urls.as_ref().unwrap()[0];
        assert_eq!(url.url, "https://example.com");
        // `meta` is typed Record<string, string> but real payloads carry non-strings.
        assert_eq!(url.meta["pageCount"], serde_json::json!(3));
        assert_eq!(url.headers.as_ref().unwrap().content_type.as_deref(), Some("text/html"));

        assert!(m.md.is_some());
        assert!(m.blocks.is_some());
        assert_eq!(m.hidden, Some(false));
        assert_eq!(m.imported, Some(true));
        assert_eq!(m.e2e, Some(E2eStatus::Done));
        assert!(m.bot.is_some());
    }

    #[test]
    fn ignores_fields_this_crate_does_not_model() {
        // `services` on a user, `translations` on a message, anything a future release adds.
        let m = message(
            r#"{"_id":"abc","_updatedAt":{"$date":1},"rid":"r","msg":"hi","ts":{"$date":1},
                "u":{"_id":"u1","username":"bob"},
                "translations":{"de":"hallo"},"someFieldFromRocketChat9":{"nested":[1,2]}}"#,
        );
        assert_eq!(m.msg, "hi");
    }

    #[test]
    fn message_round_trips_through_json() {
        let m = message(CAPTURED_MESSAGE);
        let encoded = serde_json::to_string(&m).unwrap();
        assert_eq!(message(&encoded), m);
        // Absent optionals must not come back as nulls.
        assert!(!encoded.contains("null"), "{encoded}");
        assert!(!encoded.contains("\"t\":"), "{encoded}");
    }

    // -- Room ----------------------------------------------------------------------------

    #[test]
    fn decodes_a_room_with_only_the_mandatory_fields() {
        let r = room(r#"{"_id":"GENERAL","_updatedAt":{"$date":1},"t":"c"}"#);

        assert_eq!(r.t, RoomType::Channel);
        assert!(r.t.is_public());
        // Declared non-optional by core-typings, projected away in practice.
        assert_eq!(r.msgs, None);
        assert_eq!(r.users_count, None);
        assert_eq!(r.u, None);
    }

    #[test]
    fn decodes_sys_mes_as_a_boolean() {
        let r = room(r#"{"_id":"GENERAL","_updatedAt":{"$date":1},"t":"c","sysMes":false}"#);

        let sys_mes = r.sys_mes.clone().unwrap();
        assert_eq!(sys_mes, SysMes::Enabled(false));
        assert_eq!(sys_mes.hidden_types(), None);
        // Round trip keeps the boolean form.
        assert_eq!(serde_json::to_value(&sys_mes).unwrap(), serde_json::json!(false));
    }

    #[test]
    fn a_boolean_sys_mes_hides_nothing_at_the_room_level() {
        // `getHiddenSystemMessages` is
        //     Array.isArray(room.sysMes) ? room.sysMes : <workspace Hide_System_Messages>
        // so neither boolean configures anything: `false` does not hide everything.
        for wire in ["false", "true"] {
            let r = room(&format!(
                r#"{{"_id":"GENERAL","_updatedAt":{{"$date":1}},"t":"c","sysMes":{wire}}}"#
            ));
            let sys_mes = r.sys_mes.unwrap();
            for t in [MessageType::Uj, MessageType::Ul, MessageType::Rm] {
                assert!(!sys_mes.hides(&t), "sysMes:{wire} must not hide {t}");
            }
            assert_eq!(sys_mes.hidden_types(), None);
        }
    }

    #[test]
    fn decodes_sys_mes_as_an_array_of_hidden_types() {
        let r = room(
            r#"{"_id":"GENERAL","_updatedAt":{"$date":1},"t":"c",
                "sysMes":["uj","ul","room_changed_topic","brand_new_type"]}"#,
        );

        let sys_mes = r.sys_mes.clone().unwrap();
        let hidden = sys_mes.hidden_types().unwrap();
        assert_eq!(hidden.len(), 4);
        assert_eq!(hidden[0], MessageType::Uj);
        assert_eq!(hidden[2], MessageType::RoomChangedTopic);
        assert!(hidden[3].is_unknown());

        assert!(sys_mes.hides(&MessageType::Ul));
        assert!(!sys_mes.hides(&MessageType::Rm));
        // An empty array is still the array arm, and hides nothing.
        assert!(!SysMes::Hidden(Vec::new()).hides(&MessageType::Uj));

        assert_eq!(
            serde_json::to_value(&sys_mes).unwrap(),
            serde_json::json!(["uj", "ul", "room_changed_topic", "brand_new_type"])
        );
    }

    #[test]
    fn decodes_role_priorities_created_in_both_shapes() {
        let counted =
            room(r#"{"_id":"r","_updatedAt":{"$date":1},"t":"p","rolePrioritiesCreated":3}"#);
        assert_eq!(counted.role_priorities_created, Some(RolePrioritiesCreated::Version(3)));

        let flagged =
            room(r#"{"_id":"r","_updatedAt":{"$date":1},"t":"p","rolePrioritiesCreated":true}"#);
        assert_eq!(flagged.role_priorities_created, Some(RolePrioritiesCreated::Flag(true)));

        for r in [&counted, &flagged] {
            let json = serde_json::to_string(r).unwrap();
            assert_eq!(room(&json), *r);
        }
    }

    #[test]
    fn decodes_a_realistic_channel_payload() {
        // Field set asserted by the server's own `channels.info` end-to-end test.
        let r = room(
            r#"{"_id":"GENERAL","name":"general","fname":"general","t":"c","msgs":42,
                "usersCount":7,"u":{"_id":"rocket.cat","username":"rocket.cat"},
                "ts":{"$date":1755529012000},"ro":false,"sysMes":true,"default":true,
                "_updatedAt":{"$date":1755529012390},"avatarETag":"abc123",
                "announcementDetails":null,"lastMessage":{"_id":"m1","_updatedAt":{"$date":2},
                "rid":"GENERAL","msg":"hi","ts":{"$date":2},"u":{"_id":"u1","username":"bob"}}}"#,
        );

        assert_eq!(r.msgs, Some(42));
        assert_eq!(r.users_count, Some(7));
        assert_eq!(r.display_name(), Some("general"));
        assert_eq!(r.default, Some(true));
        assert_eq!(r.avatar_etag.as_deref(), Some("abc123"));
        assert_eq!(r.announcement_details, None);
        assert_eq!(r.last_message.as_ref().unwrap().msg, "hi");
        assert!(!r.is_direct());
        assert!(!r.is_team());

        // `avatarETag` is not what camelCase would produce from `avatar_etag`.
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"avatarETag\":\"abc123\""), "{json}");
    }

    #[test]
    fn decodes_a_fractional_sla_due_time() {
        // `dueTimeInMinutes` is a bare `number` in the REST schema and the admin form only
        // checks `> 0`, so a fractional SLA is a valid policy and lands on the room.
        let r = room(
            r#"{"_id":"l1","_updatedAt":{"$date":1},"t":"l","slaId":"sla1",
                "estimatedWaitingTimeQueue":1.5,"priorityWeight":99}"#,
        );
        assert_eq!(r.estimated_waiting_time_queue, Some(1.5));
        assert_eq!(r.priority_weight, Some(99));

        // The common whole-number case still decodes.
        let default = room(
            r#"{"_id":"l2","_updatedAt":{"$date":1},"t":"l",
                "estimatedWaitingTimeQueue":9999999}"#,
        );
        assert_eq!(default.estimated_waiting_time_queue, Some(9_999_999.0));
    }

    #[test]
    fn decodes_a_direct_message_room_without_an_owner() {
        // IDirectMessageRoom omits `u` and `name` entirely.
        let r = room(
            r#"{"_id":"d1","_updatedAt":{"$date":1},"t":"d",
                "uids":["u1","u2","u3"],"usernames":["a","b","c"]}"#,
        );

        assert!(r.is_direct());
        assert!(r.is_multi_user_direct());
        assert_eq!(r.u, None);
        assert_eq!(r.name, None);
    }

    #[test]
    fn an_unknown_room_type_decodes_and_round_trips() {
        let r = room(r#"{"_id":"r","_updatedAt":{"$date":1},"t":"x"}"#);

        assert!(r.t.is_unknown());
        assert_eq!(r.t.as_str(), "x");
        assert!(!r.t.is_public() && !r.t.is_direct() && !r.t.is_private() && !r.t.is_group());
        assert_eq!(serde_json::to_value(&r.t).unwrap(), "x");
    }

    #[test]
    fn room_type_helpers_agree_with_the_wire_values() {
        assert!(RoomType::from("c").is_public());
        assert!(RoomType::from("p").is_private());
        assert!(RoomType::from("d").is_direct());
        assert!(RoomType::from("l").is_omnichannel());
        assert!(RoomType::from("c").is_group() && RoomType::from("p").is_group());
        assert!(!RoomType::from("d").is_group());
    }

    // -- Subscription --------------------------------------------------------------------

    #[test]
    fn decodes_a_subscription_missing_every_optional_field() {
        let s = subscription(
            r#"{"_id":"s1","_updatedAt":{"$date":1},"rid":"GENERAL","t":"c",
                "u":{"_id":"u1","username":"bob"},"ts":{"$date":1},"name":"general",
                "open":true,"unread":0,"userMentions":0,"groupMentions":0}"#,
        );

        assert_eq!(s.id, "s1");
        assert_eq!(s.display_name(), Some("general"));
        assert!(!s.has_unread());
        assert!(!s.is_favorite());
        assert_eq!(s.roles, None);
        assert_eq!(s.ls, None);
        assert_eq!(s.e2e_key, None);
        // `true`-only literals: absent means false.
        assert_eq!(s.hide_unread_status, None);
        assert!(!s.hides_unread_status());
        assert!(!s.hides_mention_status());
    }

    #[test]
    fn decodes_a_fully_populated_subscription() {
        let s = subscription(
            r#"{"_id":"s1","_updatedAt":{"$date":9},"rid":"GENERAL","t":"c",
                "u":{"_id":"u1","username":"bob","name":"Bob B."},"ts":{"$date":1},
                "name":"general","fname":"General","open":true,"alert":true,"f":true,
                "unread":3,"userMentions":1,"groupMentions":2,
                "ls":{"$date":5},"lr":{"$date":6},"roles":["owner","moderator"],
                "hideUnreadStatus":true,"hideMentionStatus":true,
                "E2EKey":"enc","E2ESuggestedKey":"sugg",
                "oldRoomKeys":[{"e2eKeyId":"k1","ts":{"$date":4},"E2EKey":"old"}],
                "unreadAlert":"mentions","desktopNotifications":"all",
                "mobilePushNotifications":"nothing","emailNotifications":"mentions",
                "desktopPrefOrigin":"subscription","tunread":["t1"],
                "status":"INVITED","inviter":{"_id":"u2","username":"carol"},
                "threadDrafts":{"t1":"wip"},"customFields":{"a":1},"blocked":true}"#,
        );

        assert_eq!(s.display_name(), Some("General"));
        assert!(s.has_unread());
        assert_eq!(s.total_mentions(), 3);
        assert!(s.is_favorite());
        assert!(s.has_role("owner"));
        assert!(!s.has_role("leader"));
        assert!(s.hides_unread_status() && s.hides_mention_status());
        assert_eq!(s.e2e_key.as_deref(), Some("enc"));
        assert_eq!(s.e2e_suggested_key.as_deref(), Some("sugg"));
        assert_eq!(s.old_room_keys.as_ref().unwrap()[0].e2e_key.as_deref(), Some("old"));
        assert_eq!(s.unread_alert, Some(UnreadAlert::Mentions));
        assert_eq!(s.desktop_notifications, Some(NotificationPreference::All));
        assert_eq!(s.desktop_pref_origin, Some(PreferenceOrigin::Subscription));
        assert_eq!(s.status, Some(SubscriptionStatus::Invited));
        assert_eq!(s.inviter.as_ref().unwrap().id, "u2");
        assert_eq!(s.thread_drafts.as_ref().unwrap()["t1"], "wip");

        // The oddly-capitalised keys must survive a round trip.
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"E2EKey\":\"enc\""), "{json}");
        assert!(json.contains("\"E2ESuggestedKey\":\"sugg\""), "{json}");
        assert_eq!(subscription(&json), s);
    }

    #[test]
    fn decodes_a_subscription_whose_name_is_explicitly_null() {
        // `createWithRoomAndUser` copies `name: room.name` unconditionally and the workspace
        // runs Mongo with `ignoreUndefined: false`, so joining a nameless omnichannel room
        // (`GET /v1/livechat/room.join`) persists `name: null`. `subscriptionFields` projects
        // `name`, so the null reaches the stream.
        let s = subscription(
            r#"{"_id":"s1","_updatedAt":{"$date":1},"rid":"l1","t":"l",
                "u":{"_id":"agent1","username":"agent"},"ts":{"$date":1},"name":null,
                "open":true,"unread":1,"userMentions":1,"groupMentions":0}"#,
        );

        assert_eq!(s.name, None);
        assert_eq!(s.display_name(), None);
        // A missing key decodes the same way.
        let absent = subscription(
            r#"{"_id":"s1","_updatedAt":{"$date":1},"rid":"l1","t":"l",
                "u":{"_id":"agent1"},"ts":{"$date":1},"open":true,"unread":0,
                "userMentions":0,"groupMentions":0,"fname":"Jane Doe"}"#,
        );
        assert_eq!(absent.name, None);
        assert_eq!(absent.display_name(), Some("Jane Doe"));
        // And a null name is not re-emitted as null.
        assert!(!serde_json::to_string(&s).unwrap().contains("\"name\""));
    }

    #[test]
    fn an_unknown_notification_preference_does_not_fail() {
        let s = subscription(
            r#"{"_id":"s1","_updatedAt":{"$date":1},"rid":"r","t":"p",
                "u":{"_id":"u1","username":"bob"},"ts":{"$date":1},"name":"x",
                "open":false,"unread":0,"userMentions":0,"groupMentions":0,
                "desktopNotifications":"whisper","status":"EXPELLED"}"#,
        );
        assert!(s.desktop_notifications.unwrap().is_unknown());
        assert!(s.status.unwrap().is_unknown());
    }

    // -- User ----------------------------------------------------------------------------

    #[test]
    fn decodes_a_user_from_the_narrowest_projection() {
        // The streamer's publication cache projects `{_id: 1, roles: 1}` — no `_updatedAt`.
        let u = user(r#"{"_id":"u1","roles":["admin","user"]}"#);

        assert_eq!(u.id, "u1");
        assert_eq!(u.updated_at, None);
        assert!(u.has_role("admin"));
        assert!(!u.has_role("bot"));
        // Non-optional in core-typings, absent here.
        assert_eq!(u.user_type, None);
        assert_eq!(u.active, None);
    }

    #[test]
    fn decodes_a_name_changed_payload() {
        // `Users:NameChanged` carries only `{_id, name, username}`.
        let u = user(r#"{"_id":"u1","name":"Alice A.","username":"alice"}"#);

        assert_eq!(u.display_name(true), Some("Alice A."));
        assert_eq!(u.display_name(false), Some("alice"));
        assert_eq!(u.roles, None);
        // Absent roles are not the same as "holds no roles".
        assert!(!u.has_role("admin"));
    }

    #[test]
    fn decodes_a_full_user_document() {
        let u = user(
            r#"{"_id":"u1","_updatedAt":{"$date":9},"createdAt":{"$date":1},
                "username":"alice","name":"Alice A.","nickname":"ali","bio":"hi",
                "roles":["user"],"type":"user","active":true,
                "status":"busy","statusDefault":"online","statusConnection":"online",
                "statusText":"in a meeting","statusSource":"manual",
                "statusExpiresAt":{"$date":100},
                "emails":[{"address":"a@example.com","verified":true}],
                "utcOffset":5.5,"language":"en","avatarETag":"tag1",
                "settings":{"preferences":{"idleTimeLimit":300}},
                "customFields":{"team":"core"},"__rooms":["GENERAL"],
                "roomRolePriorities":{"GENERAL":0},
                "services":{"password":{"bcrypt":"$2b$10$secret"},
                            "resume":{"loginTokens":[{"hashedToken":"nope"}]}}}"#,
        );

        // The three status concepts are distinct.
        assert_eq!(u.status, Some(UserStatus::Busy));
        assert_eq!(u.status_default, Some(UserStatus::Online));
        assert_eq!(u.status_connection.as_deref(), Some("online"));
        assert_eq!(u.status_source, Some(PresenceSource::Manual));
        assert!(u.is_connected());

        assert_eq!(u.utc_offset, Some(5.5));
        assert_eq!(u.emails.as_ref().unwrap()[0].address, "a@example.com");
        assert_eq!(u.avatar_etag.as_deref(), Some("tag1"));
        assert_eq!(u.rooms.as_ref().unwrap()[0], "GENERAL");
        assert_eq!(u.room_role_priorities.as_ref().unwrap()["GENERAL"], 0);

        // `services` is deliberately not modelled, so credentials cannot be re-emitted.
        let json = serde_json::to_string(&u).unwrap();
        assert!(!json.contains("services"), "{json}");
        assert!(!json.contains("bcrypt"), "{json}");
        assert!(!json.contains("secret"), "{json}");
    }

    #[test]
    fn an_unknown_user_status_does_not_fail() {
        let u = user(r#"{"_id":"u1","status":"hibernating"}"#);
        assert!(u.status.clone().unwrap().is_unknown());
        assert!(!u.is_connected());
        assert_eq!(serde_json::to_value(&u.status).unwrap(), "hibernating");
    }

    #[test]
    fn a_disabled_user_reports_the_disabled_status() {
        let u = user(r#"{"_id":"u1","active":false,"status":"disabled"}"#);
        assert_eq!(u.status, Some(UserStatus::Disabled));
        assert!(!u.is_connected());
    }

    // -- Presence ------------------------------------------------------------------------

    #[test]
    fn decodes_the_numeric_presence_codes() {
        // The `user-status` stream event is
        // [uid, username, statusCode, statusText, name, roles, ...].
        let event: (UserId, String, PresenceStatus, Option<String>) =
            serde_json::from_str(r#"["u1","alice",2,"brb"]"#).unwrap();

        assert_eq!(event.2, PresenceStatus::Away);
        assert_eq!(event.2.as_user_status(), Some(UserStatus::Away));
        assert_eq!(event.2.code(), 2);
    }

    #[test]
    fn presence_codes_round_trip_including_unknown_ones() {
        for (code, expected) in [
            (0u8, PresenceStatus::Offline),
            (1, PresenceStatus::Online),
            (2, PresenceStatus::Away),
            (3, PresenceStatus::Busy),
            (4, PresenceStatus::Unknown(4)),
            (200, PresenceStatus::Unknown(200)),
        ] {
            let decoded: PresenceStatus = serde_json::from_str(&code.to_string()).unwrap();
            assert_eq!(decoded, expected);
            assert_eq!(serde_json::to_string(&decoded).unwrap(), code.to_string());
        }
    }

    #[test]
    fn presence_code_zero_is_ambiguous() {
        // The server maps both `offline` and `disabled` to 0, so a disabled account is
        // indistinguishable from an offline one on the presence stream.
        assert_eq!(PresenceStatus::from(0u8), PresenceStatus::Offline);
        assert_eq!(PresenceStatus::from(0u8).as_user_status(), Some(UserStatus::Offline));
        assert!(PresenceStatus::Unknown(9).as_user_status().is_none());
    }
}
