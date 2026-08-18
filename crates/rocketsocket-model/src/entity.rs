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
//!   `bool | number`. Both are modelled as untagged enums.
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
/// Encoding is lossless, including for [`PresenceStatus::Unknown`].
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
/// `name` may be **absent or explicitly `null`** — the server unsets it rather than deleting
/// the key when a user clears their display name. Both decode to `None`.
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
        let (first, second) = if use_real_name {
            (&self.name, &self.username)
        } else {
            (&self.username, &self.name)
        };
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
/// **not** a boolean. A payload delivered to one user only ever contains that user's own
/// entry, which is why the field is so easy to mistake for a flag.
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
    /// When `Message_KeepHistory` is on, deleting a message rewrites it in place with
    /// `t = "rm"` and an `editedAt` rather than removing the document. Such a message still
    /// arrives on the stream as a normal update and must not be rendered.
    #[must_use]
    pub fn is_deleted_tombstone(&self) -> bool {
        self.is_edited() && self.t.as_ref() == Some(&MessageType::Rm)
    }

    /// Whether this message is a reply inside a thread.
    #[must_use]
    pub fn is_thread_reply(&self) -> bool {
        self.tmid.is_some()
    }

    /// Whether this message is the root of a thread that has replies.
    #[must_use]
    pub fn is_thread_main(&self) -> bool {
        self.tcount.is_some_and(|count| count > 0)
    }

    /// Whether a discussion was created from this message.
    #[must_use]
    pub fn is_discussion_parent(&self) -> bool {
        self.drid.is_some()
    }

    /// Whether `user` starred this message.
    ///
    /// Only meaningful on a payload delivered to that same user: the server projects
    /// `starred` per recipient.
    #[must_use]
    pub fn is_starred_by(&self, user: &UserId) -> bool {
        self.starred
            .as_ref()
            .is_some_and(|stars| stars.iter().any(|star| star.user_id == *user))
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
/// - `false` hides *all* system messages in the room; `true` shows all of them.
/// - an array lists the system message types to **hide**, leaving the rest visible.
///
/// The server treats "absent" as "show everything", and only an array is consulted by
/// `getHiddenSystemMessages`; a boolean falls back to the workspace-wide default list.
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

    /// Whether `t` is hidden in this room.
    ///
    /// `Enabled(false)` hides everything; `Enabled(true)` hides nothing *here* and defers to
    /// the workspace default, which this crate cannot see.
    #[must_use]
    pub fn hides(&self, t: &MessageType) -> bool {
        match self {
            Self::Enabled(enabled) => !enabled,
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
/// direct message rooms (`IDirectMessageRoom` omits it).
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

    /// URL-safe room name. Absent on direct message rooms.
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

    /// The room's owner stub. Absent on direct message rooms.
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
    #[serde(
        rename = "usersWaitingForE2EKeys",
        default,
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub lm: Option<Timestamp>,
    /// When the room was created.
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub ts: Option<Timestamp>,
    /// When a WebRTC call started in this room.
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_waiting_time_queue: Option<i64>,
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
/// The mandatory set matches `subscriptionFields` in `publishFields.ts`: id, `_updatedAt`,
/// `rid`, `u`, `t`, `ts`, `name`, `open`, `unread`, `userMentions` and `groupMentions`.
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
    pub name: String,
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub ls: Option<Timestamp>,
    /// Last time a message was received in the room ("last received").
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        rename = "E2ESuggestedKey",
        default,
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
        self.roles
            .as_ref()
            .is_some_and(|roles| roles.iter().any(|r| r == role))
    }

    /// The name to show, preferring the display name.
    #[must_use]
    pub fn display_name(&self) -> &str {
        self.fname.as_deref().unwrap_or(&self.name)
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
/// `active` non-optional, but every one of them is dropped by real projections — the streamer's
/// own user cache projects `{_id: 1, roles: 1}`, and `Users:NameChanged` carries only
/// `{_id, name, username}`. Presence updates arrive as partial diffs too.
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub status_expires_at: Option<Timestamp>,
    /// Id of the custom status in use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_id: Option<String>,
    /// When the user last logged in.
    #[serde(
        default,
        with = "crate::datetime::option",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[must_use]
    pub fn display_name(&self, use_real_name: bool) -> Option<&str> {
        let (first, second) = if use_real_name {
            (&self.name, &self.username)
        } else {
            (&self.username, &self.name)
        };
        first.as_deref().or(second.as_deref())
    }

    /// Whether this user holds `role` globally.
    ///
    /// Returns `false` when `roles` was projected away, which is not the same as the user not
    /// holding the role — check `roles.is_some()` first if the distinction matters.
    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles
            .as_ref()
            .is_some_and(|roles| roles.iter().any(|r| r == role))
    }

    /// Whether the user is reachable right now: online, away or busy.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        matches!(
            self.status,
            Some(UserStatus::Online | UserStatus::Away | UserStatus::Busy)
        )
    }
}
