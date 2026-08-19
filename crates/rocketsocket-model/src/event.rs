//! Typed stream events.
//!
//! Rocket.Chat delivers every realtime event as a DDP `changed` frame on a pseudo-collection,
//! carrying a `(eventName, args)` pair where `args` is a **positional, variable-arity**
//! JSON array. [`protocol::StreamEvent`] is that pair, borrowed and untyped.
//! [`StreamEvent`] — this module's type — is the same event after the tuple has been taken
//! apart, so a bot matches on a variant instead of remembering that a room message lives at
//! `args[0]` and a typing indicator's activity list at `args[1]`.
//!
//! ```no_run
//! # use rocketsocket_model::StreamEvent;
//! # use serde_json::Value;
//! # fn handle(stream: &str, event_name: &str, args: &[Value]) {
//! match StreamEvent::decode(stream, event_name, args) {
//!     StreamEvent::RoomMessage { room, message } => println!("[{room}] {}", message.msg),
//!     StreamEvent::UserActivity { user, activities, .. } if !activities.is_empty() => {
//!         println!("{user} is typing");
//!     }
//!     _ => {}
//! }
//! # }
//! ```
//!
//! # Decoding cannot fail
//!
//! [`StreamEvent::decode`] returns a `StreamEvent`, not a `Result`. Anything unrecognised,
//! wrong-arity or wrong-typed becomes [`StreamEvent::Unknown`] with the raw `args` intact.
//!
//! This is not politeness, it is the only safe design. The decoder sits directly in the path
//! of live traffic that nothing else will re-deliver: a stream event is fire-and-forget, with
//! no acknowledgement, no replay and no sequence number. A decoder that could return an error
//! would put the caller in the position of having to decide what to do with an event it
//! cannot see — and every caller would get that wrong in the same way, by logging and
//! dropping it. `Unknown` keeps the payload reachable, so a bot can handle an event this
//! crate has never heard of, and a crate that lags a Rocket.Chat release degrades to "less
//! convenient" rather than "loses messages".
//!
//! # Leniency rules
//!
//! Arity is not stable. `user-status` shipped as 3 elements, then 6, then 8;
//! `__my_messages__` gained a trailing element that its declared type still does not show.
//! So decoding reads the positions it needs and applies three rules:
//!
//! 1. **Extra trailing elements are ignored.** They are how Rocket.Chat extends an event.
//! 2. **`null` is absence.** EJSON drops an `undefined` object *value* but encodes an
//!    `undefined` array *element* as `null`, so tuples arrive with holes in the middle. A
//!    `null` at an optional position reads as `None`, never as a decode failure.
//! 3. **A required position that is missing or of the wrong type means `Unknown`** — the
//!    whole event, not a half-filled variant. A `RoomMessage` whose `message` failed to parse
//!    would be a lie; the raw args are more useful.
//!
//! # `streams.ts` is the index, `listeners.module.ts` is the truth
//!
//! The generated [`catalog`] is read from `packages/ddp-client/src/types/streams.ts`, which
//! is a *declared* type. The code that actually emits — `listeners.module.ts` and the
//! `notifyListener` helpers — disagrees with it in several places, and where they conflict
//! the emit site wins:
//!
//! - **`room-messages` / `__my_messages__`** is declared `[IMessage]`. The emit site sends
//!   `[...args, allowed]` (`listeners.module.ts:216`), where `allowed` is the return value of
//!   the `allowEmit` authorization hook — `{roomParticipant, roomType, roomName}`
//!   (`notifications.module.ts:112`). Every such event therefore has **two** elements. See
//!   [`StreamEvent::MyMessage`].
//! - **`notify-user` / `<uid>/subscriptions-changed`** is declared to carry a reduced
//!   `{_id, u?, rid?, t?}` document when the action is `removed`. Every current emit path
//!   sends the **full** subscription: the removal helpers pass the document returned by
//!   `findOneAndDelete` (`Subscriptions.removeByRoomIdAndUserId`, `models/Subscriptions.ts:1751`)
//!   straight to `notifyOnSubscriptionChanged(doc, 'removed')`. [`SubscriptionChange`] accepts
//!   both.
//! - **`notify-room` / `<rid>/user-activity`** is declared `[username, activities]`, and that
//!   is what the server re-broadcasts. The *client*-to-server direction sends a third element:
//!   `sdk.publish('notify-room', [key, username, activities, extras])`
//!   (`app/ui/client/lib/UserAction.ts:43`). Rule 1 above covers it.
//! - **`notify-logged` / `user-status`** is declared with 8 slots and the emit site fills all
//!   8, but older servers in the support window send 3 or 6. Rule 2 covers it.
//!
//! # What is typed and what is not
//!
//! The streams a bot actually needs are typed (see the crate's `PLAN.md` §6.3):
//! `room-messages`, `notify-room`, `notify-user` and `notify-logged`. The remaining 13
//! streams — apps, omnichannel queues, importers, webdav, video conferencing, licensing —
//! reach the caller as [`StreamEvent::Unknown`]. Half-typing them would mean modelling a
//! dozen entity types nobody in this crate's audience subscribes to, and each one would be a
//! guess this crate could not test.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::datetime::Timestamp;
use crate::entity::{
    Message, MessageType, PresenceSource, PresenceStatus, Room, RoomType, Subscription, User,
    UserRef,
};
use crate::id::{MessageId, RoleId, RoomId, SubscriptionId, UserId};
use crate::protocol;

// ---------------------------------------------------------------------------------------
// ---------- GENERATED-BEGIN ----------

/// The Rocket.Chat stream surface, read straight out of `packages/ddp-client/src/types/streams.ts`.
///
/// **Generated. Do not edit by hand** — run `cargo run -p rocketsocket-codegen`,
/// which rewrites everything between the `GENERATED-BEGIN` and `GENERATED-END`
/// markers and leaves the rest of this file alone.
///
/// This is an *index*, not a decoder. It answers "does this workspace's server
/// version declare this (stream, event) pair, and how many positional arguments
/// does it promise?" — which is what makes upstream drift visible: the tests in
/// this module assert that every stream [`StreamEvent`] types is still here, so a
/// stream that disappears upstream fails the build instead of quietly becoming
/// dead code.
///
/// It is *not* the authority on what the server actually sends; see the module
/// docs for where the declared types and the emit site disagree.
pub mod catalog {

    /// One DDP stream. The wire name is this `name` with a `stream-` prefix.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct StreamSpec {
        /// Stream name as declared upstream, without the `stream-` prefix.
        pub name: &'static str,
        /// Event entries, in declaration order.
        pub events: &'static [EventSpec],
    }

    impl StreamSpec {
        /// The entry matching a concrete `eventName`, in declaration order.
        ///
        /// Order is load-bearing: several streams declare a catch-all key after their
        /// specific ones, and `room-messages` is exactly that case — `__my_messages__` is
        /// declared before the free-form room-id key, so it must be tried first.
        #[must_use]
        pub fn event(&self, key: &str) -> Option<&'static EventSpec> {
            self.events.iter().find(|event| event.key.matches(key))
        }
    }

    /// One `{ key; args }` entry of a stream.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct EventSpec {
        /// Shape of the `eventName` this entry matches.
        pub key: KeyPattern,
        /// Declared tuple lengths, ascending. More than one means the declared type is a
        /// union of tuples of differing length.
        ///
        /// Treat these as a hint, never as a validation rule. Arity has changed across
        /// server releases — `user-status` grew from 3 to 6 to 8 — and the emit site adds
        /// elements the declared type does not show.
        pub arities: &'static [usize],
        /// Whether an alternative is an unbounded array rather than a fixed tuple.
        pub variadic: bool,
        /// The declared `args` type, verbatim, whitespace collapsed.
        pub args: &'static str,
    }

    /// The shape of an event key.
    ///
    /// Rocket.Chat keys are frequently composite — `<rid>/user-activity`, `<uid>/message` —
    /// which upstream expresses as a TypeScript template literal type.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[non_exhaustive]
    pub enum KeyPattern {
        /// A fixed key, matched exactly.
        Literal(&'static str),
        /// `` `${string}/suffix` `` — free text, a slash, then this suffix.
        Suffix(&'static str),
        /// `` `prefix/${string}` `` — this prefix, a slash, then free text.
        Prefix(&'static str),
        /// The whole key is free text, typically a room id or a uid.
        Any,
    }

    impl KeyPattern {
        /// Whether a concrete `eventName` matches this pattern.
        ///
        /// Composite keys are split on the **first** `/` only: a suffix such as
        /// `e2e.keyRequest` contains no slash, but a room id never does either, and
        /// splitting from the left is what the server itself does
        /// (`const [rid, e] = eventName.split('/')`).
        #[must_use]
        pub fn matches(&self, key: &str) -> bool {
            match *self {
                Self::Literal(value) => key == value,
                Self::Suffix(suffix) => {
                    key.split_once('/').is_some_and(|(head, rest)| !head.is_empty() && rest == suffix)
                }
                Self::Prefix(prefix) => {
                    key.split_once('/').is_some_and(|(head, rest)| head == prefix && !rest.is_empty())
                }
                Self::Any => true,
            }
        }
    }

    /// Rocket.Chat commit the catalog was generated from.
    pub const UPSTREAM_COMMIT: &str = "ea163f56b53b1dd40d49af39e0406397cbd24939";

    /// `apps/meteor` version at [`UPSTREAM_COMMIT`].
    pub const UPSTREAM_VERSION: &str = "8.8.0-develop";

    /// Number of streams declared upstream.
    pub const STREAM_COUNT: usize = 17;

    /// Number of `{ key; args }` entries declared upstream, across all streams.
    pub const EVENT_COUNT: usize = 80;

    /// Every stream, in upstream declaration order.
    pub const STREAMS: &[StreamSpec] = &[
        StreamSpec {
            name: "roles",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("roles"),
                    arities: &[1],
                    variadic: false,
                    args: "[ IRole & { type: 'inserted' | 'updated' | 'removed' | 'changed'; }, ]",
                },
            ],
        },
        StreamSpec {
            name: "notify-room",
            events: &[
                EventSpec {
                    key: KeyPattern::Suffix("user-activity"),
                    arities: &[2],
                    variadic: false,
                    args: "[username: string, activities: string[]]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("typing"),
                    arities: &[2],
                    variadic: false,
                    args: "[username: string, typing: boolean]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("deleteMessageBulk"),
                    arities: &[1],
                    variadic: false,
                    args: "[ args: { rid: IMessage['rid']; excludePinned: boolean; ignoreDiscussion: boolean; ts: Record<string, Date>; users: string[]; ids?: string[]; showDeletedStatus?: boolean; } & ( | { filesOnly: true; replaceFileAttachmentsWith?: MessageAttachment; } | { filesOnly?: false; } ), ]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("deleteMessage"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ _id: IMessage['_id'] }]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("e2e.keyRequest"),
                    arities: &[1],
                    variadic: false,
                    args: "[unknown]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("videoconf"),
                    arities: &[1],
                    variadic: false,
                    args: "[id: string]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("messagesRead"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ until: Date; tmid?: string }]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("messagesImported"),
                    arities: &[1],
                    variadic: false,
                    args: "[null]",
                },
            ],
        },
        StreamSpec {
            name: "room-messages",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("__my_messages__"),
                    arities: &[1],
                    variadic: false,
                    args: "[IMessage]",
                },
                EventSpec {
                    key: KeyPattern::Any,
                    arities: &[3],
                    variadic: false,
                    args: "[message: IMessage, user?: IUser, room?: IRoom]",
                },
            ],
        },
        StreamSpec {
            name: "notify-all",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("public-info"),
                    arities: &[1],
                    variadic: false,
                    args: "[ | [key: 'public-settings-changed', args: ['inserted' | 'updated' | 'removed' | 'changed', ISetting]] | [key: 'deleteCustomSound', args: [{ soundData: ICustomSound }]] | [key: 'updateCustomSound', args: [{ soundData: ICustomSound }]], ]",
                },
                EventSpec {
                    key: KeyPattern::Literal("public-settings-changed"),
                    arities: &[2],
                    variadic: false,
                    args: "['inserted' | 'updated' | 'removed' | 'changed', ISetting]",
                },
                EventSpec {
                    key: KeyPattern::Literal("deleteCustomSound"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ soundData: ICustomSound }]",
                },
                EventSpec {
                    key: KeyPattern::Literal("updateCustomSound"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ soundData: ICustomSound }]",
                },
                EventSpec {
                    key: KeyPattern::Literal("license"),
                    arities: &[0, 1],
                    variadic: false,
                    args: "[{ preventedActions: Record<LicenseLimitKind, boolean> }] | []",
                },
            ],
        },
        StreamSpec {
            name: "notify-user",
            events: &[
                EventSpec {
                    key: KeyPattern::Suffix("rooms-changed"),
                    arities: &[2],
                    variadic: false,
                    args: "['inserted' | 'updated' | 'removed' | 'changed', IRoom]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("subscriptions-changed"),
                    arities: &[2],
                    variadic: false,
                    args: "| [ 'removed', { _id: string; u?: Pick<IUser, '_id' | 'username' | 'name'>; rid?: string; t?: string; }, ] | [ 'inserted' | 'updated', Pick< ISubscription, | 't' | 'ts' | 'ls' | 'lr' | 'name' | 'fname' | 'rid' | 'code' | 'f' | 'u' | 'open' | 'alert' | 'roles' | 'unread' | 'prid' | 'userMentions' | 'groupMentions' | 'archived' | 'audioNotificationValue' | 'desktopNotifications' | 'mobilePushNotifications' | 'emailNotifications' | 'desktopPrefOrigin' | 'mobilePrefOrigin' | 'emailPrefOrigin' | 'unreadAlert' | '_updatedAt' | 'blocked' | 'blocker' | 'autoTranslate' | 'autoTranslateLanguage' | 'disableNotifications' | 'hideUnreadStatus' | 'hideMentionStatus' | 'muteGroupMentions' | 'ignored' | 'E2EKey' | 'E2ESuggestedKey' | 'oldRoomKeys' | 'tunread' | 'tunreadGroup' | 'tunreadUser' | 'department' | 'v' | 'onHold' >, ]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("message"),
                    arities: &[1],
                    variadic: false,
                    args: "[IMessage]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("force_logout"),
                    arities: &[1],
                    variadic: false,
                    args: "[ISession['sessionId'] | undefined]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("webdav"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ type: 'changed'; account: Partial<IWebdavAccount> } | { type: 'removed'; account: { _id: IWebdavAccount['_id'] } }]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("e2ekeyRequest"),
                    arities: &[2],
                    variadic: false,
                    args: "[string, string]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("notification"),
                    arities: &[1],
                    variadic: false,
                    args: "[INotificationDesktop]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("call.hangup"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ roomId: string }]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("uiInteraction"),
                    arities: &[1],
                    variadic: false,
                    args: "[UiKit.ServerInteraction]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("video-conference"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ action: string; params: { callId: VideoConference['_id']; uid: IUser['_id']; rid: IRoom['_id'] } }]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("media-signal"),
                    arities: &[1],
                    variadic: false,
                    args: "[ServerMediaSignal]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("userData"),
                    arities: &[1],
                    variadic: false,
                    args: "[IUserDataEvent]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("updateInvites"),
                    arities: &[1],
                    variadic: false,
                    args: "[unknown]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("departmentAgentData"),
                    arities: &[1],
                    variadic: false,
                    args: "[unknown]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("calendar"),
                    arities: &[1],
                    variadic: false,
                    args: "[ICalendarNotification]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("banners"),
                    arities: &[1],
                    variadic: false,
                    args: "[IBanner]",
                },
            ],
        },
        StreamSpec {
            name: "importers",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("progress"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ rate: number } | IImportProgress]",
                },
            ],
        },
        StreamSpec {
            name: "notify-logged",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("updateCustomUserStatus"),
                    arities: &[1],
                    variadic: false,
                    args: "[ { userStatusData: Omit<ICustomUserStatus, '_updatedAt'>; }, ]",
                },
                EventSpec {
                    key: KeyPattern::Literal("permissions-changed"),
                    arities: &[2],
                    variadic: false,
                    args: "['inserted' | 'updated' | 'removed' | 'changed', ISetting]",
                },
                EventSpec {
                    key: KeyPattern::Literal("deleteEmojiCustom"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ emojiData: IEmoji }]",
                },
                EventSpec {
                    key: KeyPattern::Literal("updateEmojiCustom"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ emojiData: IEmoji }]",
                },
                EventSpec {
                    key: KeyPattern::Literal("new-banner"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ bannerId: string }]",
                },
                EventSpec {
                    key: KeyPattern::Literal("banner-changed"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ bannerId: string }]",
                },
                EventSpec {
                    key: KeyPattern::Literal("roles-change"),
                    arities: &[1],
                    variadic: false,
                    args: "[ { type: 'added' | 'removed' | 'changed'; _id: IRole['_id']; u?: { _id: IUser['_id']; username: IUser['username']; name?: IUser['name'] }; scope?: string; }, ]",
                },
                EventSpec {
                    key: KeyPattern::Literal("Users:NameChanged"),
                    arities: &[1],
                    variadic: false,
                    args: "[Pick<IUser, '_id' | 'name' | 'username'>]",
                },
                EventSpec {
                    key: KeyPattern::Literal("private-settings-changed"),
                    arities: &[2],
                    variadic: false,
                    args: "['inserted' | 'updated' | 'removed' | 'changed', ISetting]",
                },
                EventSpec {
                    key: KeyPattern::Literal("deleteCustomUserStatus"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ userStatusData: Omit<ICustomUserStatus, '_updatedAt'> }]",
                },
                EventSpec {
                    key: KeyPattern::Literal("user-status"),
                    arities: &[1],
                    variadic: false,
                    args: "[ [ uid: IUser['_id'], username: IUser['username'], status: PresenceStatusCode, statusText: IUser['statusText'], name: IUser['name'], roles: IUser['roles'], statusSource?: IUser['statusSource'], statusExpiresAt?: IUser['statusExpiresAt'], ], ]",
                },
                EventSpec {
                    key: KeyPattern::Literal("Users:Deleted"),
                    arities: &[1],
                    variadic: false,
                    args: "[ | { userId: IUser['_id']; messageErasureType: 'Delete'; replaceByUser?: never; } | { userId: IUser['_id']; messageErasureType: 'Unlink'; replaceByUser?: { _id: IUser['_id']; username: IUser['username']; alias: string }; }, ]",
                },
                EventSpec {
                    key: KeyPattern::Literal("updateAvatar"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ username: IUser['username']; etag: IUser['avatarETag'] } | { rid: IRoom['_id']; etag: IRoom['avatarETag'] }]",
                },
                EventSpec {
                    key: KeyPattern::Literal("omnichannel.priority-changed"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ id: string; clientAction: ClientAction; name?: string }]",
                },
            ],
        },
        StreamSpec {
            name: "room-data",
            events: &[
                EventSpec {
                    key: KeyPattern::Any,
                    arities: &[1],
                    variadic: false,
                    args: "[IOmnichannelRoom | Pick<IOmnichannelRoom, '_id'>]",
                },
            ],
        },
        StreamSpec {
            name: "notify-room-users",
            events: &[
                EventSpec {
                    key: KeyPattern::Suffix("video-conference"),
                    arities: &[1],
                    variadic: false,
                    args: "[{ action: string; params: { callId: VideoConference['_id']; uid: IUser['_id']; rid: IRoom['_id'] } }]",
                },
                EventSpec {
                    key: KeyPattern::Suffix("userData"),
                    arities: &[],
                    variadic: true,
                    args: "unknown[]",
                },
            ],
        },
        StreamSpec {
            name: "livechat-room",
            events: &[
                EventSpec {
                    key: KeyPattern::Any,
                    arities: &[1],
                    variadic: false,
                    args: "[ | { type: 'agentStatus'; status: string; } | { type: 'queueData'; data: | { [k: string]: unknown; } | undefined; } | { type: 'agentData'; data: ILivechatAgent | undefined | { hiddenInfo: boolean }; } | { type: 'visitorData'; visitor: unknown; }, ]",
                },
            ],
        },
        StreamSpec {
            name: "user-presence",
            events: &[
                EventSpec {
                    key: KeyPattern::Any,
                    arities: &[1],
                    variadic: false,
                    args: "[ [ username: string, statusChanged?: PresenceStatusCode, statusText?: string, statusSource?: IUser['statusSource'], statusExpiresAt?: IUser['statusExpiresAt'], ], ]",
                },
            ],
        },
        StreamSpec {
            name: "integrationHistory",
            events: &[
                EventSpec {
                    key: KeyPattern::Any,
                    arities: &[1],
                    variadic: false,
                    args: "[ | { type: 'removed'; id: string } | { id: string; diff: unknown; type: 'updated'; } | { type: 'inserted'; data: Partial<IIntegrationHistory>; }, ]",
                },
            ],
        },
        StreamSpec {
            name: "canned-responses",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("canned-responses"),
                    arities: &[1, 2],
                    variadic: false,
                    args: "| [{ type: 'removed'; _id: string }, { agentsId: string }] | [{ type: 'removed'; _id: string }] | [ { type: 'changed' } & Omit<IOmnichannelCannedResponse, '_updatedAt' | '_createdAt'> & { _createdAt?: Date | undefined; }, ] | [{ type: 'changed' } & IOmnichannelCannedResponse, { agentsId: string }]",
                },
            ],
        },
        StreamSpec {
            name: "livechat-inquiry-queue-observer",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("public"),
                    arities: &[1],
                    variadic: false,
                    args: "[ { type: 'added' | 'removed' | 'changed'; } & ILivechatInquiryRecord, ]",
                },
                EventSpec {
                    key: KeyPattern::Prefix("department"),
                    arities: &[1],
                    variadic: false,
                    args: "[ { type: 'added' | 'removed' | 'changed'; } & ILivechatInquiryRecord, ]",
                },
                EventSpec {
                    key: KeyPattern::Prefix("agent"),
                    arities: &[1],
                    variadic: false,
                    args: "[ { type: 'added' | 'removed' | 'changed'; } & ILivechatInquiryRecord, ]",
                },
                EventSpec {
                    key: KeyPattern::Any,
                    arities: &[1],
                    variadic: false,
                    args: "[ { _id: string; clientAction: string; }, ]",
                },
            ],
        },
        StreamSpec {
            name: "apps",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("app/added"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("app/removed"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("app/updated"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("app/statusUpdate"),
                    arities: &[1],
                    variadic: false,
                    args: "[ { appId: string; status: AppStatus; }, ]",
                },
                EventSpec {
                    key: KeyPattern::Literal("app/settingUpdated"),
                    arities: &[1],
                    variadic: false,
                    args: "[ { appId: string; setting: AppsSetting; }, ]",
                },
                EventSpec {
                    key: KeyPattern::Literal("command/added"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("command/disabled"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("command/updated"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("command/removed"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("actions/changed"),
                    arities: &[0],
                    variadic: false,
                    args: "[]",
                },
                EventSpec {
                    key: KeyPattern::Literal("apps"),
                    arities: &[1],
                    variadic: false,
                    args: "[ | [key: 'app/added', args: [string]] | [key: 'app/removed', args: [string]] | [key: 'app/updated', args: [string]] | [ key: 'app/statusUpdate', args: [ { appId: string; status: AppStatus; }, ], ] | [ key: 'app/settingUpdated', args: [ { appId: string; setting: AppsSetting; }, ], ] | [key: 'command/added', args: [string]] | [key: 'command/disabled', args: [string]] | [key: 'command/updated', args: [string]] | [key: 'command/removed', args: [string]] | [key: 'actions/changed', args: []], ]",
                },
            ],
        },
        StreamSpec {
            name: "apps-engine",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("app/added"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("app/removed"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("app/updated"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("app/statusUpdate"),
                    arities: &[1],
                    variadic: false,
                    args: "[ { appId: string; status: AppStatus; }, ]",
                },
                EventSpec {
                    key: KeyPattern::Literal("app/settingUpdated"),
                    arities: &[1],
                    variadic: false,
                    args: "[ { appId: string; setting: AppsSetting; }, ]",
                },
                EventSpec {
                    key: KeyPattern::Literal("command/added"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("command/disabled"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("command/updated"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("command/removed"),
                    arities: &[1],
                    variadic: false,
                    args: "[string]",
                },
                EventSpec {
                    key: KeyPattern::Literal("actions/changed"),
                    arities: &[0],
                    variadic: false,
                    args: "[]",
                },
            ],
        },
        StreamSpec {
            name: "local",
            events: &[
                EventSpec {
                    key: KeyPattern::Literal("broadcast"),
                    arities: &[],
                    variadic: true,
                    args: "any[]",
                },
            ],
        },
    ];

    /// Looks up a stream by name, with or without the `stream-` prefix.
    #[must_use]
    pub fn stream(name: &str) -> Option<&'static StreamSpec> {
        let name = name.strip_prefix("stream-").unwrap_or(name);
        STREAMS.iter().find(|spec| spec.name == name)
    }

    /// Looks up the entry a `(stream, eventName)` pair resolves to.
    #[must_use]
    pub fn event(stream_name: &str, key: &str) -> Option<&'static EventSpec> {
        stream(stream_name)?.event(key)
    }
}

// ---------- GENERATED-END ----------
// ---------------------------------------------------------------------------------------

/// Defines a string-valued wire enum that can never fail to decode.
///
/// Same shape as `entity::wire_enum!`, which is private to that module. The duplication is
/// deliberate: `entity.rs` is hand-maintained model code and this file is partly generated,
/// and coupling the two through a shared macro would mean regeneration could break `entity`.
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
            /// A value this crate does not know about. Round-trips unchanged.
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

wire_enum! {
    /// What a user is doing in a room, as reported by `<rid>/user-activity`.
    ///
    /// The event carries a *list*, and an **empty list means "stopped"** — there is no
    /// separate stop event. A client that only checks `activities.contains(&Activity::UserTyping)`
    /// therefore needs no extra state to clear the indicator.
    pub enum Activity {
        /// Composing a message.
        UserTyping = "user-typing",
        /// Recording audio or video.
        UserRecording = "user-recording",
        /// Uploading a file.
        UserUploading = "user-uploading",
    }
}

wire_enum! {
    /// What happened to a document, on the streams that carry a change verb.
    ///
    /// Rocket.Chat is inconsistent here: the DDP-facing streams use these four values, while
    /// the internal watcher maps them onto minimongo's `added`/`changed`/`removed` before some
    /// livechat events go out. Only the DDP spelling reaches this crate.
    pub enum ClientAction {
        /// The document was created.
        Inserted = "inserted",
        /// The document was modified.
        Updated = "updated",
        /// The document was deleted.
        Removed = "removed",
        /// A generic change, used where the server does not distinguish insert from update.
        Changed = "changed",
    }
}

wire_enum! {
    /// What happened to a role assignment on `notify-logged` / `roles-change`.
    ///
    /// Spelled differently from [`ClientAction`] upstream — `added`, not `inserted`.
    pub enum RoleChangeKind {
        /// The role was granted.
        Added = "added",
        /// The role was revoked.
        Removed = "removed",
        /// The role document itself changed.
        Changed = "changed",
    }
}

// ---------------------------------------------------------------------------------------
// Event payloads
// ---------------------------------------------------------------------------------------

/// The trailing element appended to every `room-messages` / `__my_messages__` event.
///
/// It is not part of the message. It is the return value of the stream's `allowEmit`
/// authorization hook, computed per recipient, and it answers a question the message itself
/// cannot: *is this subscriber actually in the room?* `__my_messages__` delivers every message
/// the subscriber is allowed to **read**, which on a public workspace includes channels they
/// have never joined. A bot that wants "rooms I am in" must filter on
/// [`room_participant`](Self::room_participant).
///
/// All three fields are optional because the hook returns a plain object literal and
/// `roomName` is absent for direct messages.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MyMessageMeta {
    /// Whether the recipient has a subscription to the room, as opposed to merely being
    /// permitted to read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room_participant: Option<bool>,
    /// Kind of the room the message was posted in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room_type: Option<RoomType>,
    /// Name of the room. Absent for direct messages, which have none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room_name: Option<String>,
}

/// The payload of `notify-room` / `<rid>/deleteMessageBulk`.
///
/// Sent by prune and by user-deletion cleanup. **The messages are identified by a query, not
/// by a list**: unless [`ids`](Self::ids) is present the recipient is expected to delete
/// everything in the room matching the `ts` range and the `users` filter. A cache that only
/// handles [`StreamEvent::MessageDeleted`] will silently drift out of date after a prune.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkDelete {
    /// Room the deletion applies to.
    pub rid: RoomId,
    /// Message ids, when the server chose to enumerate them. Takes priority over
    /// [`ts`](Self::ts).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<MessageId>>,
    /// Mongo range query over `ts`, keyed by operator (`$gt`, `$lt`, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts: Option<BTreeMap<String, Timestamp>>,
    /// Restrict the deletion to messages by these usernames.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub users: Option<Vec<String>>,
    /// Whether pinned messages survive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_pinned: Option<bool>,
    /// Whether messages that started a discussion survive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_discussion: Option<bool>,
    /// Whether the deleted messages leave `rm` tombstones behind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub show_deleted_status: Option<bool>,
    /// Whether only messages carrying a file are affected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_only: Option<bool>,
}

/// A desktop notification pushed to one user on `notify-user` / `<uid>/notification`.
///
/// This is the *server's* decision that the user should be alerted — it has already applied
/// the room's and the user's notification preferences, mute state, and mention rules. For a
/// bot it is the cheapest possible "was I mentioned?" signal: no client-side filtering, and
/// it fires for direct messages too.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DesktopNotification {
    /// Notification title, usually the room name. Can be an empty string.
    pub title: String,
    /// Notification body, already rendered to plain text.
    pub text: String,
    /// Icon URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// How long the client should display it, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<i64>,
    /// What the notification is about.
    pub payload: NotificationPayload,
}

/// The `payload` of a [`DesktopNotification`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotificationPayload {
    /// Id of the message that triggered the notification.
    ///
    /// **May be an empty string.** The builder seeds `_id`, `rid` and `tmid` with `''` and
    /// only overwrites them when the source object has an `_id`, which omnichannel
    /// notifications do not (`lib/notifications/message/desktop.ts:44`).
    #[serde(rename = "_id")]
    pub id: MessageId,
    /// Room the message was posted in. Same empty-string caveat as [`id`](Self::id).
    pub rid: RoomId,
    /// Thread the message belongs to, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmid: Option<MessageId>,
    /// Author stub.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<UserRef>,
    /// Kind of the room.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub room_type: Option<RoomType>,
    /// Display name of the room as the recipient sees it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// A cut-down copy of the message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<NotificationMessage>,
    /// Sound the client should play.
    #[serde(rename = "audioNotificationValue", default, skip_serializing_if = "Option::is_none")]
    pub audio_notification_value: Option<String>,
}

/// The message excerpt embedded in a [`NotificationPayload`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NotificationMessage {
    /// Message body. Empty for a message that has none, such as a bare file upload.
    #[serde(default)]
    pub msg: String,
    /// System-message discriminator, when the notification is about one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t: Option<MessageType>,
    /// End-to-end ciphertext, when the room is encrypted. Not modelled further.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<Value>,
}

/// The payload of `notify-user` / `<uid>/userData`.
///
/// Modelled flat rather than as a tagged enum on purpose: an internally-tagged enum rejects a
/// `type` it has never seen, which would send a whole event to [`StreamEvent::Unknown`] just
/// because Rocket.Chat added a fourth verb.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserDataEvent {
    /// The user the change applies to. Typed `unknown` upstream; it is the uid on every emit
    /// path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<UserId>,
    /// What happened.
    #[serde(rename = "type")]
    pub action: ClientAction,
    /// The full document, on `inserted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Box<User>>,
    /// The changed fields, on `updated`.
    ///
    /// A **partial** user document — typically `{status, statusText}` — which is why it is a
    /// raw map and not a [`User`]: it has no `_id` and would not deserialize as one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<Map<String, Value>>,
    /// The fields cleared by this update, as `{field: 1}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unset: Option<Map<String, Value>>,
}

/// The subscription carried by `notify-user` / `<uid>/subscriptions-changed`.
///
/// `streams.ts` declares a reduced document for the `removed` action and a large `Pick<…>` of
/// the real one otherwise. In practice every emit path sends the whole document, including on
/// removal, because the removal helpers forward what `findOneAndDelete` returned. Both are
/// accepted; [`Full`](Self::Full) is tried first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum SubscriptionChange {
    /// A complete subscription document.
    Full(Box<Subscription>),
    /// The reduced `{_id, u?, rid?, t?}` form.
    Stub(SubscriptionStub),
}

impl SubscriptionChange {
    /// The subscription's id, whichever form arrived.
    #[must_use]
    pub fn id(&self) -> &SubscriptionId {
        match self {
            Self::Full(subscription) => &subscription.id,
            Self::Stub(stub) => &stub.id,
        }
    }

    /// The room, when the payload named one.
    #[must_use]
    pub fn room(&self) -> Option<&RoomId> {
        match self {
            Self::Full(subscription) => Some(&subscription.rid),
            Self::Stub(stub) => stub.rid.as_ref(),
        }
    }

    /// The full document, when one arrived. `None` only for the reduced form.
    #[must_use]
    pub fn full(&self) -> Option<&Subscription> {
        match self {
            Self::Full(subscription) => Some(subscription),
            Self::Stub(_) => None,
        }
    }
}

/// The reduced subscription document `streams.ts` declares for a removal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionStub {
    /// Subscription id.
    #[serde(rename = "_id")]
    pub id: SubscriptionId,
    /// The room, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rid: Option<RoomId>,
    /// The subscribing user, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub u: Option<UserRef>,
    /// Kind of the room, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t: Option<RoomType>,
}

/// The payload of `notify-logged` / `roles-change`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleChange {
    /// Whether the role was granted, revoked, or itself edited.
    #[serde(rename = "type")]
    pub action: RoleChangeKind,
    /// The role.
    #[serde(rename = "_id")]
    pub id: RoleId,
    /// The user the role was granted to or revoked from. Absent when the role document
    /// itself changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub u: Option<UserRef>,
    /// Room id, for a room-scoped role such as `owner` or `moderator`. Absent for a global
    /// role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// A presence change, from `notify-logged` / `user-status`.
///
/// Built positionally out of a **nested** one-element array — the event's `args` is
/// `[[uid, username, status, …]]`, not `[uid, username, status, …]`.
///
/// The tuple has grown twice: 3 elements originally, 6 from the status-text release, 8 once
/// `statusSource` and `statusExpiresAt` were added. Every field after the status code is
/// therefore optional, and older servers in the support window still send the short form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserStatusChange {
    /// Whose presence changed.
    pub user: UserId,
    /// Their login name. Position 1; the server refuses to emit without one, but the slot can
    /// still arrive as a `null` hole.
    pub username: Option<String>,
    /// The new presence, as the numeric code the presence service uses.
    ///
    /// Code `0` is ambiguous — it means both "offline" and "account deactivated". See
    /// [`PresenceStatus`].
    pub status: PresenceStatus,
    /// Free-text status. Position 3; absent before the 6-element form.
    pub status_text: Option<String>,
    /// Display name. Position 4.
    pub name: Option<String>,
    /// Global roles. Position 5. Empty when the slot is absent, `null`, or not an array.
    pub roles: Vec<RoleId>,
    /// Where the status came from. Position 6; absent before the 8-element form.
    pub status_source: Option<PresenceSource>,
    /// When a temporary status lapses. Position 7; absent before the 8-element form.
    pub status_expires_at: Option<Timestamp>,
}

// ---------------------------------------------------------------------------------------
// The event enum
// ---------------------------------------------------------------------------------------

/// A decoded Rocket.Chat stream event.
///
/// Produced by [`StreamEvent::decode`], which never fails. See the [module docs](self) for
/// the leniency rules and for where the declared types and the emit site disagree.
///
/// Payloads that are large or that a caller usually ignores are boxed, so moving a
/// `StreamEvent` costs the same whatever variant it holds.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum StreamEvent {
    /// `stream-room-messages`, keyed by room id: a message was created **or changed**.
    ///
    /// Rocket.Chat re-broadcasts the whole document on any mutation — reactions, pins, thread
    /// counts, link previews — so this is not "a new message". [`Message::is_edited`] and a
    /// seen-set of message ids are what distinguish the cases; `PLAN.md` §6.4 has the full
    /// rule.
    RoomMessage {
        /// Room the message belongs to, taken from the event key.
        room: RoomId,
        /// The message document.
        message: Box<Message>,
    },

    /// `stream-room-messages` / `__my_messages__`: every message the subscriber may read,
    /// over a single subscription.
    ///
    /// The one event a bot should subscribe to instead of one `RoomMessage` subscription per
    /// room. Note [`meta`](Self::MyMessage::meta): this fires for rooms the bot is *allowed*
    /// to read, not only rooms it has joined.
    MyMessage {
        /// The message document.
        message: Box<Message>,
        /// The per-recipient trailing element the declared type does not show.
        meta: MyMessageMeta,
    },

    /// `stream-notify-room` / `<rid>/user-activity`: typing, recording or uploading.
    ///
    /// An **empty** [`activities`](Self::UserActivity::activities) is the stop signal.
    UserActivity {
        /// Room the activity is happening in.
        room: RoomId,
        /// Display name of the user — the name the workspace shows, which is the real name
        /// when `UI_Use_Real_Name` is on and the username otherwise. It is not an id, and
        /// matching it against a `UserId` will not work.
        user: String,
        /// What they are doing. Empty means they stopped.
        activities: Vec<Activity>,
    },

    /// `stream-notify-room` / `<rid>/typing`: the pre-`user-activity` typing indicator.
    ///
    /// Superseded by [`UserActivity`](Self::UserActivity) but still declared upstream, and
    /// still emitted by older clients and by integrations that were written against it.
    Typing {
        /// Room the user is typing in.
        room: RoomId,
        /// Display name of the user. Same caveat as [`UserActivity::user`](Self::UserActivity).
        user: String,
        /// `true` to start, `false` to stop.
        typing: bool,
    },

    /// `stream-notify-room` / `<rid>/deleteMessage`: one message was removed.
    ///
    /// **This is the only reliable delete signal.** With `Message_ShowDeletedStatus` off — the
    /// default — a deletion produces no `stream-room-messages` frame at all, so a bot that
    /// only watches messages never learns about it.
    MessageDeleted {
        /// Room the message was in.
        room: RoomId,
        /// The deleted message.
        message: MessageId,
    },

    /// `stream-notify-room` / `<rid>/deleteMessageBulk`: a prune or a user erasure.
    MessagesDeletedBulk {
        /// Room the deletion applies to, taken from the event key. May differ from
        /// [`BulkDelete::rid`] only if the server is misbehaving.
        room: RoomId,
        /// What to delete. Usually a query rather than a list of ids.
        bulk: Box<BulkDelete>,
    },

    /// `stream-notify-user` / `<uid>/message`: an ephemeral message addressed to one user.
    ///
    /// Not persisted and not visible to anyone else — this is how slash-command errors and
    /// `rocket.cat` replies are delivered. It never appears on `room-messages`.
    EphemeralMessage {
        /// Recipient, taken from the event key.
        user: UserId,
        /// The message. Its `rid` names the room it should be shown in.
        message: Box<Message>,
    },

    /// `stream-notify-user` / `<uid>/notification`: the server decided this user should be
    /// alerted.
    Notification {
        /// Recipient, taken from the event key.
        user: UserId,
        /// The notification.
        notification: Box<DesktopNotification>,
    },

    /// `stream-notify-user` / `<uid>/rooms-changed`: room metadata this user can see changed.
    RoomsChanged {
        /// Recipient, taken from the event key.
        user: UserId,
        /// What happened to the room.
        action: ClientAction,
        /// The room document.
        room: Box<Room>,
    },

    /// `stream-notify-user` / `<uid>/subscriptions-changed`: the user joined, left, or their
    /// unread state moved.
    ///
    /// The event that drives per-room subscriptions: a bot subscribes to
    /// `<rid>/user-activity` and `<rid>/deleteMessage` off the back of this.
    SubscriptionsChanged {
        /// Recipient, taken from the event key.
        user: UserId,
        /// What happened to the subscription.
        action: ClientAction,
        /// The subscription document, full or reduced.
        subscription: Box<SubscriptionChange>,
    },

    /// `stream-notify-user` / `<uid>/userData`: the user's own document changed.
    UserData {
        /// Recipient, taken from the event key.
        user: UserId,
        /// The change.
        event: Box<UserDataEvent>,
    },

    /// `stream-notify-user` / `<uid>/force_logout`: the server is about to invalidate this
    /// session.
    ///
    /// The only in-band warning before the socket dies. Worth handling: it distinguishes "the
    /// token was revoked, stop reconnecting" from an ordinary disconnect.
    ForceLogout {
        /// Recipient, taken from the event key.
        user: UserId,
        /// The session being terminated. Declared as possibly `undefined`, which arrives as a
        /// `null` hole and reads as `None`.
        session: Option<String>,
    },

    /// `stream-notify-logged` / `user-status`: someone's presence changed.
    UserStatusChanged(Box<UserStatusChange>),

    /// `stream-notify-logged` / `Users:NameChanged`: a display name or username changed.
    ///
    /// Both fields are optional because the payload is `Pick<IUser, '_id'|'name'|'username'>`
    /// and `IUser` makes both of those optional at source.
    UserNameChanged {
        /// The user.
        user: UserId,
        /// New login name.
        username: Option<String>,
        /// New display name.
        name: Option<String>,
    },

    /// `stream-notify-logged` / `roles-change`: a role was granted, revoked, or edited.
    RolesChanged(Box<RoleChange>),

    /// Anything this crate does not type: an unknown stream, an unknown event key, or a known
    /// key whose arguments did not decode.
    ///
    /// **This variant is load-bearing, not a placeholder.** It is what makes decoding
    /// infallible, and it is the escape hatch that lets a bot handle an event newer than the
    /// crate. The arguments are the raw array, unmodified — including any `null` holes.
    Unknown {
        /// Stream name exactly as it was passed to [`decode`](Self::decode), including the
        /// `stream-` prefix if the caller supplied one.
        stream: String,
        /// The `eventName` field, uninterpreted — composite keys are **not** split here.
        event: String,
        /// The positional arguments, verbatim.
        args: Vec<Value>,
    },
}

impl StreamEvent {
    /// Decodes a stream event. Never fails.
    ///
    /// `stream` is accepted with or without the `stream-` prefix, since callers get it from
    /// the frame's `collection` (`"stream-notify-room"`) or from
    /// [`protocol::StreamEvent::stream`] (`"notify-room"`) depending on where they sit.
    ///
    /// Anything not recognised becomes [`Unknown`](Self::Unknown) carrying the inputs
    /// untouched. See the [module docs](self) for the rules.
    #[must_use]
    pub fn decode(stream: &str, event_name: &str, args: &[Value]) -> Self {
        Self::try_decode(stream, event_name, args).unwrap_or_else(|| Self::Unknown {
            stream: stream.to_owned(),
            event: event_name.to_owned(),
            args: args.to_vec(),
        })
    }

    /// Decodes the borrowed view produced by [`protocol::ServerMessage::as_stream_event`].
    #[must_use]
    pub fn decode_raw(raw: protocol::StreamEvent<'_>) -> Self {
        Self::decode(raw.stream, raw.event_name, raw.args)
    }

    /// Whether this event fell through to [`Unknown`](Self::Unknown).
    #[must_use]
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. })
    }

    /// The room this event concerns, where it names one.
    ///
    /// `None` does not mean "no room": [`MyMessage`](Self::MyMessage) carries its room inside
    /// the message document rather than in the event key, and a `notify-user` event names a
    /// user instead.
    #[must_use]
    pub fn room(&self) -> Option<&RoomId> {
        match self {
            Self::RoomMessage { room, .. }
            | Self::UserActivity { room, .. }
            | Self::Typing { room, .. }
            | Self::MessageDeleted { room, .. }
            | Self::MessagesDeletedBulk { room, .. } => Some(room),
            Self::MyMessage { message, .. } | Self::EphemeralMessage { message, .. } => {
                Some(&message.rid)
            }
            _ => None,
        }
    }

    /// The decode attempt. `None` means "fall back to `Unknown`".
    fn try_decode(stream: &str, event_name: &str, args: &[Value]) -> Option<Self> {
        match stream.strip_prefix("stream-").unwrap_or(stream) {
            "room-messages" => Self::room_messages(event_name, args),
            "notify-room" => Self::notify_room(event_name, args),
            "notify-user" => Self::notify_user(event_name, args),
            "notify-logged" => Self::notify_logged(event_name, args),
            _ => None,
        }
    }

    fn room_messages(event_name: &str, args: &[Value]) -> Option<Self> {
        if event_name == "__my_messages__" {
            return Some(Self::MyMessage {
                message: Box::new(req(args, 0)?),
                // Deliberately lenient: the trailing element is an authorization side-band,
                // not the payload. If its shape ever changes, losing the message would be a
                // far worse outcome than losing the annotation.
                meta: opt(args, 1).flatten().unwrap_or_default(),
            });
        }
        // Every other key on this stream is a bare room id.
        if event_name.is_empty() {
            return None;
        }
        Some(Self::RoomMessage {
            room: RoomId::new(event_name),
            message: Box::new(req(args, 0)?),
        })
    }

    fn notify_room(event_name: &str, args: &[Value]) -> Option<Self> {
        let (room, key) = split_key(event_name)?;
        let room = RoomId::new(room);
        match key {
            "user-activity" => Some(Self::UserActivity {
                room,
                user: str_at(args, 0)?.to_owned(),
                activities: strings_at(args, 1)?.into_iter().map(Activity::from).collect(),
            }),
            "typing" => Some(Self::Typing {
                room,
                user: str_at(args, 0)?.to_owned(),
                typing: args.get(1)?.as_bool()?,
            }),
            "deleteMessage" => {
                let deleted: DeletedMessage = req(args, 0)?;
                Some(Self::MessageDeleted { room, message: deleted.id })
            }
            "deleteMessageBulk" => {
                Some(Self::MessagesDeletedBulk { room, bulk: Box::new(req(args, 0)?) })
            }
            _ => None,
        }
    }

    fn notify_user(event_name: &str, args: &[Value]) -> Option<Self> {
        let (user, key) = split_key(event_name)?;
        let user = UserId::new(user);
        match key {
            "message" => {
                Some(Self::EphemeralMessage { user, message: Box::new(req(args, 0)?) })
            }
            "notification" => {
                Some(Self::Notification { user, notification: Box::new(req(args, 0)?) })
            }
            "rooms-changed" => Some(Self::RoomsChanged {
                user,
                action: req(args, 0)?,
                room: Box::new(req(args, 1)?),
            }),
            "subscriptions-changed" => Some(Self::SubscriptionsChanged {
                user,
                action: req(args, 0)?,
                subscription: Box::new(req(args, 1)?),
            }),
            "userData" => Some(Self::UserData { user, event: Box::new(req(args, 0)?) }),
            // `ISession['sessionId'] | undefined`: the absent case is a genuine value here,
            // not a decode failure.
            "force_logout" => Some(Self::ForceLogout { user, session: opt(args, 0)? }),
            _ => None,
        }
    }

    fn notify_logged(event_name: &str, args: &[Value]) -> Option<Self> {
        match event_name {
            "user-status" => {
                // Note the nesting: args is `[[uid, username, status, …]]`.
                let tuple = args.first()?.as_array()?;
                let user = str_at(tuple, 0)?;
                if user.is_empty() {
                    return None;
                }
                Some(Self::UserStatusChanged(Box::new(UserStatusChange {
                    user: UserId::new(user),
                    username: opt(tuple, 1)?,
                    status: req(tuple, 2)?,
                    status_text: opt(tuple, 3)?,
                    name: opt(tuple, 4)?,
                    roles: strings_at(tuple, 5)?.into_iter().map(RoleId::new).collect(),
                    status_source: opt(tuple, 6)?,
                    status_expires_at: opt(tuple, 7)?,
                })))
            }
            "Users:NameChanged" => {
                let changed: NameChanged = req(args, 0)?;
                Some(Self::UserNameChanged {
                    user: changed.id,
                    username: changed.username,
                    name: changed.name,
                })
            }
            "roles-change" => Some(Self::RolesChanged(Box::new(req(args, 0)?))),
            _ => None,
        }
    }
}

impl From<protocol::StreamEvent<'_>> for StreamEvent {
    fn from(raw: protocol::StreamEvent<'_>) -> Self {
        Self::decode_raw(raw)
    }
}

// ---------------------------------------------------------------------------------------
// Positional decoding helpers
// ---------------------------------------------------------------------------------------

/// `notify-room` / `<rid>/deleteMessage` carries `[{_id}]` and nothing else.
#[derive(Deserialize)]
struct DeletedMessage {
    #[serde(rename = "_id")]
    id: MessageId,
}

/// `notify-logged` / `Users:NameChanged` carries `Pick<IUser, '_id' | 'name' | 'username'>`.
#[derive(Deserialize)]
struct NameChanged {
    #[serde(rename = "_id")]
    id: UserId,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

/// Splits a composite event key on its **first** `/`.
///
/// First, not last: the prefix is an id, which never contains a slash, while suffixes do —
/// `e2e.keyRequest` is one token but `call.hangup` neighbours keys that are not. This is what
/// the server does too (`const [rid, e] = eventName.split('/')`). An empty prefix is rejected;
/// an empty suffix is left to the caller's `match`, which will not recognise it.
fn split_key(event_name: &str) -> Option<(&str, &str)> {
    let (prefix, suffix) = event_name.split_once('/')?;
    if prefix.is_empty() { None } else { Some((prefix, suffix)) }
}

/// Reads an optional positional argument.
///
/// Three outcomes, and the distinction is the whole point:
///
/// - `Some(Some(value))` — present and decoded.
/// - `Some(None)` — absent, or an EJSON `null` hole. Not an error.
/// - `None` — present but the wrong type. The caller must fall back to `Unknown`.
fn opt<T: DeserializeOwned>(args: &[Value], index: usize) -> Option<Option<T>> {
    match args.get(index) {
        None | Some(Value::Null) => Some(None),
        Some(value) => T::deserialize(value).ok().map(Some),
    }
}

/// Reads a required positional argument. Absent, `null` and wrong-typed all mean `None`.
fn req<T: DeserializeOwned>(args: &[Value], index: usize) -> Option<T> {
    opt(args, index).flatten()
}

/// Reads a required positional string argument.
fn str_at(args: &[Value], index: usize) -> Option<&str> {
    args.get(index)?.as_str()
}

/// Reads an optional array-of-strings argument, tolerantly.
///
/// Absent or `null` yields an empty list; `null` and non-string *elements* are skipped rather
/// than failing the whole event, because `undefined` inside an array is exactly what EJSON
/// encodes as `null` and a single hole should not cost the caller a typing indicator. Only a
/// value that is present and is not an array is a decode failure.
fn strings_at(args: &[Value], index: usize) -> Option<Vec<String>> {
    match args.get(index) {
        None | Some(Value::Null) => Some(Vec::new()),
        Some(Value::Array(items)) => {
            Some(items.iter().filter_map(|item| Some(item.as_str()?.to_owned())).collect())
        }
        Some(_) => None,
    }
}

// ---------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A message document with the fields a `stream-room-messages` frame actually carries.
    ///
    /// Shaped after a real frame: `md` (the parsed message AST) and the empty `urls` /
    /// `mentions` / `channels` arrays are always present and are exactly the kind of field a
    /// stricter model would choke on.
    fn message() -> Value {
        json!({
            "_id": "7aDSXtjMA3KPLxLjt",
            "rid": "GENERAL",
            "msg": "hello there",
            "ts": {"$date": 1_755_518_400_000_i64},
            "_updatedAt": {"$date": 1_755_518_400_000_i64},
            "u": {"_id": "rocket.cat", "username": "rocket.cat", "name": "Rocket.Cat"},
            "urls": [],
            "mentions": [],
            "channels": [],
            "md": [{"type": "PARAGRAPH", "value": [{"type": "PLAIN_TEXT", "value": "hello there"}]}]
        })
    }

    fn args(values: &[Value]) -> Vec<Value> {
        values.to_vec()
    }

    // -----------------------------------------------------------------------------------
    // room-messages
    // -----------------------------------------------------------------------------------

    #[test]
    fn a_room_message_takes_its_room_from_the_event_key() {
        let event = StreamEvent::decode("stream-room-messages", "GENERAL", &args(&[message()]));
        let StreamEvent::RoomMessage { room, message } = event else {
            panic!("expected RoomMessage, got {event:?}");
        };
        assert_eq!(room.as_str(), "GENERAL");
        assert_eq!(message.msg, "hello there");
        assert_eq!(message.u.username.as_deref(), Some("rocket.cat"));
    }

    #[test]
    fn the_stream_prefix_is_optional() {
        let with = StreamEvent::decode("stream-room-messages", "GENERAL", &args(&[message()]));
        let without = StreamEvent::decode("room-messages", "GENERAL", &args(&[message()]));
        assert_eq!(with, without);
        assert!(!with.is_unknown());
    }

    /// The divergence that motivates the whole module: `streams.ts` declares `[IMessage]`,
    /// the emit site sends `[...args, allowed]`.
    #[test]
    fn my_messages_carries_the_trailing_element_the_declared_type_does_not_show() {
        assert_eq!(
            catalog::event("room-messages", "__my_messages__").map(|spec| spec.arities),
            Some(&[1_usize][..]),
            "streams.ts still declares a single argument",
        );

        let event = StreamEvent::decode(
            "stream-room-messages",
            "__my_messages__",
            &args(&[
                message(),
                json!({"roomParticipant": true, "roomType": "c", "roomName": "general"}),
            ]),
        );
        let StreamEvent::MyMessage { message, meta } = event else {
            panic!("expected MyMessage, got {event:?}");
        };
        assert_eq!(message.rid.as_str(), "GENERAL");
        assert_eq!(meta.room_participant, Some(true));
        assert_eq!(meta.room_type, Some(RoomType::Channel));
        assert_eq!(meta.room_name.as_deref(), Some("general"));
    }

    #[test]
    fn my_messages_survives_a_missing_or_reshaped_trailing_element() {
        // The declared arity, from a server old enough not to append it.
        let bare = StreamEvent::decode("stream-room-messages", "__my_messages__", &args(&[message()]));
        assert!(matches!(&bare, StreamEvent::MyMessage { meta, .. } if *meta == MyMessageMeta::default()));

        // The authorization hook is documented as returning `true` on some paths. Losing the
        // annotation is acceptable; losing the message is not.
        let reshaped = StreamEvent::decode(
            "stream-room-messages",
            "__my_messages__",
            &args(&[message(), json!(true)]),
        );
        let StreamEvent::MyMessage { message, meta } = reshaped else {
            panic!("a reshaped trailing element must not cost us the message");
        };
        assert_eq!(message.id.as_str(), "7aDSXtjMA3KPLxLjt");
        assert_eq!(meta, MyMessageMeta::default());
    }

    #[test]
    fn a_direct_message_room_has_no_room_name() {
        let event = StreamEvent::decode(
            "stream-room-messages",
            "__my_messages__",
            &args(&[message(), json!({"roomParticipant": true, "roomType": "d"})]),
        );
        let StreamEvent::MyMessage { meta, .. } = event else { panic!("expected MyMessage") };
        assert_eq!(meta.room_type, Some(RoomType::Direct));
        assert_eq!(meta.room_name, None);
    }

    // -----------------------------------------------------------------------------------
    // notify-room
    // -----------------------------------------------------------------------------------

    #[test]
    fn typing_starts_with_an_activity_and_stops_with_an_empty_list() {
        let start = StreamEvent::decode(
            "stream-notify-room",
            "GENERAL/user-activity",
            &args(&[json!("Rocket.Cat"), json!(["user-typing"])]),
        );
        let StreamEvent::UserActivity { room, user, activities } = start else {
            panic!("expected UserActivity");
        };
        assert_eq!(room.as_str(), "GENERAL");
        assert_eq!(user, "Rocket.Cat");
        assert_eq!(activities, vec![Activity::UserTyping]);

        // There is no stop event; the server re-sends the same key with an empty list.
        let stop = StreamEvent::decode(
            "stream-notify-room",
            "GENERAL/user-activity",
            &args(&[json!("Rocket.Cat"), json!([])]),
        );
        let StreamEvent::UserActivity { activities, .. } = stop else {
            panic!("expected UserActivity");
        };
        assert!(activities.is_empty(), "an empty activity list is the stop signal");
    }

    #[test]
    fn an_unknown_activity_round_trips_instead_of_failing() {
        let event = StreamEvent::decode(
            "stream-notify-room",
            "r1/user-activity",
            &args(&[json!("bot"), json!(["user-typing", "user-thinking", null, 7])]),
        );
        let StreamEvent::UserActivity { activities, .. } = event else {
            panic!("expected UserActivity");
        };
        assert_eq!(
            activities,
            vec![Activity::UserTyping, Activity::Unknown("user-thinking".to_owned())],
            "unknown strings survive; null holes and non-strings are skipped",
        );
    }

    /// The client-to-server direction sends a third element the server type does not declare.
    #[test]
    fn extra_trailing_arguments_are_ignored() {
        let event = StreamEvent::decode(
            "stream-notify-room",
            "GENERAL/user-activity",
            &args(&[json!("bot"), json!(["user-typing"]), json!({"extras": 1}), json!("future")]),
        );
        assert!(matches!(event, StreamEvent::UserActivity { .. }));
    }

    #[test]
    fn the_legacy_typing_key_still_decodes() {
        let event = StreamEvent::decode(
            "stream-notify-room",
            "GENERAL/typing",
            &args(&[json!("rocket.cat"), json!(true)]),
        );
        assert_eq!(
            event,
            StreamEvent::Typing {
                room: RoomId::new("GENERAL"),
                user: "rocket.cat".to_owned(),
                typing: true,
            }
        );
    }

    #[test]
    fn a_message_deletion_names_the_room_and_the_message() {
        let event = StreamEvent::decode(
            "stream-notify-room",
            "GENERAL/deleteMessage",
            &args(&[json!({"_id": "7aDSXtjMA3KPLxLjt"})]),
        );
        assert_eq!(
            event,
            StreamEvent::MessageDeleted {
                room: RoomId::new("GENERAL"),
                message: MessageId::new("7aDSXtjMA3KPLxLjt"),
            }
        );
    }

    #[test]
    fn a_bulk_deletion_is_a_query_not_a_list() {
        let event = StreamEvent::decode(
            "stream-notify-room",
            "GENERAL/deleteMessageBulk",
            &args(&[json!({
                "rid": "GENERAL",
                "excludePinned": false,
                "ignoreDiscussion": true,
                "ts": {"$gt": {"$date": 1_700_000_000_000_i64}, "$lt": {"$date": 1_755_518_400_000_i64}},
                "users": [],
                "filesOnly": false
            })]),
        );
        let StreamEvent::MessagesDeletedBulk { room, bulk } = event else {
            panic!("expected MessagesDeletedBulk");
        };
        assert_eq!(room.as_str(), "GENERAL");
        assert_eq!(bulk.rid.as_str(), "GENERAL");
        assert_eq!(bulk.ids, None, "no id list — the recipient must run the query");
        let ts = bulk.ts.expect("range present");
        assert_eq!(ts.len(), 2);
        assert_eq!(ts["$gt"].unix_millis(), 1_700_000_000_000);
        assert_eq!(bulk.exclude_pinned, Some(false));
        assert_eq!(bulk.ignore_discussion, Some(true));
    }

    // -----------------------------------------------------------------------------------
    // notify-user
    // -----------------------------------------------------------------------------------

    #[test]
    fn an_ephemeral_message_is_addressed_to_a_uid_not_a_room() {
        let event = StreamEvent::decode(
            "stream-notify-user",
            "YHz2Xn9aEqSDkrgLM/message",
            &args(&[message()]),
        );
        let StreamEvent::EphemeralMessage { user, message } = event else {
            panic!("expected EphemeralMessage");
        };
        assert_eq!(user.as_str(), "YHz2Xn9aEqSDkrgLM");
        assert_eq!(message.rid.as_str(), "GENERAL", "the room lives in the document");
    }

    #[test]
    fn a_desktop_notification_decodes_with_the_empty_ids_omnichannel_leaves_behind() {
        let event = StreamEvent::decode(
            "stream-notify-user",
            "uid1/notification",
            &args(&[json!({
                "title": "#general",
                "text": "rocket.cat: hello there",
                "payload": {
                    "_id": "",
                    "rid": "",
                    "tmid": "",
                    "sender": {"_id": "rocket.cat", "username": "rocket.cat"},
                    "type": "c",
                    "message": {"msg": "hello there"},
                    "name": "general"
                }
            })]),
        );
        let StreamEvent::Notification { user, notification } = event else {
            panic!("expected Notification");
        };
        assert_eq!(user.as_str(), "uid1");
        assert_eq!(notification.title, "#general");
        assert_eq!(notification.payload.id.as_str(), "", "omnichannel leaves these empty");
        assert_eq!(notification.payload.room_type, Some(RoomType::Channel));
        assert_eq!(
            notification.payload.message.and_then(|m| m.t),
            None,
            "an ordinary user message has no system type",
        );
    }

    #[test]
    fn rooms_changed_carries_a_verb_and_a_room() {
        let event = StreamEvent::decode(
            "stream-notify-user",
            "uid1/rooms-changed",
            &args(&[
                json!("updated"),
                json!({
                    "_id": "GENERAL",
                    "_updatedAt": {"$date": 1_755_518_400_000_i64},
                    "t": "c",
                    "name": "general",
                    "msgs": 42
                }),
            ]),
        );
        let StreamEvent::RoomsChanged { user, action, room } = event else {
            panic!("expected RoomsChanged");
        };
        assert_eq!(user.as_str(), "uid1");
        assert_eq!(action, ClientAction::Updated);
        assert_eq!(room.name.as_deref(), Some("general"));
    }

    fn subscription_doc() -> Value {
        json!({
            "_id": "5v9NNcvVXvSFvBJ9y",
            "_updatedAt": {"$date": 1_755_518_400_000_i64},
            "rid": "GENERAL",
            "u": {"_id": "uid1", "username": "bot"},
            "t": "c",
            "ts": {"$date": 1_755_000_000_000_i64},
            "name": "general",
            "open": true,
            "unread": 3,
            "userMentions": 1,
            "groupMentions": 0,
            "alert": true
        })
    }

    #[test]
    fn subscriptions_changed_accepts_the_full_document() {
        let event = StreamEvent::decode(
            "stream-notify-user",
            "uid1/subscriptions-changed",
            &args(&[json!("updated"), subscription_doc()]),
        );
        let StreamEvent::SubscriptionsChanged { action, subscription, .. } = event else {
            panic!("expected SubscriptionsChanged");
        };
        assert_eq!(action, ClientAction::Updated);
        assert_eq!(subscription.id().as_str(), "5v9NNcvVXvSFvBJ9y");
        assert_eq!(subscription.room().map(RoomId::as_str), Some("GENERAL"));
        assert_eq!(subscription.full().map(|s| s.unread), Some(3));
    }

    /// `streams.ts` declares a reduced document for `removed`; every emit path sends the whole
    /// one. Both have to work, and the full form must not be mistaken for the reduced one.
    #[test]
    fn subscriptions_changed_accepts_the_reduced_removal_document() {
        let removal = StreamEvent::decode(
            "stream-notify-user",
            "uid1/subscriptions-changed",
            &args(&[
                json!("removed"),
                json!({"_id": "5v9NNcvVXvSFvBJ9y", "rid": "GENERAL", "t": "c",
                       "u": {"_id": "uid1", "username": "bot"}}),
            ]),
        );
        let StreamEvent::SubscriptionsChanged { action, subscription, .. } = removal else {
            panic!("a removal must not be dropped — it is how a bot learns it left a room");
        };
        assert_eq!(action, ClientAction::Removed);
        assert!(matches!(*subscription, SubscriptionChange::Stub(_)));
        assert_eq!(subscription.room().map(RoomId::as_str), Some("GENERAL"));
        assert!(subscription.full().is_none());

        let full = StreamEvent::decode(
            "stream-notify-user",
            "uid1/subscriptions-changed",
            &args(&[json!("removed"), subscription_doc()]),
        );
        let StreamEvent::SubscriptionsChanged { subscription, .. } = full else {
            panic!("expected SubscriptionsChanged");
        };
        assert!(matches!(*subscription, SubscriptionChange::Full(_)), "prefer the full form");
    }

    #[test]
    fn user_data_updates_carry_a_partial_document() {
        let event = StreamEvent::decode(
            "stream-notify-user",
            "uid1/userData",
            &args(&[json!({
                "type": "updated",
                "id": "uid1",
                "diff": {"status": "away", "statusText": "lunch"},
                "unset": {"statusSource": 1}
            })]),
        );
        let StreamEvent::UserData { user, event } = event else { panic!("expected UserData") };
        assert_eq!(user.as_str(), "uid1");
        assert_eq!(event.action, ClientAction::Updated);
        assert_eq!(event.data, None);
        assert_eq!(event.diff.as_ref().and_then(|d| d.get("status")), Some(&json!("away")));
        assert!(event.unset.is_some_and(|u| u.contains_key("statusSource")));
    }

    #[test]
    fn a_user_data_verb_this_crate_has_never_seen_does_not_lose_the_event() {
        let event = StreamEvent::decode(
            "stream-notify-user",
            "uid1/userData",
            &args(&[json!({"type": "reconciled", "id": "uid1"})]),
        );
        let StreamEvent::UserData { event, .. } = event else { panic!("expected UserData") };
        assert_eq!(event.action, ClientAction::Unknown("reconciled".to_owned()));
    }

    #[test]
    fn force_logout_treats_an_absent_session_as_a_value_not_an_error() {
        for payload in [args(&[]), args(&[Value::Null])] {
            let event = StreamEvent::decode("stream-notify-user", "uid1/force_logout", &payload);
            assert_eq!(
                event,
                StreamEvent::ForceLogout { user: UserId::new("uid1"), session: None },
                "`ISession['sessionId'] | undefined` — absent is legal",
            );
        }

        let event = StreamEvent::decode(
            "stream-notify-user",
            "uid1/force_logout",
            &args(&[json!("sess-1")]),
        );
        assert!(matches!(event, StreamEvent::ForceLogout { session: Some(s), .. } if s == "sess-1"));
    }

    // -----------------------------------------------------------------------------------
    // notify-logged
    // -----------------------------------------------------------------------------------

    /// The tuple grew 3 → 6 → 8 across releases, and it is nested one array deep.
    #[test]
    fn user_status_decodes_at_three_six_and_eight_elements() {
        let three = StreamEvent::decode(
            "stream-notify-logged",
            "user-status",
            &args(&[json!([["uid1", "john", 1]])[0].clone()]),
        );
        let StreamEvent::UserStatusChanged(status) = three else {
            panic!("3-element form must decode — servers in the support window still send it");
        };
        assert_eq!(status.user.as_str(), "uid1");
        assert_eq!(status.username.as_deref(), Some("john"));
        assert_eq!(status.status, PresenceStatus::Online);
        assert_eq!(status.status_text, None);
        assert!(status.roles.is_empty());
        assert_eq!(status.status_expires_at, None);

        let six = StreamEvent::decode(
            "stream-notify-logged",
            "user-status",
            &args(&[json!(["uid1", "john", 2, "Away for lunch", "John Doe", ["user", "bot"]])]),
        );
        let StreamEvent::UserStatusChanged(status) = six else { panic!("expected UserStatusChanged") };
        assert_eq!(status.status, PresenceStatus::Away);
        assert_eq!(status.status_text.as_deref(), Some("Away for lunch"));
        assert_eq!(status.name.as_deref(), Some("John Doe"));
        assert_eq!(status.roles, vec![RoleId::new("user"), RoleId::new("bot")]);
        assert_eq!(status.status_source, None);

        let eight = StreamEvent::decode(
            "stream-notify-logged",
            "user-status",
            &args(&[json!([
                "uid1", "john", 3, "In a meeting", "John Doe", ["user"], "manual",
                {"$date": 1_755_518_400_000_i64}
            ])]),
        );
        let StreamEvent::UserStatusChanged(status) = eight else {
            panic!("expected UserStatusChanged")
        };
        assert_eq!(status.status, PresenceStatus::Busy);
        assert_eq!(status.status_source, Some(PresenceSource::Manual));
        assert_eq!(status.status_expires_at.map(Timestamp::unix_millis), Some(1_755_518_400_000));
    }

    /// EJSON writes an `undefined` array element as `null`, so the tuple arrives with holes.
    #[test]
    fn user_status_tolerates_null_holes_in_the_middle_of_the_tuple() {
        let event = StreamEvent::decode(
            "stream-notify-logged",
            "user-status",
            &args(&[json!(["uid1", "john", 0, null, "John Doe", null, null, null])]),
        );
        let StreamEvent::UserStatusChanged(status) = event else {
            panic!("a null hole is absence, not a decode failure");
        };
        assert_eq!(status.status, PresenceStatus::Offline);
        assert_eq!(status.status_text, None);
        assert_eq!(status.name.as_deref(), Some("John Doe"));
        assert!(status.roles.is_empty());
        assert_eq!(status.status_source, None);
    }

    #[test]
    fn a_presence_code_this_crate_does_not_know_still_decodes() {
        let event = StreamEvent::decode(
            "stream-notify-logged",
            "user-status",
            &args(&[json!(["uid1", "john", 9])]),
        );
        let StreamEvent::UserStatusChanged(status) = event else {
            panic!("expected UserStatusChanged")
        };
        assert_eq!(status.status, PresenceStatus::Unknown(9));
    }

    #[test]
    fn user_status_without_the_nesting_is_unknown() {
        // `[uid, username, code]` rather than `[[uid, username, code]]`.
        let event = StreamEvent::decode(
            "stream-notify-logged",
            "user-status",
            &args(&[json!("uid1"), json!("john"), json!(1)]),
        );
        assert!(event.is_unknown(), "the payload is one nested array, not three arguments");
    }

    #[test]
    fn a_name_change_carries_only_the_three_projected_fields() {
        let event = StreamEvent::decode(
            "stream-notify-logged",
            "Users:NameChanged",
            &args(&[json!({"_id": "uid1", "name": "John Doe", "username": "john"})]),
        );
        assert_eq!(
            event,
            StreamEvent::UserNameChanged {
                user: UserId::new("uid1"),
                username: Some("john".to_owned()),
                name: Some("John Doe".to_owned()),
            }
        );

        // `IUser.name` and `IUser.username` are both optional at source.
        let sparse = StreamEvent::decode(
            "stream-notify-logged",
            "Users:NameChanged",
            &args(&[json!({"_id": "uid1"})]),
        );
        assert!(matches!(sparse, StreamEvent::UserNameChanged { username: None, name: None, .. }));
    }

    #[test]
    fn a_role_change_is_spelled_added_not_inserted() {
        let event = StreamEvent::decode(
            "stream-notify-logged",
            "roles-change",
            &args(&[json!({
                "type": "added",
                "_id": "owner",
                "u": {"_id": "uid1", "username": "john"},
                "scope": "GENERAL"
            })]),
        );
        let StreamEvent::RolesChanged(change) = event else { panic!("expected RolesChanged") };
        assert_eq!(change.action, RoleChangeKind::Added);
        assert_eq!(change.id.as_str(), "owner");
        assert_eq!(change.scope.as_deref(), Some("GENERAL"), "room-scoped role");
        assert_eq!(change.u.and_then(|u| u.username), Some("john".to_owned()));
    }

    // -----------------------------------------------------------------------------------
    // The fallback
    // -----------------------------------------------------------------------------------

    #[test]
    fn an_unknown_stream_keeps_everything() {
        let payload = args(&[json!({"appId": "abc", "status": "manually_enabled"})]);
        let event = StreamEvent::decode("stream-apps", "app/statusUpdate", &payload);
        assert_eq!(
            event,
            StreamEvent::Unknown {
                stream: "stream-apps".to_owned(),
                event: "app/statusUpdate".to_owned(),
                args: payload,
            },
            "the stream name is preserved verbatim, prefix included",
        );
    }

    #[test]
    fn an_unknown_key_on_a_known_stream_is_unknown_with_the_key_unsplit() {
        let payload = args(&[json!({"until": {"$date": 1_755_518_400_000_i64}})]);
        let event = StreamEvent::decode("stream-notify-room", "GENERAL/messagesRead", &payload);
        let StreamEvent::Unknown { stream, event: key, args } = event else {
            panic!("expected Unknown");
        };
        assert_eq!(stream, "stream-notify-room");
        assert_eq!(key, "GENERAL/messagesRead", "composite keys are not split in Unknown");
        assert_eq!(args, payload);
    }

    #[test]
    fn a_known_event_with_garbage_arguments_lands_in_unknown_without_panicking() {
        let cases: [(&str, &str, Vec<Value>); 6] = [
            // A number where the message document belongs.
            ("stream-room-messages", "GENERAL", args(&[json!(42)])),
            // A message missing `rid`, which the model requires.
            (
                "stream-room-messages",
                "__my_messages__",
                args(&[json!({"_id": "m1", "msg": "x"})]),
            ),
            // The username slot holds an object.
            (
                "stream-notify-room",
                "GENERAL/user-activity",
                args(&[json!({"nope": true}), json!(["user-typing"])]),
            ),
            // The activity list is not a list.
            (
                "stream-notify-room",
                "GENERAL/user-activity",
                args(&[json!("bot"), json!("user-typing")]),
            ),
            // A deletion with no `_id`.
            ("stream-notify-room", "GENERAL/deleteMessage", args(&[json!({})])),
            // A composite key with an empty prefix.
            ("stream-notify-user", "/message", args(&[message()])),
        ];

        for (stream, key, payload) in cases {
            let event = StreamEvent::decode(stream, key, &payload);
            assert!(
                event.is_unknown(),
                "{stream} {key} should have fallen back to Unknown, got {event:?}",
            );
            let StreamEvent::Unknown { args: kept, .. } = event else { unreachable!() };
            assert_eq!(kept, payload, "the raw arguments must survive the fallback");
        }
    }

    #[test]
    fn an_empty_argument_array_never_panics() {
        let streams = [
            ("stream-room-messages", "GENERAL"),
            ("stream-room-messages", "__my_messages__"),
            ("stream-notify-room", "GENERAL/user-activity"),
            ("stream-notify-room", "GENERAL/typing"),
            ("stream-notify-room", "GENERAL/deleteMessage"),
            ("stream-notify-room", "GENERAL/deleteMessageBulk"),
            ("stream-notify-user", "uid1/message"),
            ("stream-notify-user", "uid1/notification"),
            ("stream-notify-user", "uid1/rooms-changed"),
            ("stream-notify-user", "uid1/subscriptions-changed"),
            ("stream-notify-user", "uid1/userData"),
            ("stream-notify-logged", "user-status"),
            ("stream-notify-logged", "Users:NameChanged"),
            ("stream-notify-logged", "roles-change"),
            ("stream-notify-logged", "banner-changed"),
            ("stream-does-not-exist", ""),
            ("", ""),
        ];
        for (stream, key) in streams {
            let event = StreamEvent::decode(stream, key, &[]);
            assert!(
                event.is_unknown() || matches!(event, StreamEvent::ForceLogout { .. }),
                "{stream} {key} produced {event:?}",
            );
        }

        // `force_logout` is the one event whose only argument is legitimately absent.
        assert!(!StreamEvent::decode("stream-notify-user", "uid1/force_logout", &[]).is_unknown());
    }

    // -----------------------------------------------------------------------------------
    // Wiring to the protocol layer
    // -----------------------------------------------------------------------------------

    #[test]
    fn a_whole_ddp_frame_decodes_end_to_end() {
        let frame: protocol::ServerMessage = serde_json::from_str(
            r#"{"msg":"changed","collection":"stream-notify-room","id":"id",
                "fields":{"eventName":"GENERAL/user-activity",
                          "args":["Rocket.Cat",["user-typing"]]}}"#,
        )
        .expect("frame decodes");
        let raw = frame.as_stream_event().expect("it is a stream event");
        assert_eq!(raw.stream, "notify-room", "the protocol layer strips the prefix");

        let event = StreamEvent::from(raw);
        assert!(matches!(event, StreamEvent::UserActivity { .. }));
        assert_eq!(event, StreamEvent::decode_raw(raw));
        assert_eq!(event.room().map(RoomId::as_str), Some("GENERAL"));
    }

    #[test]
    fn room_reports_the_room_even_when_it_is_not_in_the_event_key() {
        let keyed = StreamEvent::decode("room-messages", "GENERAL", &args(&[message()]));
        let mine = StreamEvent::decode("room-messages", "__my_messages__", &args(&[message()]));
        assert_eq!(keyed.room().map(RoomId::as_str), Some("GENERAL"));
        assert_eq!(mine.room().map(RoomId::as_str), Some("GENERAL"), "taken from the document");
        assert_eq!(
            StreamEvent::decode("stream-notify-logged", "user-status", &args(&[json!(["u", "n", 1])]))
                .room(),
            None,
        );
    }

    // -----------------------------------------------------------------------------------
    // Drift against the pinned catalog
    // -----------------------------------------------------------------------------------

    /// Every `(stream, event key)` this module types, as a concrete key the catalog can match.
    const TYPED: &[(&str, &str)] = &[
        ("room-messages", "__my_messages__"),
        ("room-messages", "GENERAL"),
        ("notify-room", "GENERAL/user-activity"),
        ("notify-room", "GENERAL/typing"),
        ("notify-room", "GENERAL/deleteMessage"),
        ("notify-room", "GENERAL/deleteMessageBulk"),
        ("notify-user", "uid1/message"),
        ("notify-user", "uid1/notification"),
        ("notify-user", "uid1/rooms-changed"),
        ("notify-user", "uid1/subscriptions-changed"),
        ("notify-user", "uid1/userData"),
        ("notify-user", "uid1/force_logout"),
        ("notify-logged", "user-status"),
        ("notify-logged", "Users:NameChanged"),
        ("notify-logged", "roles-change"),
    ];

    /// The point of generating the catalog: if Rocket.Chat drops or renames one of these, the
    /// build fails here instead of the bot quietly never receiving the event again.
    #[test]
    fn every_typed_event_still_exists_upstream() {
        for (stream, key) in TYPED {
            assert!(
                catalog::event(stream, key).is_some(),
                "`{stream}` / `{key}` is typed here but no longer declared in streams.ts",
            );
        }
    }

    #[test]
    fn the_catalog_matches_what_the_generator_reported() {
        assert_eq!(catalog::STREAMS.len(), catalog::STREAM_COUNT);
        assert_eq!(catalog::STREAM_COUNT, 17);
        assert_eq!(
            catalog::STREAMS.iter().map(|s| s.events.len()).sum::<usize>(),
            catalog::EVENT_COUNT,
        );
        assert_eq!(catalog::EVENT_COUNT, 80);
        assert_eq!(catalog::UPSTREAM_VERSION, "8.8.0-develop");
    }

    #[test]
    fn catalog_lookup_respects_declaration_order() {
        let stream = catalog::stream("stream-room-messages").expect("declared");
        assert_eq!(stream.name, "room-messages");
        assert_eq!(
            stream.event("__my_messages__").map(|spec| spec.key),
            Some(catalog::KeyPattern::Literal("__my_messages__")),
            "the literal key must win over the catch-all room-id key that follows it",
        );
        assert_eq!(
            stream.event("GENERAL").map(|spec| spec.key),
            Some(catalog::KeyPattern::Any),
        );
    }

    #[test]
    fn key_patterns_split_on_the_first_slash_only() {
        use catalog::KeyPattern;

        assert!(KeyPattern::Suffix("user-activity").matches("GENERAL/user-activity"));
        assert!(!KeyPattern::Suffix("user-activity").matches("/user-activity"));
        assert!(!KeyPattern::Suffix("user-activity").matches("user-activity"));
        assert!(KeyPattern::Suffix("e2e.keyRequest").matches("rid/e2e.keyRequest"));
        assert!(KeyPattern::Prefix("department").matches("department/sales"));
        assert!(!KeyPattern::Prefix("department").matches("department/"));
        assert!(KeyPattern::Any.matches(""));
    }

    #[test]
    fn the_catalog_records_the_arities_this_module_relies_on() {
        let user_status = catalog::event("notify-logged", "user-status").expect("declared");
        assert_eq!(user_status.arities, &[1], "one argument, which is itself an 8-slot array");
        assert!(user_status.args.contains("statusExpiresAt"));

        let license = catalog::event("notify-all", "license").expect("declared");
        assert_eq!(license.arities, &[0, 1], "a union of tuples of differing length");

        let broadcast = catalog::event("local", "broadcast").expect("declared");
        assert!(broadcast.variadic, "`any[]`, not a fixed tuple");
    }
}
