//! An in-memory cache of Rocket.Chat entities.
//!
//! Feed it the documents that arrive on the DDP streams; read them back without a round
//! trip. It caches [`Room`](rocketsocket_model::entity::Room)s (with a name → id index),
//! [`User`](rocketsocket_model::entity::User)s (with a username → id index),
//! [`Subscription`](rocketsocket_model::entity::Subscription)s, a bounded ring of
//! [`Message`](rocketsocket_model::entity::Message)s per room, and the logged-in user.
//!
//! ```
//! use rocketsocket_cache::{Cache, ResourceType};
//! use rocketsocket_model::entity::{Message, Room};
//!
//! let cache = Cache::new();
//!
//! let room: Room = serde_json::from_str(
//!     r#"{"_id":"GENERAL","_updatedAt":{"$date":1},"t":"c","name":"general","topic":"hi"}"#,
//! )?;
//! cache.update(&room);
//!
//! // The lookup every Rocket.Chat bot makes.
//! let id = cache.room_id_by_name("general").expect("cached");
//!
//! // Room frames arrive whole, so `update` stores the payload as the whole truth: this
//! // one carries no topic, which means the topic was cleared.
//! let cleared: Room =
//!     serde_json::from_str(r#"{"_id":"GENERAL","_updatedAt":{"$date":2},"t":"c","name":"general"}"#)?;
//! cache.update(&cleared);
//! assert_eq!(cache.room(&id).unwrap().topic, None);
//!
//! // A genuinely projected payload — `rooms/get` — must not blank what it omits.
//! cache.update(&room);
//! let projected: Room =
//!     serde_json::from_str(r#"{"_id":"GENERAL","_updatedAt":{"$date":3},"t":"c"}"#)?;
//! cache.merge_room(projected);
//!
//! assert_eq!(cache.room(&id).unwrap().topic.as_deref(), Some("hi"));
//! # Ok::<(), serde_json::Error>(())
//! ```
//!
//! # Why a cache at all
//!
//! Because `getRoomIdByNameOrId` is the hottest call in any Rocket.Chat bot, and it is a
//! network round trip every time. The old JS SDK carried an LRU for exactly this and
//! nothing else; the modern SDK dropped it and pushed the problem onto consumers, who all
//! reimplement it badly.
//!
//! # Five things to know before you use it
//!
//! ## 1. Some entities merge and some replace, because the streams differ
//!
//! Every projected-away field decodes to `None`, indistinguishable from "the server says
//! this is empty". So whether [`Cache::update`] may treat an absent key as a *clear* depends
//! entirely on whether the stream that produced the payload projects — and Rocket.Chat is
//! not consistent about it:
//!
//! | Entity | [`Cache::update`] | Why |
//! |---|---|---|
//! | [`Room`](rocketsocket_model::entity::Room) | replaces | `notifyOnRoomChangedById` reads `Rooms.findByIds` with **no** projection |
//! | [`Message`](rocketsocket_model::entity::Message) | replaces | `getMessageToBroadcast` reads `Messages.findOneById` with **no** projection |
//! | [`Subscription`](rocketsocket_model::entity::Subscription) | merges | the notify path projects `subscriptionFields` |
//! | [`User`](rocketsocket_model::entity::User) | merges | `watch.users` sends `{diff, unset}`, never a document |
//!
//! Where it replaces, a field the payload omits is stored as omitted — which is the point:
//! it is how the cache learns that a topic was cleared, a team was unmade or a last reaction
//! was removed. Where it merges, a field the payload did not carry keeps its cached value,
//! and nested objects and arrays are still replaced wholesale, matching what Meteor's own
//! diffing does.
//!
//! The two escape hatches exist for sources that do not match their entity's default. The
//! projected ones — the `rooms/get` method applies `roomFields`, which omits `uids` and
//! `usernames` — want [`Cache::merge_room`] or [`Cache::merge_message`]. A complete
//! subscription or user, reconciled from a REST response, wants
//! [`Cache::replace_subscription`] or [`Cache::replace_user`].
//!
//! A merge can never see a field being *cleared*. That is the price of using one, and it is
//! why only the two genuinely-projected entities pay it.
//!
//! ## 2. Deletions arrive only on a stream, never as a message update
//!
//! A hard-deleted message is observable **only** as a `stream-notify-room` event on the
//! `<rid>/deleteMessage` key, carrying `{_id, ts}`. Nothing on `stream-room-messages` will
//! ever mention it. A cache fed only messages therefore keeps every deleted message
//! forever — until the room's ring evicts it or the process exits. Wire that event to
//! [`Cache::remove_message_in`]; it is not optional.
//!
//! ## 3. A tombstone is not a deletion
//!
//! With `Message_ShowDeletedStatus` on — and unconditionally for a thread parent — a delete
//! rewrites the document in place with `t: "rm"`, an empty `msg` and an `editedAt`, so it
//! arrives as an ordinary **edit**. By default the cache keeps it, replacing the previous
//! body rather than merging into it, because the server still has the document and merging
//! would resurrect the attachments the deletion stripped. See [`TombstonePolicy`] to
//! evict instead.
//!
//! ## 4. Never hold a [`Reference`] across an `.await`
//!
//! A [`Reference`] is a live read lock on a shard of a concurrent map, not a snapshot.
//! Holding one across a suspension point can deadlock the shard against a concurrent
//! update. Use [`Reference::cloned`], or one of the accessors that already return owned
//! values ([`Cache::current_user`], [`Cache::room_id_by_name`],
//! [`Cache::room_message_ids`]). This is the single most likely way to hang a program with
//! this crate.
//!
//! ## 5. Users are evicted; rooms and subscriptions are not
//!
//! Users are bounded at 10 000 by default and evicted first-in-first-out, because a
//! long-lived bot on a large workspace otherwise accumulates a document per stranger who
//! ever spoke. Serenity's choice — never evict, a documented permanent leak — is available
//! via [`CacheBuilder::user_cache_size`], and the trade is spelled out on
//! [`Config::user_cache_size`]. Rooms and subscriptions are bounded by the workspace
//! itself, so they are kept until explicitly removed.
//!
//! A [`Cache::user`] miss means "ask the server", never "no such user". That is true of any
//! cache, evicting or not.
//!
//! # Gating what is stored
//!
//! [`ResourceType`] is checked at the top of every write path, so clearing a flag
//! guarantees nothing of that kind is stored:
//!
//! ```
//! use rocketsocket_cache::{Cache, ResourceType};
//!
//! let cache = Cache::builder()
//!     .resource_types(ResourceType::ROOM | ResourceType::SUBSCRIPTION)
//!     .build();
//!
//! assert!(!cache.wants(ResourceType::MESSAGE));
//! ```

#![forbid(unsafe_code)]

mod cache;
mod config;
mod merge;
mod reference;
mod resource;
mod update;

#[cfg(test)]
mod tests;

pub use self::cache::{Cache, CacheStats};
pub use self::config::{
    CacheBuilder, Config, DEFAULT_MESSAGE_CACHE_SIZE, DEFAULT_USER_CACHE_SIZE, TombstonePolicy,
};
pub use self::reference::Reference;
pub use self::resource::ResourceType;
pub use self::update::UpdateCache;
