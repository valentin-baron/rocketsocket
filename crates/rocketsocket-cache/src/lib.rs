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
//! // A projected update that carries no topic must not blank the one we know.
//! let projected: Room =
//!     serde_json::from_str(r#"{"_id":"GENERAL","_updatedAt":{"$date":2},"t":"c"}"#)?;
//! cache.update(&projected);
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
//! ## 1. Partial updates merge; they do not replace
//!
//! Rocket.Chat projects documents per publication. The same room arrives with different
//! field sets from different streams, and every projected-away field decodes to `None` —
//! indistinguishable from "the server says this is empty". So [`Cache::update`] **merges**:
//! a field the payload did not carry keeps its cached value, and only fields the payload
//! actually carried are written. Nested objects and arrays are replaced wholesale, matching
//! what Meteor's own diffing does.
//!
//! The consequence is that a merge cannot see a field being *cleared*. When you have a
//! complete document — a REST response, a `rooms/get` sync — use [`Cache::replace_room`],
//! [`Cache::replace_user`], [`Cache::replace_subscription`] or [`Cache::replace_message`],
//! which store it as the whole truth.
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
