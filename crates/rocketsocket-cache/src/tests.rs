//! Behavioural tests for the cache.

use core::num::NonZeroUsize;
use core::time::Duration;
use std::sync::Arc;

use rocketsocket_model::entity::{Message, Room, Subscription, User};
use rocketsocket_model::{MessageId, RoomId, UserId};
use serde_json::{Value, json};

use crate::{Cache, Reference, ResourceType, TombstonePolicy};

// -- fixtures ----------------------------------------------------------------------------

/// Note the `from_str`: `serde_json::from_value` cannot decode any entity carrying a
/// timestamp, because the model's EJSON visitor reads its `$date` key with
/// `next_key::<&str>()` and only a borrowing deserializer can supply one.
fn entity<T: serde::de::DeserializeOwned>(value: Value) -> T {
    serde_json::from_str(&value.to_string()).expect("fixture")
}

fn room(value: Value) -> Room {
    entity(value)
}

fn user(value: Value) -> User {
    entity(value)
}

fn subscription(value: Value) -> Subscription {
    entity(value)
}

fn message(value: Value) -> Message {
    entity(value)
}

/// A minimally-populated message, the shape `stream-room-messages` delivers.
fn plain_message(id: &str, rid: &str, body: &str) -> Message {
    message(json!({
        "_id": id,
        "_updatedAt": {"$date": 1},
        "rid": rid,
        "msg": body,
        "ts": {"$date": 1},
        "u": {"_id": "u1", "username": "alice"},
    }))
}

fn room_id(id: &str) -> RoomId {
    RoomId::new(id)
}

fn user_id(id: &str) -> UserId {
    UserId::new(id)
}

fn message_id(id: &str) -> MessageId {
    MessageId::new(id)
}

// -- insert and read back ----------------------------------------------------------------

#[test]
fn inserts_and_reads_back_every_kind() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));
    cache.update(&user(json!({"_id": "u1", "username": "alice", "name": "Alice"})));
    cache.update(&subscription(json!({
        "_id": "s1", "_updatedAt": {"$date": 1}, "rid": "r1", "t": "c", "ts": {"$date": 1},
        "name": "general", "open": true, "unread": 3, "userMentions": 1, "groupMentions": 0,
        "u": {"_id": "u1", "username": "alice"},
    })));
    cache.update(&plain_message("m1", "r1", "hello"));
    cache.set_current_user(user(json!({"_id": "me", "username": "bot"})));

    assert_eq!(cache.room(&room_id("r1")).unwrap().name.as_deref(), Some("general"));
    assert_eq!(cache.room_id_by_name("general"), Some(room_id("r1")));
    assert_eq!(cache.room_by_name("general").unwrap().id, room_id("r1"));
    assert_eq!(cache.user(&user_id("u1")).unwrap().name.as_deref(), Some("Alice"));
    assert_eq!(cache.user_id_by_username("alice"), Some(user_id("u1")));
    assert_eq!(cache.user_by_username("alice").unwrap().id, user_id("u1"));
    assert_eq!(cache.subscription(&room_id("r1")).unwrap().unread, 3);
    assert_eq!(cache.message(&message_id("m1")).unwrap().msg, "hello");
    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m1")]);
    assert_eq!(cache.newest_message_id(&room_id("r1")), Some(message_id("m1")));
    assert_eq!(cache.current_user().unwrap().username.as_deref(), Some("bot"));
    assert_eq!(cache.current_user_id(), Some(user_id("me")));

    let stats = cache.stats();
    assert_eq!(stats.rooms, 1);
    assert_eq!(stats.subscriptions, 1);
    assert_eq!(stats.messages, 1);
    assert_eq!(stats.rooms_with_messages, 1);
    // `alice` plus the current user, which is stored in the users map as well.
    assert_eq!(stats.users, 2);
}

#[test]
fn reference_derefs_and_clones_out() {
    let cache = Cache::new();
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));

    let reference = cache.room(&room_id("r1")).unwrap();
    assert_eq!(reference.key(), &room_id("r1"));
    assert_eq!(reference.value().name.as_deref(), Some("general"));
    assert_eq!(reference.pair().0, &room_id("r1"));
    // Deref
    assert_eq!(reference.name.as_deref(), Some("general"));

    let owned = reference.cloned();
    drop(reference);
    assert_eq!(owned.name.as_deref(), Some("general"));
}

#[test]
fn slices_options_and_vecs_apply_elementwise() {
    let cache = Cache::new();
    let rooms = vec![
        room(json!({"_id": "r1", "_updatedAt": {"$date": 1}, "t": "c"})),
        room(json!({"_id": "r2", "_updatedAt": {"$date": 1}, "t": "p"})),
    ];

    cache.update(&rooms);
    cache.update(&Some(room(json!({"_id": "r3", "_updatedAt": {"$date": 1}, "t": "d"}))));
    cache.update(&None::<Room>);
    cache.update(&rooms[..]);

    assert_eq!(cache.stats().rooms, 3);
    assert_eq!(cache.room_ids().len(), 3);
}

// -- partial updates ---------------------------------------------------------------------

#[test]
fn a_partial_update_does_not_blank_known_fields() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c",
        "name": "general", "topic": "the topic", "description": "the description",
        "usersCount": 12, "ro": true,
    })));

    // The projection `stream-notify-user` uses carries almost nothing.
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "usersCount": 13,
    })));

    let cached = cache.room(&room_id("r1")).unwrap();
    assert_eq!(cached.name.as_deref(), Some("general"), "name was blanked by a projection");
    assert_eq!(cached.topic.as_deref(), Some("the topic"));
    assert_eq!(cached.description.as_deref(), Some("the description"));
    assert_eq!(cached.ro, Some(true));
    assert_eq!(cached.users_count, Some(13), "a carried field must win");
    assert_eq!(cached.updated_at.unix_millis(), 2);
}

#[test]
fn a_partial_update_does_not_blank_known_fields_on_any_kind() {
    let cache = Cache::new();

    cache.update(&user(json!({
        "_id": "u1", "username": "alice", "name": "Alice", "roles": ["admin"],
        "status": "online",
    })));
    // `Users:NameChanged` carries exactly Pick<IUser, '_id'|'name'|'username'>.
    cache.update(&user(json!({"_id": "u1", "username": "alice", "name": "Alicia"})));

    let cached = cache.user(&user_id("u1")).unwrap();
    assert_eq!(cached.name.as_deref(), Some("Alicia"));
    assert!(cached.has_role("admin"), "roles were blanked by a name change");
    assert!(cached.status.is_some());
    drop(cached);

    cache.update(&subscription(json!({
        "_id": "s1", "_updatedAt": {"$date": 1}, "rid": "r1", "t": "c", "ts": {"$date": 1},
        "name": "general", "open": true, "unread": 0, "userMentions": 0, "groupMentions": 0,
        "u": {"_id": "u1"}, "roles": ["owner"], "f": true,
    })));
    cache.update(&subscription(json!({
        "_id": "s1", "_updatedAt": {"$date": 2}, "rid": "r1", "t": "c", "ts": {"$date": 1},
        "open": true, "unread": 5, "userMentions": 0, "groupMentions": 0, "u": {"_id": "u1"},
    })));

    let cached = cache.subscription(&room_id("r1")).unwrap();
    assert_eq!(cached.unread, 5);
    assert_eq!(cached.name.as_deref(), Some("general"));
    assert!(cached.has_role("owner"));
    assert_eq!(cached.f, Some(true));
    drop(cached);

    cache.update(&message(json!({
        "_id": "m1", "_updatedAt": {"$date": 1}, "rid": "r1", "msg": "hi",
        "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
        "attachments": [{"text": "a"}], "pinned": true,
    })));
    cache.update(&message(json!({
        "_id": "m1", "_updatedAt": {"$date": 2}, "rid": "r1", "msg": "hi there",
        "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
        "editedAt": {"$date": 2},
    })));

    let cached = cache.message(&message_id("m1")).unwrap();
    assert_eq!(cached.msg, "hi there");
    assert!(cached.attachments.is_some(), "an edit blanked the attachments");
    assert_eq!(cached.pinned, Some(true));
    assert!(cached.is_edited());
}

#[test]
fn replace_stores_the_payload_as_the_whole_truth() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
        "topic": "the topic",
    })));

    // A complete document from REST, in which an absent topic means the topic was unset.
    let previous = cache.replace_room(room(json!({
        "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "name": "general",
    })));

    assert_eq!(previous.unwrap().topic.as_deref(), Some("the topic"));
    assert_eq!(cache.room(&room_id("r1")).unwrap().topic, None);
}

#[test]
fn replace_keeps_the_indices_consistent() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));
    cache.replace_room(room(json!({
        "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "name": "renamed",
    })));

    assert_eq!(cache.room_id_by_name("general"), None);
    assert_eq!(cache.room_id_by_name("renamed"), Some(room_id("r1")));

    cache.update(&user(json!({"_id": "u1", "username": "alice"})));
    cache.replace_user(user(json!({"_id": "u1", "username": "alicia"})));

    assert_eq!(cache.user_id_by_username("alice"), None);
    assert_eq!(cache.user_id_by_username("alicia"), Some(user_id("u1")));

    cache.update(&plain_message("m1", "r1", "one"));
    cache.replace_message(message(json!({
        "_id": "m1", "_updatedAt": {"$date": 9}, "rid": "r1", "msg": "two",
        "ts": {"$date": 1}, "u": {"_id": "u1"},
    })));

    assert_eq!(cache.message(&message_id("m1")).unwrap().msg, "two");
    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m1")], "ring duplicated");
}

// -- resource gating ---------------------------------------------------------------------

#[test]
fn resource_gating_prevents_the_write_not_just_the_read() {
    let cache = Cache::builder().resource_types(ResourceType::ROOM).build();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));
    cache.update(&user(json!({"_id": "u1", "username": "alice"})));
    cache.update(&subscription(json!({
        "_id": "s1", "_updatedAt": {"$date": 1}, "rid": "r1", "t": "c", "ts": {"$date": 1},
        "open": true, "unread": 0, "userMentions": 0, "groupMentions": 0, "u": {"_id": "u1"},
    })));
    cache.update(&plain_message("m1", "r1", "hello"));
    cache.set_current_user(user(json!({"_id": "me", "username": "bot"})));

    assert!(cache.room(&room_id("r1")).is_some());
    assert!(cache.user(&user_id("u1")).is_none());
    assert_eq!(cache.user_id_by_username("alice"), None);
    assert!(cache.subscription(&room_id("r1")).is_none());
    assert!(cache.message(&message_id("m1")).is_none());
    assert!(cache.room_message_ids(&room_id("r1")).is_empty());
    assert!(cache.current_user().is_none());

    let stats = cache.stats();
    assert_eq!(stats.rooms, 1);
    assert_eq!((stats.users, stats.subscriptions, stats.messages), (0, 0, 0));
}

#[test]
fn each_flag_gates_only_its_own_kind() {
    for (flag, present) in [
        (ResourceType::ROOM, "rooms"),
        (ResourceType::USER, "users"),
        (ResourceType::SUBSCRIPTION, "subscriptions"),
        (ResourceType::MESSAGE, "messages"),
    ] {
        let cache = Cache::builder().resource_types(ResourceType::all() - flag).build();

        cache.update(&room(json!({"_id": "r1", "_updatedAt": {"$date": 1}, "t": "c"})));
        cache.update(&user(json!({"_id": "u1"})));
        cache.update(&subscription(json!({
            "_id": "s1", "_updatedAt": {"$date": 1}, "rid": "r1", "t": "c",
            "ts": {"$date": 1}, "open": true, "unread": 0, "userMentions": 0,
            "groupMentions": 0, "u": {"_id": "u1"},
        })));
        cache.update(&plain_message("m1", "r1", "hello"));

        let stats = cache.stats();
        let counts = [
            ("rooms", stats.rooms),
            ("users", stats.users),
            ("subscriptions", stats.subscriptions),
            ("messages", stats.messages),
        ];
        for (name, count) in counts {
            let expected = usize::from(name != present);
            assert_eq!(count, expected, "{name} with {flag:?} cleared");
        }
    }
}

#[test]
fn the_current_user_can_be_cached_without_the_users_map() {
    let cache = Cache::builder().resource_types(ResourceType::CURRENT_USER).build();

    cache.set_current_user(user(json!({"_id": "me", "username": "bot"})));

    assert_eq!(cache.current_user_id(), Some(user_id("me")));
    assert_eq!(cache.stats().users, 0);
}

#[test]
fn a_zero_message_cache_size_stores_nothing() {
    let cache = Cache::builder().message_cache_size(0).build();

    cache.update(&plain_message("m1", "r1", "hello"));

    assert!(cache.message(&message_id("m1")).is_none());
    assert!(cache.room_message_ids(&room_id("r1")).is_empty());
}

// -- the message ring --------------------------------------------------------------------

#[test]
fn the_ring_bounds_at_capacity_and_evicts_oldest_first() {
    let cache = Cache::builder().message_cache_size(3).build();

    for index in 0..5 {
        cache.update(&plain_message(&format!("m{index}"), "r1", "hello"));
    }

    let ids = cache.room_message_ids(&room_id("r1"));
    assert_eq!(ids, vec![message_id("m4"), message_id("m3"), message_id("m2")]);

    // Evicted ids leave the message map too, or the ring bound would not bound anything.
    assert!(cache.message(&message_id("m0")).is_none());
    assert!(cache.message(&message_id("m1")).is_none());
    assert!(cache.message(&message_id("m4")).is_some());
    assert_eq!(cache.stats().messages, 3);
}

#[test]
fn rings_are_per_room() {
    let cache = Cache::builder().message_cache_size(2).build();

    cache.update(&plain_message("a1", "ra", "hello"));
    cache.update(&plain_message("b1", "rb", "hello"));
    cache.update(&plain_message("b2", "rb", "hello"));
    cache.update(&plain_message("b3", "rb", "hello"));

    assert_eq!(cache.room_message_ids(&room_id("ra")), vec![message_id("a1")]);
    assert_eq!(cache.room_message_ids(&room_id("rb")), vec![message_id("b3"), message_id("b2")]);
    assert_eq!(cache.stats().messages, 3);
}

#[test]
fn an_edit_updates_in_place_without_reordering_the_ring() {
    let cache = Cache::builder().message_cache_size(4).build();

    cache.update(&plain_message("m1", "r1", "first"));
    cache.update(&plain_message("m2", "r1", "second"));
    cache.update(&message(json!({
        "_id": "m1", "_updatedAt": {"$date": 9}, "rid": "r1", "msg": "first, edited",
        "ts": {"$date": 1}, "u": {"_id": "u1"}, "editedAt": {"$date": 9},
    })));

    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m2"), message_id("m1")]);
    assert_eq!(cache.message(&message_id("m1")).unwrap().msg, "first, edited");
    assert_eq!(cache.stats().messages, 2);
}

// -- deletions ---------------------------------------------------------------------------

#[test]
fn an_explicit_deletion_removes_from_the_map_and_the_ring() {
    let cache = Cache::new();

    cache.update(&plain_message("m1", "r1", "one"));
    cache.update(&plain_message("m2", "r1", "two"));

    let removed = cache.remove_message(&message_id("m1"));

    assert_eq!(removed.unwrap().msg, "one");
    assert!(cache.message(&message_id("m1")).is_none());
    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m2")]);
    assert_eq!(cache.stats().messages, 1);
}

#[test]
fn remove_message_in_works_for_a_message_that_was_never_cached() {
    // The shape of a `stream-notify-room` `<rid>/deleteMessage` event: `{_id, ts}` and the
    // room id from the event key. The document itself may predate the process.
    let cache = Cache::new();
    cache.update(&plain_message("m1", "r1", "one"));

    assert!(cache.remove_message_in(&room_id("r1"), &message_id("unseen")).is_none());
    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m1")]);

    assert!(cache.remove_message_in(&room_id("r1"), &message_id("m1")).is_some());
    assert!(cache.room_message_ids(&room_id("r1")).is_empty());
    // An emptied ring is dropped rather than left as an empty entry.
    assert_eq!(cache.stats().rooms_with_messages, 0);
}

#[test]
fn removing_a_room_cascades_to_its_subscription_and_messages() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));
    cache.update(&subscription(json!({
        "_id": "s1", "_updatedAt": {"$date": 1}, "rid": "r1", "t": "c", "ts": {"$date": 1},
        "open": true, "unread": 0, "userMentions": 0, "groupMentions": 0, "u": {"_id": "u1"},
    })));
    cache.update(&plain_message("m1", "r1", "one"));
    cache.update(&plain_message("m2", "r1", "two"));
    cache.update(&plain_message("k1", "keep", "kept"));

    let removed = cache.remove_room(&room_id("r1"));

    assert!(removed.is_some());
    assert_eq!(cache.room_id_by_name("general"), None);
    assert!(cache.subscription(&room_id("r1")).is_none());
    assert!(cache.message(&message_id("m1")).is_none());
    assert!(cache.message(&message_id("m2")).is_none());
    assert!(cache.room_message_ids(&room_id("r1")).is_empty());
    assert_eq!(cache.message(&message_id("k1")).unwrap().msg, "kept");
}

#[test]
fn clear_empties_everything() {
    let cache = Cache::new();
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));
    cache.update(&plain_message("m1", "r1", "one"));
    cache.set_current_user(user(json!({"_id": "me", "username": "bot"})));

    cache.clear();

    assert_eq!(cache.stats(), crate::CacheStats::default());
    assert_eq!(cache.room_id_by_name("general"), None);
    assert!(cache.current_user().is_none());
}

// -- tombstones --------------------------------------------------------------------------

fn tombstone(id: &str, rid: &str) -> Message {
    message(json!({
        "_id": id, "_updatedAt": {"$date": 9}, "rid": rid, "msg": "",
        "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
        "t": "rm", "editedAt": {"$date": 9}, "editedBy": {"_id": "u2"},
    }))
}

#[test]
fn the_fixture_really_is_a_tombstone() {
    assert!(tombstone("m1", "r1").is_deleted_tombstone());
}

#[test]
fn a_tombstone_replaces_the_body_instead_of_merging_into_it() {
    let cache = Cache::new();

    cache.update(&message(json!({
        "_id": "m1", "_updatedAt": {"$date": 1}, "rid": "r1", "msg": "secret",
        "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
        "attachments": [{"text": "secret attachment"}],
        "urls": [{"url": "https://example.com/secret"}],
        "reactions": {":tada:": {"usernames": ["alice"]}},
        "md": [{"type": "PARAGRAPH"}],
    })));

    cache.update(&tombstone("m1", "r1"));

    let cached = cache.message(&message_id("m1")).expect("tombstone kept, not evicted");
    assert!(cached.is_deleted_tombstone());
    assert_eq!(cached.msg, "");
    assert!(cached.attachments.is_none(), "a merge resurrected the deleted attachments");
    assert!(cached.urls.is_none());
    assert!(cached.reactions.is_none());
    assert!(cached.md.is_none());
    drop(cached);

    // The document still exists server-side, so it keeps its place in the room history.
    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m1")]);
}

#[test]
fn a_tombstone_evicts_under_the_evict_policy() {
    let cache = Cache::builder().tombstone_policy(TombstonePolicy::Evict).build();

    cache.update(&plain_message("m1", "r1", "secret"));
    cache.update(&plain_message("m2", "r1", "kept"));

    cache.update(&tombstone("m1", "r1"));

    assert!(cache.message(&message_id("m1")).is_none());
    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m2")]);
}

#[test]
fn a_tombstone_for_an_uncached_message_does_not_resurrect_it_under_evict() {
    let cache = Cache::builder().tombstone_policy(TombstonePolicy::Evict).build();

    cache.update(&tombstone("m1", "r1"));

    assert!(cache.message(&message_id("m1")).is_none());
    assert!(cache.room_message_ids(&room_id("r1")).is_empty());
}

// -- indices -----------------------------------------------------------------------------

#[test]
fn the_name_index_follows_a_rename() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "name": "lobby",
    })));

    assert_eq!(cache.room_id_by_name("general"), None, "the old name still resolves");
    assert_eq!(cache.room_id_by_name("lobby"), Some(room_id("r1")));
}

#[test]
fn a_projection_without_a_name_leaves_the_name_index_alone() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));
    cache.update(&room(json!({"_id": "r1", "_updatedAt": {"$date": 2}, "t": "c"})));

    assert_eq!(cache.room_id_by_name("general"), Some(room_id("r1")));
}

#[test]
fn removing_a_room_removes_its_name_index_entry() {
    let cache = Cache::new();
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));

    cache.remove_room(&room_id("r1"));

    assert_eq!(cache.room_id_by_name("general"), None);
    assert!(cache.room_by_name("general").is_none());
}

#[test]
fn a_name_taken_over_by_another_room_is_not_stolen_back_by_a_removal() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));
    // The name is freed server-side and reused; we learn about the new owner first.
    cache.update(&room(json!({
        "_id": "r2", "_updatedAt": {"$date": 2}, "t": "c", "name": "general",
    })));
    assert_eq!(cache.room_id_by_name("general"), Some(room_id("r2")));

    cache.remove_room(&room_id("r1"));

    assert_eq!(cache.room_id_by_name("general"), Some(room_id("r2")));
}

#[test]
fn the_username_index_follows_a_rename_and_a_removal() {
    let cache = Cache::new();

    cache.update(&user(json!({"_id": "u1", "username": "alice"})));
    cache.update(&user(json!({"_id": "u1", "username": "alicia"})));

    assert_eq!(cache.user_id_by_username("alice"), None);
    assert_eq!(cache.user_id_by_username("alicia"), Some(user_id("u1")));

    // The user cache projection is `{_id, roles}` — no username at all.
    cache.update(&user(json!({"_id": "u1", "roles": ["admin"]})));
    assert_eq!(cache.user_id_by_username("alicia"), Some(user_id("u1")));

    cache.remove_user(&user_id("u1"));
    assert_eq!(cache.user_id_by_username("alicia"), None);
    assert!(cache.user_by_username("alicia").is_none());
}

// -- the current user --------------------------------------------------------------------

#[test]
fn a_user_update_refreshes_the_current_user() {
    let cache = Cache::new();
    cache.set_current_user(user(json!({
        "_id": "me", "username": "bot", "name": "Bot", "roles": ["bot"],
    })));

    cache.update(&user(json!({"_id": "me", "name": "Bot 2"})));
    cache.update(&user(json!({"_id": "other", "name": "Somebody"})));

    let current = cache.current_user().unwrap();
    assert_eq!(current.name.as_deref(), Some("Bot 2"));
    assert_eq!(current.username.as_deref(), Some("bot"), "the refresh blanked a field");
    assert!(current.has_role("bot"));
}

#[test]
fn the_current_user_outlives_eviction_from_the_users_map() {
    let cache = Cache::builder().user_cache_size(NonZeroUsize::new(2)).build();
    cache.set_current_user(user(json!({"_id": "me", "username": "bot"})));

    for index in 0..8 {
        cache.update(&user(json!({"_id": format!("u{index}")})));
    }

    assert!(cache.user(&user_id("me")).is_none(), "the current user should have been evicted");
    assert_eq!(cache.current_user_id(), Some(user_id("me")));
}

// -- user eviction -----------------------------------------------------------------------

#[test]
fn users_are_evicted_first_in_first_out_at_capacity() {
    let cache = Cache::builder().user_cache_size(NonZeroUsize::new(3)).build();

    for index in 0..5 {
        cache.update(&user(json!({
            "_id": format!("u{index}"), "username": format!("user{index}"),
        })));
    }

    assert_eq!(cache.stats().users, 3);
    assert!(cache.user(&user_id("u0")).is_none());
    assert!(cache.user(&user_id("u1")).is_none());
    assert!(cache.user(&user_id("u4")).is_some());
    // The username index is cleaned up with the document.
    assert_eq!(cache.user_id_by_username("user0"), None);
    assert_eq!(cache.user_id_by_username("user4"), Some(user_id("u4")));
}

#[test]
fn re_seeing_a_user_does_not_renew_its_place_in_the_queue() {
    // Documented: eviction is FIFO by first insertion, not LRU. A read-path touch would
    // mean a write lock on the hottest lookup in the crate.
    let cache = Cache::builder().user_cache_size(NonZeroUsize::new(2)).build();

    cache.update(&user(json!({"_id": "u0"})));
    cache.update(&user(json!({"_id": "u1"})));
    cache.update(&user(json!({"_id": "u0", "name": "still here"})));
    cache.update(&user(json!({"_id": "u2"})));

    assert!(cache.user(&user_id("u0")).is_none());
    assert!(cache.user(&user_id("u1")).is_some());
    assert!(cache.user(&user_id("u2")).is_some());
}

#[test]
fn an_unbounded_user_cache_never_evicts() {
    let cache = Cache::builder().user_cache_size(None).build();

    for index in 0..200 {
        cache.update(&user(json!({"_id": format!("u{index}")})));
    }

    assert_eq!(cache.stats().users, 200);
    assert!(cache.user(&user_id("u0")).is_some());
}

#[test]
fn removing_a_user_frees_its_slot_in_the_queue() {
    let cache = Cache::builder().user_cache_size(NonZeroUsize::new(2)).build();

    cache.update(&user(json!({"_id": "u0"})));
    cache.update(&user(json!({"_id": "u1"})));
    cache.remove_user(&user_id("u0"));
    cache.update(&user(json!({"_id": "u2"})));

    // u1 must survive: the eviction queue must not still be holding the removed u0.
    assert!(cache.user(&user_id("u1")).is_some());
    assert!(cache.user(&user_id("u2")).is_some());
    assert_eq!(cache.stats().users, 2);
}

// -- concurrency -------------------------------------------------------------------------

/// A write must not need the reader to yield first, and dropping a guard must be enough to
/// let a writer through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_proceeds_once_no_guard_is_held() {
    let cache = Arc::new(Cache::new());
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));

    // Read, take what we need, drop the guard — the pattern the docs prescribe.
    let name = cache.room(&room_id("r1")).and_then(|room| room.name.clone());
    assert_eq!(name.as_deref(), Some("general"));

    let writer = {
        let cache = Arc::clone(&cache);
        tokio::spawn(async move {
            cache.update(&room(json!({
                "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "topic": "written",
            })));
        })
    };

    tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("the write deadlocked against a released guard")
        .expect("writer panicked");

    assert_eq!(cache.room(&room_id("r1")).unwrap().topic.as_deref(), Some("written"));
    assert_eq!(cache.room(&room_id("r1")).unwrap().name.as_deref(), Some("general"));
}

/// Readers and writers hammering the same keys from many tasks must make progress. Every
/// read clones out and holds no guard across an await, which is the contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_readers_and_writers_do_not_deadlock() {
    let cache = Arc::new(Cache::new());
    cache.set_current_user(user(json!({"_id": "me", "username": "bot"})));
    // Seeded before the readers start, so a missing index entry below means a writer lost
    // it rather than that no writer has run yet.
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 0}, "t": "c", "name": "general",
    })));

    let mut tasks = Vec::new();

    for writer in 0..4u32 {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            for round in 0..200u32 {
                cache.update(&room(json!({
                    "_id": "r1", "_updatedAt": {"$date": round}, "t": "c", "name": "general",
                    "topic": format!("w{writer}-{round}"),
                })));
                cache.update(&user(json!({
                    "_id": format!("u{}", round % 16), "username": format!("user{}", round % 16),
                })));
                cache.update(&plain_message(&format!("m{writer}-{round}"), "r1", "hello"));
                cache.update(&subscription(json!({
                    "_id": "s1", "_updatedAt": {"$date": round}, "rid": "r1", "t": "c",
                    "ts": {"$date": 1}, "open": true, "unread": round as i64,
                    "userMentions": 0, "groupMentions": 0, "u": {"_id": "me"},
                })));
                if round.is_multiple_of(5) {
                    cache.remove_message(&MessageId::new(format!("m{writer}-{round}")));
                }
                if round.is_multiple_of(37) {
                    cache.remove_user(&UserId::new(format!("u{}", round % 16)));
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for _ in 0..4 {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            for _ in 0..200 {
                // Every read drops its guard before the await below.
                let topic = cache.room(&room_id("r1")).and_then(|room| room.topic.clone());
                assert!(topic.is_none_or(|topic| topic.starts_with('w')));
                assert_eq!(cache.room_id_by_name("general"), Some(room_id("r1")));
                let _ = cache.room_message_ids(&room_id("r1"));
                let _ = cache.current_user();
                let _ = cache.user_id_by_username("user3");
                let _ = cache.stats();
                tokio::task::yield_now().await;
            }
        }));
    }

    let all = futures_join(tasks);
    tokio::time::timeout(Duration::from_secs(30), all).await.expect("the cache deadlocked");

    // The ring is still bounded and consistent after all that.
    let ids = cache.room_message_ids(&room_id("r1"));
    assert!(ids.len() <= cache.config().message_cache_size());
    for id in &ids {
        assert!(cache.message(id).is_some(), "ring holds an id the message map does not");
    }
}

/// `futures_util` is not a dependency of this crate, and a join over a `Vec` of handles is
/// three lines.
async fn futures_join(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for task in tasks {
        task.await.expect("task panicked");
    }
}

// -- misc --------------------------------------------------------------------------------

#[test]
fn debug_prints_the_shape_not_the_contents() {
    let cache = Cache::new();
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));

    let rendered = format!("{cache:?}");

    assert!(rendered.contains("Cache"));
    assert!(rendered.contains("rooms: 1"));
    assert!(!rendered.contains("general"));
}

#[test]
fn the_cache_is_shareable_across_threads() {
    fn assert_shareable<T: Send + Sync + 'static>() {}
    assert_shareable::<Cache>();
}

#[test]
fn a_removed_user_does_not_take_the_current_user_with_it() {
    let cache = Cache::new();
    cache.set_current_user(user(json!({"_id": "me", "username": "bot"})));

    cache.remove_user(&user_id("me"));

    assert!(cache.user(&user_id("me")).is_none());
    assert_eq!(cache.current_user_id(), Some(user_id("me")));
}

// ========================================================================================
// Adversarial review, 2026-08. Each test below is either a proof of a reported finding or a
// pin on behaviour the review checked. Findings that are *not* fixed say so in a comment.
// ========================================================================================

// -- 1. the JSON merge invariant ---------------------------------------------------------

/// The whole merge rests on "a `None` field serializes to no key". Prove it for every
/// cached type at once, not just `Room`: the sparsest legal document of each kind must
/// serialize to exactly its required keys and nothing else.
///
/// If the model ever grows an `Option` field without
/// `skip_serializing_if = "Option::is_none"`, that field appears here as a `null` key and
/// this test fails — before it can silently blank cached data on every partial update.
#[test]
fn the_sparsest_document_of_every_kind_carries_only_required_keys() {
    fn keys<T: serde::Serialize>(value: &T) -> Vec<String> {
        match serde_json::to_value(value) {
            Ok(Value::Object(map)) => map.keys().cloned().collect(),
            other => panic!("entity did not serialize to a JSON object: {other:?}"),
        }
    }

    assert_eq!(
        keys(&room(json!({"_id": "r", "_updatedAt": {"$date": 1}, "t": "c"}))),
        vec!["_id", "_updatedAt", "t"],
    );
    assert_eq!(keys(&user(json!({"_id": "u"}))), vec!["_id"]);
    assert_eq!(
        keys(&message(json!({
            "_id": "m", "_updatedAt": {"$date": 1}, "rid": "r", "msg": "",
            "ts": {"$date": 1}, "u": {"_id": "u"},
        }))),
        vec!["_id", "_updatedAt", "msg", "rid", "ts", "u"],
    );
    assert_eq!(
        keys(&subscription(json!({
            "_id": "s", "_updatedAt": {"$date": 1}, "rid": "r", "t": "c",
            "ts": {"$date": 1}, "u": {"_id": "u"},
            "open": false, "unread": 0, "userMentions": 0, "groupMentions": 0,
        }))),
        vec![
            "_id",
            "_updatedAt",
            "groupMentions",
            "open",
            "rid",
            "t",
            "ts",
            "u",
            "unread",
            "userMentions"
        ],
    );
}

/// The nested stubs are merged wholesale, so they only have to round-trip. They do — but
/// `UserRef` is the one the cache actually reads back (`Message::u`), so pin it.
#[test]
fn a_sparse_nested_stub_also_carries_only_required_keys() {
    let cached = message(json!({
        "_id": "m", "_updatedAt": {"$date": 1}, "rid": "r", "msg": "hi",
        "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice", "name": "Alice"},
    }));

    let serialized = serde_json::to_value(&cached).expect("serializes");
    let u = serialized.get("u").expect("u is required");
    assert_eq!(u.as_object().expect("object").len(), 3);
}

/// A projected document whose *non-`Option`* fields sit at their `Default` cannot exist:
/// the four non-optional `Subscription` counters have no `#[serde(default)]`, so a payload
/// that omits them fails to decode rather than decoding to zero. That is the right failure
/// mode for a merging cache — a zero that overwrote a real unread count would be worse —
/// but it means a projection narrower than `subscriptionFields` never reaches the cache at
/// all, silently.
#[test]
fn a_subscription_missing_a_required_counter_does_not_decode_to_a_default() {
    let narrow = r#"{"_id":"s1","_updatedAt":{"$date":1},"rid":"r1","t":"c",
        "ts":{"$date":1},"u":{"_id":"u1"},"open":true,"unread":7,"userMentions":0}"#;

    let decoded: Result<Subscription, _> = serde_json::from_str(narrow);
    assert!(decoded.is_err(), "a missing groupMentions must not silently become 0");
}

/// `Option<Vec<T>>`: `[]` is carried and overwrites, an omission is not. Both halves matter,
/// and the server uses both — `$pull` leaves `[]` (`Rooms.removeMutedUsernameByRoomId`)
/// while `$unset` removes the key (`Rooms.setSystemMessagesById`, Rooms.ts:1015).
#[test]
fn an_empty_array_overwrites_but_an_omitted_one_does_not() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
        "muted": ["bob"], "sysMes": ["uj"],
    })));

    // `$pull` down to nothing: the key is present and empty, so it wins.
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "muted": [],
    })));

    let cached = cache.room(&room_id("r1")).expect("cached");
    assert_eq!(cached.muted.as_deref(), Some(&[][..]));
    // `sysMes` was not carried, so it survives — correct for a projection, wrong for the
    // `$unset` the server actually performs. See the review notes.
    assert!(cached.sys_mes.is_some());
}

// -- 2. merge vs replace: documents that never existed on the server ----------------------

/// `stream-room-messages` does **not** project: `getMessageToBroadcast` reads the whole
/// document (`Messages.findOneById(id)`, notifyListener.ts:443) and broadcasts it as-is.
/// So for messages, and only for messages, an absent key really does mean "the server
/// unset it" — and `update()` merges anyway.
///
/// The realistic instance: removing the last reaction runs `delete message.reactions` plus
/// `Messages.unsetReactions` (setReaction.ts:54-56, Messages.ts:580-582), so the next
/// broadcast carries no `reactions` key at all. The merge keeps the stale one.
///
/// BUG (reported, not fixed — the fix is a policy change, not a bug fix): the cache reports
/// a reaction that no longer exists.
#[test]
fn unreacting_leaves_a_stale_reaction_in_the_cache() {
    let cache = Cache::new();

    let with_reaction = message(json!({
        "_id": "m1", "_updatedAt": {"$date": 1}, "rid": "r1", "msg": "hi",
        "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
        "reactions": {":tada:": {"usernames": ["alice"]}},
    }));
    cache.update(&with_reaction);

    // The document the server broadcasts after alice removes her reaction: complete, and
    // with no `reactions` key.
    let after_unreact = message(json!({
        "_id": "m1", "_updatedAt": {"$date": 2}, "rid": "r1", "msg": "hi",
        "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
    }));
    cache.update(&after_unreact);

    let cached = cache.message(&message_id("m1")).expect("cached");
    assert!(
        cached.reactions.is_some(),
        "if this now fails the merge policy for messages was changed — good"
    );

    // `replace_message` is the workaround, and it is what a message feed should call.
    drop(cached);
    cache.replace_message(after_unreact);
    assert!(cache.message(&message_id("m1")).expect("cached").reactions.is_none());
}

/// Two projections of the same room combined into a state the server cannot hold.
///
/// `Rooms.unsetTeamById` / `unsetTeamId` `$unset` `teamId`, `teamDefault` and `teamMain`
/// (Rooms.ts:445-460), and all three are in `roomFields` (publishFields.ts), so converting a
/// team back to a channel broadcasts a room document with those keys simply gone. The merge
/// keeps them: the cache then holds a `teamMain: true` room with no team.
///
/// BUG (reported, not fixed — this is the documented "a merge cannot see a clear"
/// limitation, cited here against the server so the cost is concrete).
#[test]
fn a_team_converted_back_to_a_channel_stays_a_team_in_the_cache() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "eng",
        "teamId": "t1", "teamMain": true, "teamDefault": false,
    })));

    // What the stream carries after `Rooms.unsetTeamId`.
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "name": "eng",
    })));

    let cached = cache.room(&room_id("r1")).expect("cached");
    assert_eq!(cached.team_main, Some(true), "stale: the room is no longer a team");
    assert_eq!(cached.team_id.as_deref(), Some("t1"));
}

/// The shallow merge is right for `u` and `lastMessage`: Meteor resends whole subdocuments.
/// Pin the `lastMessage` half, which the existing tests do not cover — a room update that
/// carries a newer `lastMessage` must not leave fields of the older one behind.
#[test]
fn last_message_is_replaced_wholesale_not_merged_field_by_field() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
        "lastMessage": {
            "_id": "m1", "_updatedAt": {"$date": 1}, "rid": "r1", "msg": "first",
            "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
            "attachments": [{"text": "a"}],
        },
    })));

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c",
        "lastMessage": {
            "_id": "m2", "_updatedAt": {"$date": 2}, "rid": "r1", "msg": "second",
            "ts": {"$date": 2}, "u": {"_id": "u2", "username": "bob"},
        },
    })));

    let cached = cache.room(&room_id("r1")).expect("cached");
    let last = cached.last_message.as_deref().expect("carried");
    assert_eq!(last.msg, "second");
    assert!(last.attachments.is_none(), "the older lastMessage bled through");
    // And the room's own fields survived the update that only carried lastMessage.
    assert_eq!(cached.name.as_deref(), Some("general"));
}

/// A room's `lastMessage` and the message ring are independent: caching a room whose
/// `lastMessage` the message map has never seen must not create a phantom ring entry, and
/// removing that message must not disturb the room.
#[test]
fn a_rooms_last_message_is_not_fed_into_the_message_ring() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
        "lastMessage": {
            "_id": "m1", "_updatedAt": {"$date": 1}, "rid": "r1", "msg": "hi",
            "ts": {"$date": 1}, "u": {"_id": "u1"},
        },
    })));

    assert!(cache.room_message_ids(&room_id("r1")).is_empty());
    assert!(cache.message(&message_id("m1")).is_none());
    // ... and the stale `lastMessage` outlives an explicit deletion of that message, which
    // is a real staleness the caller has to know about.
    cache.remove_message_in(&room_id("r1"), &message_id("m1"));
    assert!(cache.room(&room_id("r1")).expect("cached").last_message.is_some());
}

// -- 3. tombstones -----------------------------------------------------------------------

/// The tombstone the server actually writes. `Messages.setAsDeletedByIdAndUser`
/// (Messages.ts:1060-1085) `$set`s `msg: ''`, `t: 'rm'`, `urls: []`, `mentions: []`,
/// `attachments: []`, `reactions: {}`, `editedAt`, `editedBy`, and `$unset`s `md`, `blocks`
/// and `tshow`. Note what that means: the emptied collections *are* carried, so a merge
/// would not resurrect them — but `md`, `blocks` and `tshow` are unset, so a merge would
/// resurrect those. Replace is the right policy; the reason is `$unset`, not `$set`.
fn server_tombstone(id: &str, rid: &str) -> Message {
    message(json!({
        "_id": id, "_updatedAt": {"$date": 9}, "rid": rid, "msg": "",
        "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
        "t": "rm", "editedAt": {"$date": 9},
        "editedBy": {"_id": "u2", "username": "mod"},
        "urls": [], "mentions": [], "attachments": [], "reactions": {},
    }))
}

#[test]
fn the_server_shaped_tombstone_is_detected_and_strips_the_unset_fields() {
    let cache = Cache::new();
    assert!(server_tombstone("m1", "r1").is_deleted_tombstone());

    cache.update(&message(json!({
        "_id": "m1", "_updatedAt": {"$date": 1}, "rid": "r1", "msg": "secret",
        "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
        "md": [{"type": "PARAGRAPH"}], "blocks": [{"type": "section"}], "tshow": true,
        "attachments": [{"text": "secret"}],
    })));

    cache.update(&server_tombstone("m1", "r1"));

    let cached = cache.message(&message_id("m1")).expect("kept");
    assert!(cached.is_deleted_tombstone());
    // The `$unset` trio: these are the ones a merge would have resurrected.
    assert!(cached.md.is_none());
    assert!(cached.blocks.is_none());
    assert!(cached.tshow.is_none());
    // The `$set`-to-empty group arrives empty rather than absent.
    assert_eq!(cached.attachments.as_deref(), Some(&[][..]));
    assert!(cached.reactions.as_ref().is_some_and(|r| r.is_empty()));
}

/// A tombstone for a message the cache never saw is stored under the default policy, and
/// takes a slot in the room's ring. That is intentional (the document exists server-side)
/// but it is a slot spent on a message the bot will never render, so pin it.
#[test]
fn a_tombstone_for_an_uncached_message_is_stored_under_the_replace_policy() {
    let cache = Cache::new();

    cache.update(&server_tombstone("m1", "r1"));

    assert!(cache.message(&message_id("m1")).is_some());
    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m1")]);
}

/// A tombstone always replaces, whatever the caller asked for, and the detection runs on
/// the *incoming* payload. So a later ordinary update to a tombstoned message still carries
/// `t: "rm"` (nothing on the server ever unsets `t`) and is therefore still detected as a
/// tombstone and still replaces. The "sticky rm" a shallow merge could produce is
/// unreachable from the server for that reason; pin it so a future change to
/// `is_deleted_tombstone` cannot open it up quietly.
#[test]
fn an_update_after_a_tombstone_is_still_treated_as_a_tombstone() {
    let cache = Cache::new();

    cache.update(&plain_message("m1", "r1", "before"));
    cache.update(&server_tombstone("m1", "r1"));

    // `Messages.decreaseReplyCountById` on a tombstoned thread parent re-broadcasts the
    // whole document, still carrying `t: "rm"`.
    let mut again = server_tombstone("m1", "r1");
    again.tcount = Some(0);
    cache.update(&again);

    let cached = cache.message(&message_id("m1")).expect("kept");
    assert!(cached.is_deleted_tombstone());
    assert_eq!(cached.msg, "");
    assert_eq!(cached.tcount, Some(0));
}

/// Under `Evict`, a tombstone for a message cached in a *different* room than the tombstone
/// claims removes the document but leaves the id in the other room's ring. The tombstone
/// always carries the right `rid`, so this is unreachable from the server — but
/// `remove_message_in` is public and the same shape is reachable through it. See the
/// dangling-ring test below.
#[test]
fn evicting_a_tombstone_unlinks_from_the_rid_the_tombstone_carries() {
    let cache = Cache::builder().tombstone_policy(TombstonePolicy::Evict).build();

    cache.update(&plain_message("m1", "r1", "hi"));
    cache.update(&server_tombstone("m1", "r1"));

    assert!(cache.message(&message_id("m1")).is_none());
    assert!(cache.room_message_ids(&room_id("r1")).is_empty());
    assert_eq!(cache.stats().rooms_with_messages, 0, "the emptied ring was not reaped");
}

// -- 4. indices and eviction -------------------------------------------------------------

/// `reindex` returns early when the name did not change, so it never repairs an index entry
/// that a *different* room took over and then vacated. The cached room keeps its name and
/// becomes unreachable by it, permanently.
///
/// BUG (reported, not fixed): repairing it needs a map lookup on every update whose name is
/// unchanged — i.e. on the hottest write path — for a case that needs two rooms to share a
/// name. Recorded here rather than fixed. See the review notes for the recommendation.
#[test]
fn a_name_index_entry_vacated_by_another_room_is_never_repaired() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
    })));
    // A second room claims the same name — a rename race across two publications.
    cache.update(&room(json!({
        "_id": "r2", "_updatedAt": {"$date": 2}, "t": "c", "name": "general",
    })));
    assert_eq!(cache.room_id_by_name("general"), Some(room_id("r2")));

    cache.remove_room(&room_id("r2"));

    // r1 is still cached and still named "general" ...
    assert_eq!(cache.room(&room_id("r1")).expect("cached").name.as_deref(), Some("general"));
    // ... and no further update to it ever puts it back in the index.
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 3}, "t": "c", "name": "general", "topic": "t",
    })));
    assert_eq!(cache.room_id_by_name("general"), None, "current behaviour: unreachable by name");
}

/// The same hole in the username index.
#[test]
fn a_username_index_entry_vacated_by_another_user_is_never_repaired() {
    let cache = Cache::new();

    cache.update(&user(json!({"_id": "u1", "username": "alice"})));
    cache.update(&user(json!({"_id": "u2", "username": "alice"})));
    cache.remove_user(&user_id("u2"));

    cache.update(&user(json!({"_id": "u1", "username": "alice", "name": "Alice"})));

    assert!(cache.user(&user_id("u1")).is_some());
    assert_eq!(cache.user_id_by_username("alice"), None, "current behaviour");
}

/// Eviction must never leave an index entry pointing at a document that is gone.
#[test]
fn eviction_takes_the_username_index_with_it() {
    let cache = Cache::builder().user_cache_size(NonZeroUsize::new(2)).build();

    cache.update(&user(json!({"_id": "u0", "username": "zero"})));
    cache.update(&user(json!({"_id": "u1", "username": "one"})));
    cache.update(&user(json!({"_id": "u2", "username": "two"})));

    assert!(cache.user(&user_id("u0")).is_none());
    assert_eq!(cache.user_id_by_username("zero"), None, "index outlived its document");
    assert_eq!(cache.user_id_by_username("one"), Some(user_id("u1")));
    assert_eq!(cache.user_id_by_username("two"), Some(user_id("u2")));
    assert_eq!(cache.stats().users, 2);
}

/// A username handed from one account to another must not be dragged out of the index when
/// the *old* account is evicted.
#[test]
fn eviction_does_not_steal_a_username_another_user_has_claimed() {
    let cache = Cache::builder().user_cache_size(NonZeroUsize::new(2)).build();

    cache.update(&user(json!({"_id": "u0", "username": "alice"})));
    // u1 takes the name over (a rename the cache saw out of order).
    cache.update(&user(json!({"_id": "u1", "username": "alice"})));
    // A third insert evicts u0.
    cache.update(&user(json!({"_id": "u2", "username": "carol"})));

    assert!(cache.user(&user_id("u0")).is_none());
    assert_eq!(cache.user_id_by_username("alice"), Some(user_id("u1")));
}

/// Capacity one is the degenerate case the FIFO has to survive.
#[test]
fn a_user_capacity_of_one_keeps_exactly_the_newest() {
    let cache = Cache::builder().user_cache_size(NonZeroUsize::new(1)).build();

    for n in 0..5 {
        cache.update(&user(json!({"_id": format!("u{n}"), "username": format!("user{n}")})));
        assert_eq!(cache.stats().users, 1, "capacity broken after {n} inserts");
    }
    assert!(cache.user(&user_id("u4")).is_some());
}

/// `remove_user` for an id the cache never held must not corrupt the queue.
#[test]
fn removing_an_absent_user_is_a_no_op_for_the_queue() {
    let cache = Cache::builder().user_cache_size(NonZeroUsize::new(2)).build();

    cache.update(&user(json!({"_id": "u0"})));
    cache.remove_user(&user_id("nobody"));
    cache.update(&user(json!({"_id": "u1"})));
    cache.update(&user(json!({"_id": "u2"})));

    assert_eq!(cache.stats().users, 2);
    assert!(cache.user(&user_id("u0")).is_none(), "u0 should have been the eviction victim");
    assert!(cache.user(&user_id("u1")).is_some());
    assert!(cache.user(&user_id("u2")).is_some());
}

/// Re-inserting a user that was already evicted must give it a fresh place in the queue and
/// not a duplicate one.
#[test]
fn a_re_inserted_user_gets_one_queue_slot_not_two() {
    let cache = Cache::builder().user_cache_size(NonZeroUsize::new(2)).build();

    cache.update(&user(json!({"_id": "a"})));
    cache.update(&user(json!({"_id": "b"})));
    cache.update(&user(json!({"_id": "c"}))); // evicts a
    cache.update(&user(json!({"_id": "a"}))); // evicts b
    cache.update(&user(json!({"_id": "d"}))); // must evict c, not a

    assert_eq!(cache.stats().users, 2);
    assert!(cache.user(&user_id("a")).is_some(), "a was evicted by a stale duplicate slot");
    assert!(cache.user(&user_id("d")).is_some());
}

/// Under contention the map must stay at or below capacity: the eviction queue and the map
/// must not drift apart. `note_new_user` reads `users.len()` outside the queue lock and
/// drops evicted documents after releasing it, so this is the interleaving that would show
/// a drift if one existed.
#[test]
fn concurrent_inserts_never_leave_the_user_map_over_capacity() {
    use std::thread;

    const CAPACITY: usize = 64;

    for _round in 0..20 {
        let cache = Arc::new(Cache::builder().user_cache_size(NonZeroUsize::new(CAPACITY)).build());
        let mut handles = Vec::new();
        for worker in 0..8u32 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for n in 0..200u32 {
                    let id = format!("u{}", worker * 200 + n);
                    cache.update(&user(json!({"_id": id, "username": format!("n{id}")})));
                    if n.is_multiple_of(7) {
                        cache.remove_user(&UserId::new(format!("u{}", worker * 200 + n)));
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().expect("worker panicked");
        }

        let stats = cache.stats();
        assert!(stats.users <= CAPACITY, "user map drifted to {} over {CAPACITY}", stats.users);
        // Every surviving index entry must resolve to a live document.
        for id in cache.user_ids() {
            let username = cache.user(&id).and_then(|u| u.username.clone());
            if let Some(username) = username
                && let Some(indexed) = cache.user_id_by_username(&username)
            {
                assert!(cache.user(&indexed).is_some(), "username index outlived its document");
            }
        }
    }
}

// -- 5. deadlocks ------------------------------------------------------------------------

/// Runs `body` on a worker thread and fails — rather than hanging the suite — if it has not
/// finished within `limit`.
fn within<F>(limit: Duration, what: &str, body: F)
where
    F: FnOnce() + Send + 'static,
{
    use std::sync::mpsc;
    use std::thread;

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        body();
        let _ = tx.send(());
    });
    match rx.recv_timeout(limit) {
        Ok(()) => handle.join().expect("worker panicked"),
        Err(_) => panic!("{what} did not finish within {limit:?}: deadlock"),
    }
}

/// `unlink_message` holds a `get_mut` guard on `room_messages` and then, if the ring is now
/// empty, removes the same key from the same map. If the guard were still alive the shard
/// would deadlock against itself. This is a single-threaded hang, so it needs the timeout.
#[test]
fn emptying_a_rooms_ring_does_not_deadlock_the_ring_map_against_itself() {
    within(Duration::from_secs(5), "remove_message_in on the last message", || {
        let cache = Cache::new();
        cache.update(&plain_message("m1", "r1", "hi"));
        cache.remove_message_in(&room_id("r1"), &message_id("m1"));
        assert_eq!(cache.stats().rooms_with_messages, 0);

        // The same shape through the other entry point.
        cache.update(&plain_message("m2", "r2", "hi"));
        cache.remove_message(&message_id("m2"));
        assert_eq!(cache.stats().rooms_with_messages, 0);
    });
}

/// `link_message` holds the ring guard and then removes from the message map. Capacity one
/// makes it evict on every single insert, so if the two were ever locked together this
/// hangs immediately.
#[test]
fn a_capacity_one_ring_evicts_on_every_insert_without_deadlocking() {
    within(Duration::from_secs(5), "capacity-one ring inserts", || {
        let cache = Cache::builder().message_cache_size(1).build();
        for n in 0..50 {
            cache.update(&plain_message(&format!("m{n}"), "r1", "hi"));
            assert_eq!(cache.stats().messages, 1);
            assert_eq!(cache.room_message_ids(&room_id("r1")).len(), 1);
        }
        assert_eq!(cache.newest_message_id(&room_id("r1")), Some(message_id("m49")));
    });
}

/// The crate claims no internal path holds a guard on one map while locking another. Hold a
/// `Reference` on a room and then drive the whole write surface from another thread,
/// touching every other map and every other key. Anything that needed a lock we are holding
/// would hang here instead of returning.
#[test]
fn no_write_path_needs_a_lock_a_live_reference_is_holding() {
    let cache = Arc::new(Cache::builder().user_cache_size(NonZeroUsize::new(4)).build());

    cache.update(&room(json!({
        "_id": "held", "_updatedAt": {"$date": 1}, "t": "c", "name": "held",
    })));
    cache.update(&room(json!({
        "_id": "other", "_updatedAt": {"$date": 1}, "t": "c", "name": "other",
    })));
    cache.update(&plain_message("m1", "other", "hi"));
    cache.set_current_user(user(json!({"_id": "me", "username": "bot"})));

    let guard = cache.room(&room_id("held")).expect("cached");

    let worker = Arc::clone(&cache);
    within(Duration::from_secs(10), "the write surface with a Reference held", move || {
        for n in 0..64u32 {
            worker.update(&room(json!({
                "_id": "other", "_updatedAt": {"$date": n}, "t": "c", "name": "other",
            })));
            worker.update(&user(json!({"_id": format!("u{n}"), "username": format!("n{n}")})));
            worker.update(&subscription(json!({
                "_id": "s1", "_updatedAt": {"$date": n}, "rid": "other", "t": "c",
                "ts": {"$date": 1}, "u": {"_id": "me"},
                "open": true, "unread": 0, "userMentions": 0, "groupMentions": 0,
            })));
            worker.update(&plain_message(&format!("m{n}"), "other", "hi"));
            worker.remove_message_in(&RoomId::new("other"), &MessageId::new(format!("m{n}")));
            worker.remove_user(&UserId::new(format!("u{n}")));
            worker.set_current_user(user(json!({"_id": "me", "username": "bot"})));
            let _ = worker.current_user();
            let _ = worker.stats();
            let _ = format!("{worker:?}");
        }
        worker.remove_room(&RoomId::new("other"));
        worker.remove_subscription(&RoomId::new("other"));
    });

    // Still valid, still the document we asked for.
    assert_eq!(guard.name.as_deref(), Some("held"));
    drop(guard);
}

/// Two `Reference`s alive at once on different maps, which is the shape a caller reaches for
/// when comparing a room against its subscription. Both are read locks, so this must be
/// fine — but it is worth pinning, because it is the pattern one shard-write away from an
/// ABBA hang.
#[test]
fn two_references_on_different_maps_can_be_alive_at_once() {
    within(Duration::from_secs(5), "two live references", || {
        let cache = Cache::new();
        cache.update(&room(json!({
            "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
        })));
        cache.update(&subscription(json!({
            "_id": "s1", "_updatedAt": {"$date": 1}, "rid": "r1", "t": "c",
            "ts": {"$date": 1}, "u": {"_id": "me"},
            "open": true, "unread": 3, "userMentions": 0, "groupMentions": 0,
        })));

        let room_ref = cache.room(&room_id("r1")).expect("cached");
        let sub_ref = cache.subscription(&room_id("r1")).expect("cached");
        assert_eq!(room_ref.name.as_deref(), Some("general"));
        assert_eq!(sub_ref.unread, 3);
    });
}

// -- 6. the message ring -----------------------------------------------------------------

/// Interleaved rooms evict independently, and each ring keeps its own arrival order.
#[test]
fn interleaved_rooms_evict_independently() {
    let cache = Cache::builder().message_cache_size(2).build();

    cache.update(&plain_message("a1", "ra", "hi"));
    cache.update(&plain_message("b1", "rb", "hi"));
    cache.update(&plain_message("a2", "ra", "hi"));
    cache.update(&plain_message("b2", "rb", "hi"));
    cache.update(&plain_message("a3", "ra", "hi")); // evicts a1 only

    assert_eq!(cache.room_message_ids(&room_id("ra")), vec![message_id("a3"), message_id("a2")]);
    assert_eq!(cache.room_message_ids(&room_id("rb")), vec![message_id("b2"), message_id("b1")]);
    assert!(cache.message(&message_id("a1")).is_none());
    assert!(cache.message(&message_id("b1")).is_some());
    assert_eq!(cache.stats().messages, 4);
}

/// The same id delivered twice — a re-broadcast, or a `loadHistory` replay overlapping the
/// stream — must update in place and take exactly one ring slot.
#[test]
fn the_same_message_id_twice_takes_one_ring_slot() {
    let cache = Cache::builder().message_cache_size(3).build();

    cache.update(&plain_message("m1", "r1", "first"));
    cache.update(&plain_message("m1", "r1", "second"));
    cache.update(&plain_message("m1", "r1", "third"));

    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m1")]);
    assert_eq!(cache.message(&message_id("m1")).expect("cached").msg, "third");
    assert_eq!(cache.stats().messages, 1);
}

/// A message that fell out of the ring and comes back — someone edited an old message —
/// re-enters at the newest end, because ordering is by arrival. Pin it: it is surprising,
/// and `newest_message_id` is a public accessor that says "most recently arrived".
#[test]
fn an_edit_to_an_evicted_message_re_enters_the_ring_as_the_newest() {
    let cache = Cache::builder().message_cache_size(2).build();

    cache.update(&plain_message("m1", "r1", "old"));
    cache.update(&plain_message("m2", "r1", "mid"));
    cache.update(&plain_message("m3", "r1", "new")); // evicts m1
    assert!(cache.message(&message_id("m1")).is_none());

    cache.update(&plain_message("m1", "r1", "edited"));

    assert_eq!(cache.newest_message_id(&room_id("r1")), Some(message_id("m1")));
    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m1"), message_id("m3")]);
    assert!(cache.message(&message_id("m2")).is_none(), "m2 was evicted to make room");
}

/// `remove_message_in` unlinks from the room id it was *given*, not from the one the cached
/// document carries. Called with the wrong room it drops the document and leaves the id in
/// the real room's ring — and the next arrival of that id then links it a second time, so
/// the ring holds a duplicate and the eviction of one copy deletes a document the other copy
/// still points at.
///
/// BUG (reported, not fixed — it needs a caller error to reach, and the fix changes the
/// documented "works for a message that was never cached" behaviour). See the review notes.
#[test]
fn remove_message_in_with_the_wrong_room_leaves_a_dangling_ring_entry() {
    let cache = Cache::new();

    cache.update(&plain_message("m1", "r1", "hi"));
    cache.remove_message_in(&room_id("WRONG"), &message_id("m1"));

    // Document gone, ring entry left behind: the two now disagree.
    assert!(cache.message(&message_id("m1")).is_none());
    assert_eq!(cache.room_message_ids(&room_id("r1")), vec![message_id("m1")]);

    // And the id is linked a second time when it comes back.
    cache.update(&plain_message("m1", "r1", "hi again"));
    assert_eq!(
        cache.room_message_ids(&room_id("r1")),
        vec![message_id("m1"), message_id("m1")],
        "current behaviour: a duplicate ring entry",
    );
}

/// Whatever else happens, the ring and the map must agree after ordinary use. This is the
/// invariant the duplicate above breaks; assert it holds on every path that does not need a
/// caller error.
#[test]
fn the_ring_and_the_message_map_agree_after_ordinary_use() {
    let cache = Cache::builder().message_cache_size(4).build();

    for n in 0..40u32 {
        let rid = format!("r{}", n % 3);
        cache.update(&plain_message(&format!("m{n}"), &rid, "hi"));
        if n.is_multiple_of(5) {
            cache.remove_message(&message_id(&format!("m{n}")));
        }
        if n.is_multiple_of(11) {
            cache.update(&server_tombstone(&format!("m{n}"), &rid));
        }
    }

    let mut linked = 0;
    for n in 0..3 {
        let ids = cache.room_message_ids(&room_id(&format!("r{n}")));
        assert!(ids.len() <= 4);
        for id in &ids {
            assert!(cache.message(id).is_some(), "ring holds {id:?}, the map does not");
        }
        linked += ids.len();
    }
    assert_eq!(linked, cache.stats().messages, "the map holds messages no ring points at");
}

/// A room removed while its ring is full takes every one of its messages with it, and only
/// its own.
#[test]
fn removing_a_room_leaves_other_rooms_rings_alone() {
    let cache = Cache::builder().message_cache_size(3).build();

    for n in 0..3 {
        cache.update(&plain_message(&format!("a{n}"), "ra", "hi"));
        cache.update(&plain_message(&format!("b{n}"), "rb", "hi"));
    }

    cache.remove_room(&room_id("ra"));

    assert_eq!(cache.stats().messages, 3);
    assert_eq!(cache.stats().rooms_with_messages, 1);
    assert!(cache.room_message_ids(&room_id("ra")).is_empty());
    assert_eq!(cache.room_message_ids(&room_id("rb")).len(), 3);
}

/// A zero-capacity ring stores nothing at all — including through `replace_message`, which
/// bypasses the merge but not the gate.
#[test]
fn a_zero_capacity_ring_rejects_replace_too() {
    let cache = Cache::builder().message_cache_size(0).build();

    assert!(cache.replace_message(plain_message("m1", "r1", "hi")).is_none());
    assert!(cache.message(&message_id("m1")).is_none());
    assert_eq!(cache.stats().rooms_with_messages, 0);
}

// -- 7. the Reference contract -----------------------------------------------------------

/// A `Reference` **is** `Send`, and the compiler therefore does *not* catch the guard held
/// across an `.await` inside a `tokio::spawn`.
///
/// `dashmap`'s own lock guard is `!Send` (`GuardMarker = GuardNoSend`, dashmap-6.2.1
/// `src/lock.rs:23`), but `Ref` re-adds the impl by hand:
/// `unsafe impl<K: Eq + Hash + Sync, V: Sync> Send for Ref<'_, K, V>`
/// (dashmap-6.2.1 `src/mapref/one.rs:13`). Every key and value this cache stores is `Sync`,
/// so every `Reference` this cache hands out is `Send`.
///
/// This compiles, which is the whole point of the test. If a future `dashmap` drops that
/// impl the line below stops compiling, and the documentation in `reference.rs` can be
/// strengthened again — but until then it must not promise a safety net that is not there.
#[test]
fn a_reference_is_send_so_the_compiler_does_not_catch_the_await_hazard() {
    fn assert_send<T: Send>() {}
    assert_send::<Reference<'static, RoomId, Room>>();
    assert_send::<Reference<'static, UserId, User>>();
    assert_send::<Reference<'static, MessageId, Message>>();
}

// -- 8. the merge/replace polarity against the real wire ---------------------------------

/// `replace_room` is documented for "a REST response, a `rooms/get` sync". But `rooms/get`
/// *is* the projected payload: `roomsGetMethod` passes `{ projection: roomFields }`
/// (apps/meteor/server/publications/room/index.ts:29), and `roomFields` does not list
/// `uids` and has `usernames` commented out (apps/meteor/lib/publishFields.ts:56-62).
///
/// So following the documentation exactly — merge the stream, replace the sync — blanks
/// fields the cache already held.
///
/// BUG (reported, not fixed — the fix is to re-aim the documentation, and to say which
/// payloads really are complete).
#[test]
fn replacing_with_a_rooms_get_payload_drops_the_fields_that_projection_omits() {
    let cache = Cache::new();

    // A full room, e.g. from `channels.info`, which is not projected.
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
        "uids": ["u1", "u2"], "usernames": ["alice", "bob"], "topic": "hi",
    })));

    // What `rooms/get` actually returns for the same room.
    cache.replace_room(room(json!({
        "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "name": "general", "topic": "hi",
    })));

    let cached = cache.room(&room_id("r1")).expect("cached");
    assert!(cached.uids.is_none(), "current behaviour: the projection blanked uids");
    assert!(cached.usernames.is_none());
}

/// The counterpart: `watch.rooms` carries the **whole** room document, not a projection.
/// `notifyOnRoomChangedById` reads it with `Rooms.findByIds(eligibleIds)` and no projection
/// (apps/meteor/server/lib/notifyListener.ts:65-74), and the listener forwards it verbatim
/// to `rooms-changed` and `stream-room-data`
/// (apps/meteor/server/modules/listeners/listeners.module.ts:335-340). Direct callers do the
/// same: `archiveRoom` passes `Rooms.findOneById(rid)`.
///
/// So on the room stream an absent key really does mean "unset", and `update()` cannot see
/// it. `Rooms.setSystemMessagesById` (packages/models/src/models/Rooms.ts:1015) and
/// `Rooms.unsetTeamId` (Rooms.ts:445) are two `$unset`s that reach it; the announcement is
/// pinned here because it is the one a bot is most likely to read back.
#[test]
fn a_cleared_field_on_the_full_room_stream_document_survives_the_merge() {
    let cache = Cache::new();

    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "name": "general",
        "announcement": "maintenance at 5", "topic": "hi",
    })));

    // The whole document, after the announcement was cleared.
    cache.update(&room(json!({
        "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "name": "general", "topic": "hi",
    })));

    assert_eq!(
        cache.room(&room_id("r1")).expect("cached").announcement.as_deref(),
        Some("maintenance at 5"),
        "current behaviour: the cleared announcement is still cached",
    );

    // `replace_room` is the only escape, and it is correct here precisely because the stream
    // payload is complete.
    cache.replace_room(room(json!({
        "_id": "r1", "_updatedAt": {"$date": 3}, "t": "c", "name": "general", "topic": "hi",
    })));
    assert!(cache.room(&room_id("r1")).expect("cached").announcement.is_none());
}
