//! Behavioural tests for the cache.

use core::num::NonZeroUsize;
use core::time::Duration;
use std::sync::Arc;

use rocketsocket_model::entity::{Message, Room, Subscription, User};
use rocketsocket_model::{MessageId, RoomId, UserId};
use serde_json::{Value, json};

use crate::{Cache, ResourceType, TombstonePolicy};

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
