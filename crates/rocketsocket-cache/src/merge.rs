//! Merging a projected document into a cached one.
//!
//! # The problem
//!
//! Rocket.Chat never promises to send you a whole document. Each publication applies its
//! own projection — `publishFields.ts` for the room and subscription streams, a
//! `{_id: 1, roles: 1}` projection for the user cache, `Pick<IUser, '_id'|'name'|'username'>`
//! for `Users:NameChanged` — so the same [`Room`](rocketsocket_model::entity::Room) arrives
//! with different field sets depending on which stream produced it, and the field sets are
//! not nested: they overlap partially.
//!
//! Every projected-away field decodes to `None`, because the model crate deliberately makes
//! everything optional rather than failing. That leaves the cache unable to distinguish
//! *"the server says this field is now empty"* from *"this payload did not carry the
//! field"* — and if it guesses wrong in the second case, a cached room's topic, name or
//! member list is silently blanked by an unrelated update. That is the subtlest correctness
//! bug this crate can have, and the whole of this module exists to avoid it.
//!
//! # The rule
//!
//! **A field absent from the incoming payload never overwrites a cached value.** Only fields
//! the payload actually carried are written.
//!
//! # How
//!
//! Both documents are serialized to JSON and the incoming object's keys are written over the
//! cached object's, top level only. This works because of a property the model crate
//! guarantees and this crate depends on: **every optional field is
//! `#[serde(skip_serializing_if = "Option::is_none")]`**, so a field that is `None` — for
//! whatever reason — produces no key, and a field the payload carried always produces one.
//! Required fields (`_id`, `_updatedAt`, `msg`, `t`, …) are always present in both and so
//! always take the incoming value.
//!
//! The merge is deliberately **shallow**. A nested object — `u`, `lastMessage`,
//! `announcementDetails` — is replaced wholesale rather than merged field by field, because
//! that is what DDP itself does: Meteor's diffing operates on top-level document keys and
//! resends a whole subdocument when any part of it changes. Merging deeper would invent a
//! semantics the server does not have, and would be actively wrong for arrays.
//!
//! # What this cannot do
//!
//! It cannot see a field being *cleared*. A DDP `cleared` entry and an explicit `null` both
//! decode to `None`, which is indistinguishable from "not projected", so a merge keeps the
//! old value. Use [`Cache::replace_room`](crate::Cache::replace_room) and its siblings when
//! the payload is known to be a complete document (a REST response, a `rooms/get` sync),
//! and the removal methods when the document is gone.
//!
//! # Cost
//!
//! Two `serde_json::to_value` conversions and one `from_value` per update. That is not free,
//! and it is chosen knowingly: the alternative is 72 hand-written field assignments for
//! `Room` alone, which would silently regain the blanking bug the first time the model grows
//! a field and someone forgets to add a line. Correctness that survives model changes is
//! worth the allocation; if a profile ever says otherwise, this is the one function to
//! specialize.
//!
//! It used to cost more. Until the model's EJSON timestamp visitor accepted an owned map key
//! there was no way to deserialize an entity from a [`Value`] at all — `from_value` failed
//! with `invalid type: string "$date", expected a borrowed string` on every entity carrying
//! a timestamp — so the merged document was rendered back to text and reparsed. That detour
//! is gone.

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Returns `cached` overlaid with every field `incoming` actually carried.
///
/// Falls back to `incoming` if either document fails to round-trip through JSON, which
/// would be a bug in the model crate rather than bad input; the newer document is the
/// better answer either way, and losing a merge is better than losing an update.
pub(crate) fn merge_documents<T>(cached: &T, incoming: T) -> T
where
    T: Serialize + DeserializeOwned,
{
    let (Ok(Value::Object(mut merged)), Ok(Value::Object(patch))) =
        (serde_json::to_value(cached), serde_json::to_value(&incoming))
    else {
        tracing::warn!(
            entity = core::any::type_name::<T>(),
            "cache merge fell back to wholesale replacement: document did not serialize to a \
             JSON object"
        );
        return incoming;
    };

    // A key present in `patch` is a field the payload carried; a key only in `merged` is one
    // it did not, and keeps the cached value.
    for (key, value) in patch {
        merged.insert(key, value);
    }

    match serde_json::from_value(Value::Object(merged)) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(
                entity = core::any::type_name::<T>(),
                %error,
                "cache merge fell back to wholesale replacement: merged document did not decode"
            );
            incoming
        }
    }
}

/// The set of top-level keys a document serializes to.
///
/// Only used by tests and diagnostics, but it documents the invariant the merge relies on:
/// a `None` field must not produce a key.
#[cfg(test)]
pub(crate) fn carried_fields<T: Serialize>(value: &T) -> Vec<String> {
    match serde_json::to_value(value) {
        Ok(Value::Object(map)) => map.keys().cloned().collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
pub(crate) fn is_object<T: Serialize>(value: &T) -> bool {
    matches!(serde_json::to_value(value), Ok(Value::Object(_)))
}

#[cfg(test)]
mod tests {
    use rocketsocket_model::entity::{Message, Room, Subscription, User};
    use serde_json::json;

    use super::{carried_fields, is_object, merge_documents};

    fn entity<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
        serde_json::from_value(value).expect("fixture")
    }

    fn room(value: serde_json::Value) -> Room {
        entity(value)
    }

    #[test]
    fn absent_fields_do_not_blank_cached_ones() {
        let cached = room(json!({
            "_id": "room1",
            "_updatedAt": {"$date": 1},
            "t": "c",
            "name": "general",
            "topic": "the topic",
            "description": "the description",
            "usersCount": 12,
        }));

        // A `stream-notify-room` projection that carries neither topic nor description.
        let incoming = room(json!({
            "_id": "room1",
            "_updatedAt": {"$date": 2},
            "t": "c",
            "name": "general",
            "usersCount": 13,
        }));

        let merged = merge_documents(&cached, incoming);

        assert_eq!(merged.topic.as_deref(), Some("the topic"));
        assert_eq!(merged.description.as_deref(), Some("the description"));
        assert_eq!(merged.users_count, Some(13));
        assert_eq!(merged.updated_at.unix_millis(), 2);
    }

    #[test]
    fn carried_fields_win_even_when_they_shrink_a_value() {
        let cached = room(json!({
            "_id": "room1", "_updatedAt": {"$date": 1}, "t": "c",
            "topic": "the topic",
        }));
        let incoming = room(json!({
            "_id": "room1", "_updatedAt": {"$date": 2}, "t": "c",
            "topic": "",
        }));

        let merged = merge_documents(&cached, incoming);

        assert_eq!(merged.topic.as_deref(), Some(""));
    }

    #[test]
    fn merge_is_shallow_so_subdocuments_are_replaced() {
        // Meteor resends a whole subdocument when any part of it changes, so merging into
        // one would invent a semantics the wire does not have.
        let cached: Message = entity(json!({
            "_id": "m1", "_updatedAt": {"$date": 1}, "rid": "r1", "msg": "hi",
            "ts": {"$date": 1},
            "u": {"_id": "u1", "username": "alice", "name": "Alice"},
        }));

        let incoming: Message = entity(json!({
            "_id": "m1", "_updatedAt": {"$date": 2}, "rid": "r1", "msg": "hi",
            "ts": {"$date": 1},
            "u": {"_id": "u1"},
        }));

        let merged = merge_documents(&cached, incoming);

        assert_eq!(merged.u.username, None);
        assert_eq!(merged.u.name, None);
    }

    #[test]
    fn arrays_are_replaced_not_unioned() {
        let cached = room(json!({
            "_id": "r1", "_updatedAt": {"$date": 1}, "t": "p",
            "usernames": ["alice", "bob"],
        }));
        let incoming = room(json!({
            "_id": "r1", "_updatedAt": {"$date": 2}, "t": "p",
            "usernames": ["alice"],
        }));

        let merged = merge_documents(&cached, incoming);

        assert_eq!(merged.usernames.as_deref(), Some(&["alice".to_owned()][..]));
    }

    #[test]
    fn an_explicit_null_is_indistinguishable_from_an_omission() {
        // Documented limitation: the model decodes `null` to `None` exactly as it does an
        // absent key, so a merge cannot see a field being cleared.
        let cached = room(json!({
            "_id": "r1", "_updatedAt": {"$date": 1}, "t": "c", "topic": "kept",
        }));
        let incoming = room(json!({
            "_id": "r1", "_updatedAt": {"$date": 2}, "t": "c", "topic": null,
        }));

        let merged = merge_documents(&cached, incoming);

        assert_eq!(merged.topic.as_deref(), Some("kept"));
    }

    #[test]
    fn every_entity_round_trips_through_json() {
        // The merge is only sound if serialize -> deserialize is lossless for the cached
        // types. If the model ever grows a field that does not round-trip, this fails here
        // rather than by silently dropping data on the next update.
        let room = room(json!({
            "_id": "r1", "_updatedAt": {"$date": 5}, "t": "c", "name": "general",
            "topic": "t", "usersCount": 3, "msgs": 9, "ro": false,
            "u": {"_id": "u1", "username": "alice"},
            "lastMessage": {
                "_id": "m1", "_updatedAt": {"$date": 4}, "rid": "r1", "msg": "hi",
                "ts": {"$date": 4}, "u": {"_id": "u1", "username": "alice"},
            },
            "sysMes": ["uj"],
            "customFields": {"a": 1},
        }));
        assert_eq!(merge_documents(&room, room.clone()), room);

        let user: User = entity(json!({
            "_id": "u1", "username": "alice", "name": "Alice", "roles": ["admin", "user"],
            "status": "online", "statusText": "hi", "utcOffset": 1.5,
            "emails": [{"address": "a@example.com", "verified": true}],
        }));
        assert_eq!(merge_documents(&user, user.clone()), user);

        let subscription: Subscription = entity(json!({
            "_id": "s1", "_updatedAt": {"$date": 1}, "rid": "r1", "t": "c",
            "ts": {"$date": 1}, "name": "general", "open": true, "unread": 2,
            "userMentions": 1, "groupMentions": 0, "u": {"_id": "u1", "username": "alice"},
            "roles": ["owner"], "ls": {"$date": 2},
        }));
        assert_eq!(merge_documents(&subscription, subscription.clone()), subscription);

        let message: Message = entity(json!({
            "_id": "m1", "_updatedAt": {"$date": 1}, "rid": "r1", "msg": "hi",
            "ts": {"$date": 1}, "u": {"_id": "u1", "username": "alice"},
            "reactions": {":tada:": {"usernames": ["alice"]}},
            "starred": [{"_id": "u1"}], "pinned": true, "editedAt": {"$date": 3},
            "urls": [{"url": "https://example.com"}],
            "attachments": [{"text": "a"}],
        }));
        assert_eq!(merge_documents(&message, message.clone()), message);
    }

    #[test]
    fn optional_fields_never_serialize_a_key() {
        // The invariant the whole module rests on.
        let sparse = room(json!({"_id": "r1", "_updatedAt": {"$date": 1}, "t": "c"}));

        assert!(is_object(&sparse));
        assert_eq!(carried_fields(&sparse), vec!["_id", "_updatedAt", "t"]);
    }
}
