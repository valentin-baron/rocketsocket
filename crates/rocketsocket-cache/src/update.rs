//! The trait that feeds the cache.

use rocketsocket_model::entity::{Message, Room, Subscription, User};

use crate::cache::{Cache, StoreMode};

/// A payload the cache knows how to apply.
///
/// Implemented per payload type rather than as one big `match` on an event enum, so that
/// this crate does not depend on the event layer and so that a new stream can be wired in
/// by implementing this on its payload.
///
/// `update` takes `&self`, and so does every method it calls on the cache: writes go
/// through interior mutability, and a shared `Arc<Cache>` can be updated from the event
/// loop while handlers read it.
///
/// # Whether a payload merges or replaces is decided per entity, not globally
///
/// It depends on whether the stream that carries it projects. `Room` and `Message` arrive
/// **whole** and therefore replace; `Subscription` and `User` arrive **partial** and
/// therefore merge. Each impl below states which it is and cites the server code that
/// settles it. See the crate-level documentation for the consequences, and
/// [`Cache::merge_room`] / [`Cache::replace_subscription`] for the escape hatches when a
/// particular source does not match its entity's default.
///
/// # Implementing it
///
/// ```
/// use rocketsocket_cache::{Cache, UpdateCache};
/// use rocketsocket_model::entity::Room;
///
/// /// A `stream-notify-user` `rooms-changed` payload.
/// struct RoomsChanged {
///     rooms: Vec<Room>,
/// }
///
/// impl UpdateCache for RoomsChanged {
///     fn update(&self, cache: &Cache) {
///         cache.update(&self.rooms);
///     }
/// }
/// ```
pub trait UpdateCache {
    /// Applies this payload to `cache`.
    ///
    /// Silently does nothing for any resource kind the cache is not configured to store.
    fn update(&self, cache: &Cache);
}

impl UpdateCache for Room {
    /// Replaces rather than merges.
    ///
    /// The stream sends **complete** room documents: `notifyOnRoomChangedById` reads
    /// `Rooms.findByIds(ids)` with no projection and broadcasts the result verbatim
    /// (`server/lib/notifyListener.ts:63-74`). Merging a complete document is not merely
    /// redundant — it makes an *unset* field unobservable, because a cleared value and a
    /// projected-away value both arrive as absence. `Rooms.unsetTeamId` and
    /// `setSystemMessagesById` both `$unset`, so a merged cache keeps a `teamId` the room
    /// no longer has.
    ///
    /// The projected payloads (`roomFields`, from the `rooms/get` method) are the ones
    /// that need merging — use [`Cache::merge_room`] for those.
    fn update(&self, cache: &Cache) {
        cache.store_room(self.clone(), StoreMode::Replace);
    }
}

impl UpdateCache for User {
    /// Merges, like [`Subscription`] and unlike [`Room`] and [`Message`].
    ///
    /// A user document never arrives whole on a stream after login. `notifyOnUserChange`
    /// broadcasts `{ id, diff, unset }` for every `updated` action and only attaches a whole
    /// `data` for `inserted` (`server/lib/notifyListener.ts:377-389`), and the listener
    /// forwards exactly that to `userData` (`server/modules/listeners/listeners.module.ts`).
    /// The other sources are narrower still: `Users:NameChanged` sends
    /// `Pick<IUser, '_id' | 'name' | 'username'>`, and the publication user cache projects
    /// `{_id: 1, roles: 1}`. Replacing on any of those would blank the rest of the document.
    ///
    /// Note the asymmetry this leaves. The wire *does* say which fields were cleared — that
    /// is what the `unset` member of the payload is — but a [`User`] has nowhere to put it,
    /// so a `User`-shaped payload cannot express a clear and this merge cannot see one. A
    /// caller that decodes `unset` itself should apply it with
    /// [`Cache::replace_user`](Cache::replace_user) on a document it has reconciled.
    ///
    /// Also refreshes [`Cache::current_user`] when this is the logged-in user, so the two
    /// copies cannot drift.
    fn update(&self, cache: &Cache) {
        cache.store_user(self.clone(), StoreMode::Merge);
        cache.refresh_current_user(self, StoreMode::Merge);
    }
}

impl UpdateCache for Subscription {
    /// Merges, unlike [`Room`] and [`Message`].
    ///
    /// Subscriptions are the one entity that genuinely arrives projected:
    /// `notifyOnSubscriptionChanged` sends `subscriptionFields`
    /// (`server/lib/notifyListener.ts:510`), which omits `teamMain`, `teamId`,
    /// `broadcast`, `encrypted` and `userHighlights`. Replacing would blank whatever a
    /// wider read had already established.
    fn update(&self, cache: &Cache) {
        cache.store_subscription(self.clone(), StoreMode::Merge);
    }
}

impl UpdateCache for Message {
    /// Replaces rather than merges, for the same reason as [`Room`].
    ///
    /// `getMessageToBroadcast` reads `Messages.findOneById(id)` with no projection
    /// (`server/lib/notifyListener.ts:442-443`), so a message arrives whole. And removing
    /// the last reaction runs `delete message.reactions` plus `Messages.unsetReactions`
    /// (`app/reactions/server/setReaction.ts:54-56`), which a merge cannot see: the cache
    /// would keep showing a reaction nobody holds.
    ///
    /// Use [`Cache::merge_message`] for a source you know to be partial.
    ///
    /// # What this does **not** do: it does not cache [`Message::u`] as a user
    ///
    /// `u` is a three-field stub (`_id`, `username?`, `name?`), not a user document.
    /// Storing it as one would fill the users map with entries that answer
    /// [`Cache::user`] successfully while knowing nothing — no roles, no status, no
    /// `active` — which a caller cannot tell apart from a real document and is strictly
    /// worse than a miss, since a miss at least routes to the server. It would also churn
    /// the user eviction queue once per message. Cache the author yourself if you want it,
    /// from a real `users.info`.
    fn update(&self, cache: &Cache) {
        cache.store_message(self.clone(), StoreMode::Replace);
    }
}

impl<T: UpdateCache> UpdateCache for [T] {
    fn update(&self, cache: &Cache) {
        for item in self {
            item.update(cache);
        }
    }
}

impl<T: UpdateCache> UpdateCache for Vec<T> {
    fn update(&self, cache: &Cache) {
        self.as_slice().update(cache);
    }
}

impl<T: UpdateCache> UpdateCache for Option<T> {
    fn update(&self, cache: &Cache) {
        if let Some(value) = self {
            value.update(cache);
        }
    }
}
