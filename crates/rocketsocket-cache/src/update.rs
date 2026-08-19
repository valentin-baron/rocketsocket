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
/// # Applying a payload merges
///
/// Rocket.Chat projects documents per publication, so a payload's `None` usually means "not
/// carried" rather than "empty" and must not blank what is already cached. See the crate-level documentation on partial
/// updates. Use [`Cache::replace_room`] and its siblings for a
/// complete document.
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
    fn update(&self, cache: &Cache) {
        cache.store_room(self.clone(), StoreMode::Merge);
    }
}

impl UpdateCache for User {
    /// Also refreshes [`Cache::current_user`] when this is the logged-in user, so the two
    /// copies cannot drift.
    fn update(&self, cache: &Cache) {
        cache.store_user(self.clone(), StoreMode::Merge);
        cache.refresh_current_user(self, StoreMode::Merge);
    }
}

impl UpdateCache for Subscription {
    fn update(&self, cache: &Cache) {
        cache.store_subscription(self.clone(), StoreMode::Merge);
    }
}

impl UpdateCache for Message {
    /// Note what this does **not** do: it does not cache [`Message::u`] as a user.
    ///
    /// `u` is a three-field stub (`_id`, `username?`, `name?`), not a user document.
    /// Storing it as one would fill the users map with entries that answer
    /// [`Cache::user`] successfully while knowing nothing — no roles, no status, no
    /// `active` — which a caller cannot tell apart from a real document and is strictly
    /// worse than a miss, since a miss at least routes to the server. It would also churn
    /// the user eviction queue once per message. Cache the author yourself if you want it,
    /// from a real `users.info`.
    fn update(&self, cache: &Cache) {
        cache.store_message(self.clone(), StoreMode::Merge);
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
