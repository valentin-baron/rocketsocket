//! The cache itself.

use core::fmt;
use core::hash::Hash;
use std::collections::VecDeque;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use parking_lot::{Mutex, RwLock};
use rocketsocket_model::entity::{Message, Room, Subscription, User};
use rocketsocket_model::{MessageId, RoomId, UserId};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::config::{CacheBuilder, Config, TombstonePolicy};
use crate::merge::merge_documents;
use crate::reference::Reference;
use crate::resource::ResourceType;
use crate::update::UpdateCache;

/// Whether a write merges into what is already cached or replaces it outright.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreMode {
    /// Overlay only the fields the payload carried. The right thing for a stream frame.
    Merge,
    /// Store the payload as the whole truth. The right thing for a complete document.
    Replace,
}

/// Counts of what the cache currently holds.
///
/// A snapshot, taken without a consistent view across the maps: two counts may come from
/// either side of a concurrent write. Useful for logging and tests, not for invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct CacheStats {
    /// Cached rooms.
    pub rooms: usize,
    /// Cached users, excluding the current user if it has been evicted from the map.
    pub users: usize,
    /// Cached subscriptions.
    pub subscriptions: usize,
    /// Cached messages, across all rooms.
    pub messages: usize,
    /// Rooms with a non-empty message ring.
    pub rooms_with_messages: usize,
}

/// An in-memory cache of Rocket.Chat entities.
///
/// Share one with [`Arc`](std::sync::Arc): every write path takes `&self`, so a single
/// cache can be updated from the event loop and read from handlers concurrently.
///
/// ```
/// use rocketsocket_cache::Cache;
/// use rocketsocket_model::entity::Room;
///
/// let cache = Cache::new();
/// let room: Room = serde_json::from_str(
///     r#"{"_id":"GENERAL","_updatedAt":{"$date":1},"t":"c","name":"general"}"#,
/// )
/// .unwrap();
///
/// cache.update(&room);
///
/// // The lookup every Rocket.Chat bot makes, answered without a round trip.
/// assert_eq!(cache.room_id_by_name("general").unwrap(), room.id);
/// ```
pub struct Cache {
    config: Config,
    rooms: DashMap<RoomId, Room>,
    rooms_by_name: DashMap<String, RoomId>,
    users: DashMap<UserId, User>,
    users_by_username: DashMap<String, UserId>,
    subscriptions: DashMap<RoomId, Subscription>,
    messages: DashMap<MessageId, Message>,
    room_messages: DashMap<RoomId, VecDeque<MessageId>>,
    current_user: RwLock<Option<User>>,
    /// First-insertion order of the ids in `users`, maintained only when a user capacity is
    /// configured. See [`Config::user_cache_size`].
    user_order: Mutex<VecDeque<UserId>>,
}

impl Cache {
    /// A cache with the default configuration: everything cached, 100 messages per room,
    /// 10 000 users.
    #[must_use]
    pub fn new() -> Self {
        Self::from_config(Config::default())
    }

    /// A builder, for any other configuration.
    #[must_use]
    pub fn builder() -> CacheBuilder {
        CacheBuilder::new()
    }

    pub(crate) fn from_config(config: Config) -> Self {
        Self {
            config,
            rooms: DashMap::new(),
            rooms_by_name: DashMap::new(),
            users: DashMap::new(),
            users_by_username: DashMap::new(),
            subscriptions: DashMap::new(),
            messages: DashMap::new(),
            room_messages: DashMap::new(),
            current_user: RwLock::new(None),
            user_order: Mutex::new(VecDeque::new()),
        }
    }

    /// The configuration this cache was built with.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Whether every kind in `resource_type` is cached.
    ///
    /// Every write path checks this first, so a cleared flag is a guarantee that nothing of
    /// that kind is stored — not merely that it is not read back.
    #[must_use]
    pub fn wants(&self, resource_type: ResourceType) -> bool {
        self.config.resource_types().contains(resource_type)
    }

    /// Applies a payload.
    ///
    /// This **merges**: a field the payload did not carry keeps its cached value. See the
    /// the crate-level documentation on partial updates module for why, and use [`Cache::replace_room`] and its
    /// siblings when the payload is a complete document.
    pub fn update<T: UpdateCache + ?Sized>(&self, value: &T) {
        value.update(self);
    }

    // -- rooms ---------------------------------------------------------------------------

    /// The cached room, if any.
    ///
    /// The returned [`Reference`] holds a shard lock: **do not hold it across an `.await`**.
    #[must_use]
    pub fn room(&self, id: &RoomId) -> Option<Reference<'_, RoomId, Room>> {
        self.rooms.get(id).map(Reference::new)
    }

    /// The id of the room with this `name`, if it has been seen.
    ///
    /// This is the lookup Rocket.Chat bots actually make — the JS SDK carried an LRU for
    /// nothing else — so it answers with an owned id and holds no lock afterwards.
    ///
    /// Matching is exact and case-sensitive, against `Room::name` only. `fname` is not
    /// indexed: for a discussion or a team it holds a display name that is not unique, and
    /// for a room created with `UI_Allow_room_names_with_special_chars` it is the
    /// unnormalized form the server itself does not look rooms up by.
    ///
    /// The index is updated just after the room itself, not atomically with it. A reader
    /// racing a writer can therefore see a room through [`Cache::room`] a moment before
    /// its name resolves here, and two concurrent renames of the same room can leave the
    /// index pointing at the older of the two names until the next update carries one. The
    /// alternative — one lock over both maps — would serialize the hottest read in the
    /// crate behind every write, which is a worse trade for a cache.
    #[must_use]
    pub fn room_id_by_name(&self, name: &str) -> Option<RoomId> {
        self.rooms_by_name.get(name).map(|entry| entry.value().clone())
    }

    /// The cached room with this `name`. See [`Cache::room_id_by_name`] for the matching
    /// rules, and [`Reference`] for the borrow rules.
    #[must_use]
    pub fn room_by_name(&self, name: &str) -> Option<Reference<'_, RoomId, Room>> {
        let id = self.room_id_by_name(name)?;
        self.room(&id)
    }

    /// A snapshot of every cached room id.
    #[must_use]
    pub fn room_ids(&self) -> Vec<RoomId> {
        self.rooms.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Stores a room as the whole truth, dropping cached fields it does not carry.
    ///
    /// Use this for a complete document — a REST response, a `rooms/get` sync — where a
    /// field the payload omits genuinely means the server has unset it. Use
    /// [`Cache::update`] for stream frames, which are projected.
    ///
    /// Returns the previous document.
    pub fn replace_room(&self, room: Room) -> Option<Room> {
        self.store_room(room, StoreMode::Replace)
    }

    /// Removes a room, its name index entry, its subscription, and every message cached
    /// for it.
    ///
    /// The cascade is deliberate: the messages of a room that is gone are unreachable
    /// through any index and would never be evicted, since the ring only evicts on insert.
    pub fn remove_room(&self, id: &RoomId) -> Option<Room> {
        let removed = self.rooms.remove(id).map(|(_, room)| room);
        if let Some(name) = removed.as_ref().and_then(|room| room.name.as_ref()) {
            self.rooms_by_name.remove_if(name, |_, current| current == id);
        }
        self.subscriptions.remove(id);
        if let Some((_, ring)) = self.room_messages.remove(id) {
            for message in ring {
                self.messages.remove(&message);
            }
        }
        removed
    }

    pub(crate) fn store_room(&self, incoming: Room, mode: StoreMode) -> Option<Room> {
        if !self.wants(ResourceType::ROOM) {
            return None;
        }

        let id = incoming.id.clone();
        let (previous, new_name) =
            upsert(&self.rooms, id.clone(), incoming, mode, |room| room.name.clone());
        let old_name = previous.as_ref().and_then(|room| room.name.clone());
        reindex(&self.rooms_by_name, old_name, new_name, &id);
        previous
    }

    // -- users ---------------------------------------------------------------------------

    /// The cached user, if any.
    ///
    /// `None` means "not cached", never "no such user": users are evicted by default, and
    /// a message can reference an author whose document has never been seen. Every caller
    /// needs a server-side fallback. See [`Config::user_cache_size`].
    ///
    /// The returned [`Reference`] holds a shard lock: **do not hold it across an `.await`**.
    #[must_use]
    pub fn user(&self, id: &UserId) -> Option<Reference<'_, UserId, User>> {
        self.users.get(id).map(Reference::new)
    }

    /// The id of the user with this username, if it has been seen.
    ///
    /// Matching is exact and case-sensitive. Note that Rocket.Chat usernames are unique
    /// case-insensitively, so a lookup with the wrong case misses a user the server would
    /// have found.
    ///
    /// As with [`Cache::room_id_by_name`], the index is updated just after the document and
    /// not atomically with it.
    #[must_use]
    pub fn user_id_by_username(&self, username: &str) -> Option<UserId> {
        self.users_by_username.get(username).map(|entry| entry.value().clone())
    }

    /// The cached user with this username. See [`Reference`] for the borrow rules.
    #[must_use]
    pub fn user_by_username(&self, username: &str) -> Option<Reference<'_, UserId, User>> {
        let id = self.user_id_by_username(username)?;
        self.user(&id)
    }

    /// A snapshot of every cached user id.
    #[must_use]
    pub fn user_ids(&self) -> Vec<UserId> {
        self.users.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Stores a user as the whole truth. See [`Cache::replace_room`].
    pub fn replace_user(&self, user: User) -> Option<User> {
        let previous = self.store_user(user.clone(), StoreMode::Replace);
        self.refresh_current_user(&user, StoreMode::Replace);
        previous
    }

    /// Removes a user and its username index entry.
    ///
    /// Does **not** clear [`Cache::current_user`], which is stored separately and outlives
    /// the users map on purpose.
    pub fn remove_user(&self, id: &UserId) -> Option<User> {
        let removed = self.drop_user(id);
        if removed.is_some() && self.config.user_cache_size().is_some() {
            let mut order = self.user_order.lock();
            order.retain(|candidate| candidate != id);
        }
        removed
    }

    pub(crate) fn store_user(&self, incoming: User, mode: StoreMode) -> Option<User> {
        if !self.wants(ResourceType::USER) {
            return None;
        }

        let id = incoming.id.clone();
        let (previous, new_username) =
            upsert(&self.users, id.clone(), incoming, mode, |user| user.username.clone());
        let old_username = previous.as_ref().and_then(|user| user.username.clone());
        reindex(&self.users_by_username, old_username, new_username, &id);

        if previous.is_none() {
            self.note_new_user(&id);
        }
        previous
    }

    /// Removes a user without touching the eviction queue.
    fn drop_user(&self, id: &UserId) -> Option<User> {
        let removed = self.users.remove(id).map(|(_, user)| user);
        if let Some(username) = removed.as_ref().and_then(|user| user.username.as_ref()) {
            self.users_by_username.remove_if(username, |_, current| current == id);
        }
        removed
    }

    /// Records a newly inserted user and evicts down to capacity.
    ///
    /// The queue lock is never held while touching the users map — the eviction list is
    /// collected first and acted on after the lock is released — so the two locks are never
    /// nested and cannot deadlock against [`Cache::remove_user`], which takes them the other
    /// way round.
    fn note_new_user(&self, id: &UserId) {
        let Some(capacity) = self.config.user_cache_size() else {
            return;
        };

        let over_capacity = self.users.len().saturating_sub(capacity.get());
        let mut evicted = Vec::new();
        {
            let mut order = self.user_order.lock();
            order.push_back(id.clone());
            for _ in 0..over_capacity {
                match order.pop_front() {
                    // Never evict the user that just arrived; it is the freshest thing here.
                    Some(oldest) if oldest == *id => {
                        order.push_front(oldest);
                        break;
                    }
                    Some(oldest) => evicted.push(oldest),
                    None => break,
                }
            }
        }

        for victim in evicted {
            self.drop_user(&victim);
        }
    }

    // -- the current user ----------------------------------------------------------------

    /// The logged-in user.
    ///
    /// Returns an owned clone rather than a guard: this is read on nearly every event —
    /// "is this message mine?" — and handing out a shard lock for it would be an invitation
    /// to hold one across an `.await`.
    #[must_use]
    pub fn current_user(&self) -> Option<User> {
        self.current_user.read().clone()
    }

    /// The logged-in user's id, without cloning the document.
    #[must_use]
    pub fn current_user_id(&self) -> Option<UserId> {
        self.current_user.read().as_ref().map(|user| user.id.clone())
    }

    /// Sets the logged-in user, replacing any previous one, and stores it in the users map.
    ///
    /// The copy kept here is never evicted, so `current_user` keeps answering long after
    /// the users map has cycled.
    pub fn set_current_user(&self, user: User) {
        if self.wants(ResourceType::CURRENT_USER) {
            *self.current_user.write() = Some(user.clone());
        }
        self.store_user(user, StoreMode::Merge);
    }

    /// Applies a user document to the current user, if it is the same user.
    pub(crate) fn refresh_current_user(&self, incoming: &User, mode: StoreMode) {
        if !self.wants(ResourceType::CURRENT_USER) {
            return;
        }

        // Fast path under a read lock: almost every user update is about somebody else.
        let is_current = matches!(
            self.current_user.read().as_ref(),
            Some(current) if current.id == incoming.id
        );
        if !is_current {
            return;
        }

        let mut guard = self.current_user.write();
        if let Some(current) = guard.as_ref()
            && current.id == incoming.id
        {
            *guard = Some(match mode {
                StoreMode::Merge => merge_documents(current, incoming.clone()),
                StoreMode::Replace => incoming.clone(),
            });
        }
    }

    // -- subscriptions -------------------------------------------------------------------

    /// The cached subscription for a room.
    ///
    /// Subscriptions are keyed by **room** id, not by their own `_id`: a client only ever
    /// holds its own subscriptions, one per room, and every question worth asking one
    /// ("am I unread here?", "is this room muted for me?") starts from the room.
    ///
    /// The returned [`Reference`] holds a shard lock: **do not hold it across an `.await`**.
    #[must_use]
    pub fn subscription(&self, room_id: &RoomId) -> Option<Reference<'_, RoomId, Subscription>> {
        self.subscriptions.get(room_id).map(Reference::new)
    }

    /// Stores a subscription as the whole truth. See [`Cache::replace_room`].
    pub fn replace_subscription(&self, subscription: Subscription) -> Option<Subscription> {
        self.store_subscription(subscription, StoreMode::Replace)
    }

    /// Removes the subscription for a room.
    pub fn remove_subscription(&self, room_id: &RoomId) -> Option<Subscription> {
        self.subscriptions.remove(room_id).map(|(_, subscription)| subscription)
    }

    pub(crate) fn store_subscription(
        &self,
        incoming: Subscription,
        mode: StoreMode,
    ) -> Option<Subscription> {
        if !self.wants(ResourceType::SUBSCRIPTION) {
            return None;
        }

        let (previous, ()) =
            upsert(&self.subscriptions, incoming.rid.clone(), incoming, mode, |_| ());
        previous
    }

    // -- messages ------------------------------------------------------------------------

    /// The cached message, if any.
    ///
    /// The returned [`Reference`] holds a shard lock: **do not hold it across an `.await`**.
    #[must_use]
    pub fn message(&self, id: &MessageId) -> Option<Reference<'_, MessageId, Message>> {
        self.messages.get(id).map(Reference::new)
    }

    /// The ids cached for a room, **newest first**, as an owned snapshot.
    ///
    /// Ordering is by arrival, not by `ts` and not by `_updatedAt`: an edit or a reaction
    /// updates a message in place and does not move it. The list is bounded by
    /// [`Config::message_cache_size`].
    #[must_use]
    pub fn room_message_ids(&self, room_id: &RoomId) -> Vec<MessageId> {
        self.room_messages
            .get(room_id)
            .map(|ring| ring.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The id of the most recently arrived message cached for a room.
    #[must_use]
    pub fn newest_message_id(&self, room_id: &RoomId) -> Option<MessageId> {
        self.room_messages.get(room_id).and_then(|ring| ring.front().cloned())
    }

    /// Stores a message as the whole truth. See [`Cache::replace_room`].
    pub fn replace_message(&self, message: Message) -> Option<Message> {
        self.store_message(message, StoreMode::Replace)
    }

    /// Removes a message and unlinks it from its room's ring.
    ///
    /// # Deletions only ever arrive on a stream
    ///
    /// There is no such thing as a "deleted" message mutation. A real deletion — the
    /// `Message_ShowDeletedStatus`-off case, where the document is actually removed —
    /// is observable **only** as a `stream-notify-room` event on the `<rid>/deleteMessage`
    /// key, carrying just `{_id, ts}`. Nothing on `stream-room-messages` will ever tell you
    /// about it.
    ///
    /// A cache fed only messages therefore grows stale entries forever: every deleted
    /// message stays cached, and stays in its room's ring, until the ring evicts it or the
    /// process exits. Wiring `<rid>/deleteMessage` to this method is not optional.
    ///
    /// Prefer [`Cache::remove_message_in`] when you have the room id — the delete event
    /// always gives you one, since it is keyed by it.
    pub fn remove_message(&self, id: &MessageId) -> Option<Message> {
        let removed = self.messages.remove(id).map(|(_, message)| message);
        if let Some(message) = removed.as_ref() {
            self.unlink_message(&message.rid, id);
        }
        removed
    }

    /// Removes a message from a known room, unlinking it from that room's ring.
    ///
    /// Unlike [`Cache::remove_message`] this works even when the message document itself
    /// was never cached, which is the common case for a bot that started after the message
    /// was sent. See [`Cache::remove_message`] for where deletions come from.
    pub fn remove_message_in(&self, room_id: &RoomId, id: &MessageId) -> Option<Message> {
        let removed = self.messages.remove(id).map(|(_, message)| message);
        self.unlink_message(room_id, id);
        removed
    }

    pub(crate) fn store_message(&self, incoming: Message, mode: StoreMode) -> Option<Message> {
        if !self.wants(ResourceType::MESSAGE) || self.config.message_cache_size() == 0 {
            return None;
        }

        // A tombstone is an edit, not a deletion: `t: "rm"`, empty `msg`, `editedAt` set.
        let mode = if incoming.is_deleted_tombstone() {
            match self.config.tombstone_policy() {
                TombstonePolicy::Evict => {
                    return self.remove_message_in(&incoming.rid, &incoming.id);
                }
                // Replace, never merge: the tombstone carries no attachments, urls or
                // reactions, and merging would resurrect the content the deletion stripped.
                TombstonePolicy::Replace => StoreMode::Replace,
            }
        } else {
            mode
        };

        let room_id = incoming.rid.clone();
        let id = incoming.id.clone();
        let (previous, ()) = upsert(&self.messages, id.clone(), incoming, mode, |_| ());
        if previous.is_none() {
            self.link_message(&room_id, id);
        }
        previous
    }

    /// Pushes an id onto the front of a room's ring and evicts from the back.
    ///
    /// The ring guard is released before anything touches the messages map: the two are
    /// never locked at the same time, in either order.
    fn link_message(&self, room_id: &RoomId, id: MessageId) {
        let capacity = self.config.message_cache_size();
        let mut evicted = Vec::new();
        {
            let mut ring = self.room_messages.entry(room_id.clone()).or_default();
            ring.push_front(id);
            while ring.len() > capacity {
                match ring.pop_back() {
                    Some(oldest) => evicted.push(oldest),
                    None => break,
                }
            }
        }

        for victim in evicted {
            self.messages.remove(&victim);
        }
    }

    fn unlink_message(&self, room_id: &RoomId, id: &MessageId) {
        let emptied = match self.room_messages.get_mut(room_id) {
            Some(mut ring) => {
                ring.retain(|candidate| candidate != id);
                ring.is_empty()
            }
            None => false,
        };
        // The guard above must be dropped before removing from the same map, or the shard
        // deadlocks against itself.
        if emptied {
            self.room_messages.remove_if(room_id, |_, ring| ring.is_empty());
        }
    }

    // -- whole-cache operations ----------------------------------------------------------

    /// Empties the cache, including the current user.
    pub fn clear(&self) {
        self.rooms.clear();
        self.rooms_by_name.clear();
        self.users.clear();
        self.users_by_username.clear();
        self.subscriptions.clear();
        self.messages.clear();
        self.room_messages.clear();
        *self.current_user.write() = None;
        self.user_order.lock().clear();
    }

    /// Counts of what is currently cached.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            rooms: self.rooms.len(),
            users: self.users.len(),
            subscriptions: self.subscriptions.len(),
            messages: self.messages.len(),
            rooms_with_messages: self.room_messages.len(),
        }
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Cache {
    /// Prints the configuration and the counts, never the contents.
    ///
    /// Formatting takes read locks on every map. Do not format a cache from inside a
    /// closure that already holds a write guard on one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cache")
            .field("config", &self.config)
            .field("stats", &self.stats())
            .field("current_user", &self.current_user_id())
            .finish()
    }
}

/// Inserts or updates one entry, atomically with respect to other writers of the same key,
/// and returns the previous value alongside something derived from the stored one.
///
/// `derive` runs while the shard is locked, so it must be cheap and must not touch the
/// cache.
fn upsert<K, V, R>(
    map: &DashMap<K, V>,
    key: K,
    incoming: V,
    mode: StoreMode,
    derive: impl FnOnce(&V) -> R,
) -> (Option<V>, R)
where
    K: Eq + Hash + Clone,
    V: Serialize + DeserializeOwned,
{
    match map.entry(key) {
        Entry::Occupied(mut occupied) => {
            let stored = match mode {
                StoreMode::Merge => merge_documents(occupied.get(), incoming),
                StoreMode::Replace => incoming,
            };
            let derived = derive(&stored);
            let previous = core::mem::replace(occupied.get_mut(), stored);
            (Some(previous), derived)
        }
        Entry::Vacant(vacant) => {
            let derived = derive(&incoming);
            vacant.insert(incoming);
            (None, derived)
        }
    }
}

/// Moves a secondary-index entry from `old` to `new`, without disturbing an entry another
/// key has since claimed.
fn reindex<V>(index: &DashMap<String, V>, old: Option<String>, new: Option<String>, id: &V)
where
    V: Eq + Clone,
{
    if old == new {
        return;
    }
    if let Some(old) = old {
        index.remove_if(&old, |_, current| current == id);
    }
    if let Some(new) = new {
        index.insert(new, id.clone());
    }
}
