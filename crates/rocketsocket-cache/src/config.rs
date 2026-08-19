//! Cache configuration.

use core::num::NonZeroUsize;

use crate::cache::Cache;
use crate::resource::ResourceType;

/// The default per-room message ring capacity.
///
/// On by default, unlike serenity's `max_messages: 0`, which silently caches nothing and
/// surprises everyone who assumes `cache.message(id)` works out of the box. 100 ids and
/// documents per room is small enough to leave alone and large enough to answer "what was
/// the message this reply is threaded onto".
pub const DEFAULT_MESSAGE_CACHE_SIZE: usize = 100;

/// The default user capacity.
///
/// See [`Config::user_cache_size`] for why this is bounded at all.
pub const DEFAULT_USER_CACHE_SIZE: usize = 10_000;

/// What to do with a deletion tombstone.
///
/// When the workspace setting `Message_ShowDeletedStatus` is on — and unconditionally for a
/// thread parent — deleting a message does not remove the document. `deleteMessage` calls
/// `Messages.setAsDeletedByIdAndUser`, which rewrites it in place with `t: "rm"`, an empty
/// `msg`, an `editedAt` and an `editedBy`. It therefore arrives on `stream-room-messages` as
/// an ordinary **edit**, not as a deletion, and
/// [`Message::is_deleted_tombstone`](rocketsocket_model::entity::Message::is_deleted_tombstone)
/// is what recognises it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum TombstonePolicy {
    /// Keep the tombstone, replacing the previous body. **The default.**
    ///
    /// The rationale is that the server still has the document: the id resolves, the
    /// message still occupies its place in the room's history, and clients render it as
    /// "Message removed". Evicting would make [`Cache::message`] answer `None`, which is
    /// indistinguishable from "never cached" and would send the caller to REST for a
    /// document that does exist and says exactly this.
    ///
    /// Note that the tombstone **replaces** rather than merges, even though every other
    /// update merges. A tombstone carries no `attachments`, `urls`, `md`, `file` or
    /// `reactions`, so merging would resurrect the very content the deletion stripped —
    /// a cache that answers with the attachments of a deleted message is worse than one
    /// that answers nothing.
    #[default]
    Replace,
    /// Drop the message from the cache and from its room's ring.
    ///
    /// Choose this if you would rather `cache.message(id)` answer `None` for anything
    /// deleted, at the cost of not being able to tell "deleted" from "not cached".
    Evict,
}

/// How the cache is configured.
///
/// Build one with [`Cache::builder`]; the fields are private so that a future release can
/// add one without breaking callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    resource_types: ResourceType,
    message_cache_size: usize,
    user_cache_size: Option<NonZeroUsize>,
    tombstone_policy: TombstonePolicy,
}

impl Config {
    /// Which entity kinds are stored.
    #[must_use]
    pub fn resource_types(&self) -> ResourceType {
        self.resource_types
    }

    /// How many messages are kept per room.
    ///
    /// `0` disables message caching entirely, exactly as clearing
    /// [`ResourceType::MESSAGE`] would.
    #[must_use]
    pub fn message_cache_size(&self) -> usize {
        self.message_cache_size
    }

    /// How many users are kept, or `None` for "never evict".
    ///
    /// # Why users are bounded by default
    ///
    /// Serenity never evicts users, and documents it as a deliberate permanent leak: a
    /// member list, a message author or a mention elsewhere in the cache still holds the
    /// id, so evicting the document leaves a dangling reference. That reasoning is sound
    /// and the conclusion is still wrong for a long-lived bot on a large workspace, where
    /// every presence frame and every message from a stranger adds a document that is
    /// never read again. Unbounded is not a policy, it is the absence of one.
    ///
    /// So this cache evicts, and states the cost plainly:
    ///
    /// - eviction is **first-in, first-out by first insertion**, not LRU. Re-seeing a user
    ///   refreshes the document but does not renew its place in the queue. This is a
    ///   deliberate trade: true LRU needs a touch on every read, which means a write lock
    ///   on the read path, which is the wrong thing to pay for on the hottest lookup in
    ///   the crate.
    /// - an evicted user's id is still referenced by cached messages, rooms and
    ///   subscriptions. [`Cache::user`] then answers `None`, which the caller must treat
    ///   as "ask the server", never as "no such user". Every read of this cache needs that
    ///   fallback anyway — a cache that has not seen a document yet is the same shape.
    /// - the **current user is never evicted**: it lives outside this map, and
    ///   [`Cache::current_user`] keeps answering after the users map has cycled.
    ///
    /// Set it to `None` for serenity's behaviour, with serenity's leak.
    #[must_use]
    pub fn user_cache_size(&self) -> Option<NonZeroUsize> {
        self.user_cache_size
    }

    /// What happens to a deletion tombstone.
    #[must_use]
    pub fn tombstone_policy(&self) -> TombstonePolicy {
        self.tombstone_policy
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            resource_types: ResourceType::all(),
            message_cache_size: DEFAULT_MESSAGE_CACHE_SIZE,
            user_cache_size: NonZeroUsize::new(DEFAULT_USER_CACHE_SIZE),
            tombstone_policy: TombstonePolicy::default(),
        }
    }
}

/// Builds a configured [`Cache`].
///
/// ```
/// use core::num::NonZeroUsize;
/// use rocketsocket_cache::{Cache, ResourceType, TombstonePolicy};
///
/// let cache = Cache::builder()
///     .resource_types(ResourceType::ROOM | ResourceType::USER)
///     .message_cache_size(0)
///     .user_cache_size(NonZeroUsize::new(500))
///     .tombstone_policy(TombstonePolicy::Evict)
///     .build();
///
/// assert!(!cache.wants(ResourceType::MESSAGE));
/// ```
#[derive(Debug, Clone, Default)]
pub struct CacheBuilder {
    config: Config,
}

impl CacheBuilder {
    /// A builder with the default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets which entity kinds are stored. Defaults to [`ResourceType::all`].
    #[must_use]
    pub fn resource_types(mut self, resource_types: ResourceType) -> Self {
        self.config.resource_types = resource_types;
        self
    }

    /// Sets the per-room message ring capacity. Defaults to
    /// [`DEFAULT_MESSAGE_CACHE_SIZE`]; `0` disables message caching.
    #[must_use]
    pub fn message_cache_size(mut self, size: usize) -> Self {
        self.config.message_cache_size = size;
        self
    }

    /// Sets the user capacity. Defaults to [`DEFAULT_USER_CACHE_SIZE`]; `None` never
    /// evicts. See [`Config::user_cache_size`] for what that costs.
    #[must_use]
    pub fn user_cache_size(mut self, size: Option<NonZeroUsize>) -> Self {
        self.config.user_cache_size = size;
        self
    }

    /// Sets what happens to a deletion tombstone. Defaults to
    /// [`TombstonePolicy::Replace`].
    #[must_use]
    pub fn tombstone_policy(mut self, policy: TombstonePolicy) -> Self {
        self.config.tombstone_policy = policy;
        self
    }

    /// Builds the cache.
    #[must_use]
    pub fn build(self) -> Cache {
        Cache::from_config(self.config)
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroUsize;

    use super::{Config, DEFAULT_MESSAGE_CACHE_SIZE, DEFAULT_USER_CACHE_SIZE, TombstonePolicy};
    use crate::ResourceType;

    #[test]
    fn defaults_are_the_documented_ones() {
        let config = Config::default();

        assert_eq!(config.resource_types(), ResourceType::all());
        assert_eq!(config.message_cache_size(), DEFAULT_MESSAGE_CACHE_SIZE);
        assert_eq!(config.message_cache_size(), 100);
        assert_eq!(config.user_cache_size(), NonZeroUsize::new(DEFAULT_USER_CACHE_SIZE));
        assert_eq!(config.tombstone_policy(), TombstonePolicy::Replace);
    }
}
