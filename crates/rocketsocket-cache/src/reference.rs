//! Borrowed access to a cached value.

use core::fmt;
use core::hash::Hash;
use core::ops::Deref;

/// A borrowed reference to a value inside the cache.
///
/// # Do not hold one across an `.await`
///
/// A `Reference` is a **live read lock** on one shard of the underlying concurrent map. It
/// is not a snapshot, and it is not cheap to keep. While one is alive:
///
/// - every write to a key that hashes to the same shard blocks, and
/// - a writer that is already queued on that shard blocks every *other* reader of it.
///
/// So holding a `Reference` across a suspension point — an `.await`, a channel `recv`, a
/// blocking HTTP call — hands the shard to a task that may not run again until something
/// else completes, and if that something else is a cache write, the two deadlock. This is
/// the single most likely way to hang a program with this crate.
///
/// ```ignore
/// // WRONG: the guard is alive across the await, and `send_message` may take seconds.
/// let room = cache.room(&id).unwrap();
/// client.send_message(room.name.as_deref().unwrap()).await?;
/// ```
///
/// ```ignore
/// // RIGHT: take what you need, drop the guard, then await.
/// let name = cache.room(&id).and_then(|room| room.name.clone());
/// client.send_message(name.as_deref().unwrap()).await?;
/// ```
///
/// [`Reference::cloned`] exists to make the right version a one-liner, and the accessors
/// for small hot values ([`Cache::current_user`](crate::Cache::current_user),
/// [`Cache::room_id_by_name`](crate::Cache::room_id_by_name),
/// [`Cache::room_message_ids`](crate::Cache::room_message_ids)) return owned values
/// precisely so that no guard exists to mishandle.
///
/// **The compiler will not catch this for you.** `dashmap`'s own lock guard is `!Send`, but
/// `Ref` re-adds the impl by hand — `unsafe impl<K: Eq + Hash + Sync, V: Sync> Send for
/// Ref<'_, K, V>` — and every key and value this cache stores is `Sync`, so a `Reference`
/// held across an `.await` inside a `tokio::spawn` compiles and deadlocks at runtime. There
/// is no borrow-checker backstop here, only this paragraph.
///
/// # Why this is a newtype
///
/// The concurrent map is an implementation detail. Exposing its guard type would make the
/// map part of this crate's public API and impossible to replace without a breaking
/// change, which is exactly the trap serenity fell into by handing out its map references.
pub struct Reference<'a, K, V>
where
    K: Eq + Hash,
{
    inner: dashmap::mapref::one::Ref<'a, K, V>,
}

impl<'a, K, V> Reference<'a, K, V>
where
    K: Eq + Hash,
{
    pub(crate) fn new(inner: dashmap::mapref::one::Ref<'a, K, V>) -> Self {
        Self { inner }
    }

    /// The key the value is stored under.
    pub fn key(&self) -> &K {
        self.inner.key()
    }

    /// The value.
    pub fn value(&self) -> &V {
        self.inner.value()
    }

    /// The key and value together.
    pub fn pair(&self) -> (&K, &V) {
        self.inner.pair()
    }

    /// Clones the value out, so the guard can be dropped.
    ///
    /// This is the safe way to carry a cached value across an `.await`:
    ///
    /// ```no_run
    /// # use rocketsocket_cache::Cache;
    /// # use rocketsocket_model::RoomId;
    /// # async fn example(cache: &Cache, id: RoomId) {
    /// let room = cache.room(&id).map(|room| room.cloned());
    /// // no guard is alive here
    /// # let _ = room;
    /// # }
    /// ```
    pub fn cloned(&self) -> V
    where
        V: Clone,
    {
        self.inner.value().clone()
    }
}

impl<K, V> Deref for Reference<'_, K, V>
where
    K: Eq + Hash,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.inner.value()
    }
}

impl<K, V> fmt::Debug for Reference<'_, K, V>
where
    K: Eq + Hash + fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reference").field("key", self.key()).field("value", self.value()).finish()
    }
}
