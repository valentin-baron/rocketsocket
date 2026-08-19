//! Which entity kinds the cache is allowed to store.

use core::fmt;
use core::ops::{
    BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Not, Sub, SubAssign,
};

/// A set of entity kinds the cache stores.
///
/// This is a bitflag set rather than a handful of booleans for the reason twilight chose
/// the same: it is `const`, it composes, and adding a kind does not add a field to every
/// call site. Every write path in [`Cache`](crate::Cache) opens with
/// `if !cache.wants(ResourceType::X) { return; }`, so clearing a flag is a hard guarantee
/// that nothing of that kind is ever stored — not merely that it is not read back.
///
/// The default is [`ResourceType::all`]. Cache exactly what you use: a bot that only
/// answers messages by room id has no reason to keep every user document the presence
/// stream pushes at it.
///
/// ```
/// use rocketsocket_cache::{Cache, ResourceType};
///
/// // Rooms and messages only.
/// let cache = Cache::builder()
///     .resource_types(ResourceType::ROOM | ResourceType::MESSAGE)
///     .build();
///
/// assert!(cache.wants(ResourceType::ROOM));
/// assert!(!cache.wants(ResourceType::USER));
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ResourceType(u32);

impl ResourceType {
    /// [`Room`](rocketsocket_model::entity::Room) documents, and the name → id index.
    pub const ROOM: Self = Self(1 << 0);
    /// [`User`](rocketsocket_model::entity::User) documents, and the username → id index.
    pub const USER: Self = Self(1 << 1);
    /// [`Subscription`](rocketsocket_model::entity::Subscription) documents.
    pub const SUBSCRIPTION: Self = Self(1 << 2);
    /// [`Message`](rocketsocket_model::entity::Message) documents, and the per-room ring.
    pub const MESSAGE: Self = Self(1 << 3);
    /// The logged-in user, stored separately from [`ResourceType::USER`] and never evicted.
    pub const CURRENT_USER: Self = Self(1 << 4);

    const ALL: u32 =
        Self::ROOM.0 | Self::USER.0 | Self::SUBSCRIPTION.0 | Self::MESSAGE.0 | Self::CURRENT_USER.0;

    /// Every kind this crate knows about.
    ///
    /// A future release may add kinds to this set, so a cache built with `all()` may start
    /// storing something new across a minor version. List the kinds explicitly if that
    /// matters to you.
    #[must_use]
    pub const fn all() -> Self {
        Self(Self::ALL)
    }

    /// The empty set — a cache that stores nothing at all.
    #[must_use]
    pub const fn empty() -> Self {
        Self(0)
    }

    /// The raw bits.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Builds a set from raw bits, dropping any bit this version does not define.
    #[must_use]
    pub const fn from_bits_truncate(bits: u32) -> Self {
        Self(bits & Self::ALL)
    }

    /// Whether every kind in `other` is in `self`.
    ///
    /// An empty `other` is contained in everything, including the empty set.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Whether `self` and `other` share at least one kind.
    #[must_use]
    pub const fn intersects(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }

    /// Whether the set is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The kinds in either set.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// The kinds in both sets.
    #[must_use]
    pub const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    /// The kinds in `self` that are not in `other`.
    #[must_use]
    pub const fn difference(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    /// The kinds not in `self`.
    #[must_use]
    pub const fn complement(self) -> Self {
        Self(!self.0 & Self::ALL)
    }

    const NAMED: [(Self, &'static str); 5] = [
        (Self::ROOM, "ROOM"),
        (Self::USER, "USER"),
        (Self::SUBSCRIPTION, "SUBSCRIPTION"),
        (Self::MESSAGE, "MESSAGE"),
        (Self::CURRENT_USER, "CURRENT_USER"),
    ];
}

impl Default for ResourceType {
    /// [`ResourceType::all`] — caching everything is the useful default, and the flags
    /// exist to narrow it.
    fn default() -> Self {
        Self::all()
    }
}

impl fmt::Debug for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResourceType(")?;
        if self.is_empty() {
            f.write_str("empty")?;
        } else {
            let mut first = true;
            for (flag, name) in Self::NAMED {
                if self.contains(flag) {
                    if !first {
                        f.write_str(" | ")?;
                    }
                    f.write_str(name)?;
                    first = false;
                }
            }
        }
        f.write_str(")")
    }
}

impl BitOr for ResourceType {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        self.union(rhs)
    }
}

impl BitOrAssign for ResourceType {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = self.union(rhs);
    }
}

impl BitAnd for ResourceType {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        self.intersection(rhs)
    }
}

impl BitAndAssign for ResourceType {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = self.intersection(rhs);
    }
}

impl BitXor for ResourceType {
    type Output = Self;
    fn bitxor(self, rhs: Self) -> Self {
        Self(self.0 ^ rhs.0)
    }
}

impl BitXorAssign for ResourceType {
    fn bitxor_assign(&mut self, rhs: Self) {
        *self = *self ^ rhs;
    }
}

impl Sub for ResourceType {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        self.difference(rhs)
    }
}

impl SubAssign for ResourceType {
    fn sub_assign(&mut self, rhs: Self) {
        *self = self.difference(rhs);
    }
}

impl Not for ResourceType {
    type Output = Self;
    fn not(self) -> Self {
        self.complement()
    }
}

#[cfg(test)]
mod tests {
    use super::ResourceType;

    #[test]
    fn default_is_all() {
        assert_eq!(ResourceType::default(), ResourceType::all());
        assert!(ResourceType::all().contains(ResourceType::MESSAGE));
    }

    #[test]
    fn set_algebra() {
        let set = ResourceType::ROOM | ResourceType::USER;

        assert!(set.contains(ResourceType::ROOM));
        assert!(!set.contains(ResourceType::MESSAGE));
        assert!(set.intersects(ResourceType::USER | ResourceType::MESSAGE));
        assert_eq!(set - ResourceType::ROOM, ResourceType::USER);
        assert!(ResourceType::empty().is_empty());
        assert!((set & ResourceType::MESSAGE).is_empty());
        assert!(!(!set).contains(ResourceType::ROOM));
        assert!((!set).contains(ResourceType::MESSAGE));
    }

    #[test]
    fn unknown_bits_are_dropped() {
        assert_eq!(ResourceType::from_bits_truncate(u32::MAX), ResourceType::all());
        assert_eq!(ResourceType::from_bits_truncate(1 << 31), ResourceType::empty());
    }

    #[test]
    fn debug_names_the_flags() {
        let set = ResourceType::ROOM | ResourceType::MESSAGE;

        assert_eq!(format!("{set:?}"), "ResourceType(ROOM | MESSAGE)");
        assert_eq!(format!("{:?}", ResourceType::empty()), "ResourceType(empty)");
    }
}
