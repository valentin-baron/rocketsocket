//! Typed, opaque Rocket.Chat identifiers.
//!
//! Rocket.Chat ids are short alphanumeric strings (`Random.id()` produces 17 characters
//! from a 54-symbol alphabet), so [`Id`] is backed by a [`CompactString`] and is
//! allocation-free for every id the server actually emits.
//!
//! The marker parameter is phantom: it exists to stop a [`Id<RoomMarker>`] being passed
//! where a [`Id<UserMarker>`] is wanted. Use [`Id::cast`] for the rare legitimate
//! re-marking.

use core::fmt;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;
use core::str::FromStr;

use compact_str::CompactString;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Marker types distinguishing one kind of id from another.
pub mod marker {
    /// Marker for [`IMessage._id`](https://developer.rocket.chat).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[non_exhaustive]
    pub struct MessageMarker;

    /// Marker for a room id (`rid`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[non_exhaustive]
    pub struct RoomMarker;

    /// Marker for a user id.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[non_exhaustive]
    pub struct UserMarker;

    /// Marker for a subscription document id.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[non_exhaustive]
    pub struct SubscriptionMarker;

    /// Marker for a role id.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[non_exhaustive]
    pub struct RoleMarker;

    /// Marker for an upload / file id.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[non_exhaustive]
    pub struct UploadMarker;
}

/// An opaque Rocket.Chat identifier, tagged with a phantom `T`.
///
/// Every trait implementation is written by hand rather than derived, so that `Id<T>` is
/// `Clone`/`Hash`/`Send`/`Sync` regardless of whether `T` is. `PhantomData<fn(T) -> T>` is
/// deliberate: `PhantomData<T>` would leak `T`'s auto traits.
#[repr(transparent)]
pub struct Id<T> {
    value: CompactString,
    marker: PhantomData<fn(T) -> T>,
}

impl<T> Id<T> {
    /// Wraps an already-known id string.
    #[inline]
    pub fn new(value: impl Into<CompactString>) -> Self {
        Self { value: value.into(), marker: PhantomData }
    }

    /// The id as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        self.value.as_str()
    }

    /// Consumes the id, returning the backing string.
    #[inline]
    pub fn into_inner(self) -> CompactString {
        self.value
    }

    /// Re-tags this id with a different marker.
    ///
    /// Rocket.Chat reuses id values across entity kinds in a few places (a discussion's
    /// `drid` is both a message id and a room id, for instance). This makes those
    /// conversions explicit rather than silent.
    #[inline]
    #[must_use]
    pub fn cast<U>(self) -> Id<U> {
        Id { value: self.value, marker: PhantomData }
    }
}

impl<T> Clone for Id<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self { value: self.value.clone(), marker: PhantomData }
    }
}

impl<T> fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Print as a bare string; the marker is visible in the static type.
        fmt::Debug::fmt(self.value.as_str(), f)
    }
}

impl<T> fmt::Display for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.value.as_str())
    }
}

impl<T> PartialEq for Id<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T> Eq for Id<T> {}

impl<T> PartialOrd for Id<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Id<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl<T> Hash for Id<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<T> AsRef<str> for Id<T> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.value.as_str()
    }
}

impl<T> PartialEq<str> for Id<T> {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.value.as_str() == other
    }
}

impl<T> PartialEq<&str> for Id<T> {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.value.as_str() == *other
    }
}

impl<T> FromStr for Id<T> {
    type Err = core::convert::Infallible;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

impl<T> From<&str> for Id<T> {
    #[inline]
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl<T> From<String> for Id<T> {
    #[inline]
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl<T> Serialize for Id<T> {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.value.as_str())
    }
}

impl<'de, T> Deserialize<'de> for Id<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        CompactString::deserialize(deserializer).map(Self::new)
    }
}

/// Id of a message.
pub type MessageId = Id<marker::MessageMarker>;
/// Id of a room.
pub type RoomId = Id<marker::RoomMarker>;
/// Id of a user.
pub type UserId = Id<marker::UserMarker>;
/// Id of a subscription document.
pub type SubscriptionId = Id<marker::SubscriptionMarker>;
/// Id of a role.
pub type RoleId = Id<marker::RoleMarker>;
/// Id of an upload.
pub type UploadId = Id<marker::UploadMarker>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_as_a_bare_string() {
        let id: MessageId = Id::new("7aDSXtjMA3KPLxLjt");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"7aDSXtjMA3KPLxLjt\"");
        assert_eq!(serde_json::from_str::<MessageId>(&json).unwrap(), id);
    }

    #[test]
    fn a_real_rocket_chat_id_does_not_allocate() {
        // Random.id() is 17 chars; CompactString inlines up to 24 bytes.
        let id: RoomId = Id::new("7aDSXtjMA3KPLxLjt");
        assert!(!id.value.is_heap_allocated());
    }

    #[test]
    fn cast_preserves_the_value() {
        let room: RoomId = Id::new("GENERAL");
        assert_eq!(room.clone().cast::<marker::MessageMarker>().as_str(), room.as_str());
    }

    #[test]
    fn compares_against_str() {
        let id: UserId = Id::new("rocket.cat");
        assert_eq!(id, "rocket.cat");
    }

    #[test]
    fn is_send_and_sync_even_for_a_non_send_marker() {
        // The marker is phantom, so auto traits must not depend on it.
        struct NotSend(*const ());
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Id<NotSend>>();
    }
}
