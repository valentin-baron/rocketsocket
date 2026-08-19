//! `count` / `offset` / `total` paging.
//!
//! # Why `count` is a [`NonZeroU32`]
//!
//! ```text
//! // server/api/lib/getPaginationItems.ts
//! if (count > hardUpperLimit) { count = hardUpperLimit; }
//! if (count === 0 && !settings.get('API_Allow_Infinite_Count')) { count = defaultCount; }
//! ```
//!
//! Two things fall out of those four lines, and both are traps:
//!
//! - **`count = 0` means *unlimited*** on a workspace with `API_Allow_Infinite_Count`
//!   enabled — which is the shipped default. A Rust `u32` that defaults to `0`, or an
//!   `Option<u32>` serialized as `0` when unset, asks the server to stream an entire
//!   collection. On a workspace with a few hundred thousand messages that is a request that
//!   never finishes and a server that pages itself to death.
//! - **An over-large `count` is silently clamped, never rejected.** Ask for 5000 and you get
//!   `API_Upper_Count_Limit` rows (default 100) with a `success: true` envelope and no hint
//!   that anything was capped. Never infer "the collection is exhausted" from
//!   `returned < requested`; use [`PageInfo::has_more`], which compares against `total`.
//!
//! So zero is made unrepresentable at the type level, and [`Pagination::default`] is the
//! server's own default page size rather than "unset".
//!
//! # What is deliberately missing
//!
//! `query` and `fields` are not exposed. `parseJsonQuery` ignores both unless the server
//! process was started with `ALLOW_UNSAFE_QUERY_AND_FIELDS_API_PARAMS=TRUE`, and both are
//! documented for removal in 9.0. A parameter that is silently dropped on every default
//! deployment is worse than no parameter: it produces results that look filtered and are
//! not. (`sort` is unaffected by that switch and remains valid, but belongs to the
//! endpoints that support it rather than here.)

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// A page request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Pagination {
    /// Rows to skip.
    offset: u32,
    /// Rows to return. Never zero — see the [module docs](self).
    count: NonZeroU32,
}

impl Pagination {
    /// The server's `API_Default_Count`: the page size used when the request omits `count`.
    pub const SERVER_DEFAULT_COUNT: u32 = 50;

    /// The default `API_Upper_Count_Limit`: requests above this are clamped, not refused.
    ///
    /// Informational — the real value is a workspace setting this client cannot read
    /// without an admin token.
    pub const DEFAULT_UPPER_LIMIT: u32 = 100;

    /// A first page of `count` rows.
    #[must_use]
    pub const fn new(count: NonZeroU32) -> Self {
        Self { offset: 0, count }
    }

    /// A first page of `count` rows, or `None` if `count` is zero.
    ///
    /// Rejecting zero here is the whole point: see the [module docs](self).
    #[must_use]
    pub const fn try_new(count: u32) -> Option<Self> {
        match NonZeroU32::new(count) {
            Some(count) => Some(Self::new(count)),
            None => None,
        }
    }

    /// Skip `offset` rows.
    #[must_use]
    pub const fn with_offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }

    /// Rows to skip.
    #[must_use]
    pub const fn offset(&self) -> u32 {
        self.offset
    }

    /// Rows requested.
    #[must_use]
    pub const fn count(&self) -> NonZeroU32 {
        self.count
    }

    /// The next page of the same size.
    ///
    /// Saturates rather than wrapping, so walking past `u32::MAX` rows stalls instead of
    /// restarting at the beginning.
    #[must_use]
    pub const fn next_page(&self) -> Self {
        Self { offset: self.offset.saturating_add(self.count.get()), count: self.count }
    }
}

impl Default for Pagination {
    /// `offset = 0`, `count = 50` — the server's own default page size.
    ///
    /// Chosen so that a caller who takes the default gets exactly what an omitted `count`
    /// would have produced, and never the unlimited page that `count = 0` requests.
    fn default() -> Self {
        Self {
            offset: 0,
            count: NonZeroU32::new(Self::SERVER_DEFAULT_COUNT).expect("50 is not zero"),
        }
    }
}

/// The paging members a list response carries alongside its items.
///
/// Every paginated endpoint answers with `{success: true, <items>, count, offset, total}`.
/// `count` is what was *returned*, which after clamping is not necessarily what was asked
/// for; `total` is the size of the whole result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
pub struct PageInfo {
    /// Rows in this response.
    #[serde(default)]
    pub count: u32,
    /// Rows skipped to produce it.
    #[serde(default)]
    pub offset: u32,
    /// Rows in the full result set.
    #[serde(default)]
    pub total: u32,
}

impl PageInfo {
    /// Whether more rows exist beyond this page.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.offset.saturating_add(self.count) < self.total
    }

    /// The request for the next page, or `None` at the end of the collection.
    ///
    /// Returns `None` for an empty page even when `total` claims otherwise: a server that
    /// answers `count = 0` forever would otherwise spin the caller in place.
    #[must_use]
    pub fn next_page(&self) -> Option<Pagination> {
        if !self.has_more() {
            return None;
        }
        let count = NonZeroU32::new(self.count)?;
        Some(Pagination::new(count).with_offset(self.offset.saturating_add(self.count)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_page_is_never_unlimited() {
        let page = Pagination::default();
        assert_eq!(page.count().get(), Pagination::SERVER_DEFAULT_COUNT);
        assert_eq!(page.offset(), 0);
        assert_eq!(
            serde_json::to_value(page).expect("serializes"),
            serde_json::json!({ "offset": 0, "count": 50 }),
        );
    }

    #[test]
    fn zero_is_not_a_page_size() {
        // `count=0` asks the server for the entire collection.
        assert!(Pagination::try_new(0).is_none());
        assert!(Pagination::try_new(1).is_some());
    }

    #[test]
    fn paging_walks_forward_and_saturates() {
        let first = Pagination::try_new(25).expect("non-zero");
        assert_eq!(first.next_page().offset(), 25);
        assert_eq!(first.next_page().next_page().offset(), 50);

        let far = Pagination::try_new(10).expect("non-zero").with_offset(u32::MAX);
        assert_eq!(far.next_page().offset(), u32::MAX, "saturates instead of wrapping to 0");
    }

    #[test]
    fn page_info_knows_when_to_stop() {
        let middle = PageInfo { count: 50, offset: 0, total: 120 };
        assert!(middle.has_more());
        let next = middle.next_page().expect("another page");
        assert_eq!((next.offset(), next.count().get()), (50, 50));

        let last = PageInfo { count: 20, offset: 100, total: 120 };
        assert!(!last.has_more());
        assert!(last.next_page().is_none());
    }

    #[test]
    fn an_empty_page_never_loops() {
        // A server insisting `total` is large while returning nothing must not spin a caller.
        let stuck = PageInfo { count: 0, offset: 0, total: 500 };
        assert!(stuck.next_page().is_none());
    }

    #[test]
    fn a_clamped_response_is_visible_in_page_info() {
        // Ask for 5000, receive `API_Upper_Count_Limit` rows and no warning: only `total`
        // reveals that the collection is larger.
        let clamped = PageInfo { count: 100, offset: 0, total: 4_000 };
        assert!(clamped.has_more());
        assert_eq!(clamped.next_page().map(|page| page.offset()), Some(100));
    }
}
