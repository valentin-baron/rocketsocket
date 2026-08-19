//! Role resolution for the [`Authority`] filters.
//!
//! [`Filters::local_verdict`](crate::filter::Filters::local_verdict) settles every filter
//! that can be decided from the message document alone. The three role filters cannot:
//! whether an author holds `admin`, or `owner`/`moderator`/`leader` in one room, is state
//! that only the server knows. This module fetches that state, caches it, and — above all
//! — refuses the handler whenever it cannot establish the grant.
//!
//! # The two endpoints, and why they are these two
//!
//! Both were read out of the v8.8.0-develop server source rather than the published docs.
//!
//! ### Global `admin`: `GET /api/v1/roles.getUsersInPublicRoles`
//!
//! Answers `{"users": [{"_id", "username", "roles": [..]}, ..], "success": true}` — every
//! user holding a **public** role, meaning a role with `scope: 'Users'` and a non-empty
//! `description` (`AuthorizationService.getPublicRoles`). On a stock workspace those are
//! `admin`, `livechat-agent` and `livechat-manager`; `upsertPermissions` re-seeds
//! `{name: 'admin', scope: 'Users', description: 'Admin'}` on every boot and
//! `createOrUpdateProtectedRole` keeps a truthy description, so `admin` is reliably in the
//! set. The route is `authRequired` with **no** `permissionsRequired`.
//!
//! The obvious alternative, `users.info`, does not work for a bot. Its projection comes from
//! `getFullUserData`, where `roles` sits in `fullFields` — applied only when the caller *is*
//! the user or holds `view-full-other-user-info`, which defaults to `['admin']` alone. A
//! correctly provisioned `bot`-role account therefore gets a user document with **no
//! `roles` key at all**, which a fail-closed check must read as "unknown", i.e. reject. That
//! is why [`User::roles`](rocketsocket_model::entity::User::roles) is an `Option`, and why
//! it is the wrong source here. `roles.getUsersInRole` is worse still: it requires
//! `access-permissions`.
//!
//! One call therefore answers `admin` for the whole workspace, not for one user — so an
//! `admin` handler costs one request per TTL no matter how many people talk to it.
//!
//! ### Room roles: `GET /api/v1/rooms.roles?rid=<rid>`
//!
//! Answers `{"roles": [{"rid", "u": {"_id", "username"}, "roles": [..]}, ..], "success":
//! true}` — one entry per user holding a subscription-scoped role in that room, from
//! `getRoomRoles(rid)` via `executeGetRoomRoles`. `authRequired`, no permission required,
//! but the caller must be able to *access* the room: `executeGetRoomRoles` throws
//! `error-invalid-room` for an unknown room and `error-invalid-user` when
//! `canAccessRoomAsync` fails. Both arrive as a non-2xx, and both reject.
//!
//! The query schema is `{rid}` with `additionalProperties: false`, so nothing else may be
//! sent. Like the global endpoint it filters on `description: {$exists: true, $ne: ''}`,
//! which on a stock workspace admits exactly `owner`, `moderator` and `leader` — the three
//! [`Authority::RoomAdmin`] recognises.
//!
//! # Memoisation, because the alternative is a request per message
//!
//! Rocket.Chat's default REST limiter is **10 requests per 60 s per route per IP**. The
//! `bot` role's `api-bypass-rate-limit` removes it, but a framework that only works on a
//! correctly provisioned account is a framework that fails silently on a misconfigured one,
//! so the budget is treated as real.
//!
//! | | key | source | cost |
//! |---|---|---|---|
//! | global admins | *none* — one workspace-wide entry | `roles.getUsersInPublicRoles` | 1 request per [`ttl`](RoleCacheConfig::ttl), ever |
//! | room admins | [`RoomId`] | `rooms.roles?rid=` | 1 request per room per [`ttl`](RoleCacheConfig::ttl) |
//!
//! A lookup only happens for a handler that declared a role filter, and only for an event
//! that already survived every local filter of that handler — in practice a command
//! invocation, not a message. So the steady-state cost is one request per active room per
//! TTL, plus one for the workspace.
//!
//! Three further properties matter as much as the hit rate:
//!
//! - **No stampede.** Each cache slot is a `tokio::sync::Mutex`; the first caller holds it
//!   across the fetch and everyone else for the same key waits and then reads the answer it
//!   stored. Different keys never block each other.
//! - **Failures are cached too**, for the shorter
//!   [`failure_ttl`](RoleCacheConfig::failure_ttl). Not caching them would turn a broken or
//!   throttled server into a request per message — exactly the failure mode the cache
//!   exists to prevent — while the handler stayed closed anyway.
//! - **The room map is bounded** at [`max_rooms`](RoleCacheConfig::max_rooms), evicting the
//!   least recently consulted room. A bot in ten thousand rooms must not grow a slot per
//!   room forever.
//!
//! # Roles change, so a grant must expire
//!
//! A demoted moderator has to stop passing `room_admin` within a bounded time, and there are
//! two mechanisms here. Neither alone is sufficient.
//!
//! **TTL** ([`ttl`](RoleCacheConfig::ttl), default 5 minutes) is the backstop. Five minutes
//! is chosen against the request budget: it is short enough that a mistaken grant is
//! measured in minutes rather than in the process's uptime, and long enough that a busy
//! workspace stays far inside 10 requests per 60 s even with dozens of active rooms. Lower
//! it if role changes are frequent and the account has `api-bypass-rate-limit`; raise it if
//! the workspace is large and roles are stable. A demotion is also usually accompanied by
//! the demoter's own actions, so the practical exposure is smaller than the bound.
//!
//! **`stream-notify-logged` / `roles-change`** is the live signal, and it is strictly better
//! than the TTL when it arrives: [`apply_role_change`](RoleDirectory::apply_role_change)
//! drops the affected snapshot immediately, so the next event re-reads it.
//! [`Framework::watch_role_changes`](crate::framework::Framework::watch_role_changes)
//! subscribes, and [`Framework::dispatch`](crate::framework::Framework::dispatch) applies
//! every event it sees without the application wiring anything up.
//!
//! It is only ever a bonus, never a guarantee, for reasons in the server source:
//!
//! - **Every emission is gated on the `UI_DisplayRoles` setting.** `addUserToRole`,
//!   `removeUserFromRole`, `addRoomModerator`, `removeRoomOwner`, `roles.addUserToRole` and
//!   the rest all wrap `api.broadcast('user.roleUpdate', …)` in
//!   `if (settings.get('UI_DisplayRoles'))`. Turn that display setting off and role changes
//!   become invisible on the wire — a *cosmetic* setting silently disabling a *security*
//!   signal.
//! - **Losing a subscription is not a role change.** Kick a moderator out of a room and
//!   their subscription is deleted, so `rooms.roles` stops listing them, but no
//!   `roles-change` is emitted.
//! - **A reconnect loses events.** Nothing is replayed for the gap, which is why
//!   [`ClientEvent::Resubscribed`](rocketsocket_realtime::client::ClientEvent::Resubscribed)
//!   invalidates everything.
//!
//! So the TTL is what bounds staleness, and the event is what usually beats it. A missed
//! event costs at most one TTL; it can never pin a stale grant.
//!
//! # Fail closed
//!
//! [`is_admin`](RoleDirectory::is_admin) and
//! [`is_room_admin`](RoleDirectory::is_room_admin) return `bool`, and every way of not
//! knowing — no credentials, a transport error, a timeout, a non-2xx, `success: false`, a
//! body that does not parse — is `false`. Admitting on failure would hand an unprivileged
//! user an admin-only handler with no trace; rejecting makes the handler quiet, which is
//! visible in the logs and recoverable.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as SyncMutex, PoisonError};
use std::time::{Duration, Instant};

use rocketsocket_model::event::{RoleChange, RoleChangeKind};
use rocketsocket_model::id::{RoleId, RoomId, UserId};
use rocketsocket_rest::client::Client as RestClient;

use crate::filter::{Authority, Filtered};

/// The global role [`Authority::Admin`] requires.
const ADMIN_ROLE: &str = "admin";

/// The room-scoped roles [`Authority::RoomAdmin`] accepts.
///
/// These are the three subscription-scoped roles `upsertPermissions` seeds with a non-empty
/// description, which is also the filter `getRoomRoles` applies — so the endpoint cannot
/// report a room role this list does not cover unless an admin defined a custom one.
const ROOM_ADMIN_ROLES: [&str; 3] = ["owner", "moderator", "leader"];

/// How long a resolved role answer is trusted, and how patient a lookup is.
///
/// See the [module docs](self) for why the defaults are what they are. Every field is a
/// bound on either staleness or request volume, and the two trade against each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RoleCacheConfig {
    /// How long a successful answer is reused. Bounds how long a revoked role keeps
    /// passing when the live `roles-change` signal does not arrive.
    pub ttl: Duration,
    /// How long a *failed* lookup is remembered, during which the handler stays closed and
    /// no request is made. Shorter than [`ttl`](Self::ttl), because a failure is usually
    /// transient and locking admins out for five minutes over one 500 would be its own
    /// outage — but long enough that a persistently broken server costs a couple of
    /// requests a minute rather than one per message.
    pub failure_ttl: Duration,
    /// Budget for one lookup. It sits in the dispatch path, so a server that accepts the
    /// connection and then says nothing must not stall the event loop.
    pub timeout: Duration,
    /// Ceiling on remembered rooms. The least recently consulted room is evicted first.
    pub max_rooms: usize,
}

impl RoleCacheConfig {
    /// The defaults, as a const so struct-update syntax works across the crate boundary —
    /// [`RoleCacheConfig`] is `#[non_exhaustive]`, so `..Default::default()` is not usable
    /// from another crate.
    pub const DEFAULT: Self = Self {
        ttl: Duration::from_secs(300),
        failure_ttl: Duration::from_secs(30),
        timeout: Duration::from_secs(5),
        max_rooms: 512,
    };

    /// How long a successful answer is reused.
    #[must_use]
    pub const fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// How long a failed lookup is remembered.
    #[must_use]
    pub const fn failure_ttl(mut self, failure_ttl: Duration) -> Self {
        self.failure_ttl = failure_ttl;
        self
    }

    /// Budget for one lookup.
    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Ceiling on remembered rooms.
    #[must_use]
    pub const fn max_rooms(mut self, max_rooms: usize) -> Self {
        self.max_rooms = max_rooms;
        self
    }
}

impl Default for RoleCacheConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Why a role lookup could not answer.
///
/// Every variant rejects. It exists to make the log line say *which* way the server let us
/// down, because "the admin handler is quiet" is otherwise indistinguishable between a
/// misconfigured token and a genuinely unprivileged author.
///
/// Carries strings rather than `reqwest` or `rocketsocket-rest` error types on purpose: the
/// HTTP client is an implementation detail of this module and must not become part of its
/// public signature.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RoleLookupError {
    /// No credentials are installed on the REST client yet.
    #[error("the REST client is not authenticated")]
    NotAuthenticated,

    /// The lookup could not even be attempted.
    #[error("the role lookup could not be attempted: {0}")]
    Unusable(String),

    /// The request failed before a complete response arrived.
    #[error("transport failure: {0}")]
    Transport(String),

    /// The server answered, and said no.
    #[error("the server answered {status}: {body}")]
    Refused {
        /// HTTP status. `200` when the refusal was a `success: false` envelope.
        status: u16,
        /// The beginning of the response body.
        body: String,
    },

    /// The response did not have the shape this module expects.
    #[error("the response could not be decoded: {0}")]
    Decode(String),

    /// The lookup exceeded [`RoleCacheConfig::timeout`].
    #[error("the role lookup timed out after {0:?}")]
    Timeout(Duration),
}

/// One memoised answer: the set of users holding the role, or `None` for "we do not know".
#[derive(Debug, Clone)]
struct Entry {
    /// `None` means the lookup failed. Absent knowledge and an empty set both reject, but
    /// they expire on different clocks.
    holders: Option<Arc<HashSet<UserId>>>,
    /// When the lookup that produced this was *started*, so the TTL measures the age of the
    /// data rather than the age of the reply.
    fetched_at: Instant,
}

impl Entry {
    fn is_fresh(&self, config: &RoleCacheConfig, now: Instant) -> bool {
        let ttl = if self.holders.is_some() { config.ttl } else { config.failure_ttl };
        now.saturating_duration_since(self.fetched_at) < ttl
    }

    /// Whether this entry grants the role to `user`.
    ///
    /// A failed lookup grants nothing — this is the fail-closed decision, expressed once.
    fn grants(&self, user: &UserId) -> bool {
        self.holders.as_ref().is_some_and(|holders| holders.contains(user))
    }
}

/// A cache slot, and the single-flight lock that guards refreshing it.
///
/// Invalidation *replaces* the `Arc` rather than clearing the `Option` inside it, so a fetch
/// already in flight cannot write its now-stale answer back over the invalidation: it
/// completes into a slot nobody can reach any more.
type Slot = Arc<tokio::sync::Mutex<Option<Entry>>>;

fn empty_slot() -> Slot {
    Arc::new(tokio::sync::Mutex::new(None))
}

/// A room slot plus the bookkeeping that bounds the map.
#[derive(Debug)]
struct RoomSlot {
    slot: Slot,
    /// Last time a lookup consulted this room, for eviction. Read under the map lock only.
    touched: Instant,
}

/// Which lookup to perform.
#[derive(Debug, Clone, Copy)]
enum Lookup<'a> {
    /// Every holder of a public global role.
    Global,
    /// Every holder of a room-scoped role in one room.
    Room(&'a RoomId),
}

impl Lookup<'_> {
    fn endpoint(self) -> &'static str {
        match self {
            Self::Global => "roles.getUsersInPublicRoles",
            Self::Room(_) => "rooms.roles",
        }
    }
}

/// Resolves and caches the roles the [`Authority`] filters need.
///
/// Built for you by [`Framework::new`](crate::framework::Framework::new); construct one
/// directly only to point role lookups at a differently configured REST client. Cheap to
/// share behind a reference; every method takes `&self`.
///
/// See the [module docs](self) for the endpoints, the cache design and the TTL rationale.
#[derive(Debug)]
pub struct RoleDirectory {
    /// The bot's REST client. Role lookups share its connection pool and credentials
    /// rather than opening a second one.
    rest: RestClient,
    config: RoleCacheConfig,
    global: SyncMutex<Slot>,
    rooms: SyncMutex<HashMap<RoomId, RoomSlot>>,
}

impl RoleDirectory {
    /// A directory that resolves roles through `rest`.
    #[must_use]
    pub fn new(rest: RestClient, config: RoleCacheConfig) -> Self {
        Self {
            rest,
            config,
            global: SyncMutex::new(empty_slot()),
            rooms: SyncMutex::new(HashMap::new()),
        }
    }

    /// The cache settings in force.
    #[must_use]
    pub fn config(&self) -> &RoleCacheConfig {
        &self.config
    }

    /// Applies a role requirement to one author.
    ///
    /// Returns [`Filtered::Authority`] when the author does not hold the required role
    /// **or when that cannot be established**, and `None` when the handler may run.
    pub async fn verdict(
        &self,
        authority: Authority,
        author: &UserId,
        room: &RoomId,
    ) -> Option<Filtered> {
        // Matched variant by variant with no wildcard, even though `Authority` is
        // `#[non_exhaustive]`: within this crate that attribute does nothing, so a new
        // variant is a compile error here rather than a silent fall-through. Which way a
        // fall-through would fall is exactly the thing not to leave to a `_` arm.
        let granted = match authority {
            Authority::Anyone => return None,
            Authority::Admin => self.is_admin(author).await,
            Authority::RoomAdmin => self.is_room_admin(author, room).await,
            // `is_admin` first, and short-circuiting: a workspace admin then costs no
            // per-room request at all, which is what keeps `any_admin` affordable.
            Authority::AdminOrRoomAdmin => {
                self.is_admin(author).await || self.is_room_admin(author, room).await
            }
        };

        if granted {
            None
        } else {
            tracing::debug!(%author, %room, ?authority, "author does not hold the required role");
            Some(Filtered::Authority)
        }
    }

    /// Whether `user` holds the global `admin` role.
    ///
    /// `false` when that could not be established — see [module docs](self).
    pub async fn is_admin(&self, user: &UserId) -> bool {
        let slot = self.global_slot();
        self.entry(&slot, Lookup::Global).await.grants(user)
    }

    /// Whether `user` holds `owner`, `moderator` or `leader` in `room`.
    ///
    /// `false` when that could not be established — including when the bot cannot see the
    /// room, which `executeGetRoomRoles` reports as an invalid *user*.
    pub async fn is_room_admin(&self, user: &UserId, room: &RoomId) -> bool {
        let slot = self.room_slot(room);
        self.entry(&slot, Lookup::Room(room)).await.grants(user)
    }

    /// Drops the cached global role snapshot.
    pub fn invalidate_global(&self) {
        *lock(&self.global) = empty_slot();
    }

    /// Drops the cached role snapshot for one room.
    pub fn invalidate_room(&self, room: &RoomId) {
        lock(&self.rooms).remove(room);
    }

    /// Drops every cached snapshot.
    ///
    /// The right response to anything that means "events may have been missed" — a
    /// reconnect, or a change to a role *document* rather than to an assignment.
    pub fn invalidate_all(&self) {
        self.invalidate_global();
        lock(&self.rooms).clear();
    }

    /// Invalidates whatever a `stream-notify-logged` / `roles-change` event affects.
    ///
    /// Best effort by nature: the server only emits these when `UI_DisplayRoles` is on, and
    /// emits nothing at all when a user simply loses their subscription. The TTL, not this,
    /// is what bounds staleness — see the [module docs](self).
    pub fn apply_role_change(&self, change: &RoleChange) {
        // No subject, or the role document itself changed: either can alter what *both*
        // endpoints report, because both filter on the role's `scope` and `description`.
        // Blanking a role's description removes every one of its holders from the answer.
        if change.u.is_none() || change.action == RoleChangeKind::Changed {
            tracing::debug!(role = %change.id, "role document changed; dropping every role snapshot");
            self.invalidate_all();
            return;
        }

        match &change.scope {
            // `scope` is the room id for a subscription-scoped role.
            Some(room) => self.invalidate_room(&RoomId::new(room.as_str())),
            None => self.invalidate_global(),
        }
    }

    /// How many rooms are currently remembered. For tests and diagnostics.
    #[must_use]
    pub fn cached_rooms(&self) -> usize {
        lock(&self.rooms).len()
    }

    // ---------------------------------------------------------------------------------
    // Cache plumbing
    // ---------------------------------------------------------------------------------

    fn global_slot(&self) -> Slot {
        Arc::clone(&lock(&self.global))
    }

    fn room_slot(&self, room: &RoomId) -> Slot {
        let now = Instant::now();
        let mut rooms = lock(&self.rooms);

        if let Some(existing) = rooms.get_mut(room) {
            existing.touched = now;
            return Arc::clone(&existing.slot);
        }

        // Evict before inserting, so the map never exceeds the ceiling. Least recently
        // consulted goes: a bot's role checks cluster in a handful of rooms, and evicting
        // one of those would refetch it immediately.
        if self.config.max_rooms > 0 {
            while rooms.len() >= self.config.max_rooms {
                let Some(coldest) =
                    rooms.iter().min_by_key(|(_, slot)| slot.touched).map(|(id, _)| id.clone())
                else {
                    break;
                };
                rooms.remove(&coldest);
            }
        }

        let slot = empty_slot();
        rooms.insert(room.clone(), RoomSlot { slot: Arc::clone(&slot), touched: now });
        slot
    }

    /// The cached answer for one slot, fetching it if it is missing or stale.
    ///
    /// Holding the slot's mutex across the fetch is the single-flight: concurrent callers
    /// for the same key queue here, and the ones behind the leader find a fresh entry
    /// rather than issuing their own request.
    async fn entry(&self, slot: &Slot, lookup: Lookup<'_>) -> Entry {
        let mut guard = slot.lock().await;
        let started = Instant::now();

        if let Some(cached) = guard.as_ref()
            && cached.is_fresh(&self.config, started)
        {
            return cached.clone();
        }

        let holders = match tokio::time::timeout(self.config.timeout, self.fetch(lookup)).await {
            Ok(Ok(holders)) => Some(Arc::new(holders)),
            Ok(Err(error)) => {
                // `warn`, not `error`: the bot is still working, one class of handler is
                // simply closed. And it is logged once per `failure_ttl`, not per message,
                // because the failure itself is cached.
                tracing::warn!(
                    endpoint = lookup.endpoint(),
                    %error,
                    "role lookup failed; the role filter will reject until it succeeds"
                );
                None
            }
            Err(_) => {
                let error = RoleLookupError::Timeout(self.config.timeout);
                tracing::warn!(
                    endpoint = lookup.endpoint(),
                    %error,
                    "role lookup timed out; the role filter will reject until it succeeds"
                );
                None
            }
        };

        let entry = Entry { holders, fetched_at: started };
        *guard = Some(entry.clone());
        entry
    }

    // ---------------------------------------------------------------------------------
    // The requests
    // ---------------------------------------------------------------------------------

    /// Issues one lookup and reduces it to the set of users holding the role.
    ///
    /// Both endpoints go through [`RestClient`], so they share its connection pool,
    /// credentials, TLS backend and error handling. An earlier version built its own
    /// `reqwest::Client`, which meant a second pool and a second copy of the envelope
    /// validation.
    async fn fetch(&self, lookup: Lookup<'_>) -> Result<HashSet<UserId>, RoleLookupError> {
        match lookup {
            Lookup::Global => {
                let holders = self
                    .rest
                    .users_in_public_roles()
                    .await
                    .map_err(|error| RoleLookupError::Transport(error.to_string()))?;
                Ok(holders
                    .into_iter()
                    .filter(|user| holds(&user.roles, &[ADMIN_ROLE]))
                    .map(|user| user.id)
                    .collect())
            }
            Lookup::Room(room) => {
                let holders = self
                    .rest
                    .room_roles(room)
                    .await
                    .map_err(|error| RoleLookupError::Transport(error.to_string()))?;
                Ok(holders
                    .into_iter()
                    .filter(|entry| holds(&entry.roles, &ROOM_ADMIN_ROLES))
                    .map(|entry| entry.u.id)
                    .collect())
            }
        }
    }
}

/// Whether `held` contains any of `wanted`.
fn holds(held: &[RoleId], wanted: &[&str]) -> bool {
    held.iter().any(|role| wanted.contains(&role.as_str()))
}

/// Takes a lock, treating a poisoned mutex as usable.
///
/// The guarded values are a `HashMap` and an `Arc`, and nothing here can panic while either
/// is half-updated, so poisoning carries no information — while propagating it would turn an
/// unrelated panic somewhere else into a permanently closed role filter.
fn lock<T>(mutex: &SyncMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// The response envelopes and their `success: true` validation now live in
// `rocketsocket-rest` (`roles.rs`, `Client::users_in_public_roles`, `Client::room_roles`),
// so there is one connection pool, one set of credentials and one copy of the check.
// Requiring `success` explicitly still matters: a truncated body or a proxy's JSON error
// page would otherwise decode as an empty set and read as "nobody holds this role".

#[cfg(test)]
mod tests {
    use super::*;
    use rocketsocket_rest::roles::PublicRoleHolder;

    fn config() -> RoleCacheConfig {
        RoleCacheConfig::DEFAULT
    }

    fn known(users: &[&str], age: Duration) -> Entry {
        Entry {
            holders: Some(Arc::new(users.iter().map(|user| UserId::new(*user)).collect())),
            fetched_at: Instant::now() - age,
        }
    }

    fn unavailable(age: Duration) -> Entry {
        Entry { holders: None, fetched_at: Instant::now() - age }
    }

    #[test]
    fn a_failed_lookup_grants_nothing() {
        // The whole fail-closed decision, in one place.
        assert!(!unavailable(Duration::ZERO).grants(&UserId::new("anyone")));
    }

    #[test]
    fn a_known_answer_grants_only_its_holders() {
        let entry = known(&["admin1"], Duration::ZERO);
        assert!(entry.grants(&UserId::new("admin1")));
        assert!(!entry.grants(&UserId::new("someone-else")));
    }

    #[test]
    fn a_failure_expires_sooner_than_an_answer() {
        // A 500 must not lock admins out for a full TTL, but must still be remembered long
        // enough that a broken server is not asked once per message.
        let config = config();
        let age = config.failure_ttl + Duration::from_secs(1);

        assert!(!unavailable(age).is_fresh(&config, Instant::now()));
        assert!(
            known(&["admin1"], age).is_fresh(&config, Instant::now()),
            "the same age is still fresh for a successful answer"
        );
    }

    #[test]
    fn an_answer_goes_stale_at_the_ttl() {
        let config = config();
        let entry = known(&["admin1"], config.ttl + Duration::from_secs(1));
        assert!(!entry.is_fresh(&config, Instant::now()));
    }

    #[test]
    fn the_default_ttl_is_bounded_in_minutes() {
        // Not a tautology: the point of the assertion is that nobody "optimises" the
        // request count by making a mistaken grant last for the process's lifetime.
        assert!(RoleCacheConfig::DEFAULT.ttl <= Duration::from_secs(600));
        assert!(RoleCacheConfig::DEFAULT.failure_ttl < RoleCacheConfig::DEFAULT.ttl);
    }

    #[test]
    fn only_the_three_room_roles_count() {
        assert!(holds(&[RoleId::new("owner")], &ROOM_ADMIN_ROLES));
        assert!(holds(&[RoleId::new("leader")], &ROOM_ADMIN_ROLES));
        assert!(holds(&[RoleId::new("moderator")], &ROOM_ADMIN_ROLES));
        // A custom subscription-scoped role with a description would be reported by
        // `getRoomRoles` too. It is not a room admin.
        assert!(!holds(&[RoleId::new("archivist")], &ROOM_ADMIN_ROLES));
        assert!(!holds(&[], &ROOM_ADMIN_ROLES));
    }

    // The envelope-decoding tests that were here moved to `rocketsocket-rest`, alongside
    // the envelopes themselves. The property they pin -- that a body without an explicit
    // `success: true` is a refusal rather than an empty answer -- is unchanged.

    #[test]
    fn only_admin_holders_are_extracted_from_the_public_roles_answer() {
        // The endpoint reports every public role, not just `admin` -- livechat-agent and
        // livechat-manager are seeded with descriptions too, so filtering is required
        // rather than optional.
        let holders = vec![
            PublicRoleHolder {
                id: UserId::new("u1"),
                username: Some("alice".to_owned()),
                roles: vec![RoleId::new("admin"), RoleId::new("user")],
            },
            PublicRoleHolder {
                id: UserId::new("u2"),
                username: Some("bob".to_owned()),
                roles: vec![RoleId::new("livechat-agent")],
            },
        ];

        let admins: Vec<_> = holders
            .into_iter()
            .filter(|user| holds(&user.roles, &[ADMIN_ROLE]))
            .map(|user| user.id)
            .collect();
        assert_eq!(admins, vec![UserId::new("u1")]);
    }

    fn directory(config: RoleCacheConfig) -> RoleDirectory {
        let rest = RestClient::new("http://127.0.0.1:1").expect("a loopback URL is valid");
        RoleDirectory::new(rest, config)
    }

    #[tokio::test]
    async fn an_unauthenticated_client_rejects_without_a_request() {
        // No credentials is a configuration error, and it must read as "no", not "yes".
        let directory = directory(config());
        assert!(!directory.is_admin(&UserId::new("u1")).await);
        assert_eq!(
            directory.verdict(Authority::Admin, &UserId::new("u1"), &RoomId::new("GENERAL")).await,
            Some(Filtered::Authority)
        );
    }

    #[tokio::test]
    async fn anyone_never_consults_the_directory() {
        // The default authority must cost nothing at all: no lock, no request, no room slot.
        let directory = directory(config());
        assert_eq!(
            directory.verdict(Authority::Anyone, &UserId::new("u1"), &RoomId::new("GENERAL")).await,
            None
        );
        assert_eq!(directory.cached_rooms(), 0);
    }

    #[tokio::test]
    async fn the_room_map_is_bounded_and_evicts_the_coldest_room() {
        let directory = directory(config().max_rooms(2));

        // Each of these fails closed against the loopback address, which is fine: what is
        // under test is the bookkeeping, not the answer.
        directory.is_room_admin(&UserId::new("u1"), &RoomId::new("a")).await;
        directory.is_room_admin(&UserId::new("u1"), &RoomId::new("b")).await;
        assert_eq!(directory.cached_rooms(), 2);

        // Touch `a` so `b` becomes the coldest, then add `c`.
        directory.is_room_admin(&UserId::new("u1"), &RoomId::new("a")).await;
        directory.is_room_admin(&UserId::new("u1"), &RoomId::new("c")).await;

        assert_eq!(directory.cached_rooms(), 2, "the ceiling holds");
        let rooms = lock(&directory.rooms);
        assert!(rooms.contains_key(&RoomId::new("a")), "recently consulted rooms stay");
        assert!(rooms.contains_key(&RoomId::new("c")));
        assert!(!rooms.contains_key(&RoomId::new("b")), "the coldest room was evicted");
    }

    #[tokio::test]
    async fn a_room_scoped_role_change_invalidates_only_that_room() {
        let directory = directory(config());
        directory.is_room_admin(&UserId::new("u1"), &RoomId::new("a")).await;
        directory.is_room_admin(&UserId::new("u1"), &RoomId::new("b")).await;

        directory.apply_role_change(&role_change(
            RoleChangeKind::Removed,
            "moderator",
            Some("a"),
            Some("u1"),
        ));

        let rooms = lock(&directory.rooms);
        assert!(!rooms.contains_key(&RoomId::new("a")));
        assert!(rooms.contains_key(&RoomId::new("b")), "an unrelated room is untouched");
    }

    #[tokio::test]
    async fn a_role_document_change_invalidates_everything() {
        // Blanking a role's description removes every holder from both endpoints' answers,
        // so no snapshot survives it.
        let directory = directory(config());
        directory.is_room_admin(&UserId::new("u1"), &RoomId::new("a")).await;
        assert_eq!(directory.cached_rooms(), 1);

        directory.apply_role_change(&role_change(RoleChangeKind::Changed, "moderator", None, None));
        assert_eq!(directory.cached_rooms(), 0);
    }

    #[tokio::test]
    async fn a_global_role_change_leaves_room_snapshots_alone() {
        let directory = directory(config());
        directory.is_room_admin(&UserId::new("u1"), &RoomId::new("a")).await;

        directory.apply_role_change(&role_change(RoleChangeKind::Added, "admin", None, Some("u1")));

        assert_eq!(directory.cached_rooms(), 1, "a global grant says nothing about a room");
    }

    fn role_change(
        action: RoleChangeKind,
        role: &str,
        scope: Option<&str>,
        user: Option<&str>,
    ) -> RoleChange {
        RoleChange {
            action,
            id: RoleId::new(role),
            u: user.map(|user| rocketsocket_model::entity::UserRef {
                id: UserId::new(user),
                username: None,
                name: None,
            }),
            scope: scope.map(str::to_owned),
        }
    }
}
