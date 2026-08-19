//! Declarative handler filters.
//!
//! # Why these are defaults rather than options
//!
//! The characteristic way to break a Rocket.Chat bot is a feedback loop: the bot posts,
//! the server broadcasts that message back on the same stream the bot is subscribed to,
//! the handler runs again, and the room fills up in seconds. Rocket.Chat makes this
//! *easier* to hit than most platforms, for three reasons that are not obvious:
//!
//! 1. **There is no trustworthy "this is a bot" flag.** `IMessage.bot` is deprecated, is
//!    never set for bot-role users, and is only populated by the integrations subsystem —
//!    so the obvious guard does not work. Comparing `u._id` is the only reliable test, and
//!    it only answers *"is this me"*, never *"is this some other bot"* — see
//!    [`Filters::allow_bots`](Filters::allow_bots()), which is a no-op for that reason.
//! 2. **Any mutation re-broadcasts the whole message.** Reactions, pins, thread-count
//!    bumps and link-preview enrichment all resend the full document, none of them
//!    touching a field that says "this is not new". A handler that replies to every
//!    message it sees will reply again when someone reacts to it.
//! 3. **System messages arrive on the same stream** with an empty or repurposed `msg`, so
//!    naive text handling acts on joins and topic changes too.
//!
//! So the safe behaviour is the default, and the dangerous behaviour is opt-in and named.
//! A handler that genuinely wants to see its own messages says [`allow_self`](Self); one
//! that wants edits says `edits`. Nobody gets a loop by forgetting a check.

use rocketsocket_model::entity::Message;
use rocketsocket_model::id::{RoomId, UserId};

/// Who a handler is willing to hear from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Authority {
    /// No permission requirement.
    #[default]
    Anyone,
    /// The author must hold the global `admin` role.
    Admin,
    /// The author must hold `owner`, `moderator` or `leader` **in the event's room**.
    RoomAdmin,
    /// The author must be either a server admin or a room admin.
    AdminOrRoomAdmin,
}

/// The filters a handler was declared with.
///
/// Built by `#[event(..)]`; rarely constructed by hand. Every field is expressed so that
/// [`Default`] is the safe choice — the flags say what to *allow*, not what to block, so a
/// zeroed value blocks everything dangerous.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Filters {
    /// See the bot's own events. Off by default; this is the loop guard.
    pub allow_self: bool,
    /// See system messages (`t` set: joins, leaves, topic changes, tombstones).
    pub allow_system: bool,
    /// See edits and the re-broadcasts caused by reactions, pins and thread bumps.
    pub allow_edits: bool,
    /// **Currently a no-op**, in either position: Rocket.Chat gives a bot account no way to
    /// find out whether another account is a bot. See
    /// [`Filters::allow_bots`](Self::allow_bots()) for the endpoints that were ruled out and
    /// why the flag was not implemented as a partial guess.
    pub allow_bots: bool,
    /// Require the message text to start with this.
    pub prefix: Option<&'static str>,
    /// Restrict to one room id.
    pub room: Option<&'static str>,
    /// Require the bot to be mentioned.
    pub mentions_me: bool,
    /// Require the author to hold a role.
    pub authority: Authority,
}

/// Why an event was filtered out, for logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Filtered {
    /// The bot's own event.
    OwnEvent,
    /// A system message.
    SystemMessage,
    /// An edit or a re-broadcast.
    Edit,
    /// Another bot's message. **Never produced** — see
    /// [`Filters::allow_bots`](Filters::allow_bots()) for why that filter is a no-op.
    Bot,
    /// The text did not start with the required prefix.
    Prefix,
    /// The event was in another room.
    Room,
    /// The bot was not mentioned.
    NotMentioned,
    /// The author lacked the required role.
    Authority,
}

impl Filters {
    /// The safe configuration: hears nobody's events but other people's ordinary messages.
    ///
    /// Exposed as a const so generated code can use struct-update syntax against it —
    /// `Filters` is `#[non_exhaustive]`, so `..Default::default()` is not usable from
    /// another crate, and a future field must not silently default to the permissive value
    /// in already-generated handlers.
    pub const DEFAULT: Self = Self {
        allow_self: false,
        allow_system: false,
        allow_edits: false,
        allow_bots: false,
        prefix: None,
        room: None,
        mentions_me: false,
        authority: Authority::Anyone,
    };

    /// Hears the bot's own events too. **Removes the loop guard** — a handler that
    /// replies to what it sees will reply to its own replies.
    #[must_use]
    pub const fn allow_self(mut self, yes: bool) -> Self {
        self.allow_self = yes;
        self
    }

    /// Sees system messages (joins, leaves, topic changes, tombstones).
    #[must_use]
    pub const fn allow_system(mut self, yes: bool) -> Self {
        self.allow_system = yes;
        self
    }

    /// Sees edits, and the re-broadcasts caused by reactions, pins and thread bumps.
    #[must_use]
    pub const fn allow_edits(mut self, yes: bool) -> Self {
        self.allow_edits = yes;
        self
    }

    /// **A documented no-op.** Kept so `#[event(allow_bots)]` still compiles and so the
    /// intent stays expressible, but it changes nothing today.
    ///
    /// # Why this is not implemented
    ///
    /// The only durable definition of "another bot" on Rocket.Chat is *holds the `bot` or
    /// `app` role*, and a bot account cannot read that. Every route was checked against the
    /// v8.8.0-develop server:
    ///
    /// - **`IMessage.bot`** is `@deprecated`, never set for bot-role users, and written only
    ///   by the integrations subsystem.
    /// - **`IUser.type == 'bot'`** is set in exactly two places: the built-in `rocket.cat`
    ///   account (`initialData.ts`) and users created *by an App*. An ordinary account with
    ///   the `bot` role — which is how essentially every external bot is provisioned — has
    ///   `type: 'user'`.
    /// - **`users.info`** projects `roles` only through `getFullUserData`'s `fullFields`,
    ///   applied when the caller is the user or holds `view-full-other-user-info` — which
    ///   defaults to `['admin']`. A bot-role account gets a document with no `roles` key.
    /// - **`roles.getUsersInRole`** requires `access-permissions`, also admin-only.
    /// - **`roles.getUsersInPublicRoles`**, the endpoint
    ///   [`Authority::Admin`] uses, filters on roles with a
    ///   non-empty `description`. `bot` and `app` are seeded with `description: ''`
    ///   (`upsertPermissions.ts`), so they are precisely the roles it omits.
    ///
    /// # Why it is a no-op rather than a partial guess
    ///
    /// The safety direction is inverted from the role filters. [`Authority`] is an
    /// *allow*-list, so "cannot establish" means reject and the cost of not knowing is a
    /// quiet handler. `allow_bots` is a *block*-list that is off by default, so "cannot
    /// establish" would mean rejecting every author whose bot-ness is unknown — which, on a
    /// correctly provisioned bot account, is everybody. The filter would silence the bot
    /// entirely.
    ///
    /// The other option — treating `type: 'bot'` as the answer — would block `rocket.cat`
    /// and App users, miss every role-provisioned bot, and cost a `users.info` call per
    /// distinct author to do it. A guard that catches the rare case and misses the common
    /// one, while claiming to catch both, is worse than no guard: it is the reason someone
    /// stops adding a `prefix`.
    ///
    /// # What to use instead
    ///
    /// A bot-to-bot loop needs two handlers that answer each other's *output*. Break it the
    /// same way you would break any other: [`prefix`](Self::prefix) or
    /// [`mentions_me`](Self::mentions_me), so the bot only answers text addressed to it and
    /// its own replies do not qualify. [`allow_self`](Self::allow_self) already covers the
    /// single-bot loop, which is the one that actually happens.
    #[must_use]
    pub const fn allow_bots(mut self, yes: bool) -> Self {
        self.allow_bots = yes;
        self
    }

    /// Requires the message text to start with `prefix`.
    #[must_use]
    pub const fn prefix(mut self, prefix: Option<&'static str>) -> Self {
        self.prefix = prefix;
        self
    }

    /// Restricts to a single room.
    #[must_use]
    pub const fn room(mut self, room: Option<&'static str>) -> Self {
        self.room = room;
        self
    }

    /// Requires the bot to be mentioned by id. `@all` and `@here` do not count.
    #[must_use]
    pub const fn mentions_me(mut self, yes: bool) -> Self {
        self.mentions_me = yes;
        self
    }

    /// Requires the author to hold a role.
    #[must_use]
    pub const fn authority(mut self, authority: Authority) -> Self {
        self.authority = authority;
        self
    }

    /// Applies the filters that need no network access.
    ///
    /// Returns the reason the event was rejected, or `None` if it passes. Authority checks
    /// are not applied here — they need the server, and live in
    /// [`RoleDirectory`](crate::roles::RoleDirectory).
    /// [`allow_bots`](Self::allow_bots()) is not applied anywhere: it is a no-op, for the
    /// reasons recorded on it.
    #[must_use]
    pub fn local_verdict(
        &self,
        message: &Message,
        room: &RoomId,
        me: Option<&UserId>,
    ) -> Option<Filtered> {
        if !self.allow_self && me.is_some_and(|me| message.u.id == *me) {
            return Some(Filtered::OwnEvent);
        }
        if !self.allow_system && message.is_system() {
            return Some(Filtered::SystemMessage);
        }
        // `is_edited` covers more than a user edit: the server re-sends the whole document
        // on reaction, pin and thread-count changes, and those carry `editedAt` too.
        if !self.allow_edits && message.is_edited() {
            return Some(Filtered::Edit);
        }
        if let Some(prefix) = self.prefix
            && !message.msg.starts_with(prefix)
        {
            return Some(Filtered::Prefix);
        }
        if let Some(wanted) = self.room
            && room.as_str() != wanted
        {
            return Some(Filtered::Room);
        }
        if self.mentions_me
            && !me.is_some_and(|me| {
                message.mentions.as_ref().is_some_and(|mentions| {
                    // Only an explicit user mention counts. `@all` and `@here` arrive
                    // as mentions too, with the literal keyword in place of an id —
                    // treating those as "mentioned me" would have every bot in the
                    // workspace answer every announcement.
                    mentions.iter().any(|mention| mention.id == me.as_str())
                })
            })
        {
            return Some(Filtered::NotMentioned);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(json: serde_json::Value) -> Message {
        let mut base = serde_json::json!({
            "_id": "m1", "_updatedAt": {"$date": 1}, "rid": "GENERAL", "msg": "hello",
            "ts": {"$date": 1}, "u": {"_id": "author", "username": "alice"}
        });
        let serde_json::Value::Object(extra) = json else { panic!("object expected") };
        let serde_json::Value::Object(ref mut target) = base else { unreachable!() };
        target.extend(extra);
        serde_json::from_value(base).expect("fixture must decode")
    }

    fn me() -> UserId {
        UserId::new("me")
    }

    #[test]
    fn the_default_filters_block_a_bot_from_hearing_itself() {
        // The loop guard. Nobody has to remember to ask for it.
        let filters = Filters::default();
        let own = message(serde_json::json!({"u": {"_id": "me", "username": "bot"}}));
        assert_eq!(
            filters.local_verdict(&own, &RoomId::new("GENERAL"), Some(&me())),
            Some(Filtered::OwnEvent)
        );
    }

    #[test]
    fn the_default_filters_block_edits_and_rebroadcasts() {
        // A reaction on the bot's own reply re-sends the whole document with editedAt set.
        // Without this default, replying to every message you see is a loop.
        let filters = Filters::default();
        let edited = message(serde_json::json!({
            "editedAt": {"$date": 2},
            "editedBy": {"_id": "author", "username": "alice"}
        }));
        assert_eq!(
            filters.local_verdict(&edited, &RoomId::new("GENERAL"), Some(&me())),
            Some(Filtered::Edit)
        );
    }

    #[test]
    fn the_default_filters_block_system_messages() {
        let filters = Filters::default();
        let joined = message(serde_json::json!({"t": "uj", "msg": "alice"}));
        assert_eq!(
            filters.local_verdict(&joined, &RoomId::new("GENERAL"), Some(&me())),
            Some(Filtered::SystemMessage)
        );
    }

    #[test]
    fn an_ordinary_message_from_someone_else_passes() {
        let filters = Filters::default();
        let ordinary = message(serde_json::json!({}));
        assert_eq!(filters.local_verdict(&ordinary, &RoomId::new("GENERAL"), Some(&me())), None);
    }

    #[test]
    fn allow_self_opts_back_in() {
        let filters = Filters { allow_self: true, ..Filters::default() };
        let own = message(serde_json::json!({"u": {"_id": "me", "username": "bot"}}));
        assert_eq!(filters.local_verdict(&own, &RoomId::new("GENERAL"), Some(&me())), None);
    }

    #[test]
    fn the_self_check_is_skipped_when_the_bot_id_is_unknown() {
        // Before login completes there is nothing to compare against. Failing open here is
        // deliberate and safe: no id means no messages of ours exist yet.
        let filters = Filters::default();
        let own = message(serde_json::json!({"u": {"_id": "me", "username": "bot"}}));
        assert_eq!(filters.local_verdict(&own, &RoomId::new("GENERAL"), None), None);
    }

    #[test]
    fn a_prefix_filter_rejects_other_text() {
        let filters = Filters { prefix: Some("!"), ..Filters::default() };
        assert_eq!(
            filters.local_verdict(&message(serde_json::json!({})), &RoomId::new("GENERAL"), None),
            Some(Filtered::Prefix)
        );
        let command = message(serde_json::json!({"msg": "!ping"}));
        assert_eq!(filters.local_verdict(&command, &RoomId::new("GENERAL"), None), None);
    }

    #[test]
    fn a_room_filter_rejects_other_rooms() {
        let filters = Filters { room: Some("GENERAL"), ..Filters::default() };
        let anywhere = message(serde_json::json!({}));
        assert_eq!(
            filters.local_verdict(&anywhere, &RoomId::new("random"), None),
            Some(Filtered::Room)
        );
        assert_eq!(filters.local_verdict(&anywhere, &RoomId::new("GENERAL"), None), None);
    }

    #[test]
    fn mentions_me_requires_a_resolved_mention() {
        let filters = Filters { mentions_me: true, ..Filters::default() };
        let plain = message(serde_json::json!({}));
        assert_eq!(
            filters.local_verdict(&plain, &RoomId::new("GENERAL"), Some(&me())),
            Some(Filtered::NotMentioned)
        );

        let mentioning = message(serde_json::json!({
            "msg": "hey @bot",
            "mentions": [{"_id": "me", "username": "bot", "type": "user"}]
        }));
        assert_eq!(filters.local_verdict(&mentioning, &RoomId::new("GENERAL"), Some(&me())), None);
    }

    #[test]
    fn allow_bots_is_a_no_op_in_both_positions() {
        // Locked down deliberately. Rocket.Chat gives a bot no way to recognise another
        // bot -- see `Filters::allow_bots` -- and because the flag blocks rather than
        // allows, failing closed on "cannot establish" would reject every author on a
        // correctly provisioned account. So it does nothing, and this test is here so that
        // "fixing" it has to be a deliberate change to a stated decision rather than an
        // accident.
        let ordinary = message(serde_json::json!({}));
        let room = RoomId::new("GENERAL");

        for allow_bots in [false, true] {
            let filters = Filters { allow_bots, ..Filters::default() };
            assert_eq!(filters.local_verdict(&ordinary, &room, Some(&me())), None);
        }
    }

    #[test]
    fn the_bot_field_on_a_message_is_ignored() {
        // A message from an integration does carry `bot`, and it is the one shape where the
        // deprecated field is populated. Acting on it would make the filter work for
        // integrations and silently not for every other bot, which is the worst of both.
        let filters = Filters::default();
        let from_integration = message(serde_json::json!({
            "bot": {"i": "integration-id"},
            "u": {"_id": "other-bot", "username": "jenkins"}
        }));
        assert_eq!(
            filters.local_verdict(&from_integration, &RoomId::new("GENERAL"), Some(&me())),
            None
        );
    }

    #[test]
    fn filters_are_checked_in_a_stable_order() {
        // Self first: it is the one whose absence is dangerous, so it must not be masked by
        // another filter rejecting first for a less important reason.
        let filters = Filters { prefix: Some("!"), ..Filters::default() };
        let own_command = message(serde_json::json!({
            "msg": "!ping", "u": {"_id": "me", "username": "bot"}
        }));
        assert_eq!(
            filters.local_verdict(&own_command, &RoomId::new("GENERAL"), Some(&me())),
            Some(Filtered::OwnEvent)
        );
    }
}
