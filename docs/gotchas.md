# Gotchas

Things about Rocket.Chat that will cost you a day each. Every one is verified against the
server source (v8.8.0-develop), not the published docs — which are years out of date and
contradict the server in at least eight places (see [PLAN.md](../PLAN.md) §10).

If you are writing a bot against Rocket.Chat in **any** language, most of this applies.

---

## Setup

### Give the bot account the `bot` role

Not cosmetic. It grants, by default:

| Permission | Why it matters |
|---|---|
| `api-bypass-rate-limit` | **Skips the REST rate limiter entirely.** Without it: 10 requests / 60 s, per route, per IP. |
| `send-many-messages` | Bypasses the 5 msg/s DDP send cap. |
| `message-impersonate` | Required for `alias` and `avatar`. (Not for `emoji` — only those two are gated.) |

A bot without it looks broken under any real load.

### Use a Personal Access Token

PATs are entries in the same `services.resume.loginTokens` array as session tokens, but
they carry no `when` field — so the expiry check compares against an invalid date and
**never fires**. They also carry `bypassTwoFactor`.

The alternative — logging in with a password on every reconnect — burns through
`MAX_RESUME_LOGIN_TOKENS` (default **50**) and silently evicts your own older tokens.

### One credential works on both transports

`POST /api/v1/login` is a wrapper around the DDP `login` method, and both read the same
token array. So the REST `authToken` is simultaneously a DDP resume token.

This is not just convenience: the EE `ddp-streamer` accepts **only** `{resume}` on its
websocket login (`configureServer.ts` destructures nothing else), so authenticating over
REST and resuming over DDP is the only path that works on both the monolith and a
microservices deployment.

---

## Receiving

### Deletions never arrive as message mutations

There is **no global delete stream**. A deletion arrives on `stream-notify-room` as
`<rid>/deleteMessage` — which is *per room*, so a bot that wants deletions must subscribe
per room, driven off `subscriptions-changed`.

And when `Message_ShowDeletedStatus` is on, the deletion *also* arrives on
`stream-room-messages` as an **edit** with `t == "rm"` and an empty body. With that setting
off, no message-stream frame is produced at all.

### Any mutation re-broadcasts the whole message

Reactions, pins, thread-count bumps and URL-preview enrichment all re-send the full
document — none of which touch `editedAt`. So "I received a message document" does **not**
mean "a new message was posted". Keep a seen-set of `_id`, or compare `_updatedAt` against
`ts`.

An echo bot that skips this echoes its way into a loop on the first edit.

### Do not filter your own messages on the `bot` field

`IMessage.bot` is `@deprecated`, is **never** set for bot-role users, and is only populated
by the integrations subsystem. Compare `u._id` against your own user id instead.

### `__my_messages__` has a different shape from a per-room subscription

Per-room delivers `[message]`. The catch-all delivers
`[message, {roomParticipant, roomType, roomName}]` — an extra trailing element appended by
the server's `allowEmit` transform. Any positional decoder must tolerate it.

### Route events on `(stream, eventName)`, never on the document id

Rocket.Chat's `stream-*` publications bypass Meteor's mergebox and push every event as a
`changed` frame on a pseudo-collection, all sharing the constant document id `"id"`.
Routing on the id delivers every room's traffic to every subscriber. Routing on the
subscription id is impossible — the frame does not carry one.

`stream-user-presence` breaks even that convention and uses the uid as both `id` and
`eventName`.

### Argument arity changes between versions

`user-status` has been 3, 6 and 8 elements across releases. EJSON also encodes `undefined`
*inside an array* as `null` while dropping it as an object value, so positional tuples
legitimately contain null holes. Decode leniently or your bot breaks on upgrade.

---

## Sending

### Typing indicators fail silently if you send the wrong name

The server validates the username you emit against your own account — and **which field**
it compares to depends on a workspace setting:

```js
const key = (await Settings.get('UI_Use_Real_Name')) ? 'name' : 'username';
return user[key] === username;
```

Send the wrong one and `allowWrite` returns false. No error. The indicator just never
appears. Read `UI_Use_Real_Name` from `public-settings/get` at startup.

Also: `user-activity` is the **only** writable event on `stream-notify-room`, and the server
stopped emitting the legacy `typing` event in **6.0**.

### The websocket cannot send a rich message

DDP `sendMessage`'s argument validator is a closed whitelist that excludes `attachments`,
`blocks`, `alias`, `avatar` and `emoji` — passing any of them throws. Use REST
`chat.sendMessage`, which is also the only endpoint that accepts `blocks` at all
(`chat.postMessage`'s schema excludes them).

### File upload is a two-step transaction, and step 2 is not idempotent

`rooms.upload/:rid` was **removed in 8.0**. The current flow is
`POST /rooms.media/:rid` → `POST /rooms.mediaConfirm/:rid/:fileId`.

- Skipping step 2 leaves an orphaned upload that expires in 24 h and posts nothing.
- **Retrying step 2 after a timeout posts the file twice.** The handler looks the upload up
  without checking `expiresAt`, so a second confirm sends it again. Retry a refusal, never a
  timeout.

### Interactive UI Kit is closed to plain bots

A bot can **send** `blocks` via `chat.sendMessage`. It can never **receive** a button click:
interactions route to `POST /api/apps/ui.interaction/:appId` and 404 for anything that is
not an installed App. `stream-notify-user`'s `uiInteraction` is server→client — it tells a
*client* to open a modal and says nothing about other users' clicks.

Workaround: deprecated attachment action buttons with
`msg_processing_type: "sendMessage"` make the *clicking user's own client* post a message,
which your bot then receives normally.

### Slash commands cannot be registered externally

`slashCommands.add(...)` is an in-process server-side registry, populated by server code or
installed Apps. There is no REST or DDP endpoint for an external bot. Prefix commands are
your entire command surface.

`/`-prefixed text only reaches a bot if `Message_AllowUnrecognizedSlashCommand` is enabled
(**default off**); otherwise the sender gets "No such command" and the bot sees nothing.

---

## Operations

### Reconnect loses every subscription

The server tears subscriptions down without sending `nosub`, and **no Rocket.Chat release
implements DDP session resume** — `connect.session` is read by neither server
implementation. Every reconnect must re-issue `connect` → `login` → every `sub`.

Replays must use **fresh** subscription ids: `sub` is idempotent by id, and one the server
already knows is dropped silently — no `ready`, no `nosub` — hanging the caller forever.

A client that gets this wrong reconnects, reports healthy, and receives nothing again.

### Stop retrying a dead token

A resume token rejected with `"You've been logged out by the server"` or `"Your session has
expired"` will be rejected forever. So will a `failed` version negotiation. Treat both as
terminal; retrying is a hot loop against someone's production server.

### The binding DDP rate limit is per-method-per-connection

**10 calls per method per 10 s per connection** — not the 600/min connection budget.
`stream-*` methods get a ×4 multiplier. And `RATE_LIMITER_SLOWDOWN_RATE` makes the server
*sleep* before returning the error, so throttling looks like latency, not rejection.

### `X-RateLimit-Reset` is an absolute epoch in milliseconds

Not seconds-remaining. Feeding it to a `from_secs` is a bug.

A 429 body also carries no machine-readable code, and the `timeToReset` detail is dropped
on the way out — the headers are the only usable signal.

### `count=0` means unlimited

In REST pagination, when `API_Allow_Infinite_Count` is on. A default-zero integer
accidentally requests the entire collection. Values above `API_Upper_Count_Limit` (100) are
**silently clamped**, not rejected.

### `query` and `fields` are ignored

Silently, unless `ALLOW_UNSAFE_QUERY_AND_FIELDS_API_PARAMS=TRUE`. Slated for removal in 9.0.

### There are two DDP servers, and they differ

The Meteor monolith and the EE `ddp-streamer` diverge on: the pre-`connect` greeting, the
heartbeat budget (15+15 s vs 30+30 s), whether `result` precedes `updated`, whether a
falsy method result is even present, whether an unknown `unsub` gets a reply, EJSON scope,
and which login payloads are accepted.

A client that works against one can fail against the other in ways no test will catch unless
you run both.
