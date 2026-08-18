# rocketsocket — design plan

A Rust framework for building Rocket.Chat bots, with typed events, a cache, and an
ergonomic command layer — the Rocket.Chat equivalent of what serenity/twilight/poise
are for Discord.

This document is the result of a source-level survey of the Rocket.Chat server
(`RocketChat/Rocket.Chat` @ `ea163f56`, `apps/meteor` **8.8.0-develop**, verified against
tags 6.5.0 / 7.0.0 / 8.0.0), the embedded Meteor DDP server (`METEOR@3.4.1`), the
first-party `@rocket.chat/ddp-client`, `Rocket.Chat.js.SDK`, and the architecture of
serenity 0.12.5 / twilight 0.17.1 / poise 0.6.2.

> **Sourcing note.** `developer.rocket.chat` and `docs.rocket.chat` are unreachable from
> the build environment, and — more importantly — **the published docs are years out of
> date**. Every protocol claim below is taken from server source, not doc prose. Where
> the docs and the source disagree, that is called out explicitly in
> [§10 Documentation errata](#10-documentation-errata).

---

## 1. The finding that shapes the whole design

**Rocket.Chat is dismantling the DDP method surface.** On `develop` there are ~116
`methodDeprecationLogger.method(...)` registrations across 109 files, nearly all naming
**9.0.0** as the removal version and a specific REST replacement:

```js
// apps/meteor/server/meteor-methods/messages/sendMessage.ts
methodDeprecationLogger.method('sendMessage', '9.0.0', '/v1/chat.sendMessage');
```

This is not advisory. The enforcement already exists:

```js
// apps/meteor/server/api/ApiClass.ts:867
if (options.deprecation && shouldBreakInVersion(options.deprecation.version)) {
  throw new Meteor.Error('error-deprecated', `The endpoint ${route} should be removed`);
}
```

And methods have already been deleted on schedule: `deleteMessage`, `reportMessage`,
`eraseRoom` vanished in **7.0**; `getUserRoles`, `muteUserInRoom` and the
`rooms.upload/:rid` endpoint vanished in **8.0**.

Independently, **DDP `sendMessage` physically cannot send a rich message.** Its argument
validator is a closed whitelist:

```js
check(message, { _id, rid, msg, tmid, tshow, ts, t, bot, content,
                 e2e, e2eMentions, customFields, federation, groupable, sentByEmail });
```

`attachments`, `blocks`, `alias`, `avatar`, `emoji` are absent — passing any of them
throws `Match.Error`.

### Decision 1 — DDP is ingress, REST is egress

| | Transport | Why |
|---|---|---|
| **Receiving** events | **DDP** `stream-*` | No REST equivalent exists or ever will. Deletions, typing, presence, and read-state are *only* observable here. |
| **Acting** on the server | **REST** `/api/v1/*` | The only surface with a forward guarantee. Also the only one that can send attachments, blocks, or files. |

A small set of DDP methods stay, because they are **not** deprecated and have no REST
equal — chiefly the `updatedSince` delta-sync family (`rooms/get`, `subscriptions/get`,
`permissions/get`, `public-settings/get`) and the only cursor-paginated API in the
product (`messages/get`). See [§4](#4-transport-placement-matrix).

This is exactly twilight's gateway/http split, arrived at from the opposite direction.

---

## 2. Crate layout

Follow twilight, not serenity. The download ratio on the Discord side is instructive —
`twilight-http` has ~4.6× the downloads of `twilight-gateway`, because most consumers
want types and REST without a socket. For Rocket.Chat that skew should be larger still,
given §1.

```
rocketsocket-model      serde types, Id<Marker>, EJSON, zero IO, zero async
rocketsocket-rest       reqwest client, one module per API section
rocketsocket-realtime   DDP over WebSocket: subscriptions + liveness
rocketsocket-cache      opt-in in-memory cache, ResourceType bitflags
rocketsocket-macros     #[event] proc macro
rocketsocket            facade: Client, event dispatch, framework
rocketsocket-codegen    dev-only: generates model + event enums from vendored RC sources
```

Rationale for each split:

- **`-model` alone** is what a CI script posting a message needs. It must not drag in
  tokio.
- **`-rest` alone** is a complete, useful Rocket.Chat client. Many users will stop here.
- **`-realtime` alone** is usable as a raw DDP client for non-bot purposes.
- **`-cache` opt-in** because a bot that only answers commands does not want to hold
  every room in memory.

`rocketsocket` re-exports everything behind features (`rest`, `realtime`, `cache`,
`framework`) so the single-dependency path stays ergonomic.

---

## 3. Protocol layer (`rocketsocket-realtime`)

### 3.1 Two server implementations, not one

This is the single least-documented fact about Rocket.Chat's websocket, and it will
cause bugs if ignored. There are **two** DDP servers:

1. **Monolith** (default): stock Meteor `ddp-server`. `/websocket` is internally
   rewritten to SockJS's raw-websocket transport.
2. **`ddp-streamer`** (EE / microservices mode, `ee/apps/ddp-streamer`): a hand-written
   DDP server on `ws`, proxying unknown methods to the monolith.

They differ in ways a client must absorb:

| Behaviour | Monolith | `ddp-streamer` |
|---|---|---|
| Pre-`connect` greeting | none (removed in Meteor 2.3) | `{"msg":"server_id","server_id":"0"}` — **has a `msg` key** |
| Session id | `Random.id()`, 17 chars | UUIDv1 |
| Version negotiation | enforced, sends `failed` | ignored entirely |
| Heartbeat | 15 s idle → `ping`, +15 s → close | 30 s idle → `ping`, +30 s → close 4000 |
| `result` vs `updated` order | `updated` **then** `result` | `result` **then** `updated` |
| `result` on falsy return | present (`!== undefined` check) | **omitted** (truthiness check — a method returning `false` is indistinguishable from one returning nothing) |
| `unsub` with unknown id | always replies `nosub` | **silent**, no reply |
| EJSON scope | only `fields`/`params`/`result` | whole message |
| **`login` payload** | full handler chain (password, OAuth, LDAP, …) | **`{resume}` only** |

That last row is decisive. Verified directly:

```ts
// ee/apps/ddp-streamer/src/configureServer.ts:68
async 'login'({ resume }: { resume: string }) {
  const result = await Account.login({ resume });
```

**Password login over DDP simply does not work on EE microservice deployments.**

### 3.2 Decision 2 — authenticate over REST, resume over DDP

`POST /api/v1/login` is literally a wrapper around the DDP `login` method
(`ApiClass.ts:1089`: `Meteor.callAsync('login', args)`), and both transports read the
same `services.resume.loginTokens` array. So:

1. `POST /api/v1/login` (or use a Personal Access Token directly) → `{userId, authToken}`.
2. Use that pair as `X-User-Id` / `X-Auth-Token` for all REST.
3. Use the same token as `{"resume": "<token>"}` for the DDP `login` method.

One credential, both transports, works identically on monolith and streamer, sidesteps
2FA (logins of `type: 'resume'` are exempt from `onValidateLogin`), and avoids the
`MAX_RESUME_LOGIN_TOKENS` cap (**default 50**) that a bot re-authenticating with a
password on every reconnect would churn through.

**Personal Access Tokens are the recommended bot credential**: they are entries in the
same `loginTokens` array but carry **no `when` field**, so `_tokenExpiration(undefined)`
yields an invalid date and the expiry comparison is always false — **PATs never expire**.
They also carry `bypassTwoFactor`.

### 3.3 No session resume — reconnect is a full re-handshake

`METEOR@3.4.1` still carries the original TODO; `connect.session` is read by neither
implementation:

```js
// In the future, handle session resumption: something like:
//  socket._meteorSession = self.sessions[msg.session]
```

On close, the server tears down every subscription **without** sending `nosub`/`removed`.
So every reconnect is: `connect` → `connected` → `login {resume}` → **re-issue every
`sub`**. The framework owns this; users must never write resubscribe logic (which is
exactly what `rocketchat-async` pushes onto its users, and what makes it painful).

Ordering matters: the monolith processes a session's inbound frames strictly serially, so
dispatching `login` before the re-`sub`s guarantees they are handled in that order. Do not
re-subscribe from a task that could race ahead of login — subscribing before `login`
triggers `setUserId`, which **reruns every active subscription** and produces a diff storm.

### 3.4 Connection state machine

Model the protocol as a **synchronous state machine returning actions**, with an async
runner executing them — serenity's `ShardAction` split. It makes the handshake
unit-testable without a socket.

```
Idle → Connecting → Handshaking ─(connected)→ Authenticating ─(result)→ Ready
                          └──────(failed)────→ FatallyClosed
  Ready ─(close)→ Disconnected{attempts} → backoff → Connecting
  Authenticating ─(403 expired/logged-out)→ FatallyClosed
```

`FatallyClosed` must be a real terminal state that ends the event stream, not another
retry. A resume token rejected with `"You've been logged out by the server"` or
`"Your session has expired"` will be rejected forever; retrying it is the classic bot
hot-loop. Twilight's `ShardState::FatallyClosed` returning `Poll::Ready(None)` is the
right shape — the user's `while let` loop simply ends.

Backoff: `2^(n-1)` seconds with **full jitter**. Bots restart in fleets; unjittered
backoff synchronises them into a thundering herd.

### 3.5 Task layout

One actor task owning the **unsplit** `WebSocketStream` in a `select!` loop (siderite's
model), with an `mpsc` for outbound frames. This avoids the `BiLock` of `.split()` and,
more importantly, makes reconnect tractable — there is exactly one owner of the socket.

The reader dispatches into three maps:

```rust
calls:   HashMap<CallId, (Epoch, oneshot::Sender<Result<Value, RpcError>>)>,
subs:    HashMap<SubId, SubState>,
streams: HashMap<(Collection, EventName), broadcast::Sender<Event>>,
```

**Non-negotiables**, each learned from a specific failure in a reference implementation:

1. **Settle every waiter on disconnect.** A DDP response can only arrive on the connection
   its request went out on. On socket death, drain `calls` and send an explicit error —
   a dropped `oneshot::Sender` gives `RecvError`, which loses the reason.
2. **Distinguish "never hit the wire" from "may have executed."**
   `enum Abandoned { NotSent, Sent { id: CallId } }`. The caller of `chat.sendMessage`
   needs to know whether the message might already be posted.
3. **Tag pending entries with a connection epoch**; drop responses whose epoch is stale.
   Rust's ownership does *not* save you here — timers and in-flight futures outlive the
   socket. The JS SDK needs `if (closedConnection !== this.connection) return` for exactly
   this reason.
4. **Never kill the connection on an unknown response id** — siderite does, and it turns a
   stray frame into a full disconnect. Log and continue.
5. **Serialise `sub`/`unsub` per subscription id.** Both carry the same id; a `nosub`
   answering an `unsub` will otherwise settle the pending `sub`.
6. **`sub` is idempotent by id and silently dropped** if the id is already live — no
   `ready`, no `nosub`, your future hangs forever. Always allocate a fresh id.
7. **`ready.subs` is an array** and can batch several ids. Iterate it; do not read
   `subs[0]` (which is what `Rocket.Chat.js.SDK` does).

### 3.6 Heartbeat

The DDP heartbeat is `{"msg":"ping"}` / `{"msg":"pong"}` **inside text frames** — not
RFC 6455 ping frames. Neither server sends WS-level pings.

The server drives it, and **any inbound frame suppresses it**, so a busy bot may never see
a `ping` at all. Client policy: reset an idle timer on *every* inbound frame; send an
application `ping` at 25 s idle; declare dead at 45 s. That sits inside the monolith's
15+15 s budget and the streamer's 30+30 s budget without being chatty.

If a `ping` carries an `id`, the `pong` **must** echo it.

Pings bypass the server's message queue entirely, so they survive head-of-line blocking —
which means they are a genuine liveness probe but **cannot** be used as a barrier.

Send a low-frequency WS-level ping (~30 s) purely to keep proxies and load balancers from
idling out the TCP connection.

### 3.7 Head-of-line blocking — and Decision 3

The monolith runs a session's messages **one at a time, in order**. A slow method blocks
every later method *and every later `sub`* on that connection.

**Decision 3: two DDP connections.** One for the event stream (subscriptions only, stays
responsive), one for bulk RPC (`loadHistory` backfill, `spotlight`). Same resume token,
two sessions. This is the single biggest throughput lever, and it has no analogue in
Discord frameworks because there the gateway and REST are already separate.

Pipelining is otherwise safe: write N `method` frames back-to-back without waiting.

---

## 4. Transport placement matrix

| Capability | Use | Note |
|---|---|---|
| All messages, all rooms | **DDP** `stream-room-messages` / `__my_messages__` | one subscription covers everything the bot can read |
| Deletions | **DDP** `stream-notify-room` `<rid>/deleteMessage` | per-room; **no global delete stream** |
| Typing | **DDP** method `stream-notify-room` | see landmine §9.1 |
| Presence, read-state, room membership | **DDP** streams | not observable any other way |
| Room/subscription delta sync | **DDP** `rooms/get`, `subscriptions/get` (`updatedAt`) | polymorphic return: no arg → snapshot; `Date` → `{update, remove}` |
| History backfill with cursors | **DDP** `messages/get` | the only cursor pagination in the product |
| Send plain text | **REST** `chat.sendMessage` | DDP equivalent deprecated → 9.0 |
| Send attachments | **REST** `chat.sendMessage` / `chat.postMessage` | DDP `check()` rejects them |
| Send blocks (UI Kit) | **REST** `chat.sendMessage` **only** | `chat.postMessage` schema excludes `blocks` |
| Upload a file | **REST** `rooms.media` + `rooms.mediaConfirm` | two-step; see §9.4 |
| Delete / report / roles | **REST** | DDP methods removed in 7.0 / 8.0 |
| Reactions, edits, read receipts | **DDP** (`setReaction`, `updateMessage`, `readMessages`) | not deprecated |

---

## 5. Model layer (`rocketsocket-model`)

### 5.1 Typed IDs

Take twilight's `Id<Marker>`, not serenity's macro-generated nominal newtypes: one impl,
one `Deserialize`, uniform `HashMap<Id<M>, _>`, and an explicit `cast()` for the
legitimate aliasing cases.

```rust
pub struct Id<T> {
    phantom: PhantomData<fn(T) -> T>,   // Send+Sync+Copy regardless of T
    value: CompactString,               // RC ids are 17-char strings; inline, no alloc
}
```

Markers: `RoomMarker`, `UserMarker`, `MessageMarker`, `SubscriptionMarker`, `RoleMarker`.
`compact_str` stores up to 24 bytes inline, so every Rocket.Chat id is allocation-free.

### 5.2 Optionality is the hard part

The single most important correctness property: **the deserializer must never fail on a
missing field.** Rocket.Chat projects documents differently per stream, and the TypeScript
types lie — fields declared non-optional in `core-typings` are routinely dropped by
`publishFields.ts` projections.

Genuinely always present:

- `IMessage`: `_id`, `_updatedAt`, `rid`, `msg`, `ts`, `u{_id}` — `u.username` is
  optional in practice despite `IMessage` typing `u` as `Required<...>`, because
  `IUser.username` is optional at source and partial `changed` frames can deliver a
  stub `u`.
- `IRoom`: `_id`, `_updatedAt`, `t`
- `ISubscription`: `_id`, `_updatedAt`, `rid`, `u`, `t`, `ts`, `name`, `open`, `unread`,
  `userMentions`, `groupMentions`
- `IUser`: `_id` **only** — corrected during implementation. `_updatedAt` is *not*
  guaranteed: `Users:NameChanged` carries `Pick<IUser, '_id'|'name'|'username'>`
  (`streams.ts:223`) and the user cache projects `{_id, roles}`
  (`publication-user-cache.ts`).

**Everything else is `Option<T>` + `#[serde(default)]`.** Notably `IRoom.msgs`/`usersCount`
and `IUser.roles`/`type`/`active` are non-optional in TS but *are* dropped by stream
projections.

Type traps worth encoding up front:

- `IMessage.starred` is `Vec<{_id}>`, **not** `bool`. `IMessage.pinned` *is* `bool`.
- `IRoom.sysMes` is `bool | Vec<MessageType>` — untagged enum.
- `IRoom.rolePrioritiesCreated` is `bool | i64` — untagged enum.
- `ISubscription.hideUnreadStatus` is the TS literal `true`; absent means false.
- `u.name` may be absent **or `null`**.
- `IMessage.reactions` is a map keyed by `":emoji:"`.

### 5.3 EJSON

Only three top-level fields are EJSON-adjusted by the monolith:

```js
['fields', 'params', 'result'].forEach(field => { /* _adjustTypesFromJSONValue */ });
```

So `error.details` is **not** decoded there (its dates arrive as ISO-8601 strings), while
`ddp-streamer` EJSON-decodes the whole frame. A recursive decoder applied to
`fields`/`params`/`result` **and defensively `error.details`** is correct for both.

In practice `{"$date": ms}` is the only encoding that matters: `$binary` is absent (files
go over REST) and `grep -rn "EJSON.addType"` across the monorepo returns **zero hits**, so
there are no custom types.

Accept all three forms a date can arrive as — `{"$date": i64}`, ISO-8601 string (REST
responses use strings), and a bare number (legacy documents) — behind one
`deserialize_with`.

`cleared` ↔ `undefined`: `stringifyDDP` deletes `fields` entirely when it becomes empty, so
`{"msg":"changed","collection":"c","id":"i","cleared":["a"]}` with **no `fields` key** is
valid. `fields: Option<Map<..>>`, never a defaulted empty map.

### 5.4 Forward compatibility

The published crate must survive a Rocket.Chat minor release without a new version.

- `#[non_exhaustive]` on every public enum, and on individual variants that may gain
  fields (poise's practice). For config/builder structs prefer a real builder so users
  keep `..Default::default()`.
- **Never `deny_unknown_fields`** in the public model — gate it behind a `strict` feature
  used only by our own test suite, so CI tells us when RC adds a field without breaking
  users.
- `Unknown(String)` escape variants on every wire enum via
  `#[serde(from = "String", into = "String")]` — round-trips losslessly, never errors.
  The system-message `t` enum has grown every major release.
- Frame decoding: internally-tagged on `msg`, with `#[serde(untagged)] Unknown(UnknownMessage)`
  as the **last** variant (serenity's trick — `#[serde(other)]` cannot carry a payload).
- Keep `Box<RawValue>` on unknown events so users are never blocked by our model lagging
  the server.
- The decoder must tolerate a frame with **no `msg` key at all** (the legacy greeting) and
  one with `msg: "server_id"`.
- Model `msg: "error"` explicitly — the official client omits it from its union type and
  silently swallows protocol errors. That is our only signal that we sent a malformed frame.
- DDP `error` codes are `String | Number` (`403` arrives as a bare number):
  `#[serde(untagged)] enum ErrorCode { Num(i64), Str(String) }`.

### 5.5 Decision 4 — generate the model, don't hand-write it

`packages/ddp-client/src/types/streams.ts` is a 499-line machine-readable catalog:
`StreamerEvents` maps every stream → every event key → the exact positional `args` tuple.
`packages/core-typings/src/` holds the entity interfaces.

`rocketsocket-codegen` vendors a **pinned** copy of both and emits Rust with the TypeScript
compiler API. This is the difference between a crate that tracks Rocket.Chat releases and
one that rots in eighteen months — which is what happened to every prior art client
surveyed (§11).

Generated output is committed, reviewed like source, and regenerated by a CI job that
opens a PR when the pinned RC tag moves. Hand-written `impl` blocks live in separate files
so regeneration never clobbers them.

---

## 6. Event layer

### 6.1 Wire shape

Every stream event is a DDP `changed` on a **fake collection** with a **constant document
id `"id"`**:

```json
{"msg":"changed","collection":"stream-room-messages","id":"id",
 "fields":{"eventName":"GENERAL","args":[ /* IMessage */ ]}}
```

The design rationale is stated in `packages/ddp-client/src/types/SDK.ts`: bypassing
Meteor's mergebox, because it "doesn't scale well for many clients."

**Dispatch on `(collection, fields.eventName)` — never on `id`.** All subscriptions to one
stream share a collection *and* a document id; only `eventName` distinguishes them.
And `stream-user-presence` breaks the constant-id convention anyway, using the uid as both
`id` and `eventName`.

`args` is **positional and variable-arity**. Deserialize as `Vec<Value>` first, then
destructure leniently:

- `stream-room-messages` keyed by rid → `[IMessage]`
- `__my_messages__` → `[IMessage, {roomParticipant, roomType, roomName}]` — an **extra**
  trailing element appended by the `allowEmit` transform
- `user-status` → `[[uid, username, statusCode, statusText?, name?, roles?, ...]]` — a
  **nested** one-element array
- tuple arity has changed across versions (`user-status`: 3 → 6 → 8 elements)

Because EJSON encodes `undefined` **inside an array** as `null` but drops it as an object
value, expect literal `null` holes in these tuples.

### 6.2 Decision 5 — stream first, handler trait generated alongside

Primary surface is twilight's: `impl Stream for Connection`, user-controlled concurrency,
no `Clone` bound, no per-event task-spawn tax, trivially forwardable to a broker.

```rust
while let Some(event) = client.next_event().await {
    cache.update(&event);
    tokio::spawn(handle(event, http.clone()));
}
```

But serenity's `EventHandler` trait is genuinely more ergonomic for a first bot. Ship both
— generated from **one** declaration list by a `macro_rules!`, exactly as serenity's
`event_handler!` emits its trait, its `FullEvent` enum, and the dispatch fn from a single
source. The two surfaces then cannot drift.

Add a **`Standby` equivalent** from day one (twilight's `wait_for_message(rid, predicate)`):
a `Vec<(Predicate, oneshot::Sender)>` scanned per dispatched event. Chat bots need
"wait inline for a correlated future event" constantly — confirmations, menus, multi-turn
prompts — and a pure stream model makes that awkward.

Unknown events: skip by default (twilight), plus an opt-in raw sink carrying
`Box<RawValue>` (serenity's observability). Offer both; they cost little together.

For multi-consumer fan-out, layer an opt-in `broadcast` and **surface `Lagged(n)` to the
user rather than swallowing it** — a bot that silently missed 400 messages is worse than
one that logs it.

### 6.3 The recommended default subscription set

```
stream-room-messages   __my_messages__              all messages, one subscription
stream-notify-user     <uid>/subscriptions-changed  room join/leave, unread state
stream-notify-user     <uid>/rooms-changed          room metadata
stream-notify-user     <uid>/message                ephemeral messages to the bot
stream-notify-user     <uid>/notification           mentions and DMs, pre-filtered server-side
stream-notify-user     <uid>/force_logout           in-band warning before the socket dies
stream-notify-room     <rid>/deleteMessage          per-room, driven off subscriptions-changed
stream-notify-room     <rid>/user-activity          typing, per-room
stream-notify-logged   user-status | Users:NameChanged | roles-change
```

### 6.4 Telling a new message from an edit, a delete, or a system message

Rocket.Chat re-broadcasts the **full message document** on any mutation — reactions, pins,
thread-count bumps, URL-preview enrichment — none of which touch `editedAt`. So:

1. `editedAt` present → an edit. If additionally `t == "rm"`, it is a **delete tombstone**
   (only produced when `Message_ShowDeletedStatus` is on; with it off, a delete produces
   *no* `stream-room-messages` frame at all, only `<rid>/deleteMessage`).
2. `t` present → system message. Branch on the enum; `msg` is often empty or holds a
   username as an argument. Never render it as user text.
3. Otherwise → user message, **but that is not sufficient for "new"**. Keep a seen-set of
   `_id`, and/or compare `_updatedAt` against `ts` (equal within milliseconds on a genuinely
   new message).
4. Filter your own echo on `u._id == self_id`. Do **not** filter on `msg.bot` — that field
   is `@deprecated`, is never set for bot-role users, and is only populated by the
   integrations subsystem.

The framework should expose this as a proper `MessageEvent { Created, Edited, Deleted,
System(..) }` enum so no user ever writes this logic. `Rocket.Chat.js.SDK` put the filter
predicate in the library rather than the adapter, and `hubot-rocketchat` was 200 lines
because of it. Copy that.

---

## 7. Cache layer (`rocketsocket-cache`)

Opt-in, twilight-shaped:

- **`ResourceType` bitflags**, not serenity's three `bool`s — it scales and is `const`.
  Every update guards on `if !cache.wants(ResourceType::X) { return }`.
- **`trait UpdateCache`** implemented per payload type, taking `&self`.
- **`Reference<'a, K, V>`** newtype wrapping `dashmap::mapref::one::Ref`, so `dashmap` is
  not part of the public API and can be swapped. Serenity leaks its `ReadOnlyMapRef`; do
  not repeat that.
- Bounded message ring per room (`VecDeque` push-front/pop-back), **on by default** —
  serenity's default of `max_messages: 0` surprises people.
- Document the guard-across-`.await` deadlock hazard loudly, and return owned clones for
  small hot values (`current_user`).
- **Cache rooms and users.** The JS SDK's old `roomCacheMaxSize` LRU existed because
  `getRoomIdByNameOrId` is the hottest lookup in any Rocket.Chat bot. The modern SDK
  dropped caching and pushed it onto consumers; we should not.
- Have an **explicit, documented eviction policy for users** — serenity deliberately never
  evicts them (a permanent leak by design) because members elsewhere still reference them.
  Whatever we choose, say so in the docs.

Naming discipline, copied from serenity because it is genuinely good: `_cached` suffix for
"never hits the network, returns a guard, returns `Option`", plain name for "falls back to
HTTP, returns owned, returns `Result`". Two names, two signatures, no hidden IO.

---

## 8. Framework layer

> The full API design — `#[event]` / `#[command]` / `#[cog]` proc macros, extractor-style
> parameters, argument converters, and how entities get `msg.reply(..)` without polluting
> the model crate — is in **[docs/dx.md](docs/dx.md)**. This section covers the structural
> decisions that document builds on.

**Decision 6: generic `Client<Data>`, not a typemap.** Serenity's
`Arc<RwLock<TypeMap>>` has a global lock, a runtime `Option` on every get, and no
compile-time guarantee the key was inserted. Poise abandoned it for a generic `U`; a 2026
greenfield crate should start there.

Commands as **data produced by a macro**, poise-style: `#[command]` expands to a function
returning a `Command<Data, Error>` **value** you can inspect, mutate at runtime, and store
in a `Vec` — not a registration side effect. Callbacks are plain `fn` pointers, not boxed
closures, so `Command` stays cheap.

Keep the framework a thin, optional layer over a free function
`dispatch_event(framework, ctx, &event)` — poise's `Framework` struct is explicitly
optional sugar over exactly that, which is why serenity emits its event *enum* alongside
the trait. Users who want their own dispatch keep it.

Errors: one `#[non_exhaustive]` enum covering every phase, each variant carrying full
borrowed context, with resolution order command-level → framework-level → built-in.

**Unify the two transports' error shapes**, because they are gratuitously different:

| Unified field | REST | DDP |
|---|---|---|
| `code` | `errorType` | `error` |
| `message` | `error` | `reason` |
| `details` | `details` | `details` |

Note `errorType` means opposite things: on REST it is the machine code; on DDP it is
always the literal string `"Meteor.Error"`. Also, several DDP methods signal failure by
**returning `false`** rather than raising (`loadHistory`, `loadMissedMessages`,
`unfollowMessage`) — a `Result` mapping that only handles the `error` frame will silently
mis-handle these.

REST 403 currently returns the string `"unauthorized"` for backward compatibility
(there is a `// TODO: MAJOR` to change it). **Discriminate on HTTP status, never on the
error string.** Auth-middleware rejections return **plain text**, not JSON, so the
deserializer must tolerate a non-JSON body on 401/403.

---

## 9. Landmines

These are the things that will silently waste a day each. Every one is verified in source.

### 9.1 Typing indicators validate the username against a workspace setting

```js
const key = (await Settings.get('UI_Use_Real_Name')) ? 'name' : 'username';
return user[key] === username;
```

Send the wrong one and `allowWrite` fails **silently** — no error, the indicator just never
appears. The framework must read `UI_Use_Real_Name` from `public-settings/get` at startup
and pick the right field. This is the single most likely bug in a from-scratch client.

Also: `user-activity` is the **only** writable event on `stream-notify-room`. The legacy
`<rid>/typing` key still exists for reading but cannot be written, and the server stopped
*emitting* it in **6.0** (the 4.0–5.x bidirectional bridge was deleted).

### 9.2 The `bot` role is a hard prerequisite

It is not cosmetic. Default grants include:

- `api-bypass-rate-limit` — **skips the REST rate limiter entirely**. Without it you get
  **10 requests / 60 s per route per IP**, which will cripple a bot instantly.
- `send-many-messages` — bypasses the 5 msg/s `sendMessage` limit.
- `message-impersonate` — required for `alias` / `avatar` / `emoji`.

Document it as a setup requirement, and detect its absence at startup with a clear error.

### 9.3 DDP rate limits are per-method-per-connection

The binding constraint is **10 calls per method per 10 s per connection** (not the 600/min
connection budget). `stream-*` methods get a ×4 multiplier, so typing gets 40/40 s — the
client's 5 s renewal fits, a per-keystroke emitter does not.

`RATE_LIMITER_SLOWDOWN_RATE` makes the server *sleep* before returning the error, so a
throttled client sees **latency, not rejection**. Do not diagnose that as network trouble.
Honour `error.details.timeToReset` as the backoff.

REST `X-RateLimit-Reset` is an **absolute epoch in milliseconds**, not the conventional
seconds-remaining. Do not feed it to `Duration::from_secs`.

### 9.4 File upload is a two-step transaction

`rooms.upload/:rid` was **removed in 8.0**. The current flow:

1. `POST /api/v1/rooms.media/:rid` (multipart, field name is hard-coded `file`)
   → `{file: {_id, url}}`. The upload is **temporary, `expiresAt` = now + 24 h**.
2. `POST /api/v1/rooms.mediaConfirm/:rid/:fileId` (JSON) → `{message}`.

**Skipping step 2 leaves an orphaned file and posts nothing.** Treat it as a transaction
with retry and cleanup. DDP cannot do this at all — it is a text protocol with no binary
frame and no chunking.

### 9.5 Interactive UI Kit is closed to plain bots

A plain bot user **can send** `blocks` — but only via `POST /v1/chat.sendMessage`
(`chat.postMessage`'s schema excludes them, and DDP rejects them).

It **cannot receive** button clicks. The click path is
`POST /api/apps/ui.interaction/:appId` → Apps-Engine; an unregistered `appId` 404s. And
`stream-notify-user` `<uid>/uiInteraction` is **server→client** — its only producer is the
Apps-Engine bridge telling a *client* to open a modal. Subscribing to it tells you nothing
about other users' clicks.

The workaround, and it does work today: deprecated **attachment action buttons** with
`msg_processing_type: "sendMessage"` make the *clicking user's own client* post a message,
which the bot then receives on `__my_messages__` like any other message. Ugly (the
synthetic message is visible in the channel) but real.

**Expose `blocks` as write-only rich display; route genuine interactivity through
attachment actions or slash commands.** Say so in the docs so nobody builds on sand.

### 9.6 Other sharp edges

- **A `sub` on an id you already hold is silently dropped** — no `ready`, no `nosub`.
- **`__my_messages__` has a different arity** than the per-room key.
- **`stream-user-presence` breaks the constant-id convention** and mutates its watch set by
  re-subscribing with `{added, removed}`.
- **After `login` you receive an unsolicited `added` on the `users` collection** with no
  `sub` and no `ready` (Meteor's universal publication). The router must not assume every
  `added` belongs to a subscription it created.
- **`logout` on `ddp-streamer` closes the socket with code 1002** one millisecond later,
  deliberately. Do not misclassify it as a framing bug, and suppress reconnect after a
  deliberate logout.
- **Clock skew**: a client-supplied message `ts` more than 60 s from server time is
  rejected with `error-message-ts-out-of-sync`; 10–60 s is silently clamped. Omit `ts`.
- **`count=0` means unlimited** in REST pagination (when `API_Allow_Infinite_Count` is on).
  A Rust `Option<u32>` defaulting to 0 will request the entire collection. Requests above
  `API_Upper_Count_Limit` (100) are **silently clamped**, not rejected.
- **REST `query`/`fields` are silently ignored** unless
  `ALLOW_UNSAFE_QUERY_AND_FIELDS_API_PARAMS=TRUE`, and are slated for removal in 9.0. Do
  not expose them.
- **`createDirectMessage` is variadic** (`...usernames`), not array-taking.
- `channels.*` / `groups.*` / `im.*` are three parallel families for public / private /
  direct. There is no unified room-mutation API. Model a `RoomKind` enum that selects the
  endpoint family internally — this trichotomy is the biggest source of accidental
  complexity in every Rocket.Chat client.

---

## 10. Documentation errata

The published docs cannot be trusted for this project. Confirmed divergences:

| Doc claim | Reality |
|---|---|
| `{"msg":"callWithTwoFactorRequired", ...}` satisfies a 2FA challenge | **No such thing.** `grep` across the monorepo: zero hits. Real mechanism is a trailing `{twoFactorCode, twoFactorMethod}` param, or `{totp:{login, code}}` for `login`. |
| The trailing `false` in `sub` params is a "back-compatibility" flag; `true` gives you add-events for new items | It is `useCollection`. `true` emits **one** `added` at subscribe time and nothing after — and is a **no-op** on `ddp-streamer`. |
| "The server periodically sends a ping" | Only when **idle**. Any inbound frame suppresses it. |
| `stream-notify-all` carries `roles-change`, `updateAvatar`, `permissions-changed` | Stale — those moved to `stream-notify-logged`. |
| `stream-notify-user` carries `otr` / `webrtc` | **Removed in 8.0.** |
| `stream-notify-room` carries `typing` | Server stopped emitting it in **6.0**. |
| `stream-livechat-queue-data` | Never existed under that name since 3.0; it is `stream-livechat-inquiry-queue-observer`. |
| Docs never mention it | Session resume is **not supported**; the pre-`connect` greeting must be ignored; the post-login unsolicited `users` `added` arrives with no `sub`. |

`packages/ddp-client/src/types/streams.ts` is the only reliable catalog. Pin to a server
major and gate removed variants behind a version check.

---

## 11. Prior art — the niche is empty

There is **no publishable-quality Rust Rocket.Chat client and no maintained Meteor DDP
crate.**

| Crate | Last release | Verdict |
|---|---|---|
| `rocketchat` | 2022-11 | REST-only thin wrapper, abandoned |
| `rocketchat_client_rs` | 2019-08 | webhook sender, not a client |
| `rocketchat-hooks` | 2017-11 | repo archived |
| `siderite` | 2021-04 | **best reference in the language** — generic DDP, 580 LOC. Not usable as a dependency (ancient TLS, no reconnect, no resubscribe) but its actor loop and `Slab`-based id allocator are worth stealing |
| `ddp` | 2015-08 | pre-1.0-Rust experiment |
| `spaceman` | 2019-11 | literally "RocketChat bot framework in Rust", 1★, synchronous, hands you raw JSON strings |

Note `ddp-rs` / `ddp-connection` on crates.io are the **Distributed Display Protocol** (LED
pixels), unrelated.

Outside Rust: `Rocket.Chat.js.SDK` is actively developed and is the best architectural
source (its liveness/reconnect logic is directly portable). `hubot-rocketchat` was marked
unsupported in 2025-10. `rocketchat-async` (Python) is deprecated by its author, citing the
Realtime API deprecation. `Rocket.Chat.Go.SDK` is unmaintained.

**rocketsocket would be first.** The name also avoids the crowded `rocket*` namespace.

---

## 12. Compatibility target

**Baseline Rocket.Chat 7.0.** Supporting 6.5 means carrying `rooms.upload`, DDP
`deleteMessage`, and a different `deleteMessage` signature, for a release line that is
already two majors back.

Probe `GET /api/info` at connect, store the server version, and gate feature availability
on it. Detect monolith vs `ddp-streamer` from the greeting frame, the session-id shape, and
the `result`/`updated` ordering; cache the answer per host.

Plan explicitly for **9.0**, which will remove the ~116 deprecated methods, the REST
`query`/`fields` params, and change the 403 error string.

---

## 13. Milestones

| # | Deliverable | Exit criteria |
|---|---|---|
| **M0** | Workspace skeleton, CI, `rocketsocket-model` frame types | DDP frames round-trip; fuzz corpus of real captures decodes without panic |
| **M1** | `rocketsocket-realtime` connection actor | connect → login(resume) → sub → receive; reconnect with resubscribe survives a server restart under test |
| **M2** | `rocketsocket-rest` + auth unification | one credential drives both transports; PAT and password paths both work |
| **M3** | `rocketsocket-codegen` + generated event enum | every stream in `streams.ts` has a typed variant; regeneration is a CI job |
| **M4** | Event layer: `Stream`, generated `EventHandler`, `Standby` | echo bot in <30 lines; `MessageEvent` correctly classifies new/edit/delete/system |
| **M5** | `rocketsocket-cache` | rooms/users/messages cached; `ResourceType` gating verified; no deadlock under concurrent update+read |
| **M6a** | `#[event]` macro, extractors, registration, facade entity types | echo bot is one annotated fn; `msg.reply()` works; empty-registry startup check fires |
| **M6b** | ~~`#[command]` + `#[cog]`~~ — **cut**, see docs/dx.md §7 | text commands stay user-space: parse them in an `#[event]` handler |
| **M7** | Docs, examples, 0.1 release | published to crates.io with the landmine list as a "Gotchas" doc page |

Integration testing throughout against a **Dockerised Rocket.Chat** — pinned to 7.x and
latest 8.x, and ideally one microservices deployment to exercise the `ddp-streamer`
divergences, which no amount of unit testing will catch.

---

## 14. Open questions for you

1. **Server versions to support.** I've assumed 7.0+ baseline. Do you need 6.x?
2. **Deployment shape.** Is your target a standard monolith, or EE/microservices? It
   changes which `ddp-streamer` divergences are load-bearing rather than defensive.
3. **Scope of the first release.** Is Omnichannel/Livechat in or out? It roughly doubles
   the stream surface and the model.
4. **E2EE.** Out of scope for 0.1, I assume — it would mean implementing Rocket.Chat's
   `rc.v1.aes-sha2` key exchange.
5. **Crate split vs single crate.** I've argued for the twilight-style split. If you'd
   rather ship one crate with features, that's a defensible simplification — it's easier
   to split later than to merge.

---

## 15. Sources

Server source (primary, all claims verified here):
`RocketChat/Rocket.Chat` @ `ea163f56` — `packages/ddp-client/src/types/streams.ts`,
`apps/meteor/server/modules/streamer/streamer.module.ts`,
`.../modules/notifications/notifications.module.ts`, `.../modules/listeners/listeners.module.ts`,
`apps/meteor/server/api/ApiClass.ts`, `apps/meteor/server/meteor-methods/messages/sendMessage.ts`,
`apps/meteor/server/settings/rate.ts`, `apps/meteor/server/lib/authorization/constant/permissions.ts`,
`packages/core-typings/src/`, `ee/apps/ddp-streamer/src/`.

Meteor `METEOR@3.4.1` — `packages/ddp-server/livedata_server.js`,
`packages/ddp-common/utils.js`, `packages/accounts-base/accounts_server.js`.
DDP spec: `meteor/meteor` `packages/ddp/DDP.md`.

Clients: `@rocket.chat/ddp-client`, `RocketChat/Rocket.Chat.js.SDK`,
`hynek-urban/rocketchat-async`, `jadolg/rocketchat_API`, `maugier/siderite`.

Architecture references: `serenity-rs/serenity` 0.12.5, `twilight-rs/twilight` 0.17.1,
`serenity-rs/poise` 0.6.2.
