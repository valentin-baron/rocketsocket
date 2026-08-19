# Developer experience — the discord.py-shaped API

Companion to [../PLAN.md](../PLAN.md). That document decides what the framework talks to;
this one decides what it feels like to use.

**Goal:** a first bot should be one file, one `#[event]`, and `cargo run`.

---

## 1. What discord.py actually gives you

It's worth naming the properties rather than the syntax, because Rust reaches some of them
by different means:

1. **Declare a handler where it lives.** No central registry to edit, no trait to
   implement, no match arm to extend.
2. **Signatures say what you need.** `async def on_message(message)` — you take the one
   argument you care about.
3. **Entities are live.** `await message.channel.send(...)` — the object you were handed
   can act, without you threading a client through.
4. **Commands are functions with typed arguments.** `async def add(ctx, a: int, b: int)` —
   parsing and conversion happen before your body runs.
5. **Cogs group related behaviour with its state**, and load/unload as a unit.

All five are reachable in Rust. Property 3 is the one that fights the crate split, and §6
resolves it.

---

## 2. Target API

```rust
use rocketsocket::prelude::*;

struct Data { start: Instant }

/// Event identity comes from the parameter type, not the function name.
#[rocketsocket::event]
async fn greet(ctx: Context<Data>, msg: MessageCreate) -> Result<()> {
    if msg.is_own() { return Ok(()); }
    if msg.text().contains("hello") {
        msg.reply("hi!").await?;
    }
    Ok(())
}

/// Take only what you need, in any order — the rest are extractors.
#[rocketsocket::event]
async fn on_delete(ev: MessageDelete, State(data): State<Data>) -> Result<()> {
    tracing::info!(?ev.message_id, uptime = ?data.start.elapsed(), "message deleted");
    Ok(())
}

/// Doc comment becomes the help text. Arguments are parsed and converted.
#[rocketsocket::command(aliases("plus"), cooldown = "5s")]
async fn add(ctx: Context<Data>, a: i64, b: i64) -> Result<()> {
    ctx.say(format!("{}", a + b)).await?;
    Ok(())
}

#[rocketsocket::command(check = "is_moderator")]
async fn purge(ctx: Context<Data>, count: u32, #[rest] reason: Option<String>) -> Result<()> {
    ctx.channel().purge(count).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    Client::builder("https://chat.example.com")
        .credentials(Credentials::personal_access_token(env!("RC_TOKEN")))
        .prefix("!")
        .mention_prefix(true)
        .data(Data { start: Instant::now() })
        .cog(Moderation::new())
        .run()
        .await
}
```

Cogs — a struct plus an annotated `impl` block:

```rust
struct Moderation { warns: DashMap<Id<UserMarker>, u32> }

#[rocketsocket::cog]
impl Moderation {
    #[event]
    async fn track_joins(&self, ev: RoomMemberJoined, ctx: Context<Data>) -> Result<()> { .. }

    #[command(check = "is_moderator")]
    async fn warn(&self, ctx: Context<Data>, user: User, #[rest] reason: String) -> Result<()> {
        let n = self.warns.entry(user.id.clone()).and_modify(|n| *n += 1).or_insert(1);
        ctx.say(format!("{} warned ({n}) — {reason}", user.username)).await
    }
}
```

---

## 3. Decision — event identity comes from the type, not the name

discord.py dispatches on the function *name* (`on_message`). Ported literally, that gives
Rust the worst possible failure mode: misspell `on_mesage` and it compiles, registers, and
silently never fires.

So the event is identified by the **parameter type**:

```rust
#[rocketsocket::event]
async fn anything_you_like(ev: MessageCreate) -> Result<()> { .. }
```

Exactly one parameter must implement the `Event` trait; the macro finds it and registers
against `<T as Event>::KIND`. A typo is a compile error naming the unknown type. The
function name is free, which also means two handlers for the same event don't collide.

This needs distinct newtypes per event rather than reusing entity types —
`MessageCreate` / `MessageUpdate` / `MessageDelete`, not three handlers all taking
`Message`. That falls out of the generated event enum in PLAN.md §5.5 anyway.

**Trade-off accepted:** `on_message`-style names are more familiar to someone arriving from
discord.py. We recover the familiarity in docs and examples by *naming handlers*
`on_message` anyway — it just isn't load-bearing.

## 4. Decision — extractor-style parameters

Property 2 (take only what you need) becomes an axum-style extractor trait:

```rust
pub trait FromEventParts<D>: Sized {
    fn from_event_parts(ctx: &Context<D>, ev: &EventRef<'_>) -> Result<Self, ExtractError>;
}
```

with impls for `Context<D>`, `State<D>`, `Rest`, `Cache`, `Http`, and the event type
itself, plus tuple impls up to 8 parameters. The macro generates a call that extracts each
parameter by type, in declaration order.

Keep it constrained so error messages stay good: **exactly one parameter is the event, and
every other parameter must implement `FromEventParts`.** A missing impl produces a
`#[diagnostic::on_unimplemented]` message naming the offending parameter and listing what
is extractable. Unbounded generic extraction (bevy-style) is not worth the error-message
cost here.

## 5. Decision — auto-registration, with a guard against its failure mode

Property 1 (declare it where it lives) is the headline. Rust's mechanism is
`inventory` (0.3.24, dtolnay, ~115M downloads), which does life-before-main registration
like C's `__attribute__((constructor))`:

```rust
inventory::submit! { EventHandlerEntry { kind: EventKind::MessageCreate, call: __greet_erased } }
```

**But it has a failure mode we must not ship into.** From its own README:

> Platform support includes Linux, macOS, iOS, FreeBSD, Android, Windows, WebAssembly, and
> a few others. **Beyond this, other platforms will simply find that no plugins have been
> registered.**

Silently. A bot on an unsupported target would start, connect, log in, and handle nothing —
the single worst bug a framework can have, because everything looks healthy.

So:

- **`Client::run()` hard-errors if the registry is empty**, with a message naming the
  cause and pointing at explicit registration. Zero handlers is never a legitimate
  configuration for a bot.
- **Explicit registration is always available and always supported**, and is what the
  `no-auto-register` feature leaves you with:
  ```rust
  Client::builder(url).events([greet(), on_delete()]).commands([add(), purge()])
  ```
  This works because — poise's key trick — `#[command]` expands to a function *returning a
  value*, not to a registration side effect. `add()` is a `Command<Data, Error>` you can
  inspect, filter, or mutate at runtime.
- The auto-registration path is a thin wrapper that collects those same values.

`linkme` (0.3.37) is the alternative, avoiding life-before-main via linker sections. It has
its own sharp edges with certain linkers and `--gc-sections`. `inventory` is the more
widely used and better-behaved default; the abstraction is one internal module either way,
so this is reversible.

## 6. Decision — live entities via a facade wrapper, keeping the model crate pure

Property 3 (`message.reply(...)`) is the one that conflicts with PLAN.md §2's insistence
that `rocketsocket-model` be pure data with no IO. discord.py resolves it by giving every
model a `_state` back-reference; twilight refuses the ergonomics entirely; serenity puts
methods on models that take `impl CacheHttp`.

Take a fourth option — **pure models, live facades**:

```rust
// rocketsocket-model: plain data, serde only, no async, no Arc
pub struct Message { pub id: Id<MessageMarker>, pub rid: Id<RoomMarker>, pub msg: String, .. }

// rocketsocket: the type users actually receive
pub struct MessageCreate { inner: model::Message, ctx: ContextHandle }

impl Deref for MessageCreate { type Target = model::Message; .. }

impl MessageCreate {
    pub async fn reply(&self, text: impl Into<String>) -> Result<Message> { .. }
    pub async fn reply_in_thread(&self, text: impl Into<String>) -> Result<Message> { .. }
    pub async fn react(&self, emoji: &str) -> Result<()> { .. }
    pub async fn edit(&self, text: impl Into<String>) -> Result<Message> { .. }
    pub async fn delete(self) -> Result<()> { .. }
    pub fn room(&self) -> Room { .. }       // live, cache-backed
    pub fn author(&self) -> User { .. }
    pub fn is_own(&self) -> bool { .. }
}
```

`Deref` means field access (`msg.msg`, `msg.ts`, `msg.rid`) reads through to the plain
model, while the methods give discord.py ergonomics. The model crate stays dependency-free
and serializable; users of `rocketsocket-model` alone are unaffected.

Follow serenity's `_cached` naming discipline on the accessors: `msg.room()` may hit HTTP
and returns `Result`; `msg.room_cached()` never does and returns `Option`. No hidden IO
behind an innocent-looking getter.

## 7. Decision — no command macro; text commands stay user-space

**First, a disambiguation, because "command" means two unrelated things here.**

|  | Rocket.Chat **slash commands** (`/kick`) | **`#[command]`** in this framework (`!kick`) |
|---|---|---|
| What it is | a server feature | a client-side text convention |
| Registered | `slashCommands.add(..)` in-process, or by an installed App | nowhere — nothing is registered with the server |
| Requires the Apps Engine | **yes**, for anything external | **no** |
| How the bot learns of it | it doesn't — it cannot | a normal message arrives on `stream-room-messages` |

`#[command]` involves **no Apps integration, no slash-command registry, and no server-side
registration of any kind.** It is a parser over the message stream the bot is already
subscribed to. The entire mechanism:

```
stream-room-messages  →  IMessage { msg: "!add 1 2" }   ← the websocket, nothing else
                      →  framework sees the "!" prefix
                      →  parses "1" and "2" into i64
                      →  calls your #[command] fn
                      →  reply via REST chat.sendMessage
```

That is precisely what discord.py's `commands.Bot` was: it predates Discord slash commands
by years and worked purely by reading message text. The proc macro is sugar over a `String`
match on `MessageCreate` — you could write it by hand inside an `#[event]` handler, and
that is the fallback if the macro layer is ever cut.

Slash commands proper **are** Apps Engine territory, and are out of scope. Here is why that
is not a limitation we chose but one the server imposes:

Verified in the server source: `slashCommands.add({ command, callback, appId, .. })` is an
**in-process registry** (`apps/meteor/app/utils/client/slashCommand.ts`), populated by
server code or by installed Apps. There is no REST or DDP endpoint by which an external bot
registers a slash command. Combined with PLAN.md §9.5 — a plain bot can *send* UI Kit
blocks but can never *receive* a button click, because interactions route to
`POST /api/apps/ui.interaction/:appId` and 404 for an unregistered app — the conclusion is
firm:

**For an external Rocket.Chat bot, text commands are the entire command surface — and
that is exactly why we are *not* shipping a `#[command]` macro.**

A command framework earns its complexity when the platform gives commands an in-app
affordance: Discord renders slash-command names, descriptions, and typed argument
autocomplete from what the bot registers. Rocket.Chat gives an external bot none of that —
no registration, no autocomplete, no description surface, no interaction callback. A
`#[command]` macro would therefore be pure sugar over `msg.text().split(' ')`, bought at the
cost of a proc-macro dependency, a converter trait hierarchy, an autoref-specialization
hack for `FromStr` arguments, and a `trybuild` suite to keep its error messages usable.

So: **no `#[command]`, no `#[cog]`, no converters, no checks, no cooldowns.** Bots parse
their own text inside an `#[event]` handler, which is a few lines and stays entirely under
the author's control:

```rust
#[rocketsocket::event]
async fn commands(ctx: Context<Data>, msg: MessageCreate) -> Result<()> {
    let Some(rest) = msg.text().strip_prefix('!') else { return Ok(()) };
    match rest.split_once(' ') {
        Some(("echo", arg)) => msg.reply(arg).await?,
        _ => return Ok(()),
    };
    Ok(())
}
```

If a future Rocket.Chat release gives external bots a real command registration surface,
this decision is worth revisiting — the design sketch is in this file's git history.

**Corollary — do not default the prefix to `/`.** There is one path by which slash-looking
text reaches a bot, and it is setting-dependent. `processSlashCommand.ts` handles an
unrecognised command like this:

```ts
if (typeof command === 'string') {                                    // no such command
    if (!settings.peek('Message_AllowUnrecognizedSlashCommand')) {
        await warnUnrecognizedSlashCommand(chat, t('No_such_command', { command }));
        return true;                                                  // swallowed
    }
    return false;                                                     // falls through as a normal message
}
```

`Message_AllowUnrecognizedSlashCommand` **defaults to `false`**. So by default `/mybot foo`
shows the sender "No such command" and the bot never sees it; flip the setting on and the
same text arrives as an ordinary message the bot can parse. Support `/` as a configurable
prefix, document the setting it depends on, and default to something like `!` that always
works.

Which is why discord.py is the right model to copy rather than modern discord.py or poise's
slash-first design. The prefix-command machinery — prefix and mention triggers, argument
parsing with converters, `#[rest]` trailing capture, subcommands, aliases, cooldowns,
checks, help generation — is not a legacy compatibility layer here. It is the product.


## 7a. Filters — safe by default, dangerous by opt-in

The characteristic way to break a Rocket.Chat bot is a feedback loop, and Rocket.Chat makes
it easier to hit than most platforms for three reasons that are not obvious:

1. **No trustworthy "is a bot" flag.** `IMessage.bot` is deprecated, never set for bot-role
   users, and only populated by integrations — so the obvious guard does not work.
2. **Any mutation re-broadcasts the whole message.** Reactions, pins, thread-count bumps and
   link-preview enrichment all resend the full document. A handler that replies to every
   message it sees will reply again when someone reacts to its reply.
3. **System messages share the stream**, with an empty or repurposed `msg`.

So `#[event]` filters, and **the defaults are the safe ones**. A bare `#[event]` never sees
the bot's own events, never sees edits or re-broadcasts, never sees system messages. The
flags are named for what they *allow*, so the absence of arguments is the conservative
configuration rather than the permissive one:

```rust
#[rocketsocket::event]                              // safe: no loops possible
#[rocketsocket::event(prefix = "!echo ")]           // + text match
#[rocketsocket::event(mentions_me, room = "GENERAL")]
#[rocketsocket::event(admin)]                       // author must be a server admin
#[rocketsocket::event(room_admin)]                  // owner/moderator/leader in that room
#[rocketsocket::event(any_admin)]                   // either of the two
#[rocketsocket::event(allow_self, allow_edits)]     // opt *in* to the dangerous behaviour
```

Writing `#[event(not_self)]` is a **compile error**, not a no-op. Accepting it would let
someone conclude that the guard is opt-in and that a bare `#[event]` is therefore unsafe —
the error says the opposite explicitly.

Filters run **before** extraction and before the handler body, so a handler that cannot hear
itself cannot loop whatever its body does. Role filters **fail closed**: a lookup that
errors or times out rejects. Admitting on failure would silently hand an unprivileged user
an admin-only handler; rejecting merely makes the handler quiet.

The role filters are the only ones that need the server, so they are also the only ones with
a request budget. They resolve through `roles.getUsersInPublicRoles` (one workspace-wide
answer for `admin`) and `rooms.roles?rid=` (one answer per room), memoised behind a
five-minute TTL with single-flight per key, negative caching of failures, and immediate
invalidation on `stream-notify-logged`/`roles-change`. Rocket.Chat's default limiter is 10
requests per 60 s per route per IP, so a lookup per message is not an option — see
`crates/rocketsocket/src/roles.rs` for the full reasoning, and `docs/gotchas.md` §Roles for
the endpoint traps.

`allow_bots` is **a documented no-op**. A bot account cannot discover that another account
is a bot: `IMessage.bot` is dead, `IUser.type` is only `bot` for `rocket.cat` and App users,
and every endpoint that would expose the `bot` role needs an admin permission. Unlike the
role filters, this one blocks rather than allows — so failing closed on "cannot establish"
would reject every author and silence the bot. Use `prefix` or `mentions_me` to break
bot-to-bot loops.

## 8. Errors

Errors follow poise: one `#[non_exhaustive] FrameworkError` covering every phase (setup,
event handler, extractor failure, handler body, panic), each variant carrying borrowed
context, resolved handler-level -> framework-level -> built-in. `FrameworkError::Panic`
catches unwinds so one bad handler cannot take the bot down.

## 9. Macro hygiene

Three rules, all learned from poise's implementation:

1. **The user's body goes in an untouched inner `async fn`.** The generated wrapper calls
   it. This keeps error spans pointing at real user code instead of at macro output — the
   difference between a usable and an infuriating proc macro.
2. **Attribute arguments are parsed with `darling` into a plain struct**, so unknown keys
   produce a clear "unknown field, expected one of…" error rather than a parse failure.
3. **Generated code is thin.** `#[command]` produces a function returning a `Command`
   value; `#[event]` produces a function returning an `EventHandler` value plus one
   `inventory::submit!`. Nothing heavier — proc macros are a real compile-time cost and the
   framework should not be where a user's build time goes.

Ship `trybuild` UI tests over the macros from the start. The error messages *are* the DX.

## 10. What we deliberately don't copy

- **Name-based dispatch** (§3) — trades a compile error for a silent no-op.
- **`Bot` as a god object.** discord.py's `bot` is client, registry, and dispatcher.
  Split them; `Client` runs, the builder registers.
- **Runtime cog reloading.** discord.py's `load_extension`/`reload_extension` exist because
  Python can re-import at runtime. Rust can't meaningfully, and pretending otherwise via
  `dlopen` is a trap.
- **Implicit global state.** discord.py leans on the module-level `bot`. Use poise's
  generic `Client<Data>` and `State<D>` extractor (PLAN.md §8) — compile-time checked,
  no lock on access.

---

## 11. Impact on the plan

Crate layout gains one crate, already anticipated in PLAN.md §2:

```
rocketsocket-macros    #[event], #[command], #[cog] — syn/quote/darling
```

`inventory` becomes an optional dependency of the facade behind a default-on
`auto-register` feature.

Milestones change as follows:

| # | Was | Now |
|---|---|---|
| **M4** | Event layer: `Stream`, `EventHandler`, `Standby` | unchanged — the macro layer sits *on* this, and the raw `Stream` API stays public and supported |
| **M6** | Framework: `#[command]`, `Client<Data>`, unified errors | split: **M6a** `#[event]` + extractors + registration + facade entity types; **M6b** `#[command]` + converters + checks/cooldowns + cogs + `trybuild` suite |

The layering rule that keeps this honest: **the macro layer must be strictly optional.**
Anything `#[event]` does, a user must be able to do by hand against the `Stream` API — the
macros generate values, not magic. Poise's `Framework` is explicitly optional sugar over
`dispatch_event(...)`, and that property is why poise could be built on serenity by someone
who didn't own serenity.
