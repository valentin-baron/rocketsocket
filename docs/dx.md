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

## 7. Decision — prefix and mention commands only, and that is the right model

Verified in the server source: `slashCommands.add({ command, callback, appId, .. })` is an
**in-process registry** (`apps/meteor/app/utils/client/slashCommand.ts`), populated by
server code or by installed Apps. There is no REST or DDP endpoint by which an external bot
registers a slash command. Combined with PLAN.md §9.5 — a plain bot can *send* UI Kit
blocks but can never *receive* a button click, because interactions route to
`POST /api/apps/ui.interaction/:appId` and 404 for an unregistered app — the conclusion is
firm:

**For an external Rocket.Chat bot, text commands are the entire command surface.**

Which is why discord.py is the right model to copy rather than modern discord.py or poise's
slash-first design. The prefix-command machinery — prefix and mention triggers, argument
parsing with converters, `#[rest]` trailing capture, subcommands, aliases, cooldowns,
checks, help generation — is not a legacy compatibility layer here. It is the product.

Argument conversion mirrors discord.py's converter protocol:

```rust
pub trait FromArgs<'a, D>: Sized {
    async fn from_args(ctx: &Context<D>, args: &mut ArgStream<'a>) -> Result<Self, ParseError>;
}
```

Impls for the scalars via `FromStr`, for `Option<T>` (optional trailing), `Vec<T>`
(greedy), `#[rest] String` (consume remainder), and — the ones that make it feel native —
`User`, `Room`, `Message`, resolving `@username`, `#channel`, and message links against
cache-then-REST. That resolution is async and fallible, which is exactly why `FromArgs`
takes `&Context` rather than being a bare `FromStr`.

## 8. Checks, cooldowns, errors

discord.py stacks decorators. Rust proc-macro attributes don't stack cleanly, so these are
arguments to the single `#[command]`:

```rust
#[rocketsocket::command(
    aliases("rm", "clear"),
    check = "is_moderator",
    cooldown = "5s",
    room_only,
    on_error = "purge_error",
)]
```

Errors follow poise: one `#[non_exhaustive] FrameworkError` covering every phase (setup,
event handler, argument parse, check failed, cooldown hit, command body, panic), each
variant carrying borrowed context, resolved command-level → framework-level → built-in.
`FrameworkError::CommandPanic` catches unwinds so one bad command can't take the bot down —
discord.py's `on_command_error` behaviour, which people rely on more than they admit.

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
