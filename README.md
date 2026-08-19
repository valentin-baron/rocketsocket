# rocketsocket

A Rust framework for Rocket.Chat bots — typed events, a cache, and an ergonomic API.
Roughly what serenity/twilight are for Discord.

> **Status: early. Not published, not yet run against a live server.**
> The protocol layers are built and heavily tested against a scripted peer; see
> [Status](#status) for exactly what works and what does not.

## The shape of it

Rocket.Chat needs two transports, and this framework uses each for what it is good at:

- **The websocket (DDP) receives.** `stream-*` subscriptions have no REST equivalent and
  never will — deletions, typing, presence and read-state are observable nowhere else.
- **REST acts.** Rocket.Chat has deprecated ~116 DDP methods for removal in 9.0, and the
  surviving DDP `sendMessage` cannot carry attachments or blocks at all.

One credential drives both: `POST /api/v1/login` wraps the DDP `login` method and both read
the same token array, so the token it returns is simultaneously a DDP resume token. That is
also the only path that works against the EE `ddp-streamer`, whose websocket login accepts
only `{resume}`.

```rust
let (bot, mut events) = Bot::connect(
    "https://chat.example.com",
    Credentials::personal_access_token(user_id, token),
).await?;

bot.watch_all_messages().await?;          // one subscription, every room

while let Some(event) = events.recv().await {
    // ...
}
```

See [`crates/rocketsocket/examples/echo.rs`](crates/rocketsocket/examples/echo.rs).

## Crates

| Crate | What it is |
|---|---|
| `rocketsocket` | facade — one dependency, both transports |
| `rocketsocket-model` | serde types, typed ids, EJSON, the DDP wire protocol. No IO, no async |
| `rocketsocket-realtime` | DDP over WebSocket: handshake, liveness, reconnect, durable subscriptions |
| `rocketsocket-rest` | REST client: auth, messages, uploads |

The split is deliberate: a script that posts a message should not compile a WebSocket stack.

## Status

Built and tested:

- DDP wire protocol, entity model, EJSON timestamps, typed ids
- Realtime client: handshake, login, liveness, jittered reconnect, **subscription replay**
- REST client: auth, `chat.sendMessage`/`postMessage`, the two-step media upload
- Facade + echo example

Not built yet: typed stream events (they arrive as positional `Vec<Value>` today), the
cache, the `#[event]` macro layer.

**Never run against a real Rocket.Chat.** Every test uses a scripted in-process peer built
from the server source. `docker-compose.test.yml` plus `cargo test -- --ignored` is set up
for that and is the highest-value next step.

## Why the docs are not the source of truth

Rocket.Chat's published API docs are years out of date — they still document streams removed
in 8.0, an event the server stopped emitting in 6.0, and a 2FA message that does not exist in
the codebase. Everything here is derived from the server source instead, with the specific
divergences recorded in [PLAN.md](PLAN.md) §10.

[docs/gotchas.md](docs/gotchas.md) collects the Rocket.Chat behaviours that cost a day each —
useful whatever language you write a bot in. [PLAN.md](PLAN.md) is the architecture and the
research behind it; [docs/dx.md](docs/dx.md) is the API design.

## License

MIT OR Apache-2.0.
