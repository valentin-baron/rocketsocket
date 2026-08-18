# rocketsocket

A Rust framework for Rocket.Chat bots — typed events, a cache, and a command layer.
Roughly what serenity/twilight/poise are for Discord.

**Status: design phase.** No code yet. See [PLAN.md](PLAN.md) for the architecture,
which is grounded in a source-level survey of the Rocket.Chat server rather than its
published documentation (the docs are years out of date — the plan lists the specific
divergences).

## The short version

- **DDP websocket for ingress, REST for egress.** Rocket.Chat has ~116 deprecated DDP
  methods scheduled for removal in 9.0, and the websocket physically cannot send
  attachments, blocks, or files. Streams, meanwhile, have no REST equivalent.
- **Crate split** like twilight: `-model` / `-rest` / `-realtime` / `-cache` / facade, so a
  script that just posts a message never compiles a websocket stack.
- **Generated model and event types** from Rocket.Chat's own
  `packages/ddp-client/src/types/streams.ts`, so the crate tracks server releases instead
  of rotting.
- **Events as a `Stream` first**, with an `EventHandler` trait generated from the same
  declaration list.
- **discord.py-shaped ergonomics on top**: `#[event]` / `#[command]` / `#[cog]` proc macros,
  extractor-style parameters, and live entities (`msg.reply(..)`) — all strictly optional
  sugar over the raw API. See [docs/dx.md](docs/dx.md).
