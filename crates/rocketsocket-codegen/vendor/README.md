# Vendored upstream sources

Everything in this directory is a **verbatim, pinned copy** of a file from the
Rocket.Chat monorepo. Nothing here is edited by hand — re-vendoring means copying the
upstream file again and updating the record below.

Pinning matters for two reasons: `cargo run -p rocketsocket-codegen` must produce the
same output on every machine, and the generated Rust must be reviewable as a diff against
a known input. A codegen step that read a checkout under `/workspace` would do neither.

## `streams.ts`

| | |
|---|---|
| Upstream path | `packages/ddp-client/src/types/streams.ts` |
| Repository | `RocketChat/Rocket.Chat` |
| Commit | `ea163f56b53b1dd40d49af39e0406397cbd24939` |
| Committed | 2026-08-18 |
| `apps/meteor` version | 8.8.0-develop |
| `@rocket.chat/ddp-client` version | 1.1.1 |
| Size | 499 lines, 12392 bytes |

`interface StreamerEvents` in this file is the machine-readable catalog of every DDP
stream Rocket.Chat publishes: stream name → list of `{ key; args }` pairs, where `args`
is the positional tuple delivered in the `fields.args` array of a `changed` frame.

### It is a type, not the emit site

`streams.ts` describes what the server *declares* it sends. The code that actually sends
it is `apps/meteor/server/modules/listeners/listeners.module.ts`, and the two disagree in
places — most visibly `room-messages` / `__my_messages__`, which the type gives as
`[IMessage]` while the emit site appends a trailing element. Divergences found during the
survey are recorded in the module docs of `rocketsocket-model::event`. Trust the emit site
where they conflict; the type is the index, not the authority.
