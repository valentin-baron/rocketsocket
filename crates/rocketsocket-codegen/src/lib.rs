//! Turns the pinned Rocket.Chat stream catalog into Rust.
//!
//! `packages/ddp-client/src/types/streams.ts` is the only machine-readable description of
//! Rocket.Chat's DDP stream surface: `interface StreamerEvents` maps every stream name to
//! the list of event keys it publishes and the positional `args` tuple each key delivers.
//! A copy pinned to one upstream commit lives in `vendor/streams.ts`; see
//! `vendor/README.md`.
//!
//! # Why this is not a TypeScript parser
//!
//! It is not, and it must not pretend to be. `StreamerEvents` is written in one regular,
//! almost machine-generated shape:
//!
//! ```text
//! 'stream-name': [
//!     { key: 'literal'; args: [Foo, Bar] },
//!     { key: `${string}/suffix`; args: [Baz] | [] },
//! ];
//! ```
//!
//! [`parse`] recognises exactly that shape and **errors on everything else**. It does not
//! resolve imports, does not understand conditional or mapped types, and treats an `args`
//! entry as an opaque source string plus an arity. That is deliberate: the alternative to
//! a narrow parser that fails loudly is a broad one that silently mis-reads a construct it
//! half-understands, and a silently dropped stream is a bot that never receives those
//! events.
//!
//! Every error carries the byte offset and the offending text, so a shape change upstream
//! stops the generator with a pointer at the line that changed.
//!
//! # Output
//!
//! The generated Rust is **committed to the repository and reviewed like source**; there is
//! no `build.rs`. `cargo run -p rocketsocket-codegen` rewrites the region delimited by
//! `GENERATED-BEGIN` / `GENERATED-END` markers in
//! `crates/rocketsocket-model/src/event.rs` and leaves everything outside those markers —
//! the typed [`StreamEvent`] enum, its decoder, and the tests — untouched. Hand-written
//! code and generated code therefore share a file without the generator ever clobbering
//! the hand-written half.
//!
//! [`StreamEvent`]: https://docs.rs/rocketsocket-model

mod parse;
mod render;

pub use self::parse::{ArgsSpec, Catalog, Event, KeyPattern, ParseError, Stream, parse};
pub use self::render::{BEGIN_MARKER, END_MARKER, RenderError, render_catalog, splice};

/// The vendored, pinned copy of `packages/ddp-client/src/types/streams.ts`.
///
/// Compiled in rather than read from disk so `cargo run -p rocketsocket-codegen` behaves
/// the same whatever the working directory, and so the generator cannot accidentally be
/// pointed at a live checkout.
pub const VENDORED_STREAMS_TS: &str = include_str!("../vendor/streams.ts");

/// Upstream commit the vendored copy was taken from. Kept in sync with `vendor/README.md`
/// and stamped into the generated file's header.
pub const UPSTREAM_COMMIT: &str = "ea163f56b53b1dd40d49af39e0406397cbd24939";

/// Upstream `apps/meteor` version at [`UPSTREAM_COMMIT`].
pub const UPSTREAM_VERSION: &str = "8.8.0-develop";

/// Path of the vendored file within the Rocket.Chat monorepo.
pub const UPSTREAM_PATH: &str = "packages/ddp-client/src/types/streams.ts";
