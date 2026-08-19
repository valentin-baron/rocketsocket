//! Turns a parsed [`Catalog`] into the generated region of `rocketsocket-model`'s
//! `event.rs`.
//!
//! The output is a plain `const` table plus the three small types that describe it. It is
//! spliced between marker comments in a file that also holds hand-written code, which is
//! what [`splice`] does; regenerating therefore never touches the typed `StreamEvent` enum,
//! its decoder, or the tests.

use std::error::Error;
use std::fmt;
use std::fmt::Write as _;

use crate::parse::{Catalog, KeyPattern};
use crate::{UPSTREAM_COMMIT, UPSTREAM_PATH, UPSTREAM_VERSION};

/// Opening marker of the generated region.
pub const BEGIN_MARKER: &str = "// ---------- GENERATED-BEGIN ----------";
/// Closing marker of the generated region.
pub const END_MARKER: &str = "// ---------- GENERATED-END ----------";

/// The generated region could not be located in the target file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderError(String);

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for RenderError {}

/// Replaces the text between the markers in `existing` with `generated`.
///
/// # Errors
///
/// Returns [`RenderError`] if either marker is missing or they appear out of order — which
/// means the target file was restructured and the generator must not guess where its output
/// belongs.
pub fn splice(existing: &str, generated: &str) -> Result<String, RenderError> {
    let begin = existing
        .find(BEGIN_MARKER)
        .ok_or_else(|| RenderError(format!("target file has no `{BEGIN_MARKER}` marker")))?;
    let end = existing
        .find(END_MARKER)
        .ok_or_else(|| RenderError(format!("target file has no `{END_MARKER}` marker")))?;
    if end < begin {
        return Err(RenderError("generated-region markers are in the wrong order".to_owned()));
    }
    let head = &existing[..begin + BEGIN_MARKER.len()];
    let tail = &existing[end..];
    Ok(format!("{head}\n{generated}\n{tail}"))
}

/// Renders the catalog module.
#[must_use]
pub fn render_catalog(catalog: &Catalog) -> String {
    let mut out = String::with_capacity(16 * 1024);

    let _ = writeln!(
        out,
        "\n\
         /// The Rocket.Chat stream surface, read straight out of `{path}`.\n\
         ///\n\
         /// **Generated. Do not edit by hand** — run `cargo run -p rocketsocket-codegen`,\n\
         /// which rewrites everything between the `GENERATED-BEGIN` and `GENERATED-END`\n\
         /// markers and leaves the rest of this file alone.\n\
         ///\n\
         /// This is an *index*, not a decoder. It answers \"does this workspace's server\n\
         /// version declare this (stream, event) pair, and how many positional arguments\n\
         /// does it promise?\" — which is what makes upstream drift visible: a test in\n\
         /// this module asserts that every `(stream, event)` pair [`StreamEvent`] types is\n\
         /// still declared here, so an event that disappears upstream fails the build\n\
         /// instead of quietly becoming one the bot never receives again.\n\
         ///\n\
         /// It is *not* the authority on what the server actually sends; see the module\n\
         /// docs for where the declared types and the emit site disagree.\n\
         pub mod catalog {{",
        path = UPSTREAM_PATH,
    );

    out.push_str(
        r#"
    /// One DDP stream. The wire name is this `name` with a `stream-` prefix.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct StreamSpec {
        /// Stream name as declared upstream, without the `stream-` prefix.
        pub name: &'static str,
        /// Event entries, in declaration order.
        pub events: &'static [EventSpec],
    }

    impl StreamSpec {
        /// The entry matching a concrete `eventName`, in declaration order.
        ///
        /// Order is load-bearing: several streams declare a catch-all key after their
        /// specific ones, and `room-messages` is exactly that case — `__my_messages__` is
        /// declared before the free-form room-id key, so it must be tried first.
        #[must_use]
        pub fn event(&self, key: &str) -> Option<&'static EventSpec> {
            self.events.iter().find(|event| event.key.matches(key))
        }
    }

    /// One `{ key; args }` entry of a stream.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct EventSpec {
        /// Shape of the `eventName` this entry matches.
        pub key: KeyPattern,
        /// Declared tuple lengths, ascending. More than one means the declared type is a
        /// union of tuples of differing length.
        ///
        /// Treat these as a hint, never as a validation rule. Arity has changed across
        /// server releases — `user-status` grew from 3 to 6 to 8 — and the emit site adds
        /// elements the declared type does not show.
        pub arities: &'static [usize],
        /// Whether an alternative is an unbounded array rather than a fixed tuple.
        pub variadic: bool,
        /// The declared `args` type, verbatim, whitespace collapsed.
        pub args: &'static str,
    }

    /// The shape of an event key.
    ///
    /// Rocket.Chat keys are frequently composite — `<rid>/user-activity`, `<uid>/message` —
    /// which upstream expresses as a TypeScript template literal type.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[non_exhaustive]
    pub enum KeyPattern {
        /// A fixed key, matched exactly.
        Literal(&'static str),
        /// `` `${string}/suffix` `` — free text, a slash, then this suffix.
        Suffix(&'static str),
        /// `` `prefix/${string}` `` — this prefix, a slash, then free text.
        Prefix(&'static str),
        /// The whole key is free text, typically a room id or a uid.
        Any,
    }

    impl KeyPattern {
        /// Whether a concrete `eventName` matches this pattern.
        ///
        /// Composite keys are split on the **first** `/` only: a suffix such as
        /// `e2e.keyRequest` contains no slash, but a room id never does either, and
        /// splitting from the left is what the server itself does
        /// (`const [rid, e] = eventName.split('/')`).
        #[must_use]
        pub fn matches(&self, key: &str) -> bool {
            match *self {
                Self::Literal(value) => key == value,
                Self::Suffix(suffix) => {
                    key.split_once('/').is_some_and(|(head, rest)| !head.is_empty() && rest == suffix)
                }
                Self::Prefix(prefix) => {
                    key.split_once('/').is_some_and(|(head, rest)| head == prefix && !rest.is_empty())
                }
                Self::Any => true,
            }
        }
    }
"#,
    );

    let _ = writeln!(
        out,
        "
    /// Rocket.Chat commit the catalog was generated from.
    pub const UPSTREAM_COMMIT: &str = \"{UPSTREAM_COMMIT}\";

    /// `apps/meteor` version at [`UPSTREAM_COMMIT`].
    pub const UPSTREAM_VERSION: &str = \"{UPSTREAM_VERSION}\";

    /// Number of streams declared upstream.
    pub const STREAM_COUNT: usize = {streams};

    /// Number of `{{ key; args }}` entries declared upstream, across all streams.
    pub const EVENT_COUNT: usize = {events};",
        streams = catalog.streams.len(),
        events = catalog.event_count(),
    );

    out.push_str("\n    /// Every stream, in upstream declaration order.\n");
    out.push_str("    pub const STREAMS: &[StreamSpec] = &[\n");
    for stream in &catalog.streams {
        let _ = writeln!(out, "        StreamSpec {{");
        let _ = writeln!(out, "            name: {},", quote(&stream.name));
        let _ = writeln!(out, "            events: &[");
        for event in &stream.events {
            let key = match &event.key {
                KeyPattern::Literal(v) => format!("KeyPattern::Literal({})", quote(v)),
                KeyPattern::Suffix(v) => format!("KeyPattern::Suffix({})", quote(v)),
                KeyPattern::Prefix(v) => format!("KeyPattern::Prefix({})", quote(v)),
                KeyPattern::Any => "KeyPattern::Any".to_owned(),
            };
            let arities =
                event.args.arities.iter().map(usize::to_string).collect::<Vec<_>>().join(", ");
            let _ = writeln!(out, "                EventSpec {{");
            let _ = writeln!(out, "                    key: {key},");
            let _ = writeln!(out, "                    arities: &[{arities}],");
            let _ = writeln!(out, "                    variadic: {},", event.args.variadic);
            let _ = writeln!(out, "                    args: {},", quote(&event.args.source));
            let _ = writeln!(out, "                }},");
        }
        let _ = writeln!(out, "            ],");
        let _ = writeln!(out, "        }},");
    }
    out.push_str("    ];\n");

    out.push_str(
        r#"
    /// Looks up a stream by name, with or without the `stream-` prefix.
    #[must_use]
    pub fn stream(name: &str) -> Option<&'static StreamSpec> {
        let name = name.strip_prefix("stream-").unwrap_or(name);
        STREAMS.iter().find(|spec| spec.name == name)
    }

    /// Looks up the entry a `(stream, eventName)` pair resolves to.
    #[must_use]
    pub fn event(stream_name: &str, key: &str) -> Option<&'static EventSpec> {
        stream(stream_name)?.event(key)
    }
}
"#,
    );

    out
}

/// Renders a Rust string literal.
fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}
