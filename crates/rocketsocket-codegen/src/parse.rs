//! A deliberately narrow reader for `interface StreamerEvents`.
//!
//! See the crate docs for why this is not a TypeScript parser. The contract every function
//! here keeps: **if the input is not exactly the shape this module was written for, return
//! an error.** Nothing is skipped, nothing is guessed. A stream that silently failed to
//! parse would become a stream the generated catalog does not mention, which would become a
//! class of events a bot never sees.

use std::error::Error;
use std::fmt;

/// Everything `interface StreamerEvents` declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    /// One entry per stream, in declaration order.
    pub streams: Vec<Stream>,
}

impl Catalog {
    /// Total number of `{ key; args }` entries across all streams.
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.streams.iter().map(|s| s.events.len()).sum()
    }

    /// Looks up a stream by its declared name.
    #[must_use]
    pub fn stream(&self, name: &str) -> Option<&Stream> {
        self.streams.iter().find(|s| s.name == name)
    }
}

/// One stream: a DDP pseudo-collection named `stream-{name}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stream {
    /// Name as declared, without the `stream-` prefix the wire uses.
    pub name: String,
    /// Event entries, in declaration order.
    pub events: Vec<Event>,
}

/// One `{ key; args }` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The shape of the `eventName` this entry matches.
    pub key: KeyPattern,
    /// Source text of the key, kept verbatim for the report and for review diffs.
    pub key_source: String,
    /// What the positional `args` array carries.
    pub args: ArgsSpec,
}

/// The shape of an event key.
///
/// Rocket.Chat keys are frequently composite: `stream-notify-room` is subscribed as
/// `<rid>/user-activity`, `stream-notify-user` as `<uid>/message`. TypeScript expresses
/// that with template literal types, which is what these variants mirror.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPattern {
    /// A fixed key: `'user-status'`, `'__my_messages__'`.
    Literal(String),
    /// `` `${string}/suffix` `` — an id, a slash, then a fixed suffix.
    Suffix(String),
    /// `` `prefix/${string}` `` — a fixed prefix, a slash, then free text.
    Prefix(String),
    /// `string` or `` `${string}` `` — the whole key is free text, typically a room id.
    Any,
}

/// What an entry's `args` tuple looks like.
///
/// The element *types* are kept as source text rather than modelled. Resolving them would
/// mean following imports into `@rocket.chat/core-typings` and re-implementing a large part
/// of the TypeScript type system; the entity types this crate cares about are hand-modelled
/// in `rocketsocket-model::entity` instead. What is extracted here is the part that is both
/// mechanical and easy to get wrong by hand: how many positional slots there are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgsSpec {
    /// Normalised source text of the whole `args` type.
    pub source: String,
    /// Tuple lengths, ascending and deduplicated. More than one entry means the type is a
    /// union of tuples of differing length — `notify-all`'s `license` is `[…] | []`.
    pub arities: Vec<usize>,
    /// Whether any alternative is an unbounded array (`unknown[]`, `any[]`) rather than a
    /// fixed tuple.
    pub variadic: bool,
}

/// A shape the parser was not written to understand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    /// Byte offset into the original source.
    pub offset: usize,
    /// 1-based line number.
    pub line: usize,
    /// What went wrong.
    pub message: String,
    /// The offending source text, truncated.
    pub context: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "streams.ts:{}: {} (at byte {})\n  --> {}",
            self.line, self.message, self.offset, self.context
        )
    }
}

impl Error for ParseError {}

type Result<T> = std::result::Result<T, ParseError>;

/// Reads `interface StreamerEvents` out of a copy of `streams.ts`.
///
/// # Errors
///
/// Returns [`ParseError`] the moment anything deviates from the expected shape, including
/// an unknown key or `args` form, an unbalanced delimiter, or a member of an entry object
/// that is neither `key` nor `args`.
pub fn parse(source: &str) -> Result<Catalog> {
    let src = strip_comments(source);
    let src = src.as_str();

    const NEEDLE: &str = "interface StreamerEvents";
    let Some(start) = src.find(NEEDLE) else {
        return Err(err(src, 0, "`interface StreamerEvents` not found", ""));
    };

    let brace = src[start..]
        .find('{')
        .map(|i| start + i)
        .ok_or_else(|| err(src, start, "no `{` after `interface StreamerEvents`", NEEDLE))?;
    let end = balanced(src, brace, '{', '}')?;
    let body_start = brace + 1;

    let mut streams = Vec::new();
    let mut pos = body_start;
    loop {
        pos = skip_ws(src, pos);
        while src[pos..end].starts_with(';') {
            pos = skip_ws(src, pos + 1);
        }
        if pos >= end {
            break;
        }

        let (name, after) = property_name(src, pos, end)?;
        pos = skip_ws(src, after);
        if !src[pos..end].starts_with(':') {
            return Err(err(src, pos, "expected `:` after a stream name", &src[pos..end]));
        }
        pos = skip_ws(src, pos + 1);
        if !src[pos..end].starts_with('[') {
            return Err(err(
                src,
                pos,
                "expected a `[` — every StreamerEvents value is an array of entries",
                &src[pos..end],
            ));
        }
        let close = balanced(src, pos, '[', ']')?;
        let events = parse_entries(src, pos + 1, close)?;
        streams.push(Stream { name, events });
        pos = close + 1;
    }

    if streams.is_empty() {
        return Err(err(src, body_start, "`StreamerEvents` parsed as empty", ""));
    }
    Ok(Catalog { streams })
}

/// Parses the `[ {…}, {…} ]` list of entries between `start` and `end` (exclusive).
fn parse_entries(src: &str, start: usize, end: usize) -> Result<Vec<Event>> {
    let mut events = Vec::new();
    for (from, to) in split_top_level(src, start, end, &[','])? {
        let (from, to) = trim_range(src, from, to);
        if from == to {
            continue; // trailing comma
        }
        if !src[from..to].starts_with('{') || !src[from..to].ends_with('}') {
            return Err(err(
                src,
                from,
                "expected an entry object `{ key: …; args: … }`",
                &src[from..to],
            ));
        }
        events.push(parse_entry(src, from + 1, to - 1)?);
    }
    if events.is_empty() {
        return Err(err(src, start, "a stream declared no events", &src[start..end]));
    }
    Ok(events)
}

/// Parses the members of one `{ key: …; args: … }` object.
fn parse_entry(src: &str, start: usize, end: usize) -> Result<Event> {
    let mut key = None;
    let mut args = None;

    for (from, to) in split_top_level(src, start, end, &[';', ','])? {
        let (from, to) = trim_range(src, from, to);
        if from == to {
            continue;
        }
        let colon = find_top_level(src, from, to, &[':'])?
            .ok_or_else(|| err(src, from, "entry member has no `:`", &src[from..to]))?;
        let name = src[from..colon].trim();
        let (vf, vt) = trim_range(src, colon + 1, to);
        match name {
            "key" => {
                if key.is_some() {
                    return Err(err(src, from, "duplicate `key` member", &src[from..to]));
                }
                key = Some((parse_key(src, vf, vt)?, src[vf..vt].to_owned()));
            }
            "args" => {
                if args.is_some() {
                    return Err(err(src, from, "duplicate `args` member", &src[from..to]));
                }
                args = Some(parse_args(src, vf, vt)?);
            }
            other => {
                return Err(err(
                    src,
                    from,
                    &format!("unexpected member `{other}` — only `key` and `args` are understood"),
                    &src[from..to],
                ));
            }
        }
    }

    let (key, key_source) =
        key.ok_or_else(|| err(src, start, "entry has no `key`", &src[start..end]))?;
    let args = args.ok_or_else(|| err(src, start, "entry has no `args`", &src[start..end]))?;
    Ok(Event { key, key_source, args })
}

/// Classifies a `key:` type.
fn parse_key(src: &str, start: usize, end: usize) -> Result<KeyPattern> {
    let text = &src[start..end];
    if let Some(inner) = strip_quotes(text, '\'').or_else(|| strip_quotes(text, '"')) {
        if inner.contains('$') || inner.contains('\\') {
            return Err(err(src, start, "quoted key is not a plain literal", text));
        }
        return Ok(KeyPattern::Literal(inner.to_owned()));
    }
    if text == "string" {
        return Ok(KeyPattern::Any);
    }
    if let Some(inner) = strip_quotes(text, '`') {
        const HOLE: &str = "${string}";
        if inner == HOLE {
            return Ok(KeyPattern::Any);
        }
        if let Some(rest) = inner.strip_prefix(HOLE)
            && let Some(suffix) = rest.strip_prefix('/')
            && !suffix.is_empty()
            && !suffix.contains("${")
        {
            return Ok(KeyPattern::Suffix(suffix.to_owned()));
        }
        if let Some(head) = inner.strip_suffix(HOLE)
            && let Some(prefix) = head.strip_suffix('/')
            && !prefix.is_empty()
            && !prefix.contains("${")
        {
            return Ok(KeyPattern::Prefix(prefix.to_owned()));
        }
        return Err(err(
            src,
            start,
            "template-literal key is not one of `${string}`, `${string}/suffix`, `prefix/${string}`",
            text,
        ));
    }
    Err(err(src, start, "key is neither a string literal, a template literal, nor `string`", text))
}

/// Extracts arity information from an `args:` type.
fn parse_args(src: &str, start: usize, end: usize) -> Result<ArgsSpec> {
    let mut arities = Vec::new();
    let mut variadic = false;

    for (from, to) in split_top_level(src, start, end, &['|'])? {
        let (from, to) = trim_range(src, from, to);
        if from == to {
            continue; // leading `|` on a multi-line union
        }
        let text = &src[from..to];
        if text.starts_with('[') && text.ends_with(']') {
            let close = balanced(src, from, '[', ']')?;
            if close + 1 != to {
                return Err(err(src, from, "trailing text after an args tuple", text));
            }
            let mut n = 0;
            // TypeScript lets a tuple end in optional elements — `[a: A, b?: B]` is a
            // two-slot type that legitimately arrives with one element. Counting `b` as
            // required would report a minimum arity the server never sends: the pinned
            // `room-messages` room-keyed entry is `[message, user?, room?]`, while
            // `listeners.module.ts:223` emits exactly one element.
            let mut required = 0;
            for (ef, et) in split_top_level(src, from + 1, to - 1, &[','])? {
                let (ef, et) = trim_range(src, ef, et);
                if ef == et {
                    continue; // trailing comma
                }
                if src[ef..et].starts_with("...") {
                    // A rest element has no fixed arity at all. Nothing upstream uses one,
                    // and counting it as a single slot would be a silent mis-read.
                    return Err(err(
                        src,
                        ef,
                        "a rest element in an args tuple is not understood",
                        &src[ef..et],
                    ));
                }
                n += 1;
                if !is_optional_element(src, ef, et)? {
                    required = n;
                }
            }
            for arity in required..=n {
                if !arities.contains(&arity) {
                    arities.push(arity);
                }
            }
        } else if text.ends_with("[]") {
            variadic = true;
        } else {
            return Err(err(
                src,
                from,
                "args alternative is neither a tuple `[…]` nor an array `T[]`",
                text,
            ));
        }
    }

    if arities.is_empty() && !variadic {
        return Err(err(src, start, "args type has no alternatives", &src[start..end]));
    }
    arities.sort_unstable();
    Ok(ArgsSpec { source: normalise(&src[start..end]), arities, variadic })
}

/// Whether one tuple element is declared optional — `b?: B`, or a bare `B?`.
///
/// Only a top-level `?` counts: the `tmid?` in `[{ until: Date; tmid?: string }]` belongs to
/// an object member, not to the tuple.
fn is_optional_element(src: &str, from: usize, to: usize) -> Result<bool> {
    let label_end = match find_top_level(src, from, to, &[':'])? {
        Some(colon) => colon,
        None => to,
    };
    let (_, label_end) = trim_range(src, from, label_end);
    Ok(src[from..label_end].ends_with('?'))
}

// -------------------------------------------------------------------------------------
// Lexical helpers
// -------------------------------------------------------------------------------------

/// Blanks out `//` and `/* */` comments, preserving every byte offset.
///
/// ASCII comment bytes become spaces and newlines are kept, so offsets and line numbers
/// computed on the result apply unchanged to the original. Non-ASCII bytes inside a comment
/// are passed through: they can never be mistaken for a delimiter, and rewriting them would
/// shift every offset after them.
fn strip_comments(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(src.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            q @ (b'\'' | b'"' | b'`') => {
                let after = skip_string(src, i);
                out.extend_from_slice(&bytes[i..after]);
                debug_assert!(after > i, "string starting with {q} made no progress");
                i = after;
            }
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    out.push(blank(bytes[i]));
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let end = src[i + 2..].find("*/").map_or(bytes.len(), |p| i + 2 + p + 2);
                while i < end {
                    out.push(blank(bytes[i]));
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).expect("only ASCII bytes are rewritten, so UTF-8 survives")
}

/// Maps one comment byte to its replacement: ASCII becomes a space, newlines and non-ASCII
/// bytes are kept so offsets, line numbers and UTF-8 validity all survive unchanged.
fn blank(b: u8) -> u8 {
    if b == b'\n' || !b.is_ascii() { b } else { b' ' }
}

/// Index of the delimiter matching the one at `open_at`, skipping strings and nesting.
fn balanced(src: &str, open_at: usize, open: char, close: char) -> Result<usize> {
    let bytes = src.as_bytes();
    debug_assert_eq!(bytes[open_at] as char, open);
    let mut depth = 0usize;
    let mut i = open_at;
    while i < src.len() {
        let c = bytes[i] as char;
        if is_quote(c) {
            i = skip_string(src, i);
            continue;
        }
        if c == open {
            depth += 1;
        } else if c == close {
            depth -= 1;
            if depth == 0 {
                return Ok(i);
            }
        }
        i += 1;
    }
    Err(err(src, open_at, &format!("unbalanced `{open}`"), &src[open_at..]))
}

/// Splits `start..end` on any of `seps` occurring at nesting depth zero.
fn split_top_level(
    src: &str,
    start: usize,
    end: usize,
    seps: &[char],
) -> Result<Vec<(usize, usize)>> {
    let mut parts = Vec::new();
    let mut from = start;
    let mut i = start;
    let bytes = src.as_bytes();
    let mut depth = 0i32;
    let mut angle = 0i32;
    let mut prev = b' ';
    while i < end {
        let c = bytes[i] as char;
        if is_quote(c) {
            i = skip_string(src, i);
            prev = b'\'';
            continue;
        }
        match c {
            '{' | '[' | '(' => depth += 1,
            '}' | ']' | ')' => depth -= 1,
            // `<` only opens a generic argument list when it follows a type name.
            '<' if prev.is_ascii_alphanumeric() || prev == b'_' || prev == b'>' => angle += 1,
            // …and the `>` of a `=>` closes nothing. Without this, the commas of a callback
            // type inside a generic — `Foo<() => void, Bar>` — read as tuple separators and
            // silently inflate the arity.
            '>' if angle > 0 && prev != b'=' => angle -= 1,
            _ if depth == 0 && angle == 0 && seps.contains(&c) => {
                parts.push((from, i));
                from = i + 1;
            }
            _ => {}
        }
        if depth < 0 {
            return Err(err(src, i, "unbalanced closing delimiter", &src[start..end]));
        }
        if !c.is_ascii_whitespace() {
            prev = bytes[i];
        }
        i += 1;
    }
    if depth != 0 {
        return Err(err(src, start, "unbalanced delimiter", &src[start..end]));
    }
    parts.push((from, end));
    Ok(parts)
}

/// Offset of the first depth-zero occurrence of any of `seps`.
fn find_top_level(src: &str, start: usize, end: usize, seps: &[char]) -> Result<Option<usize>> {
    let parts = split_top_level(src, start, end, seps)?;
    Ok(if parts.len() > 1 { Some(parts[0].1) } else { None })
}

fn is_quote(c: char) -> bool {
    matches!(c, '\'' | '"' | '`')
}

/// Index just past the string starting at `at`.
///
/// The returned index is always a `char` boundary no greater than `src.len()`, even for an
/// unterminated string or a trailing backslash. Callers slice with it, so anything else
/// would turn malformed input into a panic instead of the [`ParseError`] this module
/// promises.
fn skip_string(src: &str, at: usize) -> usize {
    let bytes = src.as_bytes();
    let quote = bytes[at];
    let mut i = at + 1;
    while i < src.len() {
        let c = bytes[i];
        if c == b'\\' {
            // Step over the escape *and* the whole character it escapes, which may be
            // multi-byte — `i += 2` would land inside it, or past the end of the input.
            i = next_boundary(src, i + 1);
            continue;
        }
        i += 1;
        if c == quote {
            break;
        }
    }
    i
}

/// Index of the next `char` boundary strictly after `i`, clamped to `src.len()`.
fn next_boundary(src: &str, i: usize) -> usize {
    if i >= src.len() {
        return src.len();
    }
    let mut next = i + 1;
    while next < src.len() && !src.is_char_boundary(next) {
        next += 1;
    }
    next
}

/// Reads a property name: `'quoted'`, `"quoted"` or a bare identifier.
fn property_name(src: &str, start: usize, end: usize) -> Result<(String, usize)> {
    let bytes = src.as_bytes();
    let c = bytes[start] as char;
    if is_quote(c) {
        if c == '`' {
            return Err(err(src, start, "a stream name must be a plain string", &src[start..end]));
        }
        let after = skip_string(src, start);
        let inner = &src[start + 1..after - 1];
        if inner.contains('\\') {
            return Err(err(src, start, "escapes in a stream name are not supported", inner));
        }
        return Ok((inner.to_owned(), after));
    }
    let mut i = start;
    while i < end && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$') {
        i += 1;
    }
    if i == start {
        return Err(err(src, start, "expected a stream name", &src[start..end]));
    }
    Ok((src[start..i].to_owned(), i))
}

fn skip_ws(src: &str, mut pos: usize) -> usize {
    let bytes = src.as_bytes();
    while pos < src.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

fn trim_range(src: &str, mut from: usize, mut to: usize) -> (usize, usize) {
    let bytes = src.as_bytes();
    while from < to && bytes[from].is_ascii_whitespace() {
        from += 1;
    }
    while to > from && bytes[to - 1].is_ascii_whitespace() {
        to -= 1;
    }
    (from, to)
}

fn strip_quotes(text: &str, quote: char) -> Option<&str> {
    let inner = text.strip_prefix(quote)?.strip_suffix(quote)?;
    // Guard against `'a' | 'b'`, which starts and ends with a quote but is not one string.
    if inner.contains(quote) { None } else { Some(inner) }
}

/// Collapses whitespace runs so a multi-line type reads as one line in the report.
fn normalise(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut space = false;
    for c in text.chars() {
        if c.is_ascii_whitespace() {
            space = !out.is_empty();
        } else {
            if space {
                out.push(' ');
            }
            space = false;
            out.push(c);
        }
    }
    out
}

fn err(src: &str, offset: usize, message: &str, context: &str) -> ParseError {
    let line = src[..offset.min(src.len())].bytes().filter(|b| *b == b'\n').count() + 1;
    ParseError {
        offset,
        line,
        message: message.to_owned(),
        context: truncate(&normalise(context), 120),
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut cut = max;
    while cut > 0 && !text.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &text[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VENDORED_STREAMS_TS;

    fn catalog() -> Catalog {
        parse(VENDORED_STREAMS_TS).expect("the vendored copy parses")
    }

    #[test]
    fn the_vendored_catalog_parses_completely() {
        let catalog = catalog();
        assert_eq!(catalog.streams.len(), 17);
        assert_eq!(catalog.event_count(), 80);
        // Both ends of the interface, so a truncated read would be caught.
        assert_eq!(catalog.streams[0].name, "roles");
        assert_eq!(catalog.streams[16].name, "local");
    }

    #[test]
    fn composite_keys_are_recognised_as_templates() {
        let notify_room = catalog().stream("notify-room").expect("declared").clone();
        assert_eq!(notify_room.events.len(), 8);
        assert_eq!(notify_room.events[0].key, KeyPattern::Suffix("user-activity".to_owned()));
        assert_eq!(notify_room.events[1].key, KeyPattern::Suffix("typing".to_owned()));

        let queue = catalog().stream("livechat-inquiry-queue-observer").expect("declared").clone();
        assert_eq!(queue.events[0].key, KeyPattern::Literal("public".to_owned()));
        assert_eq!(queue.events[1].key, KeyPattern::Prefix("department".to_owned()));
        assert_eq!(queue.events[3].key, KeyPattern::Any, "a bare `${{string}}` is a free key");

        let messages = catalog().stream("room-messages").expect("declared").clone();
        assert_eq!(messages.events[0].key, KeyPattern::Literal("__my_messages__".to_owned()));
        assert_eq!(messages.events[1].key, KeyPattern::Any, "bare `string` is a free key");
    }

    /// The commas inside `Pick<IUser, '_id' | 'name'>` belong to the generic argument list,
    /// not to the tuple. Counting them would report arity 2 for a one-element tuple.
    #[test]
    fn generic_arguments_do_not_inflate_the_arity() {
        let logged = catalog().stream("notify-logged").expect("declared").clone();
        let name_changed = logged
            .events
            .iter()
            .find(|event| event.key == KeyPattern::Literal("Users:NameChanged".to_owned()))
            .expect("declared");
        assert_eq!(name_changed.args.arities, vec![1]);
        assert_eq!(name_changed.args.source, "[Pick<IUser, '_id' | 'name' | 'username'>]");
    }

    #[test]
    fn a_union_of_tuples_reports_every_arity_it_can_have() {
        let all = catalog().stream("notify-all").expect("declared").clone();
        let license = all
            .events
            .iter()
            .find(|event| event.key == KeyPattern::Literal("license".to_owned()))
            .expect("declared");
        assert_eq!(license.args.arities, vec![0, 1]);
        assert!(!license.args.variadic);

        let local = catalog().stream("local").expect("declared").clone();
        assert!(local.events[0].args.variadic, "`any[]` has no fixed arity");
        assert!(local.events[0].args.arities.is_empty());
    }

    #[test]
    fn comments_do_not_reach_the_parser() {
        // `notify-room` ends with a commented-out entry and `notify-logged` has a
        // `/* @deprecated */` marker between two live ones.
        assert_eq!(catalog().stream("notify-room").expect("declared").events.len(), 8);
        assert_eq!(catalog().stream("notify-logged").expect("declared").events.len(), 14);
    }

    // -----------------------------------------------------------------------------------
    // Failing loudly
    // -----------------------------------------------------------------------------------

    fn interface(body: &str) -> String {
        format!("export interface StreamerEvents {{\n{body}\n}}\n")
    }

    #[test]
    fn a_shape_the_parser_does_not_understand_is_an_error_not_a_skip() {
        let cases = [
            // An entry member that is neither `key` nor `args`.
            "'s': [{ key: 'k'; args: []; deprecated: true }];",
            // A value that is not an array of entries.
            "'s': SomeOtherType;",
            // An entry that is not an object.
            "'s': [SomeEntry];",
            // A key form with no fixed part to match on.
            "'s': [{ key: `${string}x${string}`; args: [] }];",
            // A key that is not a string at all.
            "'s': [{ key: 42; args: [] }];",
            // An args type that is neither a tuple nor an array.
            "'s': [{ key: 'k'; args: Foo }];",
            // A missing member.
            "'s': [{ key: 'k' }];",
            "'s': [{ args: [] }];",
            // An unbalanced delimiter.
            "'s': [{ key: 'k'; args: [Foo };",
        ];
        for case in cases {
            let source = interface(case);
            let error = parse(&source).expect_err(&format!("`{case}` must not parse"));
            assert!(!error.message.is_empty());
            assert!(error.line >= 1);
        }
    }

    #[test]
    fn a_stream_declaring_no_events_is_an_error() {
        // Silently accepting this would produce a stream nothing can ever match.
        parse(&interface("'s': [];")).expect_err("an empty stream must not parse");
    }

    #[test]
    fn a_missing_interface_is_an_error() {
        parse("export type Foo = 1;").expect_err("no StreamerEvents means no catalog");
    }

    #[test]
    fn the_error_points_at_the_offending_line() {
        let source = interface("'a': [{ key: 'k'; args: [] }];\n'b': [{ key: 'k'; oops: [] }];");
        let error = parse(&source).expect_err("must not parse");
        assert_eq!(error.line, 3, "line 1 is the interface header");
        assert!(error.message.contains("oops"), "{}", error.message);
        assert!(error.context.contains("oops"));
    }

    // -----------------------------------------------------------------------------------
    // Regressions found by adversarial review
    // -----------------------------------------------------------------------------------

    /// The contract is "return an error", not "panic". `skip_string` used to step two bytes
    /// past a backslash unconditionally, so a trailing backslash walked the cursor past the
    /// end of the input and `strip_comments` sliced out of range.
    #[test]
    fn a_malformed_string_literal_errors_instead_of_panicking() {
        for case in ["'\\", "\"\\", "`\\", "'\\\u{e9}", "'\\\u{e9}' ", "'unterminated"] {
            let error = parse(case).expect_err("no interface, so this must be an error");
            assert!(!error.message.is_empty(), "{case:?}");
        }
        // …and the same byte sequences inside an otherwise well-formed interface: an escape
        // is rejected by `parse_key`, but the scanner must reach that check without slicing
        // through the middle of the `\u{e9}`.
        let source = interface("'s': [{ key: 'a\\\u{e9}b'; args: [A] }];");
        let error = parse(&source).expect_err("an escaped key is not a plain literal");
        assert!(error.message.contains("plain literal"), "{}", error.message);
    }

    /// `[a: A, b?: B]` can legitimately arrive with one element. The pinned catalog has
    /// exactly one such entry, and the emit site (`listeners.module.ts:223`, which sends
    /// `emitWithoutBroadcast(message.rid, message)`) uses the short form.
    #[test]
    fn optional_tuple_elements_widen_the_arity_instead_of_being_counted_as_required() {
        let messages = catalog().stream("room-messages").expect("declared").clone();
        assert_eq!(messages.events[1].args.arities, vec![1, 2, 3]);

        let parsed =
            parse(&interface("'s': [{ key: 'k'; args: [a: A, b?: B, c?: C] }];")).expect("parses");
        assert_eq!(parsed.streams[0].events[0].args.arities, vec![1, 2, 3]);

        // A bare optional element with no label, and an all-optional tuple.
        let bare = parse(&interface("'s': [{ key: 'k'; args: [A, B?] }];")).expect("parses");
        assert_eq!(bare.streams[0].events[0].args.arities, vec![1, 2]);
        let all = parse(&interface("'s': [{ key: 'k'; args: [A?] }];")).expect("parses");
        assert_eq!(all.streams[0].events[0].args.arities, vec![0, 1]);

        // A `?` belonging to an object member is not the tuple's.
        let nested =
            parse(&interface("'s': [{ key: 'k'; args: [{ until: Date; tmid?: string }] }];"))
                .expect("parses");
        assert_eq!(nested.streams[0].events[0].args.arities, vec![1]);
    }

    /// Silently counting a rest element as one slot is exactly the half-understood read this
    /// module exists to avoid.
    #[test]
    fn a_rest_element_is_an_error_not_a_slot() {
        parse(&interface("'s': [{ key: 'k'; args: [a: A, ...rest: B[]] }];"))
            .expect_err("a rest element has no fixed arity");
    }

    /// The `>` of `=>` used to close the generic-argument depth counter, after which the
    /// callback's comma read as a tuple separator and the arity came out one too high.
    #[test]
    fn an_arrow_type_inside_a_generic_does_not_inflate_the_arity() {
        let parsed = parse(&interface("'s': [{ key: 'k'; args: [Foo<() => void, Bar>] }];"))
            .expect("parses");
        assert_eq!(parsed.streams[0].events[0].args.arities, vec![1]);

        // A plain nested generic still closes normally.
        let plain =
            parse(&interface("'s': [{ key: 'k'; args: [Map<K, Set<V>>, X] }];")).expect("parses");
        assert_eq!(plain.streams[0].events[0].args.arities, vec![2]);
    }

    #[test]
    fn strings_containing_delimiters_do_not_confuse_the_scanner() {
        let catalog = parse(&interface(
            "'s': [{ key: 'a/b}]'; args: [{ x: 'y;z' }] }, { key: 'c'; args: [A, B] }];",
        ))
        .expect("parses");
        let stream = &catalog.streams[0];
        assert_eq!(stream.events.len(), 2);
        assert_eq!(stream.events[0].key, KeyPattern::Literal("a/b}]".to_owned()));
        assert_eq!(stream.events[1].args.arities, vec![2]);
    }
}
