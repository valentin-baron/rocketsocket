//! What can go wrong on the REST transport, and how the server says so.
//!
//! Rocket.Chat's REST error surface is not one shape but four, and two of them invert the
//! meaning of the same field. Everything in this module exists to collapse that into a
//! single pair of accessors — [`ApiError::code`] is always the machine code, and
//! [`ApiError::message`] is always the human text — no matter which shape arrived.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use serde_json::Value;

/// How much of a non-JSON error body is kept for diagnostics.
const BODY_SNIPPET_LIMIT: usize = 512;

/// Anything that can stop a REST call from producing its result.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RestError {
    /// A 2xx response that did not confirm success.
    ///
    /// Rocket.Chat pairs failures with a non-2xx status, so this should not happen against
    /// a real server — but a proxy or a captive portal answering 200 with an empty body
    /// would otherwise decode into an empty result, and for a role lookup "no admins" is a
    /// security answer rather than a missing one.
    #[error("{endpoint} returned success = false")]
    MissingSuccess {
        /// The endpoint that answered.
        endpoint: &'static str,
    },

    /// The base URL could not be turned into an endpoint URL.
    ///
    /// Raised at call time rather than at construction only for endpoints that interpolate
    /// path segments; a malformed base URL is rejected by [`Client::new`](crate::Client::new).
    #[error("invalid URL for endpoint {endpoint}: {reason}")]
    InvalidUrl {
        /// The endpoint that was being built.
        endpoint: String,
        /// Why the URL could not be built.
        reason: String,
    },

    /// The request never completed: DNS, TLS, connect, timeout, or a broken body stream.
    ///
    /// Retryable in general, but only idempotently — a timeout does not tell you whether the
    /// server ran the request.
    #[error("HTTP transport error")]
    Transport(#[from] reqwest::Error),

    /// The server answered, and the answer was a refusal.
    ///
    /// Boxed only to keep `Result<T, RestError>` small: the failure detail is several
    /// strings wide and every REST call returns this type.
    #[error(transparent)]
    Api(Box<ApiError>),

    /// The server answered with success, but the body was not the expected shape.
    ///
    /// Treated as a bug in this crate or a genuine server change, not as user error. The
    /// snippet is truncated because bodies can be large and can contain user content.
    #[error("could not decode a {status} response body: {source}")]
    Decode {
        /// The HTTP status the body arrived with.
        status: StatusCode,
        /// The leading bytes of the body, lossily decoded and truncated.
        snippet: String,
        /// The underlying `serde_json` failure.
        source: serde_json::Error,
    },

    /// The request could not be built from the values given.
    ///
    /// Client-side validation only: nothing was sent.
    #[error("invalid request: {reason}")]
    InvalidRequest {
        /// What was wrong.
        reason: String,
    },

    /// A request that requires credentials was made on a client that has none.
    ///
    /// Checked client-side so that a missing `login()` is a clear error rather than a 401
    /// with a plain-text body.
    #[error("this request requires authentication; call `Client::login` first")]
    NotAuthenticated,
}

impl From<ApiError> for RestError {
    fn from(error: ApiError) -> Self {
        Self::Api(Box::new(error))
    }
}

impl RestError {
    /// The server-side error, if the failure came from the server rather than the wire.
    #[must_use]
    pub fn api(&self) -> Option<&ApiError> {
        match self {
            Self::Api(error) => Some(error),
            _ => None,
        }
    }

    /// Whether this is the rate limiter refusing the call.
    #[must_use]
    pub fn is_too_many_requests(&self) -> bool {
        self.api().is_some_and(ApiError::is_too_many_requests)
    }

    /// How long the server wants the caller to wait, if it said.
    ///
    /// See [`RateLimit::retry_after`] for the unit hazard this hides.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        self.api()?.rate_limit()?.retry_after()
    }
}

/// Which class of refusal this is, decided **only** by the HTTP status.
///
/// # Never discriminate on the error string
///
/// A 403 from `API.v1.forbidden()` currently answers with the string `"unauthorized"`, not
/// `"forbidden"`, purely for backward compatibility:
///
/// ```text
/// // ApiClass.ts:396 (v8.8.0-develop)
/// // TODO: MAJOR remove 'unauthorized' in favor of 'forbidden'
/// error: msg || (applyBreakingChanges ? 'forbidden' : 'unauthorized'),
/// ```
///
/// `applyBreakingChanges` is `shouldBreakInVersion('9.0.0')`, so the same deployment will
/// start sending `"forbidden"` the day it upgrades to 9.0. Code that matched the string
/// breaks on that upgrade; code that matched the status does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ApiErrorKind {
    /// 400 — `API.v1.failure()`.
    ///
    /// This is the default landing spot for *every* uncaught handler error, which is why it
    /// carries no semantics of its own: a validation failure, a missing 2FA code, a
    /// permission problem raised as a plain `Error`, and "room not found" all arrive here.
    /// Read [`ApiError::code`] to tell them apart.
    BadRequest,
    /// 401 — no usable credentials were presented.
    Unauthorized,
    /// 403 — credentials were fine, permission was not.
    Forbidden,
    /// 404 — no such route, or `API.v1.notFound()`.
    NotFound,
    /// 429 — the rate limiter. See [`ApiError::rate_limit`].
    TooManyRequests,
    /// 5xx — `API.v1.internalError()` (500) or `unavailable()` (503), or a proxy.
    Server,
    /// Any other status, including ones a reverse proxy invented.
    Other,
}

impl ApiErrorKind {
    /// Classify a status code.
    #[must_use]
    pub fn from_status(status: StatusCode) -> Self {
        match status.as_u16() {
            400 => Self::BadRequest,
            401 => Self::Unauthorized,
            403 => Self::Forbidden,
            404 => Self::NotFound,
            429 => Self::TooManyRequests,
            500..=599 => Self::Server,
            _ => Self::Other,
        }
    }
}

/// A refusal from the Rocket.Chat REST API, normalized.
///
/// # The four shapes, and the inversion
///
/// | Producer | HTTP | Body |
/// |---|---|---|
/// | `API.v1.failure(msg, errorType, …)` | **always 400** | `{success:false, error:<human>, errorType:<code>, details?, stack?}` |
/// | `unauthorized` / `forbidden` / `notFound` / `tooManyRequests` | 401 / 403 / 404 / 429 | `{success:false, error:<human>}` — no code at all |
/// | `POST /api/v1/login` failure | 401 | `{success:false, status:"error", error:<code>, message:<human>, details?}` |
/// | `authenticationMiddleware` (Express routes) | 401 / 403 | `Unauthorized` / `Forbidden` as **plain text** |
///
/// Row three is the important one: on that path `error` holds the *code* — a bare number
/// like `403` for a bad password, or `"totp-required"` — and `message` holds the prose. That
/// is the DDP convention leaking through, because `POST /api/v1/login` is a wrapper around
/// the DDP `login` method and re-exports the `Meteor.Error` it caught.
///
/// The same inversion runs the other way across transports:
///
/// | Unified | REST `failure()` | REST `login` failure | DDP `error` frame |
/// |---|---|---|---|
/// | [`code`](Self::code) | `errorType` | `error` | `error` |
/// | [`message`](Self::message) | `error` | `message` | `reason` |
/// | [`details`](Self::details) | `details` | `details` | `details` |
///
/// On DDP, `errorType` is not a code at all: it is always the literal string
/// `"Meteor.Error"`. Anything keying off a field named `errorType` across both transports is
/// wrong on one of them.
#[derive(Debug, Clone)]
pub struct ApiError {
    status: StatusCode,
    kind: ApiErrorKind,
    code: Option<String>,
    message: Option<String>,
    details: Option<Value>,
    rate_limit: Option<RateLimit>,
    body: Option<String>,
}

impl ApiError {
    /// Build from a status, the raw body bytes, and the response headers.
    ///
    /// A body that is not JSON — the plain-text `Unauthorized` from the Express auth
    /// middleware, or an HTML error page from a reverse proxy — is kept verbatim as the
    /// message instead of turning into a parse error. A client that cannot report "you are
    /// not logged in" because the proof was not JSON is worse than useless.
    pub(crate) fn from_response(status: StatusCode, headers: &HeaderMap, body: &[u8]) -> Self {
        let rate_limit = RateLimit::from_headers(headers);
        let kind = ApiErrorKind::from_status(status);

        match serde_json::from_slice::<FailureEnvelope>(body) {
            Ok(envelope) => {
                let (code, message) = envelope.normalize();
                Self {
                    status,
                    kind,
                    code,
                    message,
                    details: envelope.details,
                    rate_limit,
                    body: None,
                }
            }
            Err(_) => {
                let text = String::from_utf8_lossy(body).trim().to_owned();
                let snippet = truncate(&text, BODY_SNIPPET_LIMIT);
                Self {
                    status,
                    kind,
                    code: None,
                    message: (!snippet.is_empty()).then(|| snippet.clone()),
                    details: None,
                    rate_limit,
                    body: (!snippet.is_empty()).then_some(snippet),
                }
            }
        }
    }

    /// The HTTP status the server answered with.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// The class of refusal, derived from [`status`](Self::status) alone.
    #[must_use]
    pub fn kind(&self) -> ApiErrorKind {
        self.kind
    }

    /// The machine-readable code, e.g. `"error-not-allowed"` or `"totp-required"`.
    ///
    /// `None` is common and not exceptional: the 401/403/404/429 helpers send no code, and
    /// a handler that throws a plain JavaScript `Error` produces a 400 with no code either.
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Whether the server sent this exact code.
    ///
    /// Prefer this over `code() == Some(x)` so that a `None` code reads as "not that error"
    /// rather than needing a match.
    #[must_use]
    pub fn has_code(&self, code: &str) -> bool {
        self.code.as_deref() == Some(code)
    }

    /// The human-readable message, if any.
    ///
    /// For a non-JSON body this is the body text itself.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Structured extras. Shape is per-error; the 2FA challenge puts `method` and
    /// `availableMethods` here.
    #[must_use]
    pub fn details(&self) -> Option<&Value> {
        self.details.as_ref()
    }

    /// The raw body, kept only when it was not JSON.
    #[must_use]
    pub fn raw_body(&self) -> Option<&str> {
        self.body.as_deref()
    }

    /// Rate-limiter headers, when the response carried them.
    ///
    /// Present on ordinary responses too, not just 429s — the server sets them on every
    /// rate-limited route.
    #[must_use]
    pub fn rate_limit(&self) -> Option<&RateLimit> {
        self.rate_limit.as_ref()
    }

    /// Whether the credentials were missing or rejected (HTTP 401).
    #[must_use]
    pub fn is_unauthorized(&self) -> bool {
        self.kind == ApiErrorKind::Unauthorized
    }

    /// Whether the credentials were accepted but the action was not permitted (HTTP 403).
    ///
    /// Note that until server 9.0 the *body* of such a response says `"unauthorized"`. This
    /// method looks at the status, so it is unaffected.
    #[must_use]
    pub fn is_forbidden(&self) -> bool {
        self.kind == ApiErrorKind::Forbidden
    }

    /// Whether the rate limiter refused the call (HTTP 429).
    ///
    /// Pair with [`RateLimit::retry_after`]. The default budget is 10 requests per 60 s per
    /// route per IP; the `bot` role's `api-bypass-rate-limit` permission removes it entirely,
    /// which is why that role is a deployment prerequisite for a bot.
    #[must_use]
    pub fn is_too_many_requests(&self) -> bool {
        self.kind == ApiErrorKind::TooManyRequests
    }

    /// Whether the server is asking for a second factor.
    ///
    /// Arrives as **HTTP 400**, not 401 — `checkCodeForUser` throws a `Meteor.Error` and
    /// every uncaught handler error funnels through `API.v1.failure()`, which hardcodes 400.
    /// `details.availableMethods` lists what the account accepts; retry with
    /// [`Client::with_two_factor`](crate::Client::with_two_factor).
    #[must_use]
    pub fn requires_two_factor(&self) -> bool {
        self.has_code("totp-required")
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Rocket.Chat REST error (HTTP {})", self.status.as_u16())?;
        match (&self.code, &self.message) {
            (Some(code), Some(message)) => write!(f, ": {message} [{code}]"),
            (Some(code), None) => write!(f, ": {code}"),
            (None, Some(message)) => write!(f, ": {message}"),
            (None, None) => Ok(()),
        }
    }
}

impl std::error::Error for ApiError {}

/// The wire form of a failure body, permissive enough for all three JSON shapes.
#[derive(Debug, Default, Deserialize)]
struct FailureEnvelope {
    /// Human text in the `failure()` shape, machine code in the `login` shape.
    ///
    /// Deliberately a [`Value`]: a failed password login sends the bare number `403` here.
    #[serde(default)]
    error: Option<Value>,
    #[serde(default, rename = "errorType")]
    error_type: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    details: Option<Value>,
}

impl FailureEnvelope {
    /// Collapse the wire shapes into `(code, message)`.
    ///
    /// The rules, in order:
    ///
    /// 1. `errorType` present → it is the code and `error` is the message. (`failure()`)
    /// 2. else `message` present and different from `error` → `error` is the code.
    ///    (`login` failure, which re-exports a `Meteor.Error`)
    /// 3. else there is no code and `error` (or `message`) is the message. (the 401/403/404
    ///    helpers, and the auth middleware which sets `error` and `message` to the same
    ///    sentence — rule 2 would otherwise mistake that sentence for a code)
    fn normalize(&self) -> (Option<String>, Option<String>) {
        let error = self.error.as_ref().and_then(scalar_to_string);

        if let Some(code) = self.error_type.clone() {
            return (Some(code), error);
        }

        match (&self.message, &error) {
            (Some(message), Some(code)) if message != code => {
                (Some(code.clone()), Some(message.clone()))
            }
            (Some(message), _) => (None, Some(message.clone())),
            (None, _) => (None, error),
        }
    }
}

/// Render a JSON scalar as a string. Objects and arrays are not codes and are dropped.
fn scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

/// Truncate on a char boundary.
pub(crate) fn truncate(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }
    let mut end = limit;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

/// The `X-RateLimit-*` headers, parsed.
///
/// # `X-RateLimit-Reset` is an absolute epoch in **milliseconds**
///
/// ```text
/// // ApiClass.ts:445 (v8.8.0-develop)
/// response.headers.set('X-RateLimit-Reset', String(new Date().getTime() + attemptResult.timeToReset));
/// ```
///
/// Not seconds, and not a delay. The conventional reading — `Duration::from_secs(header)` —
/// asks a bot to sleep for roughly 55 million years. Use [`retry_after`](Self::retry_after),
/// which subtracts the local clock and converts.
///
/// The value is server-supplied and unvalidated, so every conversion here saturates rather
/// than panics, and the result is capped at [`MAX_RETRY_AFTER`](Self::MAX_RETRY_AFTER): a
/// server whose clock runs a week fast must not park the client for a week.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RateLimit {
    limit: Option<u64>,
    remaining: Option<u64>,
    reset_epoch_ms: Option<i64>,
}

impl RateLimit {
    /// Ceiling applied by [`retry_after`](Self::retry_after).
    ///
    /// The server's own windows are 60 s (REST) and 10 s (DDP), so anything beyond an hour
    /// is a clock-skew artefact rather than an instruction.
    pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(3600);

    /// Parse the three headers. Returns `None` when none of them are present.
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let parsed = Self {
            limit: header_u64(headers, "x-ratelimit-limit"),
            remaining: header_u64(headers, "x-ratelimit-remaining"),
            reset_epoch_ms: header_i64(headers, "x-ratelimit-reset"),
        };

        (parsed != Self::default()).then_some(parsed)
    }

    /// Requests allowed in the window, as the server reports it.
    ///
    /// `None` when the header was absent or unparseable — the server writes an empty string
    /// when the route has no configured limit (`String(options.numRequestsAllowed ?? '')`).
    #[must_use]
    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    /// Requests left in the current window.
    #[must_use]
    pub fn remaining(&self) -> Option<u64> {
        self.remaining
    }

    /// When the window resets, as an absolute instant.
    ///
    /// `None` if the header was absent, unparseable, negative, or so large that it does not
    /// fit in a [`SystemTime`].
    #[must_use]
    pub fn reset_at(&self) -> Option<SystemTime> {
        let millis = u64::try_from(self.reset_epoch_ms?).ok()?;
        UNIX_EPOCH.checked_add(Duration::from_millis(millis))
    }

    /// How long to wait before retrying, measured against the system clock.
    ///
    /// `Some(Duration::ZERO)` means the window has already reset. `None` means the server
    /// did not say.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after_since(SystemTime::now())
    }

    /// [`retry_after`](Self::retry_after) against a caller-supplied clock.
    ///
    /// Exposed so that retry policies stay testable without sleeping, and so that a caller
    /// with a better clock than `SystemTime::now()` can use it.
    #[must_use]
    pub fn retry_after_since(&self, now: SystemTime) -> Option<Duration> {
        let reset_ms = self.reset_epoch_ms?;
        let now_ms =
            i64::try_from(now.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis())
                .unwrap_or(i64::MAX);

        let wait_ms = reset_ms.saturating_sub(now_ms);
        if wait_ms <= 0 {
            return Some(Duration::ZERO);
        }

        let wait = Duration::from_millis(wait_ms.unsigned_abs());
        Some(wait.min(Self::MAX_RETRY_AFTER))
    }

    /// The raw `X-RateLimit-Reset` value, in milliseconds since the Unix epoch.
    ///
    /// Provided so a caller can log what the server actually sent when it looks wrong.
    #[must_use]
    pub fn reset_epoch_millis(&self) -> Option<i64> {
        self.reset_epoch_ms
    }
}

fn header_str<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
    headers.get(name)?.to_str().ok()
}

fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    header_str(headers, name)?.trim().parse().ok()
}

fn header_i64(headers: &HeaderMap, name: &str) -> Option<i64> {
    header_str(headers, name)?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    use reqwest::header::{HeaderMap, HeaderValue};

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                reqwest::header::HeaderName::from_static(name),
                HeaderValue::from_str(value).expect("valid header value"),
            );
        }
        map
    }

    fn api_error(status: u16, body: &str) -> ApiError {
        ApiError::from_response(
            StatusCode::from_u16(status).expect("valid status"),
            &HeaderMap::new(),
            body.as_bytes(),
        )
    }

    #[test]
    fn failure_shape_puts_the_code_in_error_type() {
        let error = api_error(
            400,
            r#"{"success":false,"error":"Not allowed","errorType":"error-not-allowed"}"#,
        );

        assert_eq!(error.code(), Some("error-not-allowed"));
        assert_eq!(error.message(), Some("Not allowed"));
        assert_eq!(error.kind(), ApiErrorKind::BadRequest);
    }

    #[test]
    fn login_failure_inverts_the_two_fields() {
        // `POST /api/v1/login` re-exports the Meteor error it caught, so `error` is the code
        // — here the bare number 403 — and `message` is the prose.
        let error = api_error(
            401,
            r#"{"success":false,"status":"error","error":403,"message":"Incorrect password"}"#,
        );

        assert_eq!(error.code(), Some("403"), "a numeric code must survive as a string");
        assert_eq!(error.message(), Some("Incorrect password"));
        assert!(error.is_unauthorized());
    }

    #[test]
    fn a_repeated_sentence_is_a_message_not_a_code() {
        // The auth middleware sets `error` and `message` to the same prose. Reading `error`
        // as a code there would produce an English sentence masquerading as an error code.
        let error = api_error(
            401,
            r#"{"success":false,"error":"You must be logged in to do this.","status":"error","message":"You must be logged in to do this."}"#,
        );

        assert_eq!(error.code(), None);
        assert_eq!(error.message(), Some("You must be logged in to do this."));
    }

    #[test]
    fn the_status_helpers_never_consult_the_string() {
        let forbidden = api_error(403, r#"{"success":false,"error":"unauthorized"}"#);
        assert!(forbidden.is_forbidden());
        assert!(!forbidden.is_unauthorized());

        // The same body after the 9.0 rename must classify identically.
        let renamed = api_error(403, r#"{"success":false,"error":"forbidden"}"#);
        assert_eq!(renamed.kind(), forbidden.kind());
    }

    #[test]
    fn a_non_json_body_becomes_the_message() {
        let error = api_error(403, "Forbidden");
        assert_eq!(error.kind(), ApiErrorKind::Forbidden);
        assert_eq!(error.message(), Some("Forbidden"));
        assert_eq!(error.raw_body(), Some("Forbidden"));
        assert_eq!(error.code(), None);
    }

    #[test]
    fn an_html_error_page_from_a_proxy_does_not_panic() {
        let error = api_error(502, "<html><body><h1>502 Bad Gateway</h1></body></html>");
        assert_eq!(error.kind(), ApiErrorKind::Server);
        assert!(error.message().is_some_and(|message| message.contains("502 Bad Gateway")));
    }

    #[test]
    fn an_empty_body_leaves_no_message() {
        let error = api_error(401, "");
        assert_eq!(error.kind(), ApiErrorKind::Unauthorized);
        assert_eq!(error.message(), None);
        assert_eq!(error.raw_body(), None);
    }

    #[test]
    fn a_two_factor_challenge_arrives_as_a_400() {
        let error = api_error(
            400,
            r#"{"success":false,"error":"TOTP Required","errorType":"totp-required","details":{"method":"totp","availableMethods":["totp","email"]}}"#,
        );

        assert!(error.requires_two_factor());
        assert_eq!(error.kind(), ApiErrorKind::BadRequest, "not 401, however much it looks it");
        assert_eq!(
            error.details().and_then(|details| details.get("availableMethods")),
            Some(&serde_json::json!(["totp", "email"])),
        );
    }

    #[test]
    fn a_body_snippet_is_truncated_on_a_char_boundary() {
        let body = "é".repeat(1000);
        let error = api_error(500, &body);
        let message = error.message().expect("a message");
        assert!(message.len() <= BODY_SNIPPET_LIMIT);
        assert!(message.chars().all(|character| character == 'é'));
    }

    #[test]
    fn display_names_both_halves() {
        let error = api_error(400, r#"{"success":false,"error":"Not allowed","errorType":"nope"}"#);
        assert_eq!(error.to_string(), "Rocket.Chat REST error (HTTP 400): Not allowed [nope]");
    }

    // ---------------------------------------------------------------------------------
    // Rate limiting
    // ---------------------------------------------------------------------------------

    fn at_millis(millis: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(millis)
    }

    fn reset_header(value: &str) -> RateLimit {
        RateLimit::from_headers(&headers(&[("x-ratelimit-reset", value)]))
            .expect("the header is present")
    }

    #[test]
    fn reset_is_an_absolute_epoch_in_milliseconds() {
        let limits = RateLimit::from_headers(&headers(&[
            ("x-ratelimit-limit", "10"),
            ("x-ratelimit-remaining", "3"),
            ("x-ratelimit-reset", "1755600030000"),
        ]))
        .expect("headers present");

        assert_eq!(limits.limit(), Some(10));
        assert_eq!(limits.remaining(), Some(3));
        assert_eq!(limits.reset_epoch_millis(), Some(1_755_600_030_000));

        // Thirty seconds later, not thirty seconds' worth of epoch.
        assert_eq!(
            limits.retry_after_since(at_millis(1_755_600_000_000)),
            Some(Duration::from_secs(30)),
        );
        assert_eq!(limits.reset_at(), Some(at_millis(1_755_600_030_000)));
    }

    #[test]
    fn a_reset_in_the_past_means_no_wait() {
        let limits = reset_header("1755600000000");
        assert_eq!(limits.retry_after_since(at_millis(1_755_600_030_000)), Some(Duration::ZERO));
    }

    #[test]
    fn a_negative_reset_is_survivable() {
        let limits = reset_header("-1");
        assert_eq!(limits.reset_epoch_millis(), Some(-1));
        assert_eq!(limits.reset_at(), None);
        assert_eq!(limits.retry_after_since(at_millis(1_755_600_000_000)), Some(Duration::ZERO));
        assert_eq!(limits.retry_after_since(UNIX_EPOCH), Some(Duration::ZERO));
    }

    #[test]
    fn an_enormous_reset_is_clamped_rather_than_parking_the_client_forever() {
        let limits = reset_header(&i64::MAX.to_string());
        assert_eq!(
            limits.retry_after_since(at_millis(1_755_600_000_000)),
            Some(RateLimit::MAX_RETRY_AFTER),
        );
        // Converting it must not panic either, whether or not the platform's `SystemTime`
        // happens to be wide enough to hold it.
        assert!(limits.reset_at().is_none_or(|reset| reset > at_millis(1_755_600_000_000)));
    }

    #[test]
    fn an_unparseable_reset_is_simply_absent() {
        for hostile in ["soon", "", "9999999999999999999999999", "1e9", "NaN", "12.5"] {
            let map = headers(&[("x-ratelimit-reset", hostile)]);
            let limits = RateLimit::from_headers(&map);
            assert!(
                limits.is_none_or(|limits| limits.retry_after().is_none()),
                "{hostile:?} produced a retry delay",
            );
        }
    }

    #[test]
    fn absent_headers_produce_no_rate_limit() {
        assert!(RateLimit::from_headers(&HeaderMap::new()).is_none());
    }

    #[test]
    fn an_empty_limit_header_does_not_hide_a_present_reset() {
        // The server writes `String(numRequestsAllowed ?? '')`, so an unconfigured route
        // sends an empty limit alongside a real reset.
        let limits = RateLimit::from_headers(&headers(&[
            ("x-ratelimit-limit", ""),
            ("x-ratelimit-reset", "1755600030000"),
        ]))
        .expect("the reset is still usable");

        assert_eq!(limits.limit(), None);
        assert_eq!(limits.reset_epoch_millis(), Some(1_755_600_030_000));
    }

    #[test]
    fn a_clock_far_ahead_of_the_epoch_does_not_overflow() {
        let limits = reset_header("1755600030000");
        let far_future = UNIX_EPOCH + Duration::from_secs(u64::from(u32::MAX) * 1_000);
        assert_eq!(limits.retry_after_since(far_future), Some(Duration::ZERO));
    }
}
