//! Rocket.Chat REST API client.
//!
//! REST is this framework's **egress**: everything that changes server state goes out over
//! `/api/v1/*`, while events come back over DDP in `rocketsocket-realtime`. The split is not
//! stylistic. REST is the only Rocket.Chat surface with a forward compatibility guarantee,
//! the only one that can send `blocks` or files, and the only one whose deprecations come
//! with a replacement; the DDP write methods are deprecated for removal in 9.0.
//!
//! ```no_run
//! use rocketsocket_rest::{Client, Credentials, SendMessage};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::new("https://chat.example.com")?;
//! let login = client
//!     .login(&Credentials::personal_access_token("aobEdbYhXfu5hkeqG", "my-token"))
//!     .await?;
//!
//! client.send_message(&SendMessage::new("GENERAL").text("hello")).await?;
//!
//! // The very same token logs the websocket in — one credential, both transports.
//! let resume = login.authentication.resume_token();
//! # let _ = resume;
//! # Ok(()) }
//! ```
//!
//! # What is in here, and what is deliberately not
//!
//! The surface is narrow on purpose: authentication, sending messages, uploading files, and
//! the error and paging machinery those need. Everything present is checked against the
//! server source for v8.8, and the things that are *absent* are absent for reasons recorded
//! at their nearest neighbour:
//!
//! - no message sending over DDP — see [`chat`];
//! - no `rooms.upload` — removed in server 8.0, see [`upload`];
//! - no `query` / `fields` request parameters — silently ignored and slated for removal,
//!   see [`pagination`].
//!
//! # Three things that will cost you a day if you assume otherwise
//!
//! 1. **`errorType` means the opposite thing on REST and DDP.** On REST it is the machine
//!    code and `error` is the prose; on DDP `errorType` is always the literal
//!    `"Meteor.Error"` and `error` is the code. And `POST /api/v1/login` answers with the
//!    *DDP* arrangement because it is a wrapper around the DDP method. [`ApiError`]
//!    normalizes all of it: [`code`](ApiError::code) is always the code,
//!    [`message`](ApiError::message) always the prose.
//! 2. **HTTP status is the only reliable discriminator.** `API.v1.failure()` returns 400 for
//!    everything it is given, and a 403 currently answers with the *string* `"unauthorized"`
//!    pending a `// TODO: MAJOR` that changes it in 9.0. Match on
//!    [`ApiErrorKind`] / [`ApiError::status`], never on the string.
//! 3. **`X-RateLimit-Reset` is an absolute epoch in milliseconds.** Not seconds, not a
//!    delay. Use [`RateLimit::retry_after`].
//!
//! # The `bot` role is a deployment prerequisite
//!
//! A bot account without it gets the default limiter — **10 requests per 60 s per route per
//! IP** — which throttles even a modest bot immediately. The role's
//! `api-bypass-rate-limit` permission removes the limiter, `message-impersonate` is what
//! makes `alias` and `avatar` work at all, and `send-many-messages` lifts the 5 msg/s cap on
//! the DDP `sendMessage` method (which the REST endpoint does not go through, but the
//! realtime half of a bot does).

pub mod auth;
pub mod chat;
pub mod client;
pub mod error;
pub mod pagination;
pub mod upload;

pub use self::auth::{
    AuthToken, Authentication, Credentials, LoginOutcome, TwoFactor, TwoFactorMethod,
};
pub use self::chat::{PostMessage, PostTarget, PostedMessage, SendMessage};
pub use self::client::{Client, ClientBuilder};
pub use self::error::{ApiError, ApiErrorKind, RateLimit, RestError};
pub use self::pagination::{PageInfo, Pagination};
pub use self::upload::{ConfirmUpload, FileUpload, UploadError, UploadedFile};
