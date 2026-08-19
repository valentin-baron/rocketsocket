//! Credentials, the login exchange, and the token both transports share.
//!
//! # One credential, two transports
//!
//! `POST /api/v1/login` is not a parallel implementation of authentication. It is a wrapper
//! around the DDP `login` method:
//!
//! ```text
//! // ApiClass.ts:1102 (v8.8.0-develop)
//! const auth = await DDP._CurrentInvocation.withValue(invocation, async () =>
//!     Meteor.callAsync('login', args));
//! ```
//!
//! The token it returns is written to `services.resume.loginTokens`, which is the same array
//! the DDP `login {resume: …}` handler reads and the same array `X-Auth-Token` is checked
//! against (`ApiClass.authenticatedRoute` hashes the header and looks it up there). So
//! [`Authentication::token`] is simultaneously:
//!
//! - the `X-Auth-Token` header for every REST call, and
//! - the `resume` token for the DDP `login` method on the websocket.
//!
//! That is why this framework authenticates once over REST and reuses the token on the
//! socket, and it is not merely a convenience:
//!
//! - **It is the only path that works on both deployment shapes.** The EE `ddp-streamer`
//!   microservice implements exactly one login form —
//!   `async 'login'({ resume }: { resume: string })`
//!   (`ee/apps/ddp-streamer/src/configureServer.ts:68`). Password login over DDP simply does
//!   not exist there.
//! - **It sidesteps 2FA on reconnect**, because a `resume` login is exempt from the
//!   `onValidateLogin` chain.
//! - **It avoids churning `MAX_RESUME_LOGIN_TOKENS`** (default 50). A bot that re-logs in
//!   with a password on every reconnect mints a new token each time and evicts its own.
//!
//! # Personal Access Tokens are the credential to use
//!
//! A PAT is an entry in that same `loginTokens` array, created by
//! `personalAccessTokens:generateToken` with `{hashedToken, type: 'personalAccessToken',
//! createdAt, lastTokenPart, name, bypassTwoFactor}` — note the absence of a `when` member.
//! Meteor's expiry check computes `_tokenExpiration(when)`, which for `undefined` yields an
//! invalid date, and every comparison against an invalid date is false. **PATs do not
//! expire.** They can also carry `bypassTwoFactor`.
//!
//! A PAT therefore needs no login round trip at all: it *is* the token. Hand it to
//! [`Credentials::personal_access_token`] together with the user id it belongs to.

use std::fmt;

use serde::{Deserialize, Serialize};

use rocketsocket_model::UserId;
use rocketsocket_model::entity::User;

/// A Rocket.Chat login token.
///
/// Wrapped rather than a bare `String` for one reason: its [`Debug`] implementation prints
/// `AuthToken(<redacted>)`. A token in a log line is a full account takeover, and structured
/// logging makes it very easy to print a whole request context by accident.
///
/// Use [`expose`](Self::expose) at the point where the value is genuinely needed — building
/// a header, or handing it to the realtime crate as a resume token.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AuthToken(String);

impl AuthToken {
    /// Wrap a token string.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// The token itself.
    ///
    /// Named to make call sites visible in review: every use of this method is a place where
    /// a secret leaves its wrapper.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AuthToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthToken(<redacted>)")
    }
}

impl From<String> for AuthToken {
    fn from(token: String) -> Self {
        Self(token)
    }
}

impl From<&str> for AuthToken {
    fn from(token: &str) -> Self {
        Self(token.to_owned())
    }
}

/// What the client authenticates with.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Credentials {
    /// A Personal Access Token plus the user id that owns it.
    ///
    /// **The recommended bot credential.** It never expires, it can bypass 2FA, and it
    /// requires no login round trip — see the [module docs](self).
    ///
    /// The user id is required because Rocket.Chat authenticates on the *pair*
    /// `X-User-Id` + `X-Auth-Token`; the token alone is not enough. Read it from
    /// **Account → Personal Access Tokens** in the UI, or from `_id` on any
    /// `/api/v1/me` response.
    PersonalAccessToken {
        /// The id of the user the token belongs to.
        user_id: UserId,
        /// The token as displayed once at creation time.
        token: AuthToken,
    },

    /// A username-or-email plus password, exchanged for a token by `POST /api/v1/login`.
    ///
    /// Every login mints a new resume token, so a process that logs in on every restart
    /// slowly fills the account's `loginTokens` array (capped at `MAX_RESUME_LOGIN_TOKENS`,
    /// default 50, oldest evicted). Fine for an interactive tool; prefer a PAT for a bot.
    Password {
        /// Username or email address.
        ///
        /// The server disambiguates on the presence of `@`:
        /// `user.includes('@') ? {email: user} : {username: user}`
        /// (`ApiClass.ts` `loginCompatibility`).
        user: String,
        /// The password, sent as plain text inside the TLS session.
        password: String,
    },
}

impl Credentials {
    /// A Personal Access Token credential.
    #[must_use]
    pub fn personal_access_token(user_id: impl Into<UserId>, token: impl Into<AuthToken>) -> Self {
        Self::PersonalAccessToken { user_id: user_id.into(), token: token.into() }
    }

    /// A username-or-email and password credential.
    #[must_use]
    pub fn password(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Password { user: user.into(), password: password.into() }
    }

    /// The ready-made authentication pair, for credentials that are already a token.
    #[must_use]
    pub fn as_authentication(&self) -> Option<Authentication> {
        match self {
            Self::PersonalAccessToken { user_id, token } => {
                Some(Authentication { user_id: user_id.clone(), token: token.clone() })
            }
            Self::Password { .. } => None,
        }
    }
}

/// The `X-User-Id` / `X-Auth-Token` pair that authenticates every request.
///
/// [`token`](Self::token) is also a valid DDP resume token — see the [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authentication {
    user_id: UserId,
    token: AuthToken,
}

impl Authentication {
    /// Build a pair directly, for a token obtained elsewhere.
    #[must_use]
    pub fn new(user_id: impl Into<UserId>, token: impl Into<AuthToken>) -> Self {
        Self { user_id: user_id.into(), token: token.into() }
    }

    /// The authenticated user's id — the `X-User-Id` header.
    #[must_use]
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    /// The login token — the `X-Auth-Token` header.
    #[must_use]
    pub fn token(&self) -> &AuthToken {
        &self.token
    }

    /// The same token, named for its other job: the `resume` parameter of the DDP `login`
    /// method.
    ///
    /// ```no_run
    /// # use rocketsocket_rest::Authentication;
    /// # fn example(auth: &Authentication) -> serde_json::Value {
    /// serde_json::json!({ "resume": auth.resume_token().expose() })
    /// # }
    /// ```
    ///
    /// This is the only login form the EE `ddp-streamer` accepts, and it is exempt from 2FA.
    #[must_use]
    pub fn resume_token(&self) -> &AuthToken {
        &self.token
    }
}

/// The outcome of [`Client::login`](crate::Client::login).
///
/// # `me` is not a second request
///
/// `POST /api/v1/login` already answers with the full user document —
/// `data: {userId, authToken, me: await getUserInfo(user)}` — produced by the same
/// `getUserInfo` helper that backs `GET /api/v1/me`. Calling `/api/v1/me` straight after
/// logging in is a redundant round trip; use this field.
///
/// (The two projections differ only in the `services` subtree: login sends
/// `services.github`, `services.gitlab` and `services.password.bcrypt`, while `/me` sends
/// the whole `services` object, which `getUserInfo` then narrows to the same handful of
/// keys. Nothing a bot reads is affected.)
#[derive(Debug, Clone)]
pub struct LoginOutcome {
    /// The credential pair now stored on the client.
    pub authentication: Authentication,
    /// The authenticated user.
    pub me: User,
}

/// The `data` member of a successful login response.
#[derive(Debug, Deserialize)]
pub(crate) struct LoginData {
    #[serde(rename = "userId")]
    pub(crate) user_id: UserId,
    #[serde(rename = "authToken")]
    pub(crate) auth_token: AuthToken,
    pub(crate) me: User,
}

/// The body of `POST /api/v1/login`.
#[derive(Debug, Serialize)]
pub(crate) struct LoginRequest<'c> {
    pub(crate) user: &'c str,
    pub(crate) password: &'c str,
}

/// A second-factor challenge answer, sent as request headers.
///
/// Rocket.Chat's 2FA is per-request, not per-session: an endpoint declared
/// `twoFactorRequired` calls `checkCodeForUser` with the `x-2fa-code` / `x-2fa-method`
/// headers of *that* request. There is no "unlock the session" call.
///
/// Attach one with [`Client::with_two_factor`](crate::Client::with_two_factor), which
/// returns a cheap clone of the client carrying the headers.
///
/// A challenge announces itself as **HTTP 400** with code `totp-required` — see
/// [`ApiError::requires_two_factor`](crate::ApiError::requires_two_factor) — and
/// `details.availableMethods` lists what the account will accept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwoFactor {
    code: String,
    method: TwoFactorMethod,
}

impl TwoFactor {
    /// A code for an explicit method.
    #[must_use]
    pub fn new(method: TwoFactorMethod, code: impl Into<String>) -> Self {
        Self { code: code.into(), method }
    }

    /// A TOTP code from an authenticator app.
    #[must_use]
    pub fn totp(code: impl Into<String>) -> Self {
        Self::new(TwoFactorMethod::Totp, code)
    }

    /// A code delivered by email.
    #[must_use]
    pub fn email(code: impl Into<String>) -> Self {
        Self::new(TwoFactorMethod::Email, code)
    }

    /// The code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// The method the code belongs to.
    #[must_use]
    pub fn method(&self) -> &TwoFactorMethod {
        &self.method
    }
}

/// Which second factor a code answers.
///
/// The names are the `name` members of the server's `ICodeCheck` implementations
/// (`server/lib/2fa/code/`), which is what `getSecondFactorMethod` matches the
/// `x-2fa-method` header against.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TwoFactorMethod {
    /// `totp` — an authenticator app.
    Totp,
    /// `email` — a code mailed to the account's verified address.
    Email,
    /// `password` — the password-confirmation fallback, used when no real second factor is
    /// enrolled but the endpoint still demands re-authentication.
    Password,
    /// Any other method name, including the `-oauth` variants.
    Other(String),
}

impl TwoFactorMethod {
    /// The wire name for the `x-2fa-method` header.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Totp => "totp",
            Self::Email => "email",
            Self::Password => "password",
            Self::Other(name) => name,
        }
    }
}

impl fmt::Display for TwoFactorMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
