//! The client: base URL, HTTP connection pool, credentials, and the endpoints.

use std::sync::Arc;

use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{Method, Url};
use serde::de::DeserializeOwned;
use tokio::sync::RwLock;

use rocketsocket_model::entity::{Message, User};
use rocketsocket_model::{RoomId, UploadId};

use crate::auth::{Authentication, Credentials, LoginData, LoginOutcome, LoginRequest, TwoFactor};
use crate::chat::{MessageEnvelope, PostMessage, PostedMessage, SendMessage};
use crate::error::{ApiError, RestError, truncate};
use crate::roles::{PublicRoleHolder, PublicRolesEnvelope, RoomRoleHolder, RoomRolesEnvelope};
use crate::upload::{ConfirmUpload, FileUpload, MediaEnvelope, UploadError, UploadedFile};

/// Path appended to the workspace URL to reach the v1 API.
const API_PREFIX: &str = "api/v1/";

/// `X-User-Id`. Lowercase because `HeaderName::from_static` requires it; HTTP/1.1 header
/// names are case-insensitive and HTTP/2 requires lowercase on the wire anyway.
const HEADER_USER_ID: HeaderName = HeaderName::from_static("x-user-id");
/// `X-Auth-Token`.
const HEADER_AUTH_TOKEN: HeaderName = HeaderName::from_static("x-auth-token");
/// `x-2fa-code`.
const HEADER_2FA_CODE: HeaderName = HeaderName::from_static("x-2fa-code");
/// `x-2fa-method`.
const HEADER_2FA_METHOD: HeaderName = HeaderName::from_static("x-2fa-method");

/// How much of an undecodable body is kept in the error.
const SNIPPET_LIMIT: usize = 512;

/// A Rocket.Chat REST client.
///
/// Cheap to clone: everything shared sits behind one [`Arc`], so a clone costs a refcount
/// bump plus at most a small per-request override. Clone it freely into tasks; they share
/// one connection pool and one credential slot, so a [`login`](Self::login) on any clone is
/// visible to all of them.
///
/// ```no_run
/// # use rocketsocket_rest::{Client, Credentials, SendMessage};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new("https://chat.example.com")?;
/// let login = client
///     .login(&Credentials::personal_access_token("aobEdbYhXfu5hkeqG", "…"))
///     .await?;
///
/// // The same token authenticates the websocket — see `Authentication::resume_token`.
/// let resume = login.authentication.resume_token().expose().to_owned();
///
/// client.send_message(&SendMessage::new("GENERAL").text("hello")).await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Inner>,
    /// Per-clone, not shared: [`with_two_factor`](Self::with_two_factor) hands out a client
    /// carrying one challenge answer without disturbing anyone else's.
    two_factor: Option<TwoFactor>,
}

#[derive(Debug)]
struct Inner {
    http: reqwest::Client,
    /// Workspace URL with `api/v1/` appended, always ending in `/`.
    api_base: Url,
    auth: RwLock<Option<Authentication>>,
}

impl Client {
    /// A client for the workspace at `base_url`.
    ///
    /// `base_url` is the workspace root — `https://chat.example.com`, or
    /// `https://example.com/rocketchat` for a sub-path deployment — **not** the `/api/v1`
    /// path, which is appended here. Any query string or fragment is discarded.
    ///
    /// # Errors
    ///
    /// [`RestError::InvalidUrl`] if `base_url` is not an absolute URL.
    pub fn new(base_url: &str) -> Result<Self, RestError> {
        Self::builder(base_url)?.build()
    }

    /// A builder, for supplying your own [`reqwest::Client`] or pre-set credentials.
    ///
    /// # Errors
    ///
    /// [`RestError::InvalidUrl`] if `base_url` is not an absolute URL.
    pub fn builder(base_url: &str) -> Result<ClientBuilder, RestError> {
        ClientBuilder::new(base_url)
    }

    /// The API root, i.e. the workspace URL with `api/v1/` appended.
    #[must_use]
    pub fn api_base(&self) -> &Url {
        &self.inner.api_base
    }

    /// The credentials currently in use, if any.
    pub async fn authentication(&self) -> Option<Authentication> {
        self.inner.auth.read().await.clone()
    }

    /// Install or clear credentials without contacting the server.
    ///
    /// Useful for restoring a token persisted across restarts — including one obtained by
    /// the realtime crate — which avoids minting a new one on every process start.
    pub async fn set_authentication(&self, authentication: Option<Authentication>) {
        *self.inner.auth.write().await = authentication;
    }

    /// A clone of this client that answers a 2FA challenge on every request it makes.
    ///
    /// Rocket.Chat validates the second factor **per request**, not per session: an endpoint
    /// declared `twoFactorRequired` reads `x-2fa-code` / `x-2fa-method` from that request and
    /// nothing is remembered afterwards. So the natural scope for a code is one call:
    ///
    /// ```no_run
    /// # use rocketsocket_rest::{Client, TwoFactor};
    /// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// let me = client.with_two_factor(TwoFactor::totp("123456")).me().await?;
    /// # Ok(()) }
    /// ```
    ///
    /// A challenge arrives as **HTTP 400** with code `totp-required`; see
    /// [`ApiError::requires_two_factor`](crate::ApiError::requires_two_factor).
    #[must_use]
    pub fn with_two_factor(&self, two_factor: TwoFactor) -> Self {
        Self { inner: Arc::clone(&self.inner), two_factor: Some(two_factor) }
    }

    /// A clone of this client with any 2FA challenge answer removed.
    #[must_use]
    pub fn without_two_factor(&self) -> Self {
        Self { inner: Arc::clone(&self.inner), two_factor: None }
    }

    // ---------------------------------------------------------------------------------
    // Authentication
    // ---------------------------------------------------------------------------------

    /// Authenticate, and store the resulting credentials on this client.
    ///
    /// The two credential kinds take different routes to the same place:
    ///
    /// - [`Credentials::Password`] posts to `/api/v1/login`, which is a wrapper around the
    ///   DDP `login` method, and mints a fresh entry in `services.resume.loginTokens`.
    /// - [`Credentials::PersonalAccessToken`] **is** already such an entry, so no login call
    ///   is made. The credentials are installed and validated with a single `/api/v1/me`
    ///   call, so that a bad token fails here rather than at the first real request. Skip
    ///   even that with [`set_authentication`](Self::set_authentication).
    ///
    /// Either way the returned [`LoginOutcome::authentication`] holds a token that is
    /// simultaneously the REST `X-Auth-Token` and the DDP `resume` token — see the
    /// [`auth`](crate::auth) module docs for why that matters on EE deployments.
    ///
    /// [`LoginOutcome::me`] is the authenticated user; a follow-up `/api/v1/me` call would
    /// be redundant, since the login response already carries it.
    ///
    /// # Errors
    ///
    /// A wrong password answers **401** with the DDP error inverted into the body — `error`
    /// holds the code (often the bare number `403`) and `message` the prose. Both are
    /// normalized: [`ApiError::code`] and [`ApiError::message`] read the same way as for any
    /// other failure.
    pub async fn login(&self, credentials: &Credentials) -> Result<LoginOutcome, RestError> {
        match credentials {
            Credentials::PersonalAccessToken { .. } => {
                let authentication =
                    credentials.as_authentication().expect("a PAT is an authentication pair");
                let previous = self.inner.auth.write().await.replace(authentication.clone());

                match self.me().await {
                    Ok(me) => Ok(LoginOutcome { authentication, me }),
                    Err(error) => {
                        *self.inner.auth.write().await = previous;
                        Err(error)
                    }
                }
            }
            Credentials::Password { user, password } => {
                let url = self.endpoint(&["login"])?;
                let response = self
                    .anonymous_request(Method::POST, url)
                    .json(&LoginRequest { user, password })
                    .send()
                    .await?;

                let envelope: LoginEnvelope = decode(response).await?;
                let authentication =
                    Authentication::new(envelope.data.user_id, envelope.data.auth_token);
                *self.inner.auth.write().await = Some(authentication.clone());

                Ok(LoginOutcome { authentication, me: envelope.data.me })
            }
        }
    }

    /// Invalidate the current token server-side and forget it.
    ///
    /// `POST /api/v1/logout` pulls the token out of `services.resume.loginTokens`, which
    /// kills it for **both** transports at once: an open websocket authenticated with the
    /// same token is now running on a dead credential and will be rejected on its next
    /// resume. Do not log out while the realtime connection is meant to stay up.
    ///
    /// Local credentials are cleared on success, and also when the server answers 401 (the
    /// token was already dead). Any other failure leaves them in place so the call can be
    /// retried.
    ///
    /// A Personal Access Token must **not** be logged out unless you mean to destroy it —
    /// this is the same call the UI's "remove token" button makes, and it is permanent.
    pub async fn logout(&self) -> Result<(), RestError> {
        let url = self.endpoint(&["logout"])?;
        let response = self.authenticated_request(Method::POST, url).await?.send().await?;

        match decode::<serde_json::Value>(response).await {
            Ok(_) => {
                *self.inner.auth.write().await = None;
                Ok(())
            }
            Err(error) => {
                if error.api().is_some_and(ApiError::is_unauthorized) {
                    *self.inner.auth.write().await = None;
                }
                Err(error)
            }
        }
    }

    /// `GET /api/v1/me` — the authenticated user.
    ///
    /// Redundant immediately after [`login`](Self::login), which already returns it.
    ///
    /// # Errors
    ///
    /// [`RestError::NotAuthenticated`] if no credentials are installed.
    pub async fn me(&self) -> Result<User, RestError> {
        let url = self.endpoint(&["me"])?;
        let response = self.authenticated_request(Method::GET, url).await?.send().await?;
        decode(response).await
    }

    // ---------------------------------------------------------------------------------
    // Roles
    // ---------------------------------------------------------------------------------

    /// `GET /api/v1/roles.getUsersInPublicRoles` — every holder of a globally-scoped
    /// public role, workspace-wide.
    ///
    /// The only role source a plain bot can reach: it is `authRequired` with **no**
    /// `permissionsRequired`, unlike `users.info` (whose `roles` field needs
    /// `view-full-other-user-info`, default `admin` only) and `roles.getUsersInRole`
    /// (needs `access-permissions`).
    ///
    /// It answers for the whole workspace in one request, so an `admin` check costs one
    /// call however much traffic there is — which matters, because the default limiter
    /// allows ten requests per route per minute.
    ///
    /// Note the server filters to roles with a **non-empty `description`**. On a stock
    /// workspace that admits `admin` but *excludes* `bot` and `app`, which are seeded with
    /// an empty description — so this cannot be used to identify other bots.
    ///
    /// # Errors
    /// Returns [`RestError`] if the request fails or the response does not decode.
    pub async fn users_in_public_roles(&self) -> Result<Vec<PublicRoleHolder>, RestError> {
        let url = self.endpoint(&["roles.getUsersInPublicRoles"])?;
        let response = self.authenticated_request(Method::GET, url).await?.send().await?;
        let envelope: PublicRolesEnvelope = decode(response).await?;
        if !envelope.success {
            return Err(RestError::MissingSuccess { endpoint: "roles.getUsersInPublicRoles" });
        }
        Ok(envelope.users)
    }

    /// `GET /api/v1/rooms.roles?rid=…` — subscription-scoped roles within one room.
    ///
    /// `authRequired`, no permission required. Returns one entry per user holding a
    /// room-scoped role; conventionally `owner`, `moderator` and `leader`, though a
    /// workspace that defines its own subscription-scoped role with a description will see
    /// that here too — so match the roles you mean explicitly rather than treating any
    /// entry as authority.
    ///
    /// # Errors
    /// Returns [`RestError`] if the request fails or the response does not decode. Note an
    /// inaccessible room reports `error-invalid-user` and an unknown room
    /// `error-invalid-room`, neither distinguishable from a permission problem — treat any
    /// failure as "unknown", never as "no roles".
    pub async fn room_roles(&self, room: &RoomId) -> Result<Vec<RoomRoleHolder>, RestError> {
        let url = self.endpoint(&["rooms.roles"])?;
        let response = self
            .authenticated_request(Method::GET, url)
            .await?
            .query(&[("rid", room.as_str())])
            .send()
            .await?;
        let envelope: RoomRolesEnvelope = decode(response).await?;
        if !envelope.success {
            return Err(RestError::MissingSuccess { endpoint: "rooms.roles" });
        }
        Ok(envelope.roles)
    }

    // ---------------------------------------------------------------------------------
    // Messages
    // ---------------------------------------------------------------------------------

    /// `POST /api/v1/chat.sendMessage` — the primary way to send a message.
    ///
    /// See [`SendMessage`] for what it can carry and why it is preferred over
    /// [`post_message`](Self::post_message).
    pub async fn send_message(&self, message: &SendMessage) -> Result<Message, RestError> {
        let url = self.endpoint(&["chat.sendMessage"])?;
        let response =
            self.authenticated_request(Method::POST, url).await?.json(message).send().await?;
        let envelope: MessageEnvelope = decode(response).await?;
        Ok(envelope.message)
    }

    /// `POST /api/v1/chat.postMessage` — the webhook-style send.
    ///
    /// Use it for multiple rooms in one call, or for a `#channel` / `@user` target. Note the
    /// auto-join and the forced `groupable: false` documented on [`PostMessage`], and that
    /// the response describes only the first target.
    pub async fn post_message(&self, message: &PostMessage) -> Result<PostedMessage, RestError> {
        let url = self.endpoint(&["chat.postMessage"])?;
        let response =
            self.authenticated_request(Method::POST, url).await?.json(message).send().await?;
        decode(response).await
    }

    // ---------------------------------------------------------------------------------
    // File upload
    // ---------------------------------------------------------------------------------

    /// Upload a file and post it, as one transaction.
    ///
    /// Runs `rooms.media/:rid` then `rooms.mediaConfirm/:rid/:fileId`. **Both steps are
    /// required**: the first only parks the bytes in temporary storage with a 24-hour
    /// expiry and posts nothing at all.
    ///
    /// If the second step fails the error is [`UploadError::Confirm`], carrying the
    /// [`UploadedFile`] whose id is the only way to finish or clean up the transaction.
    /// Retry it with [`confirm_media`](Self::confirm_media) — re-running this method instead
    /// uploads the bytes a second time and orphans the first copy. Note that confirmation is
    /// not idempotent: retry a refusal, not a timeout.
    ///
    /// ```no_run
    /// # use rocketsocket_rest::{Client, ConfirmUpload, FileUpload};
    /// # async fn example(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    /// let file = FileUpload::new("report.pdf", std::fs::read("report.pdf")?)
    ///     .content_type("application/pdf");
    ///
    /// match client.upload_file("GENERAL", file, ConfirmUpload::new().text("last night's run")).await {
    ///     Ok(message) => println!("posted {}", message.id),
    ///     Err(error) => {
    ///         if let Some(orphan) = error.orphaned_file() {
    ///             eprintln!("retry confirm for upload {}", orphan.id);
    ///         }
    ///         return Err(error.into());
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn upload_file(
        &self,
        rid: impl Into<RoomId>,
        file: FileUpload,
        confirm: ConfirmUpload,
    ) -> Result<Message, UploadError> {
        let rid = rid.into();
        let uploaded = self.upload_media(rid.clone(), file).await.map_err(UploadError::Upload)?;

        match self.confirm_media(rid, &uploaded.id, &confirm).await {
            Ok(message) => Ok(message),
            Err(source) => {
                tracing::warn!(
                    file_id = %uploaded.id,
                    "upload confirmation failed; the file is stored but no message was posted",
                );
                Err(UploadError::Confirm { file: uploaded, source })
            }
        }
    }

    /// Step 1 alone: `POST /api/v1/rooms.media/:rid`.
    ///
    /// Stores the bytes temporarily and returns their id. **Nothing is posted to the room**
    /// until [`confirm_media`](Self::confirm_media) runs; an upload left unconfirmed is
    /// deleted after [`UploadedFile::TTL_HOURS`] hours. Prefer
    /// [`upload_file`](Self::upload_file) unless you need the two halves apart.
    ///
    /// The multipart field name is `file`, hard-coded on both sides.
    pub async fn upload_media(
        &self,
        rid: impl Into<RoomId>,
        file: FileUpload,
    ) -> Result<UploadedFile, RestError> {
        let rid = rid.into();
        let url = self.endpoint(&["rooms.media", rid.as_ref()])?;

        let file_name = file.file_name().to_owned();
        let mime = file.mime().map(str::to_owned);
        let mut part = reqwest::multipart::Part::bytes(file.into_bytes()).file_name(file_name);
        if let Some(mime) = mime {
            part = part.mime_str(&mime).map_err(|error| RestError::InvalidRequest {
                reason: format!("invalid content type: {error}"),
            })?;
        }

        let form = reqwest::multipart::Form::new().part("file", part);
        let response =
            self.authenticated_request(Method::POST, url).await?.multipart(form).send().await?;

        let envelope: MediaEnvelope = decode(response).await?;
        Ok(envelope.file)
    }

    /// Step 2 alone: `POST /api/v1/rooms.mediaConfirm/:rid/:fileId`.
    ///
    /// Posts the message that carries the uploaded file and makes the upload permanent.
    ///
    /// **Not idempotent.** Confirming only clears the upload's `expiresAt`; the lookup that
    /// guards it (`findOneByIdAndUserIdAndRoomId`) does not consider whether the file was
    /// already confirmed, so a second call posts the file to the room a second time. Retry
    /// it after a failure that is *known* to have been refused — which is what
    /// [`upload_file`](Self::upload_file) hands you in [`UploadError::Confirm`] — but not
    /// after a timeout, where the first attempt may well have succeeded.
    pub async fn confirm_media(
        &self,
        rid: impl Into<RoomId>,
        file_id: &UploadId,
        confirm: &ConfirmUpload,
    ) -> Result<Message, RestError> {
        let rid = rid.into();
        let url = self.endpoint(&["rooms.mediaConfirm", rid.as_ref(), file_id.as_ref()])?;
        let response =
            self.authenticated_request(Method::POST, url).await?.json(confirm).send().await?;
        let envelope: MessageEnvelope = decode(response).await?;
        Ok(envelope.message)
    }

    // ---------------------------------------------------------------------------------
    // Plumbing
    // ---------------------------------------------------------------------------------

    /// Build an endpoint URL from path segments, percent-encoding each one.
    ///
    /// Segments are encoded rather than interpolated because room ids reach this code from
    /// user input often enough, and a `/` or `?` inside one would otherwise rewrite the
    /// request's target.
    fn endpoint(&self, segments: &[&str]) -> Result<Url, RestError> {
        let mut url = self.inner.api_base.clone();
        {
            let mut path = url.path_segments_mut().map_err(|()| RestError::InvalidUrl {
                endpoint: segments.join("/"),
                reason: "the base URL cannot have path segments".to_owned(),
            })?;
            path.pop_if_empty().extend(segments);
        }
        Ok(url)
    }

    /// A request with no credentials — used only for `login`.
    fn anonymous_request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        self.apply_two_factor(self.inner.http.request(method, url))
    }

    /// A request carrying `X-User-Id` / `X-Auth-Token`, and any 2FA answer.
    async fn authenticated_request(
        &self,
        method: Method,
        url: Url,
    ) -> Result<reqwest::RequestBuilder, RestError> {
        let auth = self.authentication().await.ok_or(RestError::NotAuthenticated)?;

        let user_id = HeaderValue::from_str(auth.user_id().as_ref()).map_err(|_| {
            RestError::InvalidRequest { reason: "user id is not a valid header value".to_owned() }
        })?;
        let token = HeaderValue::from_str(auth.token().expose()).map_err(|_| {
            RestError::InvalidRequest {
                reason: "auth token is not a valid header value".to_owned(),
            }
        })?;

        let request = self
            .inner
            .http
            .request(method, url)
            .header(HEADER_USER_ID, user_id)
            .header(HEADER_AUTH_TOKEN, token);

        Ok(self.apply_two_factor(request))
    }

    fn apply_two_factor(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.two_factor {
            Some(two_factor) => request
                .header(HEADER_2FA_CODE, two_factor.code())
                .header(HEADER_2FA_METHOD, two_factor.method().as_str()),
            None => request,
        }
    }
}

/// Configuration for a [`Client`].
#[derive(Debug)]
pub struct ClientBuilder {
    api_base: Url,
    http: Option<reqwest::Client>,
    authentication: Option<Authentication>,
}

impl ClientBuilder {
    /// Start from a workspace URL.
    ///
    /// # Errors
    ///
    /// [`RestError::InvalidUrl`] if `base_url` is not an absolute URL.
    pub fn new(base_url: &str) -> Result<Self, RestError> {
        Ok(Self { api_base: api_base(base_url)?, http: None, authentication: None })
    }

    /// Use an existing [`reqwest::Client`], sharing its connection pool and settings.
    ///
    /// Supply one to set timeouts, proxies, or a custom root certificate — a self-hosted
    /// Rocket.Chat behind an internal CA is the common case.
    #[must_use]
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Start out authenticated, without a login round trip.
    ///
    /// Unlike [`Client::login`] this does not verify the credentials; a bad token surfaces
    /// as a 401 on the first real call.
    #[must_use]
    pub fn authentication(mut self, authentication: Authentication) -> Self {
        self.authentication = Some(authentication);
        self
    }

    /// Start out authenticated with a Personal Access Token.
    ///
    /// Ignores a [`Credentials::Password`], which cannot become a token without a login.
    #[must_use]
    pub fn credentials(mut self, credentials: &Credentials) -> Self {
        self.authentication = credentials.as_authentication();
        self
    }

    /// Build the client.
    ///
    /// # Errors
    ///
    /// [`RestError::Transport`] if no HTTP client was supplied and the default one cannot be
    /// constructed — in practice, a TLS backend that fails to initialise.
    pub fn build(self) -> Result<Client, RestError> {
        let http = match self.http {
            Some(http) => http,
            None => reqwest::Client::builder().build()?,
        };

        Ok(Client {
            inner: Arc::new(Inner {
                http,
                api_base: self.api_base,
                auth: RwLock::new(self.authentication),
            }),
            two_factor: None,
        })
    }
}

/// Turn a workspace URL into the `…/api/v1/` root.
fn api_base(base_url: &str) -> Result<Url, RestError> {
    let mut url = Url::parse(base_url).map_err(|error| RestError::InvalidUrl {
        endpoint: base_url.to_owned(),
        reason: error.to_string(),
    })?;

    url.set_query(None);
    url.set_fragment(None);

    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }

    url.join(API_PREFIX).map_err(|error| RestError::InvalidUrl {
        endpoint: base_url.to_owned(),
        reason: error.to_string(),
    })
}

/// The envelope `POST /api/v1/login` answers with.
#[derive(Debug, serde::Deserialize)]
struct LoginEnvelope {
    data: LoginData,
}

/// Turn a response into `T`, or into the most specific error available.
///
/// The order of checks is what makes a non-JSON 401 survive: the status is examined *before*
/// the body is parsed, so the plain-text `Unauthorized` produced by the Express
/// authentication middleware becomes an [`ApiError`] with a message, not a decode failure.
pub(crate) async fn decode<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, RestError> {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await?;

    if !status.is_success() {
        return Err(ApiError::from_response(status, &headers, &body).into());
    }

    match serde_json::from_slice(&body) {
        Ok(value) => Ok(value),
        Err(source) => {
            // A 2xx carrying `success: false` should not happen — the server's helpers pair
            // every failure with a non-2xx status — but reporting it as a decode bug would
            // bury the server's own explanation, so check before giving up.
            if is_failure_envelope(&body) {
                return Err(ApiError::from_response(status, &headers, &body).into());
            }
            Err(RestError::Decode {
                status,
                snippet: truncate(&String::from_utf8_lossy(&body), SNIPPET_LIMIT),
                source,
            })
        }
    }
}

/// Whether a body is JSON with `success: false`.
fn is_failure_envelope(body: &[u8]) -> bool {
    #[derive(serde::Deserialize)]
    struct Flag {
        #[serde(default)]
        success: Option<bool>,
    }

    serde_json::from_slice::<Flag>(body).is_ok_and(|flag| flag.success == Some(false))
}
