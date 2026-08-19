//! The two-step file upload transaction.
//!
//! Since server 8.0 there is no single-call file upload. `rooms.upload/:rid` was **removed**;
//! what replaced it is two endpoints that must both succeed:
//!
//! 1. `POST /api/v1/rooms.media/:rid` — multipart, binary field name hard-coded `file`.
//!    Stores the bytes as a **temporary** upload with
//!    `expiresAt = now + 24 h` and answers `{file: {_id, url}}`. **Nothing is posted to the
//!    room.**
//! 2. `POST /api/v1/rooms.mediaConfirm/:rid/:fileId` — JSON. Builds the file attachment,
//!    sends the message, and clears the expiry. Answers `{message}`.
//!
//! **Stopping after step 1 posts nothing and leaves an orphaned upload** that occupies
//! storage until the sweeper collects it a day later. Nobody sees an error; the file simply
//! never appears in the room. That is why [`Client::upload_file`](crate::Client::upload_file)
//! performs both steps as one call, and why failing the second step returns
//! [`UploadError::Confirm`] carrying the [`UploadedFile`] — the id is the only handle that
//! makes the transaction retryable, and it exists nowhere else.
//!
//! DDP cannot do any of this: it is a text protocol with no binary frame and no chunking.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use rocketsocket_model::{MessageId, UploadId};

use crate::error::RestError;

/// File bytes plus the metadata the multipart part carries.
///
/// The bytes are held in memory. Rocket.Chat's `FileUpload_MaxFileSize` default is 104 MB
/// and the server buffers the part to a temp file before storing it, so streaming from the
/// client would not avoid the server-side copy anyway.
#[derive(Debug, Clone)]
pub struct FileUpload {
    file_name: String,
    content_type: Option<String>,
    bytes: Vec<u8>,
}

impl FileUpload {
    /// A file to upload.
    ///
    /// `file_name` is what the room will show and what the attachment's title links to.
    #[must_use]
    pub fn new(file_name: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self { file_name: file_name.into(), content_type: None, bytes: bytes.into() }
    }

    /// Declare the MIME type.
    ///
    /// Worth setting: the server picks the attachment renderer from the stored `type`, so an
    /// image sent without one shows as a generic file rather than a preview. Omitted means
    /// `application/octet-stream`.
    #[must_use]
    pub fn content_type(mut self, mime: impl Into<String>) -> Self {
        self.content_type = Some(mime.into());
        self
    }

    /// The file name.
    #[must_use]
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// The declared MIME type, if any.
    #[must_use]
    pub fn mime(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// The bytes, consuming the value.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// The result of step 1: bytes are stored, nothing is posted yet.
#[derive(Debug, Clone, Deserialize)]
pub struct UploadedFile {
    /// The upload id, for `rooms.mediaConfirm/:rid/:fileId`.
    #[serde(rename = "_id")]
    pub id: UploadId,
    /// Server-relative path to the stored file, e.g. `/file-upload/<id>/<name>`.
    ///
    /// Not usable as-is by an unauthenticated client: file routes are behind the same
    /// credentials as the API.
    pub url: String,
}

impl UploadedFile {
    /// How long the server keeps an unconfirmed upload: 24 hours.
    ///
    /// ```text
    /// // server/api/v1/rooms.ts, rooms.media/:rid
    /// const expiresAt = new Date();
    /// expiresAt.setHours(expiresAt.getHours() + 24);
    /// ```
    ///
    /// After that the sweeper deletes it. Confirming inside the window is what makes the
    /// upload permanent.
    pub const TTL_HOURS: u32 = 24;
}

/// The `{file: …}` envelope of `rooms.media/:rid`.
#[derive(Debug, Deserialize)]
pub(crate) struct MediaEnvelope {
    pub(crate) file: UploadedFile,
}

/// Step 2's body: the message that will carry the file.
///
/// # Everything here is whitelisted server-side
///
/// After the handler strips `description`, `fileName` and `fileContent`, whatever remains is
/// passed to `sendFileMessage` and run through a Meteor `check()` that permits exactly
/// `{avatar, emoji, alias, groupable, msg, tmid, customFields, t, content}`. An unexpected
/// member does not get ignored — it makes the whole call fail. This type emits only members
/// from that list, which is why it is a closed builder rather than a free-form map.
///
/// Note `customFields` is checked as a **`String`** and `JSON.parse`d afterwards, not as an
/// object; [`custom_fields`](Self::custom_fields) does that encoding.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ConfirmUpload {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(rename = "fileName", skip_serializing_if = "Option::is_none")]
    file_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tmid: Option<MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    emoji: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avatar: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    groupable: Option<bool>,
    #[serde(rename = "customFields", skip_serializing_if = "Option::is_none")]
    custom_fields: Option<String>,
}

impl ConfirmUpload {
    /// A confirmation with no extra message.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Text posted alongside the file.
    #[must_use]
    pub fn text(mut self, msg: impl Into<String>) -> Self {
        self.msg = Some(msg.into());
        self
    }

    /// The file's description, which also becomes the image alt text.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Rename the stored file at confirmation time.
    #[must_use]
    pub fn file_name(mut self, name: impl Into<String>) -> Self {
        self.file_name = Some(name.into());
        self
    }

    /// Post the file into a thread.
    #[must_use]
    pub fn thread(mut self, tmid: impl Into<MessageId>) -> Self {
        self.tmid = Some(tmid.into());
        self
    }

    /// Display name to post under. Requires `message-impersonate`.
    #[must_use]
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Emoji avatar. Needs no permission, unlike [`alias`](Self::alias).
    #[must_use]
    pub fn emoji(mut self, emoji: impl Into<String>) -> Self {
        self.emoji = Some(emoji.into());
        self
    }

    /// Image-URL avatar. Requires `message-impersonate`.
    #[must_use]
    pub fn avatar(mut self, avatar: impl Into<String>) -> Self {
        self.avatar = Some(avatar.into());
        self
    }

    /// Whether the message groups with adjacent ones. Defaults to `false` server-side.
    #[must_use]
    pub fn groupable(mut self, groupable: bool) -> Self {
        self.groupable = Some(groupable);
        self
    }

    /// Custom fields, JSON-encoded as the server's `check()` requires.
    ///
    /// Returns the builder unchanged if the value cannot be serialized, which for
    /// [`Value`] cannot happen.
    #[must_use]
    pub fn custom_fields(mut self, fields: &Value) -> Self {
        if let Ok(encoded) = serde_json::to_string(fields) {
            self.custom_fields = Some(encoded);
        }
        self
    }
}

/// Which half of the upload transaction failed.
///
/// The distinction matters because the two failures need different recovery: step 1 leaves
/// nothing behind and can simply be retried, while step 2 has already consumed storage and
/// must be retried against the *same* file id rather than re-uploaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum UploadError {
    /// `rooms.media/:rid` failed. Nothing was stored; retry the whole thing.
    #[error("uploading the file failed")]
    Upload(#[source] RestError),

    /// The bytes are on the server but no message was posted.
    ///
    /// The upload is orphaned and expires in [`UploadedFile::TTL_HOURS`] hours. Retry with
    /// [`Client::confirm_media`](crate::Client::confirm_media) and the enclosed
    /// [`UploadedFile`]; re-uploading instead leaves a second orphan.
    #[error("the file was uploaded but confirming it failed; upload {} is orphaned and expires in {}h", file.id, UploadedFile::TTL_HOURS)]
    Confirm {
        /// The stored-but-unposted upload.
        file: UploadedFile,
        /// Why confirmation failed.
        #[source]
        source: RestError,
    },
}

impl UploadError {
    /// The orphaned upload, when there is one.
    #[must_use]
    pub fn orphaned_file(&self) -> Option<&UploadedFile> {
        match self {
            Self::Confirm { file, .. } => Some(file),
            Self::Upload(_) => None,
        }
    }

    /// The underlying REST failure, whichever step produced it.
    #[must_use]
    pub fn source_error(&self) -> &RestError {
        match self {
            Self::Upload(source) | Self::Confirm { source, .. } => source,
        }
    }
}
