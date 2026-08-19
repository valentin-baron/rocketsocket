//! Role lookups.
//!
//! Only the two endpoints a bot can actually reach. The obvious ones cannot be used:
//! `users.info` puts `roles` in `getFullUserData`'s `fullFields`, applied only when the
//! caller is the user or holds `view-full-other-user-info` — which defaults to the `admin`
//! role alone — so a correctly provisioned bot account gets a user document with no
//! `roles` key at all. `roles.getUsersInRole` requires `access-permissions`.

use serde::{Deserialize, Serialize};

use rocketsocket_model::id::{RoleId, RoomId, UserId};

/// One holder of a globally-scoped public role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicRoleHolder {
    /// The user.
    #[serde(rename = "_id")]
    pub id: UserId,
    /// Their username, when the projection carries it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The public roles they hold.
    #[serde(default)]
    pub roles: Vec<RoleId>,
}

/// Response of `roles.getUsersInPublicRoles`.
///
/// `success` is checked explicitly rather than relying on the status code. Both fields
/// default, so a body that merely *looks* right — an empty object from a proxy, a
/// `success: false` carried on a 200 — would otherwise decode as "this workspace has no
/// admins", which is a security answer, not a missing one.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PublicRolesEnvelope {
    #[serde(default)]
    pub(crate) success: bool,
    #[serde(default)]
    pub(crate) users: Vec<PublicRoleHolder>,
}

/// One user's roles **within a room**.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomRoleHolder {
    /// The room.
    pub rid: RoomId,
    /// The user holding the roles.
    pub u: RoomRoleUser,
    /// Subscription-scoped roles, conventionally `owner`, `moderator` or `leader`.
    #[serde(default)]
    pub roles: Vec<RoleId>,
}

/// The user stub carried by [`RoomRoleHolder`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomRoleUser {
    /// The user id.
    #[serde(rename = "_id")]
    pub id: UserId,
    /// Their username, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Their display name, filled in only when `UI_Use_Real_Name` is on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Response of `rooms.roles`.
///
/// `success` is checked explicitly, for the same reason as [`PublicRolesEnvelope`].
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RoomRolesEnvelope {
    #[serde(default)]
    pub(crate) success: bool,
    #[serde(default)]
    pub(crate) roles: Vec<RoomRoleHolder>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_without_an_explicit_success_is_not_an_empty_answer() {
        // `{"users": []}` from a proxy and a real "no admins" answer are the same JSON
        // apart from `success`. For a role lookup, reading the first as the second grants
        // nobody admin — which is safe — but reading a *room* body that way would be a
        // silent "this room has no owners". Both must require the flag.
        let envelope: PublicRolesEnvelope =
            serde_json::from_str(r#"{"users":[]}"#).expect("valid json");
        assert!(!envelope.success, "an absent success flag must not default to true");

        let envelope: RoomRolesEnvelope =
            serde_json::from_str(r#"{"success":false,"error":"unauthorized"}"#).expect("json");
        assert!(!envelope.success);
    }

    #[test]
    fn a_room_roles_body_decodes_the_documented_shape() {
        let envelope: RoomRolesEnvelope = serde_json::from_str(
            r#"{"success":true,"roles":[
                {"rid":"GENERAL","u":{"_id":"u1","username":"alice"},"roles":["owner"]},
                {"rid":"GENERAL","u":{"_id":"u2","username":"bob"},"roles":["archivist"]}
            ]}"#,
        )
        .expect("the documented shape must decode");

        assert!(envelope.success);
        assert_eq!(envelope.roles.len(), 2);
        assert_eq!(envelope.roles[0].u.id.as_str(), "u1");
        assert_eq!(envelope.roles[0].roles[0].as_str(), "owner");
        // A workspace-defined subscription role with a description appears here too, which
        // is why callers must match the roles they mean rather than treating any entry as
        // authority.
        assert_eq!(envelope.roles[1].roles[0].as_str(), "archivist");
    }

    #[test]
    fn a_public_roles_body_tolerates_a_missing_username() {
        let envelope: PublicRolesEnvelope =
            serde_json::from_str(r#"{"success":true,"users":[{"_id":"u1","roles":["admin"]}]}"#)
                .expect("must decode");
        assert_eq!(envelope.users[0].username, None);
        assert_eq!(envelope.users[0].roles[0].as_str(), "admin");
    }
}
