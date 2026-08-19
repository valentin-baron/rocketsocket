//! EJSON timestamps.
//!
//! Rocket.Chat serializes DDP frames with Meteor's `stringifyDDP`, which EJSON-encodes
//! every `Date` as `{"$date": <epoch millis>}`. The same logical field arriving over REST
//! is usually an ISO-8601 string instead, and a handful of legacy documents store a bare
//! number. [`Timestamp`] accepts all three and always writes the `$date` form.
//!
//! Note that EJSON adjustment is applied by the monolith only to the top-level `fields`,
//! `params` and `result` members of a frame, while the EE `ddp-streamer` applies it to the
//! whole message. Decoding `$date` wherever it appears is correct for both.

use core::fmt;
use std::borrow::Cow;

use serde::de::{self, MapAccess, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const NANOS_PER_MILLI: i128 = 1_000_000;

/// A point in time, wire-encoded as EJSON `{"$date": <millis>}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(OffsetDateTime);

impl Timestamp {
    /// The current time.
    #[must_use]
    pub fn now() -> Self {
        Self(OffsetDateTime::now_utc())
    }

    /// Builds a timestamp from milliseconds since the Unix epoch.
    ///
    /// Returns `None` if the value is outside the representable range.
    #[must_use]
    pub fn from_unix_millis(millis: i64) -> Option<Self> {
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * NANOS_PER_MILLI)
            .ok()
            .map(Self)
    }

    /// Milliseconds since the Unix epoch, rounded towards negative infinity.
    #[must_use]
    pub fn unix_millis(self) -> i64 {
        // Rocket.Chat only ever emits whole milliseconds, but flooring keeps the
        // round-trip monotonic for any value that somehow carries sub-millisecond
        // precision (e.g. an RFC 3339 string from REST).
        (self.0.unix_timestamp_nanos().div_euclid(NANOS_PER_MILLI)) as i64
    }

    /// The underlying [`OffsetDateTime`].
    #[must_use]
    pub fn into_inner(self) -> OffsetDateTime {
        self.0
    }
}

impl From<OffsetDateTime> for Timestamp {
    fn from(value: OffsetDateTime) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for OffsetDateTime {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.format(&Rfc3339) {
            Ok(s) => f.write_str(&s),
            Err(_) => write!(f, "{}", self.unix_millis()),
        }
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("$date", &self.unix_millis())?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match deserializer.deserialize_any(TimestampVisitor)? {
            Some(ts) => Ok(ts),
            None => Err(de::Error::custom("expected an EJSON date, found null")),
        }
    }
}

/// Visitor accepting every shape Rocket.Chat uses for a date, yielding `None` for the
/// explicitly-null forms (`null` and `{"$date": null}`).
struct TimestampVisitor;

impl<'de> Visitor<'de> for TimestampVisitor {
    type Value = Option<Timestamp>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an EJSON date `{\"$date\": millis}`, an RFC 3339 string, or epoch millis")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_any(self)
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Timestamp::from_unix_millis(v)
            .ok_or_else(|| E::custom(format!("epoch millis {v} is out of range")))
            .map(Some)
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        let v =
            i64::try_from(v).map_err(|_| E::custom(format!("epoch millis {v} is out of range")))?;
        self.visit_i64(v)
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
        if !v.is_finite() {
            return Err(E::custom("epoch millis must be finite"));
        }
        self.visit_i64(v.trunc() as i64)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        OffsetDateTime::parse(v, &Rfc3339)
            .map(|dt| Some(Timestamp(dt)))
            .map_err(|_| E::invalid_value(Unexpected::Str(v), &"an RFC 3339 timestamp"))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut found: Option<Option<Timestamp>> = None;

        // `Cow`, not `&str`: a borrowed key can only be produced by a borrowing
        // deserializer, so `next_key::<&str>()` makes every timestamped entity fail under
        // `serde_json::from_value` — which is how anyone building a payload with `json!`
        // or re-deserializing a `Value` will reach these types.
        while let Some(key) = map.next_key::<Cow<'_, str>>()? {
            if key == "$date" {
                // The value is itself permissive: RC has shipped `$date` as millis, and
                // PATs carry no expiry at all, which surfaces as an explicit null.
                found = Some(map.next_value_seed(AnyTimestamp)?);
            } else {
                // Ignore siblings rather than failing; `$escape` and future EJSON
                // wrappers must not break an otherwise-valid frame.
                map.next_value::<de::IgnoredAny>()?;
            }
        }

        found.ok_or_else(|| de::Error::missing_field("$date"))
    }
}

/// Seed reusing [`TimestampVisitor`] for the *value* of a `$date` key.
struct AnyTimestamp;

impl<'de> de::DeserializeSeed<'de> for AnyTimestamp {
    type Value = Option<Timestamp>;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_any(TimestampVisitor)
    }
}

/// `#[serde(with = "...")]` adapter for timestamps that may be absent or explicitly null.
///
/// Use this — not a bare `Option<Timestamp>` — for any field the server can send as
/// `{"$date": null}`, which a plain `Option` would reject because the *outer* value is a
/// map rather than JSON null. Personal access tokens are the common case: they have no
/// expiry, so `tokenExpires` arrives null.
pub mod option {
    use super::{Timestamp, TimestampVisitor};
    use serde::{Deserializer, Serializer};

    /// Serializes `Some` as `{"$date": millis}` and `None` as `null`.
    ///
    /// # Errors
    /// Propagates any error from the underlying serializer.
    pub fn serialize<S: Serializer>(
        value: &Option<Timestamp>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(ts) => serializer.serialize_some(ts),
            None => serializer.serialize_none(),
        }
    }

    /// Accepts `null`, `{"$date": null}`, `{"$date": millis}`, an RFC 3339 string, or
    /// bare epoch millis.
    ///
    /// # Errors
    /// Returns an error if the value is present but is none of the accepted forms.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Timestamp>, D::Error> {
        deserializer.deserialize_option(TimestampVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Timestamp {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn accepts_the_ejson_date_form() {
        assert_eq!(parse(r#"{"$date":1688421337724}"#).unix_millis(), 1_688_421_337_724);
    }

    #[test]
    fn accepts_an_rfc3339_string_from_rest() {
        // REST responses and DDP `error.details` use ISO strings, not `$date`.
        assert_eq!(parse(r#""2019-12-31T22:05:22.159Z""#).unix_millis(), 1_577_829_922_159);
    }

    #[test]
    fn accepts_bare_millis_from_legacy_documents() {
        assert_eq!(parse("1688421337724").unix_millis(), 1_688_421_337_724);
    }

    #[test]
    fn ignores_unknown_siblings_of_dollar_date() {
        assert_eq!(parse(r#"{"$date":42,"$whatever":true}"#).unix_millis(), 42);
    }

    #[test]
    fn serializes_to_the_ejson_form() {
        let ts = Timestamp::from_unix_millis(1_688_421_337_724).unwrap();
        assert_eq!(serde_json::to_string(&ts).unwrap(), r#"{"$date":1688421337724}"#);
    }

    #[test]
    fn round_trips() {
        let ts = Timestamp::from_unix_millis(1_688_421_337_724).unwrap();
        let json = serde_json::to_string(&ts).unwrap();
        assert_eq!(parse(&json), ts);
    }

    #[test]
    fn pre_epoch_millis_floor_consistently() {
        let ts = Timestamp::from_unix_millis(-1).unwrap();
        assert_eq!(ts.unix_millis(), -1);
    }

    #[test]
    fn rejects_a_null_date_when_the_field_is_required() {
        assert!(serde_json::from_str::<Timestamp>(r#"{"$date":null}"#).is_err());
        assert!(serde_json::from_str::<Timestamp>("null").is_err());
    }

    #[test]
    fn missing_dollar_date_key_is_an_error() {
        assert!(serde_json::from_str::<Timestamp>(r#"{"other":1}"#).is_err());
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct LoginResult {
        #[serde(default, with = "super::option")]
        token_expires: Option<Timestamp>,
    }

    #[test]
    fn option_adapter_accepts_the_personal_access_token_shape() {
        // A PAT has no `when`, so the server reports no expiry.
        let v: LoginResult = serde_json::from_str(r#"{"token_expires":{"$date":null}}"#).unwrap();
        assert_eq!(v.token_expires, None);

        let v: LoginResult = serde_json::from_str(r#"{"token_expires":null}"#).unwrap();
        assert_eq!(v.token_expires, None);

        let v: LoginResult = serde_json::from_str("{}").unwrap();
        assert_eq!(v.token_expires, None);

        let v: LoginResult = serde_json::from_str(r#"{"token_expires":{"$date":5}}"#).unwrap();
        assert_eq!(v.token_expires.unwrap().unix_millis(), 5);
    }

    #[test]
    fn option_adapter_round_trips() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct W(#[serde(with = "super::option")] Option<Timestamp>);

        for value in [None, Timestamp::from_unix_millis(7)] {
            let w = W(value);
            let json = serde_json::to_string(&w).unwrap();
            assert_eq!(serde_json::from_str::<W>(&json).unwrap(), w);
        }
    }

    #[test]
    fn decodes_from_a_value_as_well_as_from_a_string() {
        // `from_value` hands out owned keys. Requiring a borrowed one made every entity
        // carrying a timestamp undeserializable from a `serde_json::Value`, which is the
        // natural path for anything built with `json!` or re-read from a cache.
        let value = serde_json::json!({ "$date": 1_755_529_012_345_i64 });
        let from_value: Timestamp = serde_json::from_value(value.clone()).expect("from_value");
        let from_str: Timestamp = serde_json::from_str(&value.to_string()).expect("from_str");
        assert_eq!(from_value, from_str);
        assert_eq!(from_value.unix_millis(), 1_755_529_012_345);
    }

    #[test]
    fn the_option_adapter_also_works_from_a_value() {
        #[derive(Deserialize)]
        struct W(#[serde(with = "super::option")] Option<Timestamp>);

        let w: W = serde_json::from_value(serde_json::json!({ "$date": null })).expect("null");
        assert_eq!(w.0, None);

        let w: W = serde_json::from_value(serde_json::json!({ "$date": 5 })).expect("value");
        assert_eq!(w.0.unwrap().unix_millis(), 5);
    }
}
