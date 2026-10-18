use std::{fmt, fmt::Display, marker::PhantomData};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Error as _, MapAccess, SeqAccess, Visitor},
    ser::Error as _,
};
use serde_with::{DeserializeAs, SerializeAs};
use time::Duration;

// TODO: This type can be fancier once const for generic types is implemented
// https://doc.rust-lang.org/beta/unstable-book/language-features/adt-const-params.html
// This means that we could add different bound from a const configuration
// checked on compile time.
/// This is a proxy type to be used to bound `Duration` deserialization checks.
///
/// It embeds the checks at type level. It implements the pertinent types from
/// the `serde_with` crate so it can be used within `serde_as` macros.
///
/// In human-readable formats (YAML, JSON, ...) durations are serialized as
/// [`humantime`] strings (`250ms`, `5s`, `1h 30m`). Deserialization accepts:
/// - a [`humantime`] string: `"250ms"`, `"5s"`, `"24h"`;
/// - a bare integer, read in the unit of `TAG` (e.g. `5000` is `5s` for
///   [`MILLI`] and `5000s` for [`SECOND`]);
/// - the legacy `"<secs>.<nanos>"` string (`"5.000000000"`);
/// - the legacy `{ secs, nanos }` map or `[secs, nanos]` sequence.
///
/// Non human-readable formats keep the [`time::Duration`] encoding.
///
/// # Examples
/// ```rust
///     use serde_with::serde_as;
///     use time::Duration;
///     use logos_blockchain_utils::bounded_duration::{SECOND, MinimalBoundedDuration};
///
///     #[serde_as]
///     #[derive(serde::Serialize, serde::Deserialize)]
///     struct Foo {
///         #[serde_as(as = "MinimalBoundedDuration<1, SECOND>")]
///         duration: Duration,
///     }
/// ```
#[expect(
    private_bounds,
    reason = "The `TimeTag` trait is not public, is just to tie up types for `MinimalBoundedDuration` and its implemented only by the supported types."
)]
#[derive(Copy, Clone, Debug)]
pub struct MinimalBoundedDuration<const MIN_DURATION: usize, TAG: TimeTag> {
    duration: Duration,
    _tag: PhantomData<*const TAG>,
}

pub(crate) trait TimeTag {
    fn inner() -> char;
}

pub struct BoundTag<const TAG: char>;
impl<const TAG: char> TimeTag for BoundTag<TAG> {
    fn inner() -> char {
        TAG
    }
}

/// *Nanoseconds* tag type for `MinimalBoundedDuration`
pub type NANO = BoundTag<'n'>;
/// *Milliseconds* tag type for `MinimalBoundedDuration`
pub type MILLI = BoundTag<'l'>;
/// *Seconds* tag type for `MinimalBoundedDuration`
pub type SECOND = BoundTag<'s'>;
/// *Minute* tag type for `MinimalBoundedDuration`
pub type MINUTE = BoundTag<'m'>;
/// *Hour* tag type for `MinimalBoundedDuration`
pub type HOUR = BoundTag<'h'>;
/// *Day* tag type for `MinimalBoundedDuration`
pub type DAY = BoundTag<'d'>;

// we have a limitation to const types, so we better use the ones defined.
fn fill_duration_measure<T: TimeTag>() -> Result<String, impl Display> {
    match T::inner() {
        v @ ('d' | 'h' | 'm' | 's') => Ok(v.to_string()),
        'l' => Ok("ms".to_owned()),
        'n' => Ok("ns".to_owned()),
        other => Err(format!("'{other}' measure not supported")),
    }
}

/// Builds a [`Duration`] of `amount` units of the `TAG` measure.
fn duration_in_tag_unit<T: TimeTag>(amount: i64) -> Result<Duration, String> {
    let unit = match T::inner() {
        'd' => Duration::DAY,
        'h' => Duration::HOUR,
        'm' => Duration::MINUTE,
        's' => Duration::SECOND,
        'l' => Duration::MILLISECOND,
        'n' => Duration::NANOSECOND,
        other => return Err(format!("'{other}' measure not supported")),
    };
    unit.checked_mul(
        amount
            .try_into()
            .map_err(|_| format!("Duration of {amount} units overflowed"))?,
    )
    .ok_or_else(|| format!("Duration of {amount} units overflowed"))
}

/// Parses a legacy `"<secs>.<nanos>"` duration string, as produced by the
/// human-readable `time::Duration` serde implementation.
fn parse_legacy_seconds(value: &str) -> Option<Duration> {
    let (secs, nanos) = value.split_once('.')?;
    let negative = secs.starts_with('-');
    let secs: i64 = secs.parse().ok()?;
    if nanos.is_empty() || nanos.len() > 9 || !nanos.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let nanos: i32 = format!("{nanos:0<9}").parse().ok()?;
    Some(Duration::new(secs, if negative { -nanos } else { nanos }))
}

fn std_to_time_duration(value: core::time::Duration) -> Result<Duration, String> {
    Ok(Duration::new(
        value.as_secs().try_into().map_err(|e| format!("{e}"))?,
        value
            .subsec_nanos()
            .try_into()
            .map_err(|e| format!("{e}"))?,
    ))
}

struct HumanReadableVisitor<TAG>(PhantomData<*const TAG>);

impl<'de, TAG: TimeTag> Visitor<'de> for HumanReadableVisitor<TAG> {
    type Value = Duration;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(
            "a duration such as \"250ms\", \"5s\" or \"24h\", or an integer in the field's unit",
        )
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Duration, E> {
        let value = i64::try_from(value).map_err(E::custom)?;
        duration_in_tag_unit::<TAG>(value).map_err(E::custom)
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Duration, E> {
        duration_in_tag_unit::<TAG>(value).map_err(E::custom)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Duration, E> {
        let trimmed = value.trim();
        if let Some(duration) = parse_legacy_seconds(trimmed) {
            return Ok(duration);
        }
        if let Ok(amount) = trimmed.parse::<i64>() {
            return duration_in_tag_unit::<TAG>(amount).map_err(E::custom);
        }
        let duration = humantime::parse_duration(trimmed)
            .map_err(|e| E::custom(format!("invalid duration {value:?}: {e}")))?;
        std_to_time_duration(duration).map_err(E::custom)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Duration, A::Error> {
        let secs: i64 = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let nanos: i32 = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;
        Ok(Duration::new(secs, nanos))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Duration, A::Error> {
        let mut secs: Option<i64> = None;
        let mut nanos: Option<i32> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "secs" | "seconds" => secs = Some(map.next_value()?),
                "nanos" | "nanoseconds" => nanos = Some(map.next_value()?),
                other => return Err(A::Error::unknown_field(other, &["secs", "nanos"])),
            }
        }
        Ok(Duration::new(
            secs.ok_or_else(|| A::Error::missing_field("secs"))?,
            nanos.unwrap_or_default(),
        ))
    }
}

impl<const MIN_DURATION: usize, TAG: TimeTag> Serialize
    for MinimalBoundedDuration<MIN_DURATION, TAG>
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if !serializer.is_human_readable() {
            return self.duration.serialize(serializer);
        }
        if self.duration.is_negative() {
            return Err(S::Error::custom(
                "Negative durations cannot be serialized in human-readable formats",
            ));
        }
        let std_duration = core::time::Duration::new(
            self.duration
                .whole_seconds()
                .try_into()
                .map_err(S::Error::custom)?,
            self.duration
                .subsec_nanoseconds()
                .try_into()
                .map_err(S::Error::custom)?,
        );
        serializer.collect_str(&humantime::format_duration(std_duration))
    }
}

impl<const MIN_DURATION: usize, TAG: TimeTag> SerializeAs<Duration>
    for MinimalBoundedDuration<MIN_DURATION, TAG>
{
    fn serialize_as<S>(source: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let to_serialize: Self = Self {
            duration: *source,
            _tag: PhantomData,
        };
        to_serialize.serialize(serializer)
    }
}

impl<'de, const MIN_DURATION: usize, TAG: TimeTag> DeserializeAs<'de, Duration>
    for MinimalBoundedDuration<MIN_DURATION, TAG>
{
    fn deserialize_as<D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let duration: Self = Self::deserialize(deserializer)?;
        Ok(duration.duration)
    }
}

impl<const MIN_DURATION: usize, TAG: TimeTag> SerializeAs<core::time::Duration>
    for MinimalBoundedDuration<MIN_DURATION, TAG>
{
    fn serialize_as<S>(source: &core::time::Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let to_serialize: Self = Self {
            duration: Duration::new(
                source.as_secs().try_into().map_err(S::Error::custom)?,
                source.subsec_nanos().try_into().map_err(S::Error::custom)?,
            ),
            _tag: PhantomData,
        };
        to_serialize.serialize(serializer)
    }
}
impl<'de, const MIN_DURATION: usize, TAG: TimeTag> DeserializeAs<'de, core::time::Duration>
    for MinimalBoundedDuration<MIN_DURATION, TAG>
{
    fn deserialize_as<D>(deserializer: D) -> Result<core::time::Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let duration: Self = Self::deserialize(deserializer)?;
        if duration.duration.is_negative() {
            return Err(D::Error::custom(
                "Negative duration is not supported for std::time::Duration",
            ));
        }
        duration.try_into().map_err(D::Error::custom)
    }
}

impl<'de, const MIN_DURATION: usize, TAG: TimeTag> Deserialize<'de>
    for MinimalBoundedDuration<MIN_DURATION, TAG>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: Duration = if deserializer.is_human_readable() {
            deserializer.deserialize_any(HumanReadableVisitor::<TAG>(PhantomData))?
        } else {
            Duration::deserialize(deserializer)?
        };

        let reformat_measure = fill_duration_measure::<TAG>().map_err(D::Error::custom)?;
        let parsed_duration =
            humantime::parse_duration(format!("{MIN_DURATION}{reformat_measure}").as_str())
                .map_err(D::Error::custom)?;
        let min_duration = Duration::new(
            parsed_duration
                .as_secs()
                .try_into()
                .map_err(D::Error::custom)?,
            parsed_duration
                .subsec_nanos()
                .try_into()
                .map_err(D::Error::custom)?,
        );
        if value < min_duration {
            return Err(D::Error::custom(format!(
                "Minimal duration is {min_duration} but got {value}"
            )));
        }
        Ok(Self {
            duration: value,
            _tag: PhantomData,
        })
    }
}

impl<const MIN_DURATION: usize, TAG: TimeTag> From<MinimalBoundedDuration<MIN_DURATION, TAG>>
    for Duration
{
    fn from(value: MinimalBoundedDuration<MIN_DURATION, TAG>) -> Self {
        value.duration
    }
}

impl<const MIN_DURATION: usize, TAG: TimeTag> TryFrom<MinimalBoundedDuration<MIN_DURATION, TAG>>
    for core::time::Duration
{
    type Error = std::io::Error;

    fn try_from(value: MinimalBoundedDuration<MIN_DURATION, TAG>) -> Result<Self, Self::Error> {
        let secs = value
            .duration
            .whole_seconds()
            .try_into()
            .map_err(Self::Error::other)?;
        let nanos = value
            .duration
            .subsec_nanoseconds()
            .try_into()
            .map_err(Self::Error::other)?;
        Ok(Self::new(secs, nanos))
    }
}

#[cfg(test)]
mod test {
    use serde_json::json;
    use serde_with::serde_as;
    use time::Duration;

    use crate::bounded_duration::{DAY, MinimalBoundedDuration, SECOND};

    #[test]
    fn success_deserialize() {
        let json_value = json!([1, 0]);
        let value = serde_json::to_value(json_value).unwrap();
        let _duration: MinimalBoundedDuration<1, SECOND> = serde_json::from_value(value).unwrap();
    }

    #[test]
    #[should_panic(expected = "Minimal duration is 1d but got 1s")]
    fn fail_deserialize_with_type_bound() {
        let value = serde_json::to_value(Duration::seconds(1)).unwrap();

        let _duration: MinimalBoundedDuration<1, DAY> = serde_json::from_value(value).unwrap();
    }

    #[serde_as]
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Foo {
        #[serde_as(as = "MinimalBoundedDuration<1, SECOND>")]
        duration: Duration,
    }

    #[test]
    fn deserialize_proxy_type_success() {
        let foo = Foo {
            duration: Duration::seconds(10),
        };
        let _foo: Foo = serde_json::from_value(serde_json::to_value(&foo).unwrap()).unwrap();
    }

    #[test]
    fn deserialize_proxy_type_success_for_std() {
        #[serde_as]
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Foo {
            #[serde_as(as = "MinimalBoundedDuration<1, SECOND>")]
            duration: std::time::Duration,
        }
        let foo = Foo {
            duration: std::time::Duration::from_secs(10),
        };
        let value = serde_json::to_value(&foo).unwrap();
        let _foo: Foo = serde_json::from_value(value).unwrap();
    }

    #[test]
    #[should_panic(expected = "Minimal duration is 1s but got 10ms")]
    fn deserialize_proxy_type_fails() {
        let foo = Foo {
            duration: Duration::milliseconds(10),
        };
        let _foo: Foo = serde_json::from_value(serde_json::to_value(&foo).unwrap()).unwrap();
    }
}
