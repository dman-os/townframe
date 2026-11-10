//! `dtxt:blob:<multihash>` (ADR 035 §3): content identity only. There is no query, fragment or
//! locator hint: a blob is reached through its `Blob` datum, not through its URL.

use crate::BlobId;
use crate::BlobIdDecodeError;
use std::fmt;
use std::str::FromStr;

const PREFIX: &str = "dtxt:blob:";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobUrl(BlobId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BlobUrlError {
    #[error("not a `dtxt:blob:` URL")]
    NotABlobUrl,
    #[error("a blob URL carries no query, fragment or path after its digest")]
    Hint,
    #[error(transparent)]
    Digest(#[from] BlobIdDecodeError),
}

impl BlobUrl {
    pub const fn new(id: BlobId) -> Self {
        Self(id)
    }

    pub const fn id(&self) -> BlobId {
        self.0
    }
}

/// The canonical spelling: the BLAKE3 multihash in multibase base58btc, as
/// [`BlobId::digest_str`] writes it. Parsing also accepts the bare 32-byte digest.
impl fmt::Display for BlobUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{PREFIX}{}", self.0.digest_str())
    }
}

impl FromStr for BlobUrl {
    type Err = BlobUrlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let scheme = value.get(..PREFIX.len()).ok_or(BlobUrlError::NotABlobUrl)?;
        if !scheme.eq_ignore_ascii_case(PREFIX) {
            return Err(BlobUrlError::NotABlobUrl);
        }
        let digest = &value[PREFIX.len()..];
        if digest.contains(['?', '#', '/']) {
            return Err(BlobUrlError::Hint);
        }
        Ok(Self(digest.parse()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_in_the_multihash_spelling() {
        let id = BlobId::new([4; 32]);
        let url = BlobUrl::new(id);
        let text = url.to_string();
        assert_eq!(text, format!("dtxt:blob:{}", id.digest_str()));
        assert_eq!(text.parse::<BlobUrl>().unwrap(), url);
        // The bare digest spelling is accepted and canonicalized.
        let bare = format!("dtxt:blob:{id}");
        assert_eq!(bare.parse::<BlobUrl>().unwrap().to_string(), text);
        assert_eq!(url.id(), id);
    }

    #[test]
    fn refuses_hints_other_kinds_and_dropped_schemes() {
        let digest = BlobId::new([4; 32]).digest_str();
        for value in [
            "".to_owned(),
            "dtxt:blob:".to_owned(),
            "dtxt:blob:zHash".to_owned(),
            "dtxt:blob:///zHash".to_owned(),
            format!("dtxt:blob:{digest}?size=4"),
            format!("dtxt:blob:{digest}?via=x"),
            format!("dtxt:blob:{digest}#frag"),
            format!("dtxt:blob:{digest}/path"),
            format!("dtxt:datum:{digest}"),
            format!("db+blob:///{digest}"),
            format!("future+blob:{digest}"),
        ] {
            assert!(value.parse::<BlobUrl>().is_err(), "{value}");
        }
    }
}
