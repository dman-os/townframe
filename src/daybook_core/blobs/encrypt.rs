//! Shared RFC 8188 codec and JWK decoding for blob and register consumers.
//! Document lookup and publication stay with the host; the codec has no repository authority.
use crate::interlude::*;
pub type Res<T> = eyre::Result<T>;
mod codec;
mod keys;
mod params;
pub use codec::{decrypt_bytes, encrypt_bytes, encrypt_with_rs};
pub use keys::{JwkOct, MasterKey};
pub use params::{
    EncodingParams, Padding, CONTENT_ENCODING_AES128GCM, DEFAULT_PADDING, RECORD_SIZE,
};
