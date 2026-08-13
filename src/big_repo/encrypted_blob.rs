use crate::interlude::*;

use atproto_dasl::drisl;
use beekem::encrypted::EncryptedContent;

const ENCRYPTED_BLOB_DISCRIMINATOR: u8 = 0x02;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EncryptedBlobEnvelope {
    #[serde(flatten)]
    encrypted: EncryptedContent<Vec<u8>, Vec<u8>>,
}

pub(crate) fn encode_encrypted_blob(
    encrypted: &EncryptedContent<Vec<u8>, Vec<u8>>,
) -> Res<Vec<u8>> {
    let env = EncryptedBlobEnvelope {
        encrypted: encrypted.clone(),
    };
    let mut encoded = vec![ENCRYPTED_BLOB_DISCRIMINATOR];
    drisl::to_writer(&mut encoded, &env)
        .map_err(|err| ferr!("DRISL encode encrypted blob envelope: {err}"))?;
    Ok(encoded)
}

pub(crate) fn decode_encrypted_blob(raw_bytes: &[u8]) -> Res<EncryptedContent<Vec<u8>, Vec<u8>>> {
    let Some((discriminator, body)) = raw_bytes.split_first() else {
        return Err(ferr!("blob is missing encrypted discriminator"));
    };
    if *discriminator != ENCRYPTED_BLOB_DISCRIMINATOR {
        return Err(ferr!(
            "blob is not encrypted (expected 0x02 discriminator, got 0x{discriminator:02x})"
        ));
    }
    let envelope: EncryptedBlobEnvelope = drisl::from_slice(body)
        .map_err(|decode_err| ferr!("DRISL decode encrypted blob envelope: {decode_err}"))?;
    Ok(envelope.encrypted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use keyhive_crypto::{digest::Digest, signed::Signed, siv::Siv};
    use serde::Serialize;
    use std::marker::PhantomData;

    fn digest<T: Serialize>(bytes: [u8; 32]) -> Digest<T> {
        let raw = Digest::<Vec<u8>>::hash(&bytes.to_vec()).raw;
        Digest {
            raw,
            _phantom: PhantomData,
        }
    }

    fn sample_encrypted_content() -> EncryptedContent<Vec<u8>, Vec<u8>> {
        EncryptedContent::new(
            Siv::from([1u8; 24]),
            b"ciphertext".to_vec(),
            digest::<beekem::pcs_key::PcsKey>([2u8; 32]),
            digest::<Signed<beekem::operation::CgkaOperation>>([3u8; 32]),
            b"content-ref".to_vec(),
            digest::<Vec<Vec<u8>>>([4u8; 32]),
        )
    }

    #[test]
    fn encrypted_blob_codec_roundtrip_preserves_fields() {
        let encrypted = sample_encrypted_content();
        let encoded = encode_encrypted_blob(&encrypted).expect("encode");
        let decoded = decode_encrypted_blob(&encoded).expect("decode");
        assert_eq!(decoded, encrypted);
    }

    #[test]
    fn encrypted_blob_codec_rejects_missing_discriminator() {
        let err = decode_encrypted_blob(&[]).expect_err("missing discriminator should fail");
        assert!(
            err.to_string().contains("missing encrypted discriminator"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn encrypted_blob_codec_rejects_unknown_discriminator() {
        let err =
            decode_encrypted_blob(&[0x03, 0x00]).expect_err("unknown discriminator should fail");
        assert!(
            err.to_string().contains("expected 0x02 discriminator"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn encrypted_blob_codec_rejects_malformed_drisl() {
        let err = decode_encrypted_blob(&[ENCRYPTED_BLOB_DISCRIMINATOR, 0xff, 0x00])
            .expect_err("malformed drisl should fail");
        assert!(
            err.to_string()
                .contains("DRISL decode encrypted blob envelope"),
            "unexpected error: {err}"
        );
    }
}
