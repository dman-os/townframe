use crate::interlude::*;

use crate::doc::{
    BlobPin, ChangeHashSet, CipherBlob, FacetRaw, Representation, WellKnownFacet, WellKnownFacetTag,
};

#[test]
fn test_blob_pin_facet_schema() -> Res<()> {
    assert_eq!(
        WellKnownFacetTag::BlobPin.as_str(),
        "org.example.daybook.blobPin"
    );

    let facet = WellKnownFacet::BlobPin(BlobPin {
        length_octets: 12345,
    });
    let json = FacetRaw::from(facet);
    assert_eq!(json, serde_json::json!({ "lengthOctets": 12345 }));
    assert_eq!(
        FacetRaw::from(WellKnownFacet::from_json(
            json.clone(),
            WellKnownFacetTag::BlobPin
        )?),
        json,
        "a blobPin facet read from a peer must parse back to what was written"
    );
    Ok(())
}

/// The cipherBlob/JWK pair is the encrypted-representation schema (ADR 003 §3,
/// §5). This pins the contract peers see: the facet tags, the exact JSON shape,
/// and the fact that a cross-document `keyRef` carries the heads that decide
/// which JWK state it means.
#[test]
fn test_cipherblob_facet_schema() -> Res<()> {
    assert_eq!(
        WellKnownFacetTag::CipherBlob.as_str(),
        "org.example.daybook.cipherBlob"
    );
    assert_eq!(WellKnownFacetTag::Jwk.as_str(), "org.example.daybook.jwk");
    // Encryption metadata is the encryption worker's to maintain: an ordinary
    // facet write must not be able to repoint `keyRef` at another document's
    // key, nor claim a representation digest the store cannot serve.
    assert!(WellKnownFacetTag::CipherBlob.is_system_managed());
    assert!(WellKnownFacetTag::Jwk.is_system_managed());

    let head = am_utils_rs::serialize_commit_heads(&[automerge::ChangeHash([1u8; 32])])[0].clone();
    let cipher_blob = CipherBlob {
        representation: Representation {
            digest: "bafkrei-example-ciphertext-digest".to_string(),
            length_octets: 125_337,
        },
        content_encoding: "aes128gcm".to_string(),
        // A key document rather than `/self/`: the JWK is readable only by
        // decryptors, while whoever may *serve* the ciphertext reads this facet
        // (ADR 003 §19).
        key_ref: "db+facet:///abc123/org.example.daybook.jwk/relay".parse()?,
        key_ref_heads: ChangeHashSet(Arc::from([automerge::ChangeHash([1u8; 32])])),
        encoding_parameters: serde_json::json!({
            "recordSize": 65_536,
            "padding": "record",
        }),
    };

    let facet = WellKnownFacet::CipherBlob(cipher_blob);
    let json = FacetRaw::from(facet.clone());
    assert_eq!(
        json,
        serde_json::json!({
            "representation": {
                "digest": "bafkrei-example-ciphertext-digest",
                "lengthOctets": 125_337,
            },
            "contentEncoding": "aes128gcm",
            "keyRef": "db+facet:///abc123/org.example.daybook.jwk/relay",
            "keyRefHeads": [head],
            "encodingParameters": {"recordSize": 65_536, "padding": "record"},
        }),
        "the cipherBlob facet JSON is the wire contract (ADR 003 §3)"
    );
    assert_eq!(
        FacetRaw::from(WellKnownFacet::from_json(
            json.clone(),
            WellKnownFacetTag::CipherBlob
        )?),
        json,
        "a facet read from a peer must parse back to what was written"
    );

    // The JWK stays a plain RFC 7517 JWK rather than a Daybook-specific shape,
    // so a key type Daybook never interprets has to round-trip unchanged.
    let jwk_json = serde_json::json!({
        "kty": "EC",
        "crv": "P-256",
        "x": "unused-by-this-test",
    });
    let jwk = WellKnownFacet::from_json(jwk_json.clone(), WellKnownFacetTag::Jwk)?;
    assert_eq!(FacetRaw::from(jwk.clone()), jwk_json);

    // Facets are also read through untagged deserialization - the drawer does
    // exactly that for the branches facet - where variant order decides the
    // result. So the JWK must be discriminated by `kty`, and must not swallow
    // payloads that are not JWKs.
    assert_eq!(
        serde_json::from_value::<WellKnownFacet>(json.clone())?.tag(),
        WellKnownFacetTag::CipherBlob
    );
    assert_eq!(
        serde_json::from_value::<WellKnownFacet>(FacetRaw::from(jwk.clone()))?.tag(),
        WellKnownFacetTag::Jwk
    );
    assert_ne!(
        serde_json::from_value::<WellKnownFacet>(serde_json::json!({
            "byName": {},
            "byId": {},
        }))?
        .tag(),
        WellKnownFacetTag::Jwk,
        "the JWK facet must not swallow payloads that are not JWKs"
    );

    Ok(())
}
