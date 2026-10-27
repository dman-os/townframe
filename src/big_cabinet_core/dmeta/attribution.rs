use super::ActorMetadata;
use super::Bytes;
use super::MultibaseKey;
use super::SignatureBytes;
use super::Uuid7;
use ciborium::Value;
use coset::AsCborValue;
use coset::CborSerializable;
use coset::CoseSign1;
use coset::CoseSign1Builder;
use coset::HeaderBuilder;
use coset::iana;
use ed25519_dalek::Signature;
use ed25519_dalek::VerifyingKey;

const PRIVATE_UNREGISTERED_MEDIA_TYPE: &str = "application/vnd.daybook.actor-attribution.v1+cwt";
const PURPOSE: &str = "daybook.actor-attribution";
const ACTOR_UUID: i64 = -65537;
const ACTOR_URL: i64 = -65538;
const ATTRIBUTION_PURPOSE: i64 = -65539;
const MAX_CERTIFICATE_BYTES: usize = 4096;
const MAX_ACTOR_URL_BYTES: usize = 2048;
const MAX_CBOR_DEPTH: usize = 4;
const MAX_CBOR_ITEMS: usize = 64;
const MAX_COLLECTION_ITEMS: u64 = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribution {
    pub issuer: MultibaseKey,
    pub actor_uuid: Uuid7,
    pub pubkey: MultibaseKey,
    pub actor_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttributionError {
    #[error("unsupported attribution certificate: {0}")]
    Unsupported(&'static str),
    #[error("malformed attribution certificate: {0}")]
    Malformed(&'static str),
    #[error("invalid attribution signature")]
    InvalidSignature,
    #[error("attribution issuer does not match the expected issuer")]
    WrongIssuer,
    #[error("attribution does not match the actor binding")]
    WrongBinding,
    #[error("attribution resource limit exceeded")]
    ResourceLimit,
}

impl Attribution {
    pub fn signing_bytes(&self) -> Result<Vec<u8>, AttributionError> {
        Ok(self.unsigned()?.tbs_data(&[]))
    }

    pub fn certificate(&self, signature: SignatureBytes) -> Result<Bytes, AttributionError> {
        let mut envelope = self.unsigned()?;
        envelope.signature = signature.as_bytes().to_vec();
        verify_signature(&envelope, &self.issuer)?;
        let value = envelope
            .to_cbor_value()
            .map_err(|_| AttributionError::Malformed("COSE encoding"))?;
        let bytes = encode(&Value::Tag(61, Box::new(Value::Tag(18, Box::new(value)))))?;
        if bytes.len() > MAX_CERTIFICATE_BYTES {
            return Err(AttributionError::ResourceLimit);
        }
        Ok(bytes.into())
    }

    pub fn verify(
        certificate: &Bytes,
        expected_issuer: &MultibaseKey,
        actor_uuid: Uuid7,
        actor: &ActorMetadata,
    ) -> Result<Self, AttributionError> {
        let Value::Tag(61, cose) = decode(certificate.as_slice())? else {
            return Err(AttributionError::Unsupported("CWT tag"));
        };
        let Value::Tag(18, envelope) = *cose else {
            return Err(AttributionError::Unsupported("COSE_Sign1 tag"));
        };
        let Value::Array(fields) = envelope.as_ref() else {
            return Err(AttributionError::Malformed("COSE array"));
        };
        let [
            Value::Bytes(protected),
            Value::Map(unprotected),
            Value::Bytes(payload),
            Value::Bytes(signature),
        ] = fields.as_slice()
        else {
            return Err(AttributionError::Malformed("COSE fields"));
        };
        if !unprotected.is_empty() {
            return Err(AttributionError::Unsupported("unprotected headers"));
        }
        if signature.len() != 64 {
            return Err(AttributionError::Malformed("signature length"));
        }

        let protected = decode(protected)?;
        let headers = closed_map(&protected, &[1, 2, 3, 16])?;
        match field(headers, 1)? {
            Value::Integer(algorithm) if i128::from(*algorithm) == -19 => {}
            Value::Integer(_) | Value::Text(_) => {
                return Err(AttributionError::Unsupported("algorithm"));
            }
            _ => return Err(AttributionError::Malformed("algorithm")),
        }
        if text(field(headers, 3)?)? != "application/cbor" {
            return Err(AttributionError::Unsupported("payload content type"));
        }
        if text(field(headers, 16)?)? != PRIVATE_UNREGISTERED_MEDIA_TYPE {
            return Err(AttributionError::Unsupported("private Daybook profile"));
        }
        let Value::Array(critical) = field(headers, 2)? else {
            return Err(AttributionError::Malformed("critical headers"));
        };
        if critical.is_empty()
            || critical
                .iter()
                .enumerate()
                .any(|(index, label)| critical[..index].contains(label))
        {
            return Err(AttributionError::Malformed("critical headers"));
        }
        if critical.as_slice() != [Value::Integer(16.into())] {
            return Err(AttributionError::Unsupported("critical headers"));
        }

        let payload = decode(payload)?;
        let claims = closed_map(
            &payload,
            &[1, 2, ACTOR_UUID, ACTOR_URL, ATTRIBUTION_PURPOSE],
        )?;
        if text(field(claims, ATTRIBUTION_PURPOSE)?)? != PURPOSE {
            return Err(AttributionError::Unsupported("purpose"));
        }
        let issuer = parse_key(text(field(claims, 1)?)?)?;
        if &issuer != expected_issuer {
            return Err(AttributionError::WrongIssuer);
        }
        let pubkey = parse_key(text(field(claims, 2)?)?)?;
        let Value::Bytes(uuid_bytes) = field(claims, ACTOR_UUID)? else {
            return Err(AttributionError::Malformed("actor UUID bytes"));
        };
        let claimed_uuid = uuid::Uuid::from_slice(uuid_bytes)
            .map_err(|_| AttributionError::Malformed("actor UUID length"))?
            .try_into()
            .map_err(|_| AttributionError::Malformed("actor UUIDv7"))?;
        let actor_url = text(field(claims, ACTOR_URL)?)?;
        if actor_url.len() > MAX_ACTOR_URL_BYTES {
            return Err(AttributionError::ResourceLimit);
        }
        let attribution = Self {
            issuer,
            actor_uuid: claimed_uuid,
            pubkey,
            actor_url: actor_url.to_owned(),
        };
        let envelope = CoseSign1::from_cbor_value(*envelope)
            .map_err(|_| AttributionError::Malformed("COSE headers"))?;
        verify_signature(&envelope, expected_issuer)?;
        if attribution.actor_uuid != actor_uuid
            || attribution.pubkey != actor.pubkey
            || attribution.actor_url != actor.actor_url
        {
            return Err(AttributionError::WrongBinding);
        }
        Ok(attribution)
    }

    fn unsigned(&self) -> Result<CoseSign1, AttributionError> {
        if self.actor_url.len() > MAX_ACTOR_URL_BYTES {
            return Err(AttributionError::ResourceLimit);
        }
        verifying_key(&self.issuer)?;
        verifying_key(&self.pubkey)?;
        let payload = coset::cwt::ClaimsSetBuilder::new()
            .issuer(self.issuer.to_string())
            .subject(self.pubkey.to_string())
            .private_claim(
                ACTOR_UUID,
                Value::Bytes(self.actor_uuid.as_uuid().as_bytes().to_vec()),
            )
            .private_claim(ACTOR_URL, Value::Text(self.actor_url.clone()))
            .private_claim(ATTRIBUTION_PURPOSE, Value::Text(PURPOSE.into()))
            .build()
            .to_vec()
            .map_err(|_| AttributionError::Malformed("claims encoding"))?;
        let protected = HeaderBuilder::new()
            .algorithm(iana::Algorithm::Ed25519)
            .add_critical(iana::HeaderParameter::ObjectContentType)
            .content_type("application/cbor".into())
            .value(16, Value::Text(PRIVATE_UNREGISTERED_MEDIA_TYPE.into()))
            .build();
        Ok(CoseSign1Builder::new()
            .protected(protected)
            .payload(payload)
            .build())
    }
}

fn verifying_key(key: &MultibaseKey) -> Result<VerifyingKey, AttributionError> {
    let verifying_key = VerifyingKey::from_bytes(key.as_bytes())
        .map_err(|_| AttributionError::Malformed("Ed25519 public key"))?;
    if verifying_key.is_weak() || verifying_key.to_edwards().compress().as_bytes() != key.as_bytes()
    {
        return Err(AttributionError::Malformed(
            "weak or noncanonical Ed25519 key",
        ));
    }
    Ok(verifying_key)
}

fn parse_key(value: &str) -> Result<MultibaseKey, AttributionError> {
    if value.len() > 45 {
        return Err(AttributionError::Malformed("multibase public key length"));
    }
    let key: MultibaseKey = value
        .parse()
        .map_err(|_| AttributionError::Malformed("multibase public key"))?;
    if key.to_string() != value {
        return Err(AttributionError::Malformed(
            "noncanonical multibase public key",
        ));
    }
    verifying_key(&key)?;
    Ok(key)
}

fn verify_signature(envelope: &CoseSign1, issuer: &MultibaseKey) -> Result<(), AttributionError> {
    let key = verifying_key(issuer)?;
    envelope.verify_signature(&[], |signature, message| {
        let signature = Signature::from_slice(signature)
            .map_err(|_| AttributionError::Malformed("signature length"))?;
        key.verify_strict(message, &signature)
            .map_err(|_| AttributionError::InvalidSignature)
    })
}

fn closed_map<'a>(
    value: &'a Value,
    allowed: &[i64],
) -> Result<&'a [(Value, Value)], AttributionError> {
    let Value::Map(entries) = value else {
        return Err(AttributionError::Malformed("expected map"));
    };
    for (index, (label, _)) in entries.iter().enumerate() {
        if entries[..index].iter().any(|(prior, _)| prior == label) {
            return Err(AttributionError::Malformed("duplicate map key"));
        }
    }
    for (label, _) in entries {
        match label {
            Value::Integer(label)
                if allowed
                    .iter()
                    .any(|allowed| i128::from(*allowed) == i128::from(*label)) => {}
            Value::Integer(_) | Value::Text(_) => {
                return Err(AttributionError::Unsupported("extension field"));
            }
            _ => return Err(AttributionError::Malformed("map label")),
        }
    }
    if entries.len() != allowed.len() {
        return Err(AttributionError::Malformed("missing field"));
    }
    Ok(entries)
}

fn field(entries: &[(Value, Value)], label: i64) -> Result<&Value, AttributionError> {
    entries
        .iter()
        .find_map(|(key, value)| (key == &Value::Integer(label.into())).then_some(value))
        .ok_or(AttributionError::Malformed("missing field"))
}

fn text(value: &Value) -> Result<&str, AttributionError> {
    value
        .as_text()
        .ok_or(AttributionError::Malformed("expected text"))
}

fn encode(value: &Value) -> Result<Vec<u8>, AttributionError> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes)
        .map_err(|_| AttributionError::Malformed("CBOR encoding"))?;
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<Value, AttributionError> {
    fn scan(
        input: &mut &[u8],
        depth: usize,
        remaining: &mut usize,
    ) -> Result<(), AttributionError> {
        if depth > MAX_CBOR_DEPTH || *remaining == 0 {
            return Err(AttributionError::ResourceLimit);
        }
        *remaining -= 1;
        let (&head, rest) = input
            .split_first()
            .ok_or(AttributionError::Malformed("truncated CBOR"))?;
        *input = rest;
        let major = head >> 5;
        let additional = head & 31;
        let argument = match additional {
            0..=23 => u64::from(additional),
            24..=27 => {
                let width = 1 << (additional - 24);
                let (bytes, rest) = input
                    .split_at_checked(width)
                    .ok_or(AttributionError::Malformed("truncated CBOR argument"))?;
                let mut argument = [0; 8];
                argument[8 - width..].copy_from_slice(bytes);
                *input = rest;
                u64::from_be_bytes(argument)
            }
            _ => return Err(AttributionError::Malformed("indefinite or reserved CBOR")),
        };
        match major {
            0 | 1 => {}
            2 | 3 => {
                if argument > MAX_CERTIFICATE_BYTES as u64 {
                    return Err(AttributionError::ResourceLimit);
                }
                let (_, rest) = input
                    .split_at_checked(argument as usize)
                    .ok_or(AttributionError::Malformed("truncated CBOR string"))?;
                *input = rest;
            }
            4 | 5 => {
                if argument > MAX_COLLECTION_ITEMS {
                    return Err(AttributionError::ResourceLimit);
                }
                let count = argument as usize * if major == 5 { 2 } else { 1 };
                if count > *remaining {
                    return Err(AttributionError::ResourceLimit);
                }
                for _ in 0..count {
                    scan(input, depth + 1, remaining)?;
                }
            }
            6 => scan(input, depth + 1, remaining)?,
            7 if matches!(additional, 20..=22 | 25..=27) => {}
            _ => return Err(AttributionError::Malformed("unsupported CBOR value")),
        }
        Ok(())
    }

    if bytes.len() > MAX_CERTIFICATE_BYTES {
        return Err(AttributionError::ResourceLimit);
    }
    let mut input = bytes;
    let mut remaining = MAX_CBOR_ITEMS;
    scan(&mut input, 0, &mut remaining)?;
    if !input.is_empty() {
        return Err(AttributionError::Malformed("trailing CBOR"));
    }
    ciborium::from_reader(bytes).map_err(|_| AttributionError::Malformed("invalid CBOR"))
}

#[cfg(test)]
mod tests;
