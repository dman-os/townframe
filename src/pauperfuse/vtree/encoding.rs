use crate::backends::RelPath;

use super::StoreError;

pub const FORMAT: &str = "utf8-components-v1";

// NUL terminators preserve component-wise UTF-8 ordering, with prefixes first.
pub fn encode_path(path: &RelPath) -> Vec<u8> {
    let mut encoded = Vec::new();
    for component in path.components() {
        encoded.extend_from_slice(component.as_bytes());
        encoded.push(0);
    }
    encoded
}

pub fn decode_path(encoded: &[u8]) -> Result<RelPath, StoreError> {
    if encoded.is_empty() {
        return Ok(RelPath::root());
    }
    if encoded.last() != Some(&0) {
        return Err(StoreError::Encoding("unterminated path component".into()));
    }
    let components = encoded[..encoded.len() - 1]
        .split(|byte| *byte == 0)
        .map(|bytes| {
            std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|error| StoreError::Encoding(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    RelPath::try_new(components).map_err(|error| StoreError::Encoding(error.to_string()))
}

pub fn encode_target(target: &str) -> Result<Vec<u8>, StoreError> {
    if target.contains('\0') {
        return Err(StoreError::Encoding("NUL in symlink target".into()));
    }
    Ok(target.as_bytes().to_vec())
}

pub fn decode_target(bytes: Vec<u8>) -> Result<String, StoreError> {
    let target =
        String::from_utf8(bytes).map_err(|error| StoreError::Encoding(error.to_string()))?;
    if target.contains('\0') {
        return Err(StoreError::Encoding("NUL in stored symlink target".into()));
    }
    Ok(target)
}
