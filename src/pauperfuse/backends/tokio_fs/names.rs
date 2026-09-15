use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};

use super::TokioFs;
use crate::backends::{PathError, RelPath};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum NativePathError {
    #[error("invalid native name: {0}")]
    Invalid(String),
    #[error(transparent)]
    Key(#[from] utils_rs::byte_key::DecodeError),
    #[error(transparent)]
    Path(#[from] PathError),
}

impl TokioFs {
    /// Converts relative Unix native components into escaped, literal tree keys.
    pub fn from_native_path(path: &Path) -> Result<RelPath, NativePathError> {
        let bytes = path.as_os_str().as_bytes();
        if bytes.is_empty() {
            return Ok(RelPath::root());
        }
        let keys = bytes
            .split(|byte| *byte == b'/')
            .map(encode_name)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RelPath::try_new(keys)?)
    }

    /// The explicit native boundary: byte-unescape each key and validate its native name.
    pub fn to_native_path(path: &RelPath) -> Result<PathBuf, NativePathError> {
        let mut native = PathBuf::new();
        for key in path.components() {
            let bytes = utils_rs::byte_key::decode(key)?;
            validate_name(&bytes)?;
            native.push(OsString::from_vec(bytes));
        }
        Ok(native)
    }

    /// Symlink targets are opaque here; traversal is not implemented.
    pub fn from_native_target(target: &OsStr) -> Result<String, NativePathError> {
        if target.as_bytes().contains(&0) {
            return Err(NativePathError::Invalid("NUL in symlink target".into()));
        }
        Ok(utils_rs::byte_key::encode(target.as_bytes()))
    }

    pub fn to_native_target(target: &str) -> Result<OsString, NativePathError> {
        let bytes = utils_rs::byte_key::decode(target)?;
        if bytes.contains(&0) {
            return Err(NativePathError::Invalid("NUL in symlink target".into()));
        }
        Ok(OsString::from_vec(bytes))
    }
}

fn validate_name(bytes: &[u8]) -> Result<(), NativePathError> {
    if bytes.is_empty()
        || bytes == b"."
        || bytes == b".."
        || bytes.contains(&0)
        || bytes.contains(&b'/')
    {
        return Err(NativePathError::Invalid(
            "empty, dot, NUL, or separator component".into(),
        ));
    }
    Ok(())
}

fn encode_name(bytes: &[u8]) -> Result<String, NativePathError> {
    validate_name(bytes)?;
    Ok(utils_rs::byte_key::encode(bytes))
}

// One-time legacy storage conversion only. Generic tree readers never decode native bytes.
#[cfg(feature = "sqlite")]
pub(crate) fn legacy_path(encoded: &[u8]) -> Result<RelPath, NativePathError> {
    if encoded.is_empty() {
        return Ok(RelPath::root());
    }
    if encoded.last() != Some(&0) {
        return Err(NativePathError::Invalid(
            "unterminated legacy component".into(),
        ));
    }
    let keys = encoded[..encoded.len() - 1]
        .split(|byte| *byte == 0)
        .map(encode_name)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RelPath::try_new(keys)?)
}

#[cfg(feature = "sqlite")]
pub(crate) fn legacy_target(bytes: &[u8]) -> Result<String, NativePathError> {
    TokioFs::from_native_target(OsStr::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_keys_preserve_bytes_and_escape_collisions() {
        for bytes in [
            b"\xff".as_slice(),
            br"\xff",
            b"%FF",
            "café".as_bytes(),
            b"line\n",
            b"backslash\\",
        ] {
            let native = PathBuf::from(OsString::from_vec(bytes.to_vec()));
            let key = TokioFs::from_native_path(&native).unwrap();
            assert_eq!(TokioFs::to_native_path(&key).unwrap(), native);
        }
        assert_ne!(
            TokioFs::from_native_path(Path::new(OsStr::from_bytes(&[0xff]))).unwrap(),
            TokioFs::from_native_path(Path::new(r"\xff")).unwrap()
        );
        assert_eq!(
            TokioFs::from_native_path(Path::new("")).unwrap(),
            RelPath::root()
        );
        assert_eq!(
            TokioFs::to_native_path(&RelPath::root()).unwrap(),
            PathBuf::new()
        );
    }

    #[test]
    fn native_boundary_refuses_unsafe_and_noncanonical_keys() {
        for key in [r"\x00", r"\x2f", r"\x2e", r"\x2e\x2e", r"\q", r"\xFF", "\\"] {
            let path = RelPath::parse(key).unwrap();
            assert!(TokioFs::to_native_path(&path).is_err(), "accepted {key}");
        }
        for native in [
            "/absolute",
            "../escape",
            "./name",
            "a/../b",
            "a/./b",
            "a//b",
            "a/",
            "a\0b",
        ] {
            assert!(TokioFs::from_native_path(Path::new(native)).is_err());
        }
    }

    #[test]
    fn nested_native_names_and_symlink_targets_roundtrip() {
        let native = Path::new("café")
            .join(OsString::from_vec(vec![0xff]))
            .join(r"literal\xff%");
        let key = TokioFs::from_native_path(&native).unwrap();
        assert_eq!(key.to_string(), "café/\\xff/literal\\\\xff%");
        assert_eq!(TokioFs::to_native_path(&key).unwrap(), native);
        let target = OsString::from_vec(b"../target/\xff\\%".to_vec());
        let key = TokioFs::from_native_target(&target).unwrap();
        assert_eq!(TokioFs::to_native_target(&key).unwrap(), target);
        assert!(TokioFs::from_native_target(OsStr::new("a\0b")).is_err());
        assert!(TokioFs::to_native_target(r"a\x00b").is_err());
    }
}
