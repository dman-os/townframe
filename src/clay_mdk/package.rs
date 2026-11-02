//! Self-describing micro packages: one component file whose `dayframe.manifest` custom
//! section carries the micro's [`MicroManifest`] as JSON. Reading a package is purely
//! static — it parses the component's section table (including the nested core module a
//! componentized guest wraps) and never instantiates or executes the payload. Package
//! identity is the SHA-256 of the exact bytes, so a launcher can pin what it installed
//! and a shell can verify what it fetched before an engine ever sees it.
use crate::manifest::MicroManifest;
use sha2::Digest;
use sha2::Sha256;

/// Custom section name carrying the serialized [`MicroManifest`]. A guest crate's build
/// script emits it from its manifest crate's `manifest()` (see `micro_counter/build.rs`),
/// so the declaration that ships is the declaration the composition root composed against.
pub const MANIFEST_SECTION: &str = "dayframe.manifest";

/// A component's bytes together with the manifest they declare and their digest.
#[derive(Clone, Debug, PartialEq)]
pub struct Package {
    manifest: MicroManifest,
    digest: String,
    bytes: Vec<u8>,
}

impl Package {
    /// Read a package from component bytes. Errors are plain messages: an unreadable
    /// package is an installation defect a launcher reports per entry, not a host fault.
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Result<Self, String> {
        let bytes = bytes.into();
        let section = manifest_section(&bytes)?
            .ok_or_else(|| format!("component carries no {MANIFEST_SECTION} custom section"))?;
        let manifest: MicroManifest = serde_json::from_slice(section).map_err(|error| {
            format!("{MANIFEST_SECTION} custom section is not a manifest: {error}")
        })?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        Ok(Self {
            manifest,
            digest,
            bytes,
        })
    }

    pub fn manifest(&self) -> &MicroManifest {
        &self.manifest
    }

    /// Hex SHA-256 of the exact component bytes.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
fn manifest_section_bytes(manifest: &MicroManifest) -> Vec<u8> {
    serde_json::to_vec(manifest).unwrap()
}

fn manifest_section(bytes: &[u8]) -> Result<Option<&[u8]>, String> {
    // `parse_all` descends into nested modules/components itself, so a section the
    // componentizer left inside the wrapped core module is found beside top-level ones.
    for payload in wasmparser::Parser::new(0).parse_all(bytes) {
        let payload = payload.map_err(|error| format!("not a wasm component: {error}"))?;
        if let wasmparser::Payload::CustomSection(reader) = payload
            && reader.name() == MANIFEST_SECTION
        {
            return Ok(Some(reader.data()));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MicroId;

    fn manifest() -> MicroManifest {
        MicroManifest {
            id: MicroId::new("dayframe.test"),
            version: 1,
            requirements: vec![],
            provisions: vec![],
            config: None,
            contributions: vec![],
        }
    }

    /// A minimal core module carrying one custom section, hand-encoded: section id 0,
    /// LEB sizes, name then data.
    fn module_with_section(name: &str, data: &[u8]) -> Vec<u8> {
        fn leb(mut value: usize, out: &mut Vec<u8>) {
            loop {
                let mut byte = (value & 0x7f) as u8;
                value >>= 7;
                if value != 0 {
                    byte |= 0x80;
                }
                out.push(byte);
                if value == 0 {
                    break;
                }
            }
        }
        let mut payload = Vec::new();
        leb(name.len(), &mut payload);
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(data);
        let mut module = b"\0asm\x01\0\0\0".to_vec();
        module.push(0);
        leb(payload.len(), &mut module);
        module.extend_from_slice(&payload);
        module
    }

    #[test]
    fn package_reads_manifest_and_digest_from_a_core_module() {
        let bytes = module_with_section(MANIFEST_SECTION, &manifest_section_bytes(&manifest()));
        let package = Package::from_bytes(bytes.clone()).unwrap();
        assert_eq!(package.manifest(), &manifest());
        assert_eq!(package.bytes(), bytes.as_slice());
        assert_eq!(package.digest(), format!("{:x}", Sha256::digest(&bytes)));
    }

    #[test]
    fn package_finds_the_section_inside_a_nested_component_module() {
        // Wrap the module in a component: header (layer 1) + core module section (id 1).
        let module = module_with_section(MANIFEST_SECTION, &manifest_section_bytes(&manifest()));
        let mut component = b"\0asm\x0d\0\x01\0".to_vec();
        component.push(1);
        let mut size = Vec::new();
        let mut value = module.len();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            size.push(byte);
            if value == 0 {
                break;
            }
        }
        component.extend_from_slice(&size);
        component.extend_from_slice(&module);
        let package = Package::from_bytes(component).unwrap();
        assert_eq!(package.manifest().id.as_str(), "dayframe.test");
    }

    #[test]
    fn package_without_a_manifest_section_is_an_installation_error() {
        let bytes = module_with_section("other", b"{}");
        assert!(
            Package::from_bytes(bytes)
                .unwrap_err()
                .contains(MANIFEST_SECTION)
        );
        let malformed = module_with_section(MANIFEST_SECTION, b"not json");
        assert!(Package::from_bytes(malformed).is_err());
    }

    #[test]
    fn prepared_counter_component_declares_its_manifest_statically() {
        let Ok(path) = std::env::var("DAYFRAME_COUNTER_COMPONENT") else {
            return;
        };
        let bytes = std::fs::read(path).unwrap();
        let package = Package::from_bytes(bytes).unwrap();
        assert_eq!(package.manifest().id.as_str(), "dayframe.counter");
    }
}
