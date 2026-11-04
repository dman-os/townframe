//! Micro packages: a component's bytes paired with the [`MicroManifest`] that declares them.
//! Reading a package is purely static; the component is never instantiated or executed.
//! Package identity is the SHA-256 of the exact component bytes, so a launcher can pin what
//! it installed and a shell can verify what it fetched before an engine ever sees it.
//!
//! STAND-IN: until `big_plugs` delivers and manages manifests, a package on disk is two
//! sibling files, `<name>.component.wasm` and `<name>.manifest.json` (the serialized
//! manifest, emitted from the micro's manifest crate), and launchers pair them by hand.
//! Nothing here is the final distribution format; OCI packaging is not Clay's job.
use crate::manifest::MicroManifest;
use sha2::Digest;
use sha2::Sha256;

/// A component's bytes together with the manifest that declares them and their digest.
#[derive(Clone, Debug, PartialEq)]
pub struct Package {
    manifest: MicroManifest,
    digest: String,
    bytes: Vec<u8>,
}

impl Package {
    /// Pair a manifest with its component bytes. The manifest must pass
    /// [`MicroManifest::validate`], so a held `Package` always declares a schema this build
    /// reads and internally consistent references. Errors are plain messages: an incoherent
    /// package is an installation defect a launcher reports per entry, not a host fault.
    /// The digest covers the component bytes only: the manifest is a separate sidecar, so
    /// editing it does not change the identity of the code it describes.
    pub fn new(manifest: MicroManifest, bytes: impl Into<Vec<u8>>) -> Result<Self, String> {
        let bytes = bytes.into();
        manifest
            .validate()
            .map_err(|error| format!("package declares an invalid manifest: {error}"))?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        Ok(Self {
            manifest,
            digest,
            bytes,
        })
    }

    /// [`Package::new`] from the sidecar manifest's JSON bytes.
    pub fn from_json(manifest_json: &[u8], bytes: impl Into<Vec<u8>>) -> Result<Self, String> {
        let manifest = serde_json::from_slice(manifest_json)
            .map_err(|error| format!("manifest JSON is not a manifest: {error}"))?;
        Self::new(manifest, bytes)
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
mod tests {
    use super::*;
    use crate::Entrypoints;
    use crate::MANIFEST_SCHEMA;
    use crate::MicroId;

    fn manifest() -> MicroManifest {
        MicroManifest {
            schema: MANIFEST_SCHEMA,
            id: MicroId::new("dayframe.test"),
            package_version: 1,
            entrypoints: Entrypoints::micro_client(),
            publications: vec![],
            requires: vec![],
            configuration_requests: vec![],
            collection_contracts: vec![],
            collection_requests: vec![],
            contributions: vec![],
            extra: Default::default(),
        }
    }

    /// The sidecar manifest of the lifecycle fixture package.
    fn lifecycle_fixture_manifest() -> MicroManifest {
        serde_json::from_str(include_str!("lifecycle_fixture.manifest.json")).unwrap()
    }

    #[test]
    fn package_pairs_manifest_json_with_component_bytes() {
        let bytes = b"\0asm\x0d\0\x01\0".to_vec();
        let json = serde_json::to_vec(&manifest()).unwrap();
        let package = Package::from_json(&json, bytes.clone()).unwrap();
        assert_eq!(package.manifest(), &manifest());
        assert_eq!(package.bytes(), bytes.as_slice());
        assert_eq!(package.digest(), format!("{:x}", Sha256::digest(&bytes)));
    }

    #[test]
    fn malformed_manifest_json_is_an_installation_error() {
        assert!(
            Package::from_json(b"not json", vec![])
                .unwrap_err()
                .contains("not a manifest")
        );
    }

    #[test]
    fn package_with_an_invalid_manifest_is_an_installation_error() {
        let mut newer = manifest();
        newer.schema = MANIFEST_SCHEMA + 1;
        assert_eq!(
            Package::new(newer, vec![]).unwrap_err(),
            format!(
                "package declares an invalid manifest: manifest declares schema {}, this build \
                 reads schema {MANIFEST_SCHEMA}",
                MANIFEST_SCHEMA + 1
            )
        );
    }

    /// The check script prepares the counter as a package pair in `DAYFRAME_PACKAGE_DIR`.
    #[test]
    fn prepared_counter_package_pairs_its_sidecar_manifest() {
        let Ok(directory) = std::env::var("DAYFRAME_PACKAGE_DIR") else {
            return;
        };
        let directory = std::path::Path::new(&directory);
        let json = std::fs::read(directory.join("counter.manifest.json")).unwrap();
        let bytes = std::fs::read(directory.join("counter.component.wasm")).unwrap();
        let package = Package::from_json(&json, bytes).unwrap();
        assert_eq!(package.manifest().id.as_str(), "dayframe.counter");
    }

    #[test]
    fn lifecycle_fixture_manifest_declares_its_publications() {
        let manifest = lifecycle_fixture_manifest();
        assert_eq!(manifest.id.as_str(), "dayframe.lifecycle-fixture");
        assert_eq!(
            manifest
                .publications
                .iter()
                .map(|publication| publication.name.as_str())
                .collect::<Vec<_>>(),
            ["presentation", "echo", "relay"]
        );
        manifest.validate().unwrap();
    }
}
