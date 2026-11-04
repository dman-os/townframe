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
use crate::manifest::Sidecar;
use sha2::Digest;
use sha2::Sha256;

/// A component's bytes together with the sidecar (package identity plus manifest) that declares
/// them and their digest.
#[derive(Clone, Debug, PartialEq)]
pub struct Package {
    sidecar: Sidecar,
    digest: String,
    bytes: Vec<u8>,
}

impl Package {
    /// Pair a sidecar with its component bytes. The manifest must pass
    /// [`Sidecar::validate`], so a held `Package` always declares internally consistent
    /// references and only NSIDs under its authority. Errors are plain messages: an incoherent package is an installation defect
    /// a launcher reports per entry, not a host fault. The digest covers the component bytes
    /// only: the manifest is a separate sidecar, so editing it does not change the identity of
    /// the code it describes.
    pub fn new(sidecar: Sidecar, bytes: impl Into<Vec<u8>>) -> Result<Self, String> {
        let bytes = bytes.into();
        sidecar
            .validate()
            .map_err(|error| format!("package declares an invalid manifest: {error}"))?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        Ok(Self {
            sidecar,
            digest,
            bytes,
        })
    }

    /// [`Package::new`] from the sidecar JSON bytes: the manifest's members plus `id` and
    /// `package_version`, which this splits into the package identity and the manifest.
    pub fn from_json(sidecar_json: &[u8], bytes: impl Into<Vec<u8>>) -> Result<Self, String> {
        let sidecar = serde_json::from_slice(sidecar_json)
            .map_err(|error| format!("manifest JSON is not a manifest: {error}"))?;
        Self::new(sidecar, bytes)
    }

    pub fn sidecar(&self) -> &Sidecar {
        &self.sidecar
    }

    pub fn manifest(&self) -> &MicroManifest {
        &self.sidecar.manifest
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
    use crate::ContributionDeclaration;
    use crate::Entrypoints;

    fn sidecar() -> Sidecar {
        Sidecar::new(
            "dayframe.test",
            1,
            MicroManifest {
                entrypoints: Entrypoints::micro_client(),
                publications: vec![],
                requires: vec![],
                configuration_requests: vec![],
                collection_contracts: vec![],
                collection_requests: vec![],
                contributions: vec![],
                extra: Default::default(),
            },
        )
    }

    /// The sidecar of the lifecycle fixture package.
    fn lifecycle_fixture_sidecar() -> Sidecar {
        serde_json::from_str(include_str!("lifecycle_fixture.manifest.json")).unwrap()
    }

    #[test]
    fn package_pairs_sidecar_json_with_component_bytes() {
        let bytes = b"\0asm\x0d\0\x01\0".to_vec();
        let json = serde_json::to_vec(&sidecar()).unwrap();
        let package = Package::from_json(&json, bytes.clone()).unwrap();
        assert_eq!(package.sidecar(), &sidecar());
        assert_eq!(package.manifest(), &sidecar().manifest);
        assert_eq!(package.bytes(), bytes.as_slice());
        assert_eq!(package.digest(), format!("{:x}", Sha256::digest(&bytes)));
    }

    /// The sidecar is flat JSON (the manifest's members beside `id` and `package_version`), the
    /// shape `big_plugs`' micro importer reads; the manifest itself carries neither.
    #[test]
    fn sidecar_json_is_flat_and_the_manifest_carries_no_identity() {
        let json = serde_json::to_value(sidecar()).unwrap();
        assert_eq!(json["id"], "dayframe.test");
        assert_eq!(json["package_version"], 1);
        assert!(json.get("schema").is_none());
        assert!(json["entrypoints"].is_object());
        let manifest = serde_json::to_value(&sidecar().manifest).unwrap();
        assert!(manifest.get("id").is_none() && manifest.get("package_version").is_none());
        // Members a newer sidecar adds ride in the manifest's extras, and identity stays out.
        let mut newer = json;
        newer["future_member"] = serde_json::json!({"a": 1});
        let parsed: Sidecar = serde_json::from_value(newer).unwrap();
        assert_eq!(parsed.id.as_str(), "dayframe.test");
        assert_eq!(parsed.manifest.extra.len(), 1);
        assert!(!parsed.manifest.extra.contains_key("id"));
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
        let mut invalid = sidecar();
        invalid
            .manifest
            .contributions
            .push(ContributionDeclaration {
                id: "row".into(),
                binding: "nowhere".into(),
                key: "k".into(),
                payload: serde_json::Value::Null,
                attachments: vec![],
            });
        assert_eq!(
            Package::new(invalid, vec![]).unwrap_err(),
            "package declares an invalid manifest: contribution \"row\" targets undeclared \
             collection binding \"nowhere\""
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
        assert_eq!(package.sidecar().id.as_str(), "dayframe.counter");
    }

    #[test]
    fn lifecycle_fixture_sidecar_declares_its_publications() {
        let sidecar = lifecycle_fixture_sidecar();
        assert_eq!(sidecar.id.as_str(), "dayframe.lifecycle-fixture");
        assert_eq!(
            sidecar
                .manifest
                .publications
                .iter()
                .map(|publication| publication.name.as_str())
                .collect::<Vec<_>>(),
            ["presentation", "echo", "relay"]
        );
        sidecar.manifest.validate().unwrap();
    }
}
