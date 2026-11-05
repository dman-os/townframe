//! Importers that turn files on this node into published plug versions.
//!
//! These are library functions and not service operations on purpose: an operation that reads
//! a caller-named path would let any publish handle read any file the node can (a confused
//! deputy). The embedder, who already owns the filesystem, reads the bytes and calls in here.
//! Every importer ends in the ordinary [`PublishPlugVersion`] path, so signatures, immutability,
//! the downgrade rule and namespace scopes apply to an imported version exactly as to any other.

use big_blobs::BlobService;
use big_cabinet_core::authority::Authorization;
use big_plugs_core::Artifact;
use big_plugs_core::Contribution;
use big_plugs_core::Digest;
use big_plugs_core::MicroContribution;
use big_plugs_core::Plug;
use big_plugs_core::PlugId;
use big_plugs_core::protocol::GetPublisher;
use big_plugs_core::protocol::PlugsError;
use big_plugs_core::protocol::PlugsProtocol;
use big_plugs_core::protocol::PublishPlugVersion;
use big_plugs_core::protocol::PublishedPlugVersion;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;

/// The artifact key a micro's component is published under.
pub const MICRO_COMPONENT_ARTIFACT: &str = "component";

/// The description schema of a micro contribution built from Clay's manifest. Following ADR 028
/// (compatibility lives in the NSID), this NSID is the manifest format's identity: a breaking
/// manifest change mints a new NSID rather than bumping a revision. Clay's older manifests also
/// carry an integer `schema` member; it is not read, and stays in the description if present.
pub const CLAY_MICRO_MANIFEST_SCHEMA: &str = "org.example.clay.micro.manifest";

const WASM_MEDIA_TYPE: &str = "application/wasm";

/// Why an import did not publish. Bad input is the caller's to see; a refusal from the registry
/// is carried as it was given.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("the manifest is not importable: {0}")]
    Manifest(String),
    #[error("the component is not importable: {0}")]
    Component(String),
    #[error("importing the component into the blob store failed: {0}")]
    Blob(String),
    #[error("the registry refused the version: {0}")]
    Registry(#[from] PlugsError),
    #[error("the registry could not be reached: {0}")]
    Transport(String),
}

fn registry_error(error: ClientError<PlugsError>) -> ImportError {
    match error {
        ClientError::Operation(error) => ImportError::Registry(error),
        ClientError::Transport { message, .. } => ImportError::Transport(message),
    }
}

/// Clay's stand-in package, a `<name>.component.wasm` beside its `<name>.manifest.json`
/// (`clay_mdk::Package`), published as one plug version under the node's own publisher DID.
///
/// The manifest is read generically, with no dependency on Clay's crates, and only two members
/// are interpreted:
/// - `id` becomes the plug name, so the plug id is `<node publisher DID>/<id>`. The id must
///   already be a valid plug name segment; it is refused rather than rewritten, because a
///   rewrite (such as `.` to `-`) could make two micro ids one plug.
/// - `package_version`, an integer, becomes the semver `<n>.0.0`.
///
/// The contribution's schema is always [`CLAY_MICRO_MANIFEST_SCHEMA`].
///
/// The component is imported into big_blobs and referenced as the artifact
/// [`MICRO_COMPONENT_ARTIFACT`] by its BLAKE3 digest. Every other member of the manifest is the
/// micro contribution's `description`, unread. The envelope is left unsigned, so the node signs
/// it through the registry's own-namespace convenience.
pub async fn micro_pair(
    plugs: &Client<PlugsProtocol>,
    blobs: &BlobService,
    authorization: Authorization,
    component: Vec<u8>,
    manifest_json: &[u8],
) -> Result<PublishedPlugVersion, ImportError> {
    let serde_json::Value::Object(mut manifest) = serde_json::from_slice(manifest_json)
        .map_err(|error| ImportError::Manifest(format!("not JSON: {error}")))?
    else {
        return Err(ImportError::Manifest("not a JSON object".into()));
    };
    let id = match manifest.remove("id") {
        Some(serde_json::Value::String(id)) => id,
        _ => return Err(ImportError::Manifest("`id` is not a string".into())),
    };
    let package_version = manifest
        .remove("package_version")
        .and_then(|value| value.as_u64())
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| ImportError::Manifest("`package_version` is not a u32".into()))?;
    // The envelope owns the NSID authority, as it owns identity (ADR 028 §5).
    let authority = match manifest.remove("authority") {
        None => None,
        Some(serde_json::Value::String(authority)) => Some(authority),
        Some(_) => return Err(ImportError::Manifest("`authority` is not a string".into())),
    };
    if !component.starts_with(b"\0asm") {
        return Err(ImportError::Component("not a WebAssembly binary".into()));
    }

    let publisher = plugs
        .call_with(
            RequestContext::root(),
            authorization.metadata(),
            GetPublisher {},
        )
        .await
        .map_err(registry_error)?;
    let plug_id = PlugId::new(&publisher, &id).map_err(|error| {
        ImportError::Manifest(format!(
            "`id` is not a plug name (a lowercase letter, then lowercase letters, digits, `-`, \
             `_` or single inner `.`, at most 64): {error}"
        ))
    })?;
    let version = format!("{package_version}.0.0")
        .parse()
        .expect("an integer with two zeroes is a semver");

    let size = component.len() as u64;
    let imported = blobs
        .import_stream(futures::stream::iter([Ok(bytes::Bytes::from(component))]))
        .await
        .map_err(|error| ImportError::Blob(error.to_string()))?;
    let mut envelope = Plug::new(plug_id, version);
    envelope.authority = authority;
    envelope.artifacts.push(Artifact {
        key: MICRO_COMPONENT_ARTIFACT.into(),
        digest: Digest(imported.info.blob_id.to_bytes32()),
        size: Some(size),
        media_type: Some(WASM_MEDIA_TYPE.into()),
        extra: Default::default(),
    });
    envelope
        .contributions
        .push(Contribution::Micro(MicroContribution {
            schema: CLAY_MICRO_MANIFEST_SCHEMA.into(),
            description: manifest.into_iter().collect(),
            extra: Default::default(),
        }));
    plugs
        .call_with(
            RequestContext::root(),
            authorization.metadata(),
            PublishPlugVersion { envelope },
        )
        .await
        .map_err(registry_error)
}
