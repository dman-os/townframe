//! The static plug catalog: which package revisions a node knows (ADR 027 §1, §4 "Known").
//!
//! The catalog is deliberately inert. It stores validated envelopes and answers version questions;
//! it owns no actor, grant, enablement, configuration or readiness, and it does not touch artifact
//! bytes — blob storage and materialization belong to the blob system, which will report that state
//! separately. Nothing here substitutes another revision for one that is absent, because a pin that
//! silently means different bytes is exactly what the revision scheme exists to prevent.

use crate::Plug;
use crate::PlugError;
use crate::PlugId;
use crate::Revision;
use crate::Version;
use crate::VersionReq;
use std::collections::BTreeMap;

/// Every revision imported into this node.
///
/// Two catalogs that imported the same bytes agree without coordination: a revision is the content
/// hash of its envelope. The same package id may be catalogued at several versions (that is what an
/// upgrade looks like) but one version names exactly one revision; importing a second envelope under
/// a version already taken is a conflict, not a newer revision.
#[derive(Debug, Default)]
pub struct Catalog {
    revisions: BTreeMap<Revision, Plug>,
    versions: BTreeMap<PlugId, BTreeMap<Version, Revision>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one envelope and answers with the revision it is. Validation is the envelope's
    /// distribution checks; contribution payloads stay opaque. Re-importing the same envelope is a
    /// no-op that returns the same revision.
    pub fn import(&mut self, plug: Plug) -> Result<Revision, PlugError> {
        plug.validate()?;
        let revision = plug.revision();
        if self.revisions.contains_key(&revision) {
            return Ok(revision);
        }
        if let Some(existing) = self
            .versions
            .get(&plug.id)
            .and_then(|versions| versions.get(&plug.version))
        {
            return Err(PlugError::Conflict {
                id: plug.id.clone(),
                version: Box::new(plug.version.clone()),
                existing: *existing,
                incoming: revision,
            });
        }
        self.versions
            .entry(plug.id.clone())
            .or_default()
            .insert(plug.version.clone(), revision);
        self.revisions.insert(revision, plug);
        Ok(revision)
    }

    pub fn plug(&self, revision: &Revision) -> Option<&Plug> {
        self.revisions.get(revision)
    }

    /// The highest catalogued version of `id` satisfying `requirement`, or `None` when the package is
    /// absent or no catalogued version fits. Resolution never reaches outside the catalog: an
    /// unsatisfied dependency is reported to the caller, not guessed at.
    pub fn resolve(&self, id: &PlugId, requirement: &VersionReq) -> Option<&Plug> {
        let (_, revision) = self
            .versions
            .get(id)?
            .iter()
            .rev()
            .find(|(version, _)| requirement.0.matches(&version.0))?;
        self.revisions.get(revision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Artifact;
    use crate::Contribution;
    use crate::Digest;
    use crate::NoduleContribution;
    use indexmap::IndexMap;
    use serde_json::json;

    fn plug(version: &str) -> Plug {
        let mut plug = Plug::new(
            crate::example_plug!("photos").parse().unwrap(),
            version.parse().unwrap(),
        );
        plug.artifacts.push(Artifact {
            key: "routines".into(),
            digest: Digest::of(b"wasm"),
            size: Some(4),
            media_type: None,
            extra: Default::default(),
        });
        plug.contributions
            .push(Contribution::Nodule(NoduleContribution {
                schema: "org.example.daybook.nodule".into(),
                description: IndexMap::from([("id".to_owned(), json!("org.example.photos"))]),
                extra: Default::default(),
            }));
        plug
    }

    fn catalog(versions: &[&str]) -> Catalog {
        let mut catalog = Catalog::new();
        for version in versions {
            catalog.import(plug(version)).unwrap();
        }
        catalog
    }

    #[test]
    fn importing_is_idempotent_and_validation_still_applies() {
        let mut catalog = Catalog::new();
        let envelope = plug("1.2.3");
        let revision = catalog.import(envelope.clone()).unwrap();
        assert_eq!(revision, envelope.revision());
        assert_eq!(catalog.import(envelope.clone()).unwrap(), revision);
        assert_eq!(catalog.plug(&revision), Some(&envelope));

        let mut invalid = plug("0.1.0");
        invalid.artifacts[0].key.clear();
        let error = catalog.import(invalid).unwrap_err().to_string();
        assert!(error.contains("artifact key is not empty"), "{error}");
        assert_eq!(catalog.plug(&Revision([0; 32])), None);
    }

    #[test]
    fn one_version_names_one_revision() {
        let mut catalog = catalog(&["1.2.3"]);
        let mut republished = plug("1.2.3");
        republished.title = Some("Photos, republished".into());
        let error = catalog.import(republished).unwrap_err().to_string();
        assert!(error.contains(crate::example_plug!("photos")), "{error}");
        assert!(error.contains("1.2.3"), "{error}");
        assert!(error.contains("already catalogued"), "{error}");
        // A later version is an ordinary upgrade, not a conflict.
        catalog.import(plug("1.3.0")).unwrap();
    }

    #[test]
    fn resolution_picks_the_highest_matching_version() {
        let catalog = catalog(&["1.2.3", "2.0.0", "1.10.0"]);
        let id: PlugId = crate::example_plug!("photos").parse().unwrap();
        let resolved = |requirement: &str| {
            catalog
                .resolve(&id, &requirement.parse().unwrap())
                .map(|plug| plug.version.to_string())
        };
        assert_eq!(resolved("^1"), Some("1.10.0".into()));
        assert_eq!(resolved("=1.2.3"), Some("1.2.3".into()));
        assert_eq!(resolved("^3"), None);
        assert_eq!(resolved("^2"), Some("2.0.0".into()));
        let absent: PlugId = crate::example_plug!("absent").parse().unwrap();
        assert!(catalog.resolve(&absent, &"^1".parse().unwrap()).is_none());
    }
}
