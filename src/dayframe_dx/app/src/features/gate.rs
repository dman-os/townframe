use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct FeatureId(String);

impl FeatureId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FeatureAttribute {
    Developer,
    InternalTester,
    BetaTester,
    Desktop,
    Mobile,
    Web,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseChannel {
    Stable,
    Experimental,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuildCapability {
    Camera,
    Mltools,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildCapabilities {
    pub camera: bool,
    pub mltools: bool,
}

impl BuildCapabilities {
    pub const fn current() -> Self {
        Self {
            camera: cfg!(feature = "camera"),
            mltools: cfg!(feature = "mltools"),
        }
    }

    fn contains(self, capability: BuildCapability) -> bool {
        match capability {
            BuildCapability::Camera => self.camera,
            BuildCapability::Mltools => self.mltools,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureDefinition {
    pub channel: ReleaseChannel,
    pub default_enabled: bool,
    pub required_attribute: Option<FeatureAttribute>,
    pub required_capability: Option<BuildCapability>,
}

impl FeatureDefinition {
    pub const fn stable() -> Self {
        Self {
            channel: ReleaseChannel::Stable,
            default_enabled: true,
            required_attribute: None,
            required_capability: None,
        }
    }

    pub const fn experimental(required_attribute: FeatureAttribute) -> Self {
        Self {
            channel: ReleaseChannel::Experimental,
            default_enabled: false,
            required_attribute: Some(required_attribute),
            required_capability: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureContext {
    pub channel: ReleaseChannel,
    pub attributes: BTreeSet<FeatureAttribute>,
    pub capabilities: BuildCapabilities,
    overrides: BTreeMap<FeatureId, bool>,
}

impl FeatureContext {
    pub fn stable(capabilities: BuildCapabilities) -> Self {
        Self {
            channel: ReleaseChannel::Stable,
            attributes: BTreeSet::new(),
            capabilities,
            overrides: BTreeMap::new(),
        }
    }

    pub fn experimental(capabilities: BuildCapabilities) -> Self {
        Self {
            channel: ReleaseChannel::Experimental,
            ..Self::stable(capabilities)
        }
    }

    pub fn with_attribute(mut self, attribute: FeatureAttribute) -> Self {
        self.attributes.insert(attribute);
        self
    }

    pub fn with_override(mut self, feature: FeatureId, enabled: bool) -> Self {
        self.overrides.insert(feature, enabled);
        self
    }

    pub fn has_attribute(&self, attribute: FeatureAttribute) -> bool {
        self.attributes.contains(&attribute)
    }
}

#[derive(Clone, Debug)]
pub struct FeatureRegistry {
    definitions: BTreeMap<FeatureId, FeatureDefinition>,
}

impl Default for FeatureRegistry {
    fn default() -> Self {
        let mut definitions = BTreeMap::new();
        definitions.insert(FeatureId::new("home"), FeatureDefinition::stable());
        definitions.insert(FeatureId::new("settings"), FeatureDefinition::stable());
        definitions.insert(FeatureId::new("workspace"), FeatureDefinition::stable());
        definitions.insert(
            FeatureId::new("experimental.lab"),
            FeatureDefinition::experimental(FeatureAttribute::Developer),
        );
        Self { definitions }
    }
}

impl FeatureRegistry {
    pub fn register(&mut self, feature: FeatureId, definition: FeatureDefinition) {
        self.definitions.insert(feature, definition);
    }

    pub fn definition(&self, feature: &FeatureId) -> Option<&FeatureDefinition> {
        self.definitions.get(feature)
    }

    pub fn is_enabled(&self, feature: &FeatureId, context: &FeatureContext) -> bool {
        let Some(definition) = self.definition(feature) else {
            return false;
        };
        if definition
            .required_capability
            .is_some_and(|capability| !context.capabilities.contains(capability))
        {
            return false;
        }
        // Stable production contexts cannot use a runtime override to bypass an experimental
        // channel or audience gate. Development/test contexts may opt in after those gates pass.
        if definition.channel == ReleaseChannel::Experimental
            && (context.channel != ReleaseChannel::Experimental
                || definition
                    .required_attribute
                    .is_some_and(|attribute| !context.has_attribute(attribute)))
        {
            return false;
        }
        if let Some(enabled) = context.overrides.get(feature) {
            return *enabled;
        }
        definition.default_enabled || definition.channel == ReleaseChannel::Experimental
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaced_ids_are_extensible_without_enum_variants() {
        let mut registry = FeatureRegistry::default();
        let feature = FeatureId::new("settings.plugs.import");
        assert_eq!(feature.as_str(), "settings.plugs.import");
        assert!(registry.definition(&FeatureId::new("settings")).is_some());
        assert!(registry.definition(&feature).is_none());
        registry.register(feature.clone(), FeatureDefinition::stable());
        assert!(registry.definition(&feature).is_some());
    }

    #[test]
    fn stable_features_are_enabled_without_experimental_access() {
        let registry = FeatureRegistry::default();
        let context = FeatureContext::stable(BuildCapabilities {
            camera: false,
            mltools: false,
        });

        assert!(registry.is_enabled(&FeatureId::new("settings"), &context));
        assert!(!registry.is_enabled(&FeatureId::new("experimental.lab"), &context));
    }

    #[test]
    fn experimental_features_require_channel_and_attribute() {
        let registry = FeatureRegistry::default();
        let experimental = FeatureContext::experimental(BuildCapabilities::current());
        let developer = experimental
            .clone()
            .with_attribute(FeatureAttribute::Developer);
        let feature = FeatureId::new("experimental.lab");

        assert!(!registry.is_enabled(&feature, &experimental));
        assert!(registry.is_enabled(&feature, &developer));
    }

    #[test]
    fn stable_override_cannot_enable_experimental_feature() {
        let registry = FeatureRegistry::default();
        let feature = FeatureId::new("experimental.lab");
        let stable_override = FeatureContext::stable(BuildCapabilities::current())
            .with_override(feature.clone(), true);
        let authorized_override = FeatureContext::experimental(BuildCapabilities::current())
            .with_attribute(FeatureAttribute::Developer)
            .with_override(feature.clone(), true);

        assert!(!registry.is_enabled(&feature, &stable_override));
        assert!(registry.is_enabled(&feature, &authorized_override));
    }
}
