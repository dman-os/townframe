//! Publishing a cartridge fixture through a booted node's registry, for tests across the
//! workspace (feature `test-support`): the way a publisher does, so installs resolve the version
//! through the cartridges service as in production.

use crate::BigNode;
use big_cabinet_core::DatumPin;
use big_cartridges_core::test_support::FixtureCartridge;
use big_cartridges_core::test_support::sign_as_example;

impl BigNode {
    /// Publishes `fixture` as the node operator, signed as the example namespace, and returns
    /// the pin of its version at the registry document's current heads, which is what an
    /// install takes. A version is published once, so a fixture whose version is already
    /// published is reused rather than staged again.
    pub async fn publish_fixture(&self, fixture: &FixtureCartridge) -> DatumPin {
        self.publish_version(&fixture.resolved, &fixture.blobs, sign_as_example)
            .await
            .expect("the fixture publishes")
    }
}
