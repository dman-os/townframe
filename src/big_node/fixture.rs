//! Publishing a cartridge fixture through a booted node's registry, for tests across the
//! workspace (feature `test-support`): the way a publisher does, so installs resolve the version
//! through the cartridges service as in production.

use crate::BigNode;
use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
use big_cabinet_core::DatumPin;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cartridges_core::protocol::GetCartridge;
use big_cartridges_core::protocol::GetCartridgeVersion;
use big_cartridges_core::protocol::PublishCartridgeVersion;
use big_cartridges_core::registry::version_pin;
use big_cartridges_core::test_support::FixtureCartridge;
use method_core::ByteBody;
use service_utils_rs::RequestContext;
use std::collections::BTreeSet;

impl BigNode {
    /// Publishes `fixture` as the node operator and returns the pin of its version at the
    /// registry document's current heads, which is what an install takes.
    ///
    /// The component bytes go into the blob store, the payloads are staged into the cartridge's
    /// registry document, and the envelope carrying the staged pins (signed as the example
    /// namespace unless it is signed already) is published. A version is published once, so a
    /// fixture whose version is already published is reused rather than staged again.
    pub async fn publish_fixture(&self, fixture: &FixtureCartridge) -> DatumPin {
        let operator = Authorization::identity(self.authority.root);
        let published = self
            .cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                GetCartridgeVersion {
                    id: fixture.id.clone(),
                    version: fixture.version.clone(),
                },
            )
            .await
            .is_ok();
        if !published {
            let put = self
                .blobs
                .client
                .call_with(
                    RequestContext::root(),
                    operator.metadata(),
                    MintBlobsCapability {
                        scope: BlobsScope {
                            put: true,
                            ..Default::default()
                        },
                        rights: Rights::USE,
                        subject: None,
                    },
                )
                .await
                .expect("a blob put handle")
                .capability_id;
            for bytes in fixture.blobs.values() {
                self.blobs
                    .put(
                        RequestContext::root(),
                        put,
                        ByteBody::from_bytes(bytes.clone()),
                    )
                    .await
                    .expect("a component's bytes");
            }
            let staged = self
                .cartridges
                .client
                .call_with(
                    RequestContext::root(),
                    operator.metadata(),
                    fixture.stage_request(),
                )
                .await
                .expect("the version's payloads stage");
            self.cartridges
                .client
                .call_with(
                    RequestContext::root(),
                    operator.metadata(),
                    PublishCartridgeVersion {
                        envelope: fixture.publishable(staged),
                    },
                )
                .await
                .expect("the version publishes");
        }
        let doc_id = self
            .cartridges
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                GetCartridge {
                    id: fixture.id.clone(),
                },
            )
            .await
            .expect("the published cartridge")
            .doc_id;
        let capability = self
            .docs
            .client
            .call_with(
                RequestContext::root(),
                operator.metadata(),
                MintDocCapability {
                    scope: DocScope {
                        read: BTreeSet::from([doc_id.clone()]),
                        ..Default::default()
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .expect("a registry document read handle")
            .capability_id;
        let heads = self
            .docs
            .client
            .call(
                RequestContext::root(),
                ReadDatums {
                    capability,
                    doc_id: doc_id.clone(),
                    at: At::Current,
                    datums: Some(BTreeSet::new()),
                    optimistic: false,
                },
            )
            .await
            .expect("the registry document's heads")
            .heads;
        version_pin(doc_id, &fixture.version, heads)
    }
}
