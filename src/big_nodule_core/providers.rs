//! Resolving a nodule's operation requests to providers (ADR 028 §3–§4).
//!
//! A request names a contract (an NSID), never a provider; whichever enabled provider offers the
//! NSID satisfies it. The runtime records which one did, node-locally, like a lockfile: the
//! [`LockedProvider`] a binding resolved to. Resolution is a pure function of three inputs, the
//! providers on offer, the request and the pin recorded earlier, so every reconcile recomputes it
//! from the same snapshot that decides which nodules are enabled and no provider can appear or
//! vanish without every dependent being re-evaluated in that same pass.
//!
//! Every request resolves the same way. A data request (one with `access`) names a datum NSID,
//! and its provider is the enabled cartridge that holds the datum (declares the record under its
//! authority); a copy another cartridge vendors is not an offer, because implementing or vendoring is
//! not owning (ADR 028 §5). The datums the node itself registers at boot are native offers,
//! like routes registered straight into dispatch.

use crate::config::NoduleResolution;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Version;
use big_cartridges_core::request::Request;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// One provider's claim on a contract, and the lockfile entry a request resolved to it records:
/// `{contract, provider, version, revision}`.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LockedProvider {
    pub contract: String,
    /// The provider's cartridge id for a nodule; the registrant's own name for a native provider
    /// registered straight into dispatch's routing table.
    pub provider: String,
    /// The provider's package version, absent for a native provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<Version>,
    /// The `revision` of the provider's Lexicon for the contract, when it carries one that
    /// declares it. Distinct from a cartridge revision, which is a content hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lexicon_revision: Option<u32>,
}

/// Why a request is not satisfied by a provider.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Unresolved {
    /// No enabled provider offers the contract.
    NoProvider,
    /// More than one does. Disabling one resolves it, as with dispatch's `Ambiguous`.
    Ambiguous { providers: Vec<String> },
    /// The one provider's Lexicon is older than the request's `minRevision`.
    RevisionTooOld {
        provider: String,
        required: u32,
        offered: u32,
    },
    /// The contract is now provided by another cartridge than the one recorded. See
    /// [`RequestStatus::evaluate`].
    ProviderChanged { from: String, to: String },
}

/// Where one operation request stands: what it is pinned to and, if it is not satisfied, why.
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestStatus {
    pub contract: String,
    /// The lockfile entry after this evaluation: the provider it resolved to, or the entry
    /// recorded earlier while unresolved (so a missing provider coming back as the same cartridge
    /// resolves without asking again).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locked: Option<LockedProvider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unresolved: Option<Unresolved>,
}

impl RequestStatus {
    /// Evaluates `request` against the `offers` for its contract and the `pinned` entry
    /// recorded for its binding (one for this contract, or none).
    ///
    /// Exactly one offer resolves, provided its Lexicon revision (when it declares one) is at
    /// least the request's `minRevision`. With a pin, the offer must also be the pinned cartridge:
    /// the first resolution needs no confirmation (enabling the nodule is the consent), but a
    /// different cartridge now providing the contract is a different publisher's code receiving the
    /// nodule's calls and what it sends, so cartridge identity is the consent boundary and
    /// [`Unresolved::ProviderChanged`] holds the request until the operator confirms. The same
    /// cartridge at another version is an ordinary upgrade by the same publisher: it re-pins
    /// silently. Two or more offers are ambiguous, never decided by order.
    pub fn evaluate(
        request: &Request,
        offers: &[LockedProvider],
        pinned: Option<&LockedProvider>,
    ) -> Self {
        let unresolved = |unresolved| Self {
            contract: request.contract.clone(),
            locked: pinned.cloned(),
            unresolved: Some(unresolved),
        };
        match offers {
            [] => unresolved(Unresolved::NoProvider),
            [offer] => {
                if let (Some(required), Some(offered)) =
                    (request.min_revision, offer.lexicon_revision)
                    && offered < required
                {
                    return unresolved(Unresolved::RevisionTooOld {
                        provider: offer.provider.clone(),
                        required,
                        offered,
                    });
                }
                match pinned {
                    Some(pin) if pin.provider != offer.provider => {
                        unresolved(Unresolved::ProviderChanged {
                            from: pin.provider.clone(),
                            to: offer.provider.clone(),
                        })
                    }
                    _ => Self {
                        contract: request.contract.clone(),
                        locked: Some(offer.clone()),
                        unresolved: None,
                    },
                }
            }
            _ => {
                let mut providers: Vec<String> =
                    offers.iter().map(|offer| offer.provider.clone()).collect();
                providers.sort();
                unresolved(Unresolved::Ambiguous { providers })
            }
        }
    }
}

/// The enabled nodules' offers for one reconcile, indexed by contract.
pub struct ProviderIndex {
    offers: BTreeMap<String, Vec<LockedProvider>>,
    /// Every nodule in the snapshot, enabled or not: a registration under one of these ids in
    /// dispatch's routing table is the nodule service's own, so the snapshot decides it (the
    /// table lags it until that nodule's turn in the pass).
    nodules: BTreeSet<CartridgeId>,
}

impl ProviderIndex {
    /// Indexes what each enabled, admitted nodule implements and the datum records it holds.
    ///
    /// A slot filled by a substitution (see [`crate::inventory::InstalledNodule::filled_by`]) is
    /// offered under the slot's cartridge id with the filler's version and Lexicons. Dependents name
    /// the slot, so a substitution is not a provider change for them: their pin stays on the slot
    /// and only its version moves, as for any upgrade. The operator's substitution is itself the
    /// consent, and enablement survives it (ADR 028 §3).
    pub fn new(resolved: &BTreeMap<CartridgeId, NoduleResolution>) -> Self {
        let mut offers: BTreeMap<String, Vec<LockedProvider>> = BTreeMap::new();
        for (cartridge_id, resolution) in resolved {
            let NoduleResolution::Admitted { installed, .. } = resolution else {
                continue;
            };
            if !installed.enabled {
                continue;
            }
            for nsid in installed.admitted.operations.keys() {
                offers
                    .entry(nsid.clone())
                    .or_default()
                    .push(LockedProvider {
                        contract: nsid.clone(),
                        provider: cartridge_id.to_string(),
                        version: Some(installed.version.clone()),
                        lexicon_revision: installed.nodule.lexicon_revision(nsid),
                    });
            }
            let authority = installed.admitted.authority.as_deref();
            for (nsid, _) in installed.nodule.registrable_datums(authority).held {
                offers
                    .entry(nsid.to_owned())
                    .or_default()
                    .push(LockedProvider {
                        contract: nsid.to_owned(),
                        provider: cartridge_id.to_string(),
                        version: Some(installed.version.clone()),
                        lexicon_revision: installed.nodule.lexicon_revision(nsid),
                    });
            }
        }
        Self {
            offers,
            nodules: resolved.keys().cloned().collect(),
        }
    }

    /// Everything on offer for `nsid`: the enabled nodules that implement or hold it plus
    /// `native`, the provider ids the node itself serves it under (dispatch's routing table for
    /// an operation, the boot schemas for a datum), from which the nodule service's
    /// own registrations are dropped.
    pub fn offers(
        &self,
        nsid: &str,
        native: impl IntoIterator<Item = String>,
    ) -> Vec<LockedProvider> {
        let mut offers = self.offers.get(nsid).cloned().unwrap_or_default();
        offers.extend(
            native
                .into_iter()
                .filter(|provider_id| {
                    !provider_id
                        .parse::<CartridgeId>()
                        .is_ok_and(|cartridge_id| self.nodules.contains(&cartridge_id))
                })
                .map(|provider| LockedProvider {
                    contract: nsid.to_owned(),
                    provider,
                    version: None,
                    lexicon_revision: None,
                }),
        );
        offers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer(provider: &str, revision: Option<u32>) -> LockedProvider {
        LockedProvider {
            contract: "org.example.a.run".into(),
            provider: provider.into(),
            version: Some("1.0.0".parse().unwrap()),
            lexicon_revision: revision,
        }
    }

    fn request(min_revision: Option<u32>) -> Request {
        let mut request = Request::new("run", "org.example.a.run");
        request.min_revision = min_revision;
        request
    }

    #[test]
    fn one_offer_resolves_and_a_revision_floor_is_checked_only_against_a_declared_revision() {
        let status = RequestStatus::evaluate(&request(Some(2)), &[offer("p", Some(2))], None);
        assert_eq!(status.locked, Some(offer("p", Some(2))));
        assert_eq!(status.unresolved, None);
        // A provider that declares no revision is not held to the floor.
        assert_eq!(
            RequestStatus::evaluate(&request(Some(2)), &[offer("p", None)], None).unresolved,
            None
        );
        let status = RequestStatus::evaluate(&request(Some(3)), &[offer("p", Some(2))], None);
        assert_eq!(
            status.unresolved,
            Some(Unresolved::RevisionTooOld {
                provider: "p".into(),
                required: 3,
                offered: 2
            })
        );
        assert_eq!(status.locked, None);
    }

    #[test]
    fn none_and_many_offers_are_unresolved_and_a_pin_survives_them() {
        let pin = offer("p", None);
        let none = RequestStatus::evaluate(&request(None), &[], Some(&pin));
        assert_eq!(none.unresolved, Some(Unresolved::NoProvider));
        assert_eq!(none.locked, Some(pin.clone()));
        let many = RequestStatus::evaluate(
            &request(None),
            &[offer("q", None), offer("p", None)],
            Some(&pin),
        );
        assert_eq!(
            many.unresolved,
            Some(Unresolved::Ambiguous {
                providers: vec!["p".into(), "q".into()]
            })
        );
        assert_eq!(many.locked, Some(pin));
    }

    #[test]
    fn another_cartridge_is_a_change_and_the_same_cartridge_at_a_new_version_is_not() {
        let pin = offer("p", Some(1));
        let mut upgraded = offer("p", Some(2));
        upgraded.version = Some("1.1.0".parse().unwrap());
        let status = RequestStatus::evaluate(&request(None), &[upgraded.clone()], Some(&pin));
        assert_eq!(status.unresolved, None);
        assert_eq!(status.locked, Some(upgraded));
        let status = RequestStatus::evaluate(&request(None), &[offer("q", None)], Some(&pin));
        assert_eq!(
            status.unresolved,
            Some(Unresolved::ProviderChanged {
                from: "p".into(),
                to: "q".into()
            })
        );
        assert_eq!(status.locked, Some(pin));
    }
}
