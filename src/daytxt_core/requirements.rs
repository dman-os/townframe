//! Resolves what micros require of each other (ADR 028 §4). A request names the NSID of what it
//! wants, never a supplier, so the supplier is whichever installed micro serves that NSID: a
//! method endpoint under its `nsid`, a view endpoint under its `implements`. A callback is
//! never a supplier, since it is reached through a link a row carries and many micros
//! implement the same contract. Resolution here only picks the micro; the transport stays
//! Clay-local, the supervisor binding the supplier's publication of the endpoint's name.
use crate::InstalledMicro;
use clay_host::Necessity;
use clay_host::Requirement;
use clay_mdk::Endpoint;
use clay_mdk::MicroId;
use clay_mdk::Necessity as Declared;
use clay_mdk::Nsid;
use std::collections::BTreeMap;

/// Every installed micro's `requires` resolved against `installed`, in catalog order and by
/// binding, with a message for each requirement that several micros serve.
///
/// A requirement nothing serves has no supplier: a required one blocks its micro when it
/// starts and an optional one is left unbound (see [`Requirement::provider`]). One that several
/// serve has none either, and its message names every candidate: choosing among them is
/// configuration this catalog does not have, and picking one silently would let the
/// installation order decide.
pub(crate) fn resolve(
    installed: &[InstalledMicro],
) -> (Vec<BTreeMap<String, Requirement>>, Vec<String>) {
    let mut errors = Vec::new();
    let resolved = installed
        .iter()
        .map(|consumer| {
            consumer
                .sidecar
                .manifest
                .requires
                .iter()
                .map(|request| {
                    // A micro never supplies its own requirement: the supervisor would wait on
                    // itself at start.
                    let candidates: Vec<_> = suppliers(installed, &request.contract)
                        .into_iter()
                        .filter(|(provider, _)| **provider != consumer.id)
                        .collect();
                    let (provider, publication) = match candidates.as_slice() {
                        [(provider, endpoint)] => (
                            Some(clay_host::MicroId::from(*provider)),
                            (*endpoint).to_owned(),
                        ),
                        // The publication is only read of a supplied requirement.
                        [] => (None, request.binding.clone()),
                        several => {
                            errors.push(format!(
                                "{} requires {:?} ({}), which several installed micros serve: {}",
                                consumer.id,
                                request.binding,
                                request.contract,
                                several
                                    .iter()
                                    .map(|(provider, endpoint)| format!("{provider} ({endpoint})"))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ));
                            (None, request.binding.clone())
                        }
                    };
                    (
                        request.binding.clone(),
                        Requirement {
                            provider,
                            publication,
                            // An unknown necessity likely comes from a newer manifest format,
                            // and blocking the start is the conservative reading.
                            necessity: if request.necessity == Declared::Optional {
                                Necessity::Optional
                            } else {
                                Necessity::Required
                            },
                        },
                    )
                })
                .collect()
        })
        .collect();
    (resolved, errors)
}

/// Each installed micro's endpoint that serves `contract`: a method under it, a view
/// implementing it.
fn suppliers<'a>(installed: &'a [InstalledMicro], contract: &Nsid) -> Vec<(&'a MicroId, &'a str)> {
    installed
        .iter()
        .flat_map(|micro| {
            micro
                .sidecar
                .manifest
                .endpoints
                .iter()
                .filter(move |(_, endpoint)| match endpoint {
                    Endpoint::Method { nsid } => nsid == contract,
                    Endpoint::View { implements } => implements.as_ref() == Some(contract),
                    Endpoint::Callback { .. } | Endpoint::Other(_) => false,
                })
                .map(move |(name, _)| (&micro.id, name.as_str()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_mdk::ManifestBuilder;
    use clay_mdk::Sidecar;

    const CONTRACT: &str = "org.example.test.count";

    /// A micro named `name` with `manifest`, whose factory is never built.
    fn micro(name: &str, manifest: ManifestBuilder) -> InstalledMicro {
        InstalledMicro::new(
            MicroId::local(name),
            Sidecar::new(name, 1, manifest.build()),
            |_, _, _| unreachable!("resolution builds no factory"),
        )
    }

    fn consumer(necessity: Declared) -> InstalledMicro {
        micro(
            "consumer",
            ManifestBuilder::new().requires("count", CONTRACT, necessity),
        )
    }

    fn supplied(resolved: &[BTreeMap<String, Requirement>], consumer: usize) -> &Requirement {
        &resolved[consumer]["count"]
    }

    #[test]
    fn a_method_resolves_to_its_sole_provider_under_the_endpoint_name() {
        let installed = [
            consumer(Declared::Required),
            micro("counter", ManifestBuilder::new().method("tally", CONTRACT)),
            micro(
                "other",
                ManifestBuilder::new().method("x", "org.example.test.other"),
            ),
        ];
        let (resolved, errors) = resolve(&installed);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(
            supplied(&resolved, 0),
            &Requirement {
                provider: Some(clay_host::MicroId::from(&MicroId::local("counter"))),
                publication: "tally".into(),
                necessity: Necessity::Required,
            }
        );
    }

    #[test]
    fn a_view_resolves_by_the_contract_it_implements() {
        struct Button;
        impl clay_mdk::ComponentDecl for Button {
            const NAME: &'static str = "button";
            const IMPLEMENTS: Option<&'static str> = Some(CONTRACT);
        }
        struct Plain;
        impl clay_mdk::ComponentDecl for Plain {
            const NAME: &'static str = "plain";
        }
        let installed = [
            consumer(Declared::Required),
            micro(
                "shell",
                ManifestBuilder::new().view::<Button>().view::<Plain>(),
            ),
        ];
        let (resolved, errors) = resolve(&installed);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(supplied(&resolved, 0).publication, "button");
        assert!(supplied(&resolved, 0).provider.is_some());
    }

    #[test]
    fn nothing_serving_the_contract_leaves_the_requirement_unsupplied() {
        let installed = [
            consumer(Declared::Required),
            micro(
                "other",
                ManifestBuilder::new().method("x", "org.example.test.other"),
            ),
        ];
        let (resolved, errors) = resolve(&installed);
        // Not an installation error: the micro installs and blocks when it starts.
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(supplied(&resolved, 0).provider, None);
        assert_eq!(supplied(&resolved, 0).necessity, Necessity::Required);
    }

    #[test]
    fn necessity_carries_over_and_only_a_known_optional_is_optional() {
        for (declared, expected) in [
            (Declared::Optional, Necessity::Optional),
            (Declared::Required, Necessity::Required),
            (Declared::Other("eventually".into()), Necessity::Required),
        ] {
            let (resolved, _) = resolve(&[consumer(declared)]);
            assert_eq!(supplied(&resolved, 0).necessity, expected);
        }
    }

    #[test]
    fn several_providers_are_an_error_naming_each_and_supply_none() {
        let installed = [
            consumer(Declared::Optional),
            micro("first", ManifestBuilder::new().method("tally", CONTRACT)),
            micro("second", ManifestBuilder::new().method("count", CONTRACT)),
        ];
        let (resolved, errors) = resolve(&installed);
        assert_eq!(supplied(&resolved, 0).provider, None);
        let [error] = errors.as_slice() else {
            panic!("one error expected: {errors:?}");
        };
        for named in ["count", CONTRACT, "first (tally)", "second (count)"] {
            assert!(error.contains(named), "{named} missing from {error}");
        }
    }

    #[test]
    fn a_callback_never_supplies_a_requirement() {
        let installed = [
            consumer(Declared::Required),
            micro(
                "counter",
                ManifestBuilder::new().callback("increment", CONTRACT),
            ),
            micro("other", ManifestBuilder::new().callback("bump", CONTRACT)),
        ];
        let (resolved, errors) = resolve(&installed);
        // Neither is a supplier, and two implementers of one contract are not ambiguous.
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(supplied(&resolved, 0).provider, None);
    }
}
