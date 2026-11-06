//! The micro lifecycle controls, as a Settings section.
//!
//! Enabling, disabling and retrying micros, and the installation errors, are the composition's
//! own business: the supervisor and the install documents are not reachable from a micro's
//! `Context`. So the composition links one more built-in micro, [`micro`], whose only job is to
//! contribute a Settings section (a manifest-declared row, so it is listed while the micro is
//! stopped) and publish the presentation that section attaches. That presentation is mounted
//! in-process by the shell's details position, so it reads the [`CompositionRuntime`] from the
//! Dioxus context [`Application`](crate::Application) provides, the same way the shell reads
//! its `HostInputs`.
use crate::CompositionRuntime;
use crate::InstalledMicro;
use clay_host::MicroId as SupervisorId;
use clay_host::MicroState;
use clay_mdk::host::factory;
use clay_mdk::Access;
use clay_mdk::AttachmentDeclaration;
use clay_mdk::BindingResolution;
use clay_mdk::ComponentDecl;
use clay_mdk::ContributionDeclaration;
use clay_mdk::Entrypoints;
use clay_mdk::MicroId;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::Nsid;
use clay_mdk::PublicationDeclaration;
use clay_mdk::PublicationKind;
use clay_mdk::Request;
use clay_mdk::Sidecar;
use dioxus::prelude::*;
use std::collections::BTreeSet;
use std::rc::Rc;

pub const ID: &str = "dayframe.lifecycle";
const AUTHORITY: &str = "org.example.dayframe.lifecycle";
/// The one publication: the presentation the Settings section attaches.
const VIEW: &str = "view";

/// The lifecycle controls' one presentation, published as [`VIEW`].
struct LifecycleView;

impl ComponentDecl for LifecycleView {
    const NAME: &'static str = VIEW;
}
const SECTIONS_BINDING: &str = "sections";

fn manifest() -> MicroManifest {
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: vec![PublicationDeclaration {
            name: VIEW.into(),
            kind: PublicationKind::Presentation,
            contract: None,
        }],
        requires: vec![],
        configuration_requests: vec![],
        collection_contracts: vec![],
        collection_requests: vec![Request {
            binding: SECTIONS_BINDING.into(),
            contract: Nsid::literal(micro_settings_manifest::SECTIONS),
            access: Some(Access::Write),
            necessity: Necessity::Required,
            resolution: BindingResolution::Configured,
            min_revision: None,
        }],
        contributions: vec![ContributionDeclaration {
            id: "section".into(),
            binding: SECTIONS_BINDING.into(),
            key: ID.into(),
            payload: serde_json::json!({
                "title": "Micro lifecycle",
                "detail": { "$attachment": 0 },
            }),
            attachments: vec![AttachmentDeclaration {
                index: 0,
                publication: VIEW.into(),
            }],
        }],
        extra: Default::default(),
    }
}

/// The lifecycle controls as a catalog entry. Locked: the controls cannot disable themselves
/// away, and they have no install datum.
pub fn micro() -> InstalledMicro {
    InstalledMicro {
        locked: true,
        ..InstalledMicro::new(
            MicroId::local(ID),
            Sidecar::new(ID, 1, manifest()).with_authority(AUTHORITY),
            |id, resources, bindings| {
                factory(resources, id, manifest(), bindings, |context| async move {
                    context.publish::<LifecycleView>(|| rsx! { LifecyclePanel {} })?;
                    Ok(())
                })
            },
        )
    }
}

/// Every micro's state with enable/disable/retry controls, and the installation errors.
#[component]
fn LifecyclePanel() -> Element {
    let shell = use_context::<CompositionRuntime>();
    // The lifecycle shows the supervisor's states: re-render when any of them changes.
    let mut states = use_signal(|| 0_u64);
    use_future({
        let supervisor = Rc::clone(&shell.supervisor);
        move || {
            let mut changes = supervisor.changes();
            async move {
                while changes.changed().await.is_ok() {
                    states += 1;
                }
            }
        }
    });
    let _ = states();
    let mut failure = use_signal(|| None::<String>);
    // Micros whose toggle is being written; the button is inert until the datum is stored.
    let mut toggling = use_signal(BTreeSet::<MicroId>::new);
    use_future({
        let failures = shell.lifecycle_failures();
        move || {
            let mut failures = failures.clone();
            async move {
                while failures.changed().await.is_ok() {
                    failure.set(failures.borrow_and_update().clone());
                }
            }
        }
    });

    rsx! {
        div { class: "flex flex-col gap-4",
            p { "Enablement and configuration are stored in documents on this device. A mock node stands in for sync, so they are not shared with other devices." }
            for error in shell.installation_errors.clone() {
                p { key: "install-{error}", role: "status", "Package installation failed: {error}" }
            }
            if let Some(problem) = failure() {
                p { role: "alert", "{problem}" }
            }
            for micro in shell.micros() {
                p {
                    key: "status-{micro}",
                    role: "status",
                    {
                        let state = match shell.state(micro) {
                            MicroState::Unconfigured(missing) => {
                                format!("needs configuration ({})", missing.join(", "))
                            }
                            state => format!("{state:?}"),
                        };
                        if shell.is_locked(micro) {
                            format!("{} runtime (locked): {state}", micro.name())
                        } else {
                            format!("{} runtime: {state}", micro.name())
                        }
                    }
                }
            }
            div { class: "flex flex-wrap gap-2",
                for micro in shell.micros().to_vec() {
                    if !shell.is_locked(&micro) {
                        {
                            let state = shell.state(&micro);
                            let toggle = micro.clone();
                            let runtime = shell.clone();
                            // The label follows what the user asked for, not the supervisor's
                            // state, which lags it while a close is in flight.
                            let wanted = shell.is_enabled(&micro);
                            // Labels show the plug name, never the namespace DID.
                            let name = micro.name().to_owned();
                            let label = if wanted {
                                format!("Disable {name} micro")
                            } else {
                                format!("Enable {name} micro")
                            };
                            let busy = toggling.read().contains(&micro);
                            let retry = matches!(state, MicroState::Faulted(_)).then(|| {
                                (Rc::clone(&shell.supervisor), SupervisorId::from(&micro))
                            });
                            rsx! {
                                button {
                                    key: "toggle-{micro}",
                                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                    "aria-label": "{label}",
                                    disabled: busy,
                                    onclick: move |_| {
                                        let runtime = runtime.clone();
                                        let micro = toggle.clone();
                                        toggling.write().insert(micro.clone());
                                        spawn(async move {
                                            runtime.set_enabled(&micro, !wanted).await;
                                            toggling.write().remove(&micro);
                                        });
                                    },
                                    "{label}"
                                }
                                if let Some((supervisor, micro)) = retry {
                                    button {
                                        key: "retry-{micro}",
                                        class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                        "aria-label": "Retry {name} micro",
                                        onclick: move |_| supervisor.retry(&micro),
                                        "Retry {name} micro"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
