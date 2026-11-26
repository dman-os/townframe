//! The toolbar owns the button vocabulary; the host owns storage and authority. Authored against
//! the MDK only (ADR 015 §5): activation receives a [`Context`], never the registry, renderer, or
//! host kernel. Each button is a row of the buttons collection with a lazy reference to the
//! call it performs, so the toolbar renders while every contributor is stopped and only a press
//! starts one (ADR 031 §13).
pub use micro_toolbar_manifest as contract;

/// The toolbar's own stylesheet: the layout of its parts, which the kit has no component for.
const STYLES: &str = include_str!("styles.css");

use clay_mdk::Capability;
use clay_mdk::Collection;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::hooks::Stylesheet;
use clay_mdk::hooks::use_ref_status;
use clay_mdk::hooks::use_rows;
use contract::Button;
use dioxus::prelude::*;
use std::collections::BTreeSet;

/// The toolbar reads the declared buttons collection (its configuration binds it) and publishes
/// its view, which the shell mounts as a root.
pub async fn activate(context: Context) -> Result<(), Error> {
    let buttons: Collection<String, Button> = context
        .collection::<contract::ToolbarButtons>()
        .expect("configuration binds the buttons collection");
    context.publish::<contract::ToolbarView>(move || view(&buttons))?;
    Ok(())
}

/// The toolbar's model and view in one render. A press calls the button's link
/// (empty request, empty response); the redeemed call holds its provider live until it returns,
/// and a failure is shown, not swallowed.
fn view(buttons: &Collection<String, Button>) -> Element {
    let rows = use_rows(buttons);
    // Keys whose call is in flight: the button is inert meanwhile, so one press is one call.
    let pending = use_signal(BTreeSet::<String>::new);
    let failure = use_signal(|| None::<String>);
    rsx! {
        div { class: "ct-toolbar", role: "toolbar", "aria-label": "Toolbar",
            Stylesheet { css: STYLES }
            div { "part": "toolbar-buttons",
                for row in rows.iter() {
                    ToolbarButton {
                        key: "{row.key}",
                        id: row.key.clone(),
                        label: row.value.label.clone(),
                        icon: row.value.icon.clone(),
                        action: row.links[row.value.action.index()].clone(),
                        pending,
                        failure,
                    }
                }
            }
            if let Some(problem) = failure() {
                p { role: "alert", "{problem}" }
            }
        }
    }
}

/// One button. Its provider's status is followed without starting it, so a contributor that
/// cannot run (a discovered package not yet enabled in Settings, a faulted micro) shows an
/// inert button that says why, and comes back by itself when the provider can start.
#[component]
fn ToolbarButton(
    id: String,
    label: String,
    icon: String,
    action: Capability,
    pending: Signal<BTreeSet<String>>,
    failure: Signal<Option<String>>,
) -> Element {
    let status = use_ref_status(&action);
    let blocked = status.blocked_reason();
    let label_text = match blocked {
        Some(reason) => format!("{label} (unavailable: {reason})"),
        None => label.clone(),
    };
    let disabled = blocked.is_some() || pending.read().contains(&id);
    rsx! {
        // The kit's `Button` is not used here: a button that says why it is unavailable carries
        // `aria-description`, which is native-only (not in the guest wire vocabulary a kit
        // component is held to), so the toolbar emits the button vocabulary of the styling contract
        // (`ct-button`, `data-variant`, `part="button-label"`) itself and takes the kit's skin.
        button {
            class: "ct-button",
            "part": "button variant-outline size-md",
            r#type: "button",
            "data-variant": "outline",
            "data-size": "md",
            "data-disabled": disabled.then_some(""),
            "data-icon": "{icon}",
            "aria-label": "{label}",
            "aria-description": blocked.map(|reason| format!("Unavailable: {reason}")),
            title: "{label_text}",
            disabled,
            onclick: move |_| {
                let id = id.clone();
                let action = action.clone();
                pending.write().insert(id.clone());
                failure.set(None);
                spawn(async move {
                    if let Err(error) = action.call::<(), ()>(&()).await {
                        failure.set(Some(format!("{id}: {error}")));
                    }
                    pending.write().remove(&id);
                });
            },
            span { "part": "button-label", "{label}" }
        }
    }
}
