//! The toolbar owns the button vocabulary; the host owns storage and authority. Authored against
//! the MDK only (ADR 015 §5): activation receives a [`Context`], never the registry, renderer, or
//! host kernel. Each button is a row of the buttons collection with a lazy reference to the
//! call it performs, so the toolbar renders while every contributor is stopped and only a press
//! starts one (ADR 031 §13).
pub use micro_toolbar_manifest as contract;

use clay_mdk::Capability;
use clay_mdk::Collection;
use clay_mdk::Context;
use clay_mdk::Error;
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

/// The toolbar's model and view in one render. A press calls the button's attachment
/// (empty request, empty response); the redeemed call holds its provider live until it returns,
/// and a failure is shown, not swallowed.
fn view(buttons: &Collection<String, Button>) -> Element {
    let rows = use_rows(buttons);
    // Keys whose call is in flight: the button is inert meanwhile, so one press is one call.
    let pending = use_signal(BTreeSet::<String>::new);
    let failure = use_signal(|| None::<String>);
    rsx! {
        div { class: "flex flex-col gap-2", role: "toolbar", "aria-label": "Toolbar",
            div { class: "flex flex-wrap gap-2",
                for row in rows.iter() {
                    ToolbarButton {
                        key: "{row.key}",
                        id: row.key.clone(),
                        label: row.value.label.clone(),
                        icon: row.value.icon.clone(),
                        action: row.attachments[row.value.action.index()].clone(),
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
    rsx! {
        button {
            class: "rounded-md border border-border px-4 py-2 focus-visible:outline disabled:opacity-50",
            "aria-label": "{label}",
            "aria-description": blocked.map(|reason| format!("Unavailable: {reason}")),
            title: "{label_text}",
            "data-icon": "{icon}",
            disabled: blocked.is_some() || pending.read().contains(&id),
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
            "{label}"
        }
    }
}
