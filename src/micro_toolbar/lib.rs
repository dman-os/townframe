//! The toolbar owns the button vocabulary; the host owns storage and authority. Authored against
//! the MDK only (ADR 015 §5): activation receives a [`Context`], never the registry, renderer, or
//! host kernel. Each button is a row of the buttons collection with a lazy reference to the
//! call it performs, so the toolbar renders while every contributor is stopped and only a press
//! starts one (ADR 031 §13).
pub use micro_toolbar_manifest as contract;

use clay_mdk::Collection;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::hooks::use_rows;
use contract::Button;
use dioxus::prelude::*;
use std::collections::BTreeSet;

/// The toolbar reads the declared buttons collection (its configuration binds it) and publishes
/// its view, which the shell mounts as a root.
pub async fn activate(context: Context) -> Result<(), Error> {
    let buttons: Collection<String, Button> = context
        .collection(contract::BUTTONS_BINDING)
        .expect("configuration binds the buttons collection");
    context.publish_component(contract::VIEW, move || view(&buttons))?;
    Ok(())
}

/// The toolbar's model and view in one root-scope render. A press calls the button's attachment
/// (empty request, empty response); the redeemed call holds its provider live until it returns,
/// and a failure (a disabled or faulted provider) is shown, not swallowed.
fn view(buttons: &Collection<String, Button>) -> Element {
    let rows = use_rows(buttons);
    // Keys whose call is in flight: the button is inert meanwhile, so one press is one call.
    let mut pending = use_signal(BTreeSet::<String>::new);
    let mut failure = use_signal(|| None::<String>);
    rsx! {
        div { class: "flex flex-col gap-2", role: "toolbar", "aria-label": "Toolbar",
            div { class: "flex flex-wrap gap-2",
                for row in rows.iter() {
                    {
                        let key = row.key.clone();
                        let action = row.attachments[row.value.action.index()].clone();
                        rsx! {
                            button {
                                key: "{row.key}",
                                class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                "aria-label": "{row.value.label}",
                                "data-icon": "{row.value.icon}",
                                disabled: pending.read().contains(&row.key),
                                onclick: move |_| {
                                    let key = key.clone();
                                    let action = action.clone();
                                    pending.write().insert(key.clone());
                                    failure.set(None);
                                    spawn(async move {
                                        if let Err(error) = action.call::<(), ()>(&()).await {
                                            failure.set(Some(format!("{key}: {error}")));
                                        }
                                        pending.write().remove(&key);
                                    });
                                },
                                "{row.value.label}"
                            }
                        }
                    }
                }
            }
            if let Some(problem) = failure() {
                p { role: "alert", "{problem}" }
            }
        }
    }
}
