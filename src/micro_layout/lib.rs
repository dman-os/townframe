//! The layout, authored once against the MDK's [`Context`] and exported as a `micro-client`
//! guest on wasm32-wasip3 (and as the native `ACTIVATE` entry everywhere else): it arranges
//! other micros' panels (ADR 031 §13).
//!
//! The mechanism: the `layout.panels` collection describes the installed panels, each row a
//! title and a lazy presentation reference; the viewer's [`LayoutConfig`] (a facet of the
//! layout's configuration document) lists the registration ids that are shown. The view mounts
//! exactly the configured ids, each through a [`PresentationSlot`], and only a mounted slot
//! redeems its reference, so a panel that is installed but not shown never activates its
//! provider. A configured id with no row, or whose provider is unavailable, is a placeholder in
//! its place; nothing here ever edits the configured list except the viewer's own toggles.
use clay_dioxus_kit::AppBar;
use clay_mdk::Capability;
use clay_mdk::Collection;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::Facet;
use clay_mdk::FacetState;
use clay_mdk::hooks::PresentationSlot;
use clay_mdk::hooks::use_facet;
use clay_mdk::hooks::use_rows;
use dioxus::prelude::*;
use micro_layout_manifest::CONFIG_KEY;
use micro_layout_manifest::CONFIG_NAME;
use micro_layout_manifest::CONFIG_TAG;
use micro_layout_manifest::LayoutConfig;
use micro_layout_manifest::PANELS_BINDING;
use micro_layout_manifest::Panel;
use micro_layout_manifest::SHELL_NAV_BUTTON;
use micro_layout_manifest::VIEW;

/// Bind the panels collection and the arrangement facet, and publish the view over them. The
/// shell's sidebar/back button is required (the view mounts it in its app bar), so the layout can
/// be left on a compact host.
async fn activate(context: Context) -> Result<(), Error> {
    let panels: Collection<String, Panel> = context
        .collection(PANELS_BINDING)
        .expect("configuration binds the panels collection");
    let config = context
        .config(CONFIG_NAME)
        .expect("manifest declares the layout configuration")
        .facet::<LayoutConfig>(CONFIG_TAG, CONFIG_KEY)?;
    let nav_button = context
        .require(SHELL_NAV_BUTTON)
        .expect("a required binding is always resolved")
        .clone();
    context.publish_component(VIEW, move || {
        view(panels.clone(), config.clone(), nav_button.clone())
    })?;
    Ok(())
}

clay_mdk::export_micro!(activate, micro_layout_manifest::manifest);

/// The layout's model and view in one root-scope render. First the app bar, then the toggles, one
/// per installed panel (the viewer's way to edit the arrangement), then the arrangement itself.
fn view(
    panels: Collection<String, Panel>,
    config: Facet<LayoutConfig>,
    nav_button: Capability,
) -> Element {
    let rows = use_rows(&panels);
    let snapshot = use_facet(&config);
    let mut failure = use_signal(|| None::<String>);
    let arrangement = match snapshot.state {
        FacetState::Loading => {
            return rsx! { p { role: "status", "Loading layout…" } };
        }
        // No document yet: nothing is shown (see `LayoutConfig`).
        FacetState::Absent => LayoutConfig::default(),
        FacetState::Present(arrangement) => arrangement,
    };
    rsx! {
        div { class: "flex flex-col gap-4",
            AppBar {
                title: "Layout",
                leading: rsx! { PresentationSlot { reference: nav_button.clone() } },
            }
            div { class: "mx-4 flex flex-wrap gap-2", role: "toolbar", "aria-label": "Panels",
                for row in rows.iter() {
                    {
                        let shown = arrangement.visible.contains(&row.key);
                        let label = if shown {
                            format!("Hide {} panel", row.value.title)
                        } else {
                            format!("Show {} panel", row.value.title)
                        };
                        let key = row.key.clone();
                        let arrangement = arrangement.clone();
                        let config = config.clone();
                        rsx! {
                            button {
                                key: "toggle-{row.key}",
                                class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                                "aria-label": "{label}",
                                "aria-pressed": "{shown}",
                                onclick: move |_| {
                                    let mut next = arrangement.clone();
                                    if shown {
                                        next.visible.retain(|id| *id != key);
                                    } else {
                                        next.visible.push(key.clone());
                                    }
                                    failure.set(config.write(next).err().map(|error| format!("Write failed: {error}")));
                                },
                                "{label}"
                            }
                        }
                    }
                }
            }
            if let Some(problem) = failure() {
                p { class: "mx-4", role: "alert", "{problem}" }
            }
            for id in arrangement.visible.iter() {
                {
                    let row = rows.iter().find(|row| row.key == *id);
                    let title = row.map_or(id.as_str(), |row| row.value.title.as_str());
                    rsx! {
                        section {
                            key: "panel-{id}",
                            class: "mx-4 rounded-md border border-border p-4",
                            "aria-label": "{title} panel",
                            match row {
                                Some(row) => rsx! {
                                    PresentationSlot {
                                        reference: row.attachments[row.value.content.index()].clone(),
                                    }
                                },
                                None => rsx! {
                                    p { role: "status", "Panel {id} is not installed." }
                                },
                            }
                        }
                    }
                }
            }
        }
    }
}
