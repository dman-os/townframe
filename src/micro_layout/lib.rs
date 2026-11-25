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
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::Card;
use clay_dioxus_kit::CardContent;
use clay_dioxus_kit::CardVariant;
use clay_dioxus_kit::PanelPage;
use clay_dioxus_kit::use_pane_rack;
use clay_mdk::Capability;
use clay_mdk::Collection;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::Facet;
use clay_mdk::FacetState;
use clay_mdk::hooks::PresentationSlot;
use clay_mdk::hooks::Stylesheet;
use clay_mdk::hooks::use_facet;
use clay_mdk::hooks::use_rows;
use dioxus::prelude::*;
use micro_layout_manifest::LayoutArrangement;
use micro_layout_manifest::LayoutConfig;
use micro_layout_manifest::LayoutPanels;
use micro_layout_manifest::LayoutView;
use micro_layout_manifest::Panel;
use micro_layout_manifest::SHELL_NAVIGATION_RACK;

/// Bind the panels collection and the arrangement facet, and publish the view over them. The
/// shell's navigation rack is required (the view mounts it in its app bar), so the layout can
/// be left on a compact host.
async fn activate(context: Context) -> Result<(), Error> {
    let panels: Collection<String, Panel> = context
        .collection::<LayoutPanels>()
        .expect("configuration binds the panels collection");
    let config = context
        .config::<LayoutArrangement>()?
        .expect("manifest declares the layout configuration");
    let navigation_rack = context
        .require(SHELL_NAVIGATION_RACK)
        .expect("a required binding is always resolved")
        .clone();
    context
        .publish::<LayoutView>(move || view(panels.clone(), config.clone(), navigation_rack.clone()))?;
    Ok(())
}

clay_mdk::export_micro!(activate, micro_layout_manifest::manifest);

/// The layout's own stylesheet: the layout of its parts, which the kit has no component for. It
/// names no colour, so a theme restyles the layout through the kit's tokens and parts.
const STYLES: &str = include_str!("styles.css");

/// The layout's model and view in one render. First the app bar, then the toggles, one
/// per installed panel (the viewer's way to edit the arrangement), then the arrangement itself.
fn view(
    panels: Collection<String, Panel>,
    config: Facet<LayoutConfig>,
    navigation_rack: Capability,
) -> Element {
    use_pane_rack(rsx! { PresentationSlot { reference: navigation_rack } });
    let rows = use_rows(&panels);
    let snapshot = use_facet(&config);
    let mut failure = use_signal(|| None::<String>);
    // Every state is the body of one frame (see `framed`). `Loading` is also what a released or
    // revoked document watch reads as, so it must keep the frame's app bar (the shell's
    // navigation rack): without it a compact host has no way out of the panel.
    let arrangement = match snapshot.state {
        FacetState::Loading => {
            return framed(rsx! { p { role: "status", "Loading layout…" } });
        }
        // No document yet: nothing is shown (see `LayoutConfig`).
        FacetState::Absent => LayoutConfig::default(),
        FacetState::Present(arrangement) => arrangement,
    };
    framed(
        rsx! {
            div { "data-part": "toggles", role: "toolbar", "aria-label": "Panels",
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
                            Button {
                                key: "toggle-{row.key}",
                                variant: ButtonVariant::Outline,
                                label: label.clone(),
                                pressed: Some(shown),
                                onclick: move |()| {
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
                p { "data-part": "problem", role: "alert", "{problem}" }
            }
            for id in arrangement.visible.iter() {
                {
                    let row = rows.iter().find(|row| row.key == *id);
                    let title = row.map_or(id.as_str(), |row| row.value.title.as_str());
                    rsx! {
                        section {
                            key: "panel-{id}",
                            "data-part": "slot",
                            "aria-label": "{title} panel",
                            Card { variant: CardVariant::Outline,
                                CardContent {
                                    match row {
                                        Some(row) => rsx! {
                                            PresentationSlot {
                                                reference: row.links[row.value.content.index()].clone(),
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
        },
    )
}

/// The layout's frame: the app bar, then `body`, under the shell's navigation rack that
/// `use_pane_rack` places at the top-left. Every state of the view goes through it, so no state
/// leaves the panel without a way out.
fn framed(body: Element) -> Element {
    rsx! {
        PanelPage { app_bar: rsx! { AppBar { title: "Layout" } },
            div { class: "ct-layout",
                Stylesheet { css: STYLES }
                {body}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dioxus_core::Mutation;
    use dioxus_core::VirtualDom;

    // The pane's rack is rendered beside the body whatever the body is, which is what keeps the
    // `Loading layout…` state from being a dead end on a compact host.
    #[test]
    fn the_frame_keeps_the_rack_beside_a_loading_body() {
        fn app() -> Element {
            use_pane_rack(rsx! { button { "aria-label": "Open sidebar" } });
            framed(rsx! { p { role: "status", "Loading layout…" } })
        }
        let mut dom = VirtualDom::new(app);
        let edits = dom.rebuild_to_vec().edits;
        let attribute = |value: &str| {
            edits.iter().any(|edit| {
                matches!(edit, Mutation::SetAttribute { value: dioxus_core::AttributeValue::Text(text), .. } if text == value)
            })
        };
        let text = |value: &str| {
            edits
                .iter()
                .any(|edit| matches!(edit, Mutation::CreateText { value: text } if text == value))
        };
        assert!(attribute("Open sidebar"), "{edits:?}");
        assert!(text("Loading layout…"), "{edits:?}");
        assert!(text("Layout"), "the app bar's title: {edits:?}");
    }
}
