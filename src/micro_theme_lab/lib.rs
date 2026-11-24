//! The theme lab, authored once against the MDK's [`Context`]: the end user's theme editor, the
//! home of iterating on the design language. A theme is one stylesheet of CSS text. The lab shows
//! it in a plain monospace textarea, previews the draft on the specimen board (every foundation
//! and kit component page, `clay_dioxus_specimens`) before anything is applied, and Apply sends it to the host, which applies it to
//! the shell and every micro live and keeps it across restarts. Revert returns the draft to what
//! is applied.
//!
//! The preview is the draft as this micro's own stylesheet (`Stylesheet`), which is the
//! cascade every micro has: the host's applied theme sits in `@layer user`, beneath a micro's own
//! rules, so the draft (not the applied sheet) decides what the lab shows. The host refuses an
//! applied theme that could make it fetch anything (`url(`, `@import`, `image-set(`, ...); the
//! preview is sanitized by the same rule on its way to the mount, so a draft's fetch paths are
//! silently absent from the preview and Apply reports the refusal.
//!
//! [`clay_mdk::export_micro!`] exports the lab as the `micro-client` guest on wasm32-wasip3 and as
//! the native `ACTIVATE` entry everywhere else.
use clay_dioxus_kit::Alert;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::PanelPage;
use clay_dioxus_kit::Renderer;
use clay_dioxus_kit::Status as Tone;
use clay_dioxus_specimens::AlertSpecimens;
use clay_dioxus_specimens::AppBarSpecimens;
use clay_dioxus_specimens::ButtonSpecimens;
use clay_dioxus_specimens::CardSpecimens;
use clay_dioxus_specimens::ChipSpecimens;
use clay_dioxus_specimens::DrawerSpecimens;
use clay_dioxus_specimens::Foundation;
use clay_dioxus_specimens::FoundationPage;
use clay_dioxus_specimens::ListDetailsSpecimens;
use clay_dioxus_specimens::ListRowSpecimens;
use clay_dioxus_specimens::NavButtonSpecimens;
use clay_dioxus_specimens::PanelPageSpecimens;
use clay_dioxus_specimens::ScrubStripSpecimens;
use clay_dioxus_specimens::SectionsSpecimens;
use clay_dioxus_specimens::SeparatorSpecimens;
use clay_dioxus_specimens::SheetSpecimens;
use clay_dioxus_specimens::TabsSpecimens;
use clay_mdk::Capability;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::hooks::PresentationSlot;
use clay_mdk::hooks::Stylesheet;
use dioxus::prelude::*;
use micro_shell_manifest::ThemeSheet;
use micro_theme_lab_manifest::SHELL_GET_THEME;
use micro_theme_lab_manifest::SHELL_NAV_BUTTON;
use micro_theme_lab_manifest::SHELL_SET_THEME;
use micro_theme_lab_manifest::ThemeLabView;
use std::collections::BTreeMap;
use std::rc::Rc;

/// What the view asks of the host: the shell's sidebar/back button, and the theme calls, which a
/// view may await (`Capability::call` runs them on a task of the incarnation).
#[derive(Clone)]
struct Host {
    nav_button: Capability,
    get_theme: Capability,
    set_theme: Capability,
}

async fn activate(context: Context) -> Result<(), Error> {
    let required = |binding: &str| {
        context
            .require(binding)
            .expect("a required binding is always resolved")
            .clone()
    };
    let host = Host {
        nav_button: required(SHELL_NAV_BUTTON),
        get_theme: required(SHELL_GET_THEME),
        set_theme: required(SHELL_SET_THEME),
    };
    context.publish::<ThemeLabView>(move || view(host.clone()))?;
    Ok(())
}

clay_mdk::export_micro!(activate, micro_theme_lab_manifest::manifest);

/// The lab's own stylesheet: the layout of its parts, the monospace field and the text roles, which
/// the kit has no component for. It names no colour that is not a kit token. While the draft
/// previews, the draft follows it in the same `Stylesheet` (a micro has one stylesheet, the
/// latest `Stylesheet`), so a draft rule beats a rule here.
const STYLES: &str = include_str!("styles.css");

/// The outcome the status line shows after the last Apply.
#[derive(Clone, PartialEq)]
enum Status {
    Idle,
    Applied,
    /// The host refused the theme or could not store it: its reason, shown as it gave it.
    Failed(String),
}

/// The lab's model and view in one root-scope render. `render` runs in a component scope of its
/// own for each mount, so these signals belong to one surface.
fn view(host: Host) -> Element {
    // What the host applies now; read once on mount and again after each Apply.
    let mut applied = {
        let host = host.clone();
        use_resource(move || {
            let host = host.clone();
            async move { host.get_theme.call::<(), ThemeSheet>(&()).await }
        })
    };
    // The draft, `None` until the first edit: until then the textarea shows what is applied, and
    // Revert returns to that state.
    let mut draft = use_signal(|| None::<String>);
    let mut preview = use_signal(|| true);
    let mut status = use_signal(|| Status::Idle);
    let mut applying = use_signal(|| false);

    let leading = rsx! { PresentationSlot { reference: host.nav_button.clone() } };
    let applied_css = match &*applied.read() {
        Some(Ok(sheet)) => sheet.css.clone(),
        Some(Err(error)) => {
            return page(
                leading,
                rsx! { p { role: "alert", "The host's theme could not be read: {error}" } },
            );
        }
        None => {
            return page(leading, rsx! { p { role: "status", "Loading the theme…" } });
        }
    };
    let text = draft().unwrap_or_else(|| applied_css.clone());
    let dirty = text != applied_css;
    let preview_css = if preview() {
        text.clone()
    } else {
        String::new()
    };

    let apply = {
        let host = host.clone();
        let text = text.clone();
        move |()| {
            let host = host.clone();
            let sheet = ThemeSheet { css: text.clone() };
            applying.set(true);
            spawn(async move {
                let outcome = host.set_theme.call::<ThemeSheet, ()>(&sheet).await;
                applying.set(false);
                match outcome {
                    Ok(()) => {
                        status.set(Status::Applied);
                        draft.set(None);
                        applied.restart();
                    }
                    Err(error) => status.set(Status::Failed(error.to_string())),
                }
            });
        }
    };

    // What the canvas shows, and the width it is framed at; both belong to this mount.
    let selection = use_signal(|| Selection::Foundation(Foundation::Colour));
    let frame = use_signal(|| Frame::Fill);
    // The pages' sections register their mounted handles here (`FoundationSection`), and the
    // Tabs and ScrubStrip pages read the renderer. A micro renders under a browser engine on
    // every host that adopts its shadow root (Wry, the web build); Blitz never shapes this.
    use_context_provider(|| Signal::new(BTreeMap::<String, Rc<MountedData>>::new()));
    use_context_provider(|| Renderer::Browser);

    page(
        leading,
        rsx! {
            // The draft is this micro's stylesheet while it previews, which the host adopts for
            // this mount alone. It follows the lab's own rules in the one stylesheet a micro has.
            Stylesheet { css: format!("{STYLES}\n{preview_css}") }
            div { class: "ct-theme-lab",
                Navigator { selection }
                Canvas { selection: selection(), frame }
                aside { "data-part": "inspector", "aria-label": "Inspector",
                    section { "data-part": "editor", "aria-label": "Theme CSS",
                        label { "data-part": "heading", r#for: "theme-css", "Theme CSS" }
                        textarea {
                            id: "theme-css",
                            "data-part": "field",
                            spellcheck: "false",
                            rows: "14",
                            value: "{text}",
                            oninput: move |event| {
                                draft.set(Some(event.value()));
                                status.set(Status::Idle);
                            },
                        }
                        div { "data-part": "actions",
                            Button {
                                variant: ButtonVariant::Outline,
                                label: "Apply theme".to_owned(),
                                disabled: !dirty || applying(),
                                onclick: apply,
                                "Apply"
                            }
                            Button {
                                variant: ButtonVariant::Outline,
                                label: "Revert to the applied theme".to_owned(),
                                disabled: !dirty,
                                onclick: move |()| {
                                    draft.set(None);
                                    status.set(Status::Idle);
                                },
                                "Revert"
                            }
                            label { "data-part": "toggle",
                                input {
                                    r#type: "checkbox",
                                    checked: preview(),
                                    onchange: move |event| {
                                        preview.set(event.checked());
                                    },
                                }
                                "Preview the draft here"
                            }
                        }
                        match status() {
                            Status::Idle => rsx! {},
                            Status::Applied => rsx! {
                                Alert { status: Tone::Success, "Applied to the whole app." }
                            },
                            Status::Failed(reason) => rsx! {
                                Alert { status: Tone::Danger, "Not applied: {reason}" }
                            },
                        }
                    }
                }
            }
        },
    )
}

/// The lab's frame: the app bar with `leading` (the shell's sidebar/back button), then `body`.
/// Every state of the view goes through it, so none leaves the panel without a way out.
fn page(leading: Element, body: Element) -> Element {
    rsx! {
        PanelPage { app_bar: rsx! { AppBar { title: "Theme lab", leading } }, {body} }
    }
}

/// What the canvas shows: one entry of the navigator.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Selection {
    /// The stylesheet stack. One sheet today, the theme the lab edits.
    Sheets,
    Foundation(Foundation),
    Component(Component),
}

/// The kit components the board has a specimen page for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Component {
    Button,
    Card,
    Alert,
    Chip,
    Tabs,
    Separator,
    AppBar,
    NavButton,
    ListRow,
    ListDetails,
    PanelPage,
    Sections,
    Sheet,
    Drawer,
    ScrubStrip,
}

impl Component {
    const ALL: [Component; 15] = [
        Self::AppBar,
        Self::Alert,
        Self::Button,
        Self::Card,
        Self::Chip,
        Self::Drawer,
        Self::ListDetails,
        Self::ListRow,
        Self::NavButton,
        Self::PanelPage,
        Self::ScrubStrip,
        Self::Sections,
        Self::Separator,
        Self::Sheet,
        Self::Tabs,
    ];

    fn title(self) -> &'static str {
        match self {
            Self::Button => "Button",
            Self::Card => "Card",
            Self::Alert => "Alert",
            Self::Chip => "Chip",
            Self::Tabs => "Tabs",
            Self::Separator => "Separator",
            Self::AppBar => "App bar",
            Self::NavButton => "Nav button",
            Self::ListRow => "List row",
            Self::ListDetails => "List details",
            Self::PanelPage => "Panel page",
            Self::Sections => "Sections",
            Self::Sheet => "Sheet",
            Self::Drawer => "Drawer",
            Self::ScrubStrip => "Scrub strip",
        }
    }

    fn render(self) -> Element {
        match self {
            Self::Button => rsx! { ButtonSpecimens {} },
            Self::Card => rsx! { CardSpecimens {} },
            Self::Alert => rsx! { AlertSpecimens {} },
            Self::Chip => rsx! { ChipSpecimens {} },
            Self::Tabs => rsx! { TabsSpecimens {} },
            Self::Separator => rsx! { SeparatorSpecimens {} },
            Self::AppBar => rsx! { AppBarSpecimens {} },
            Self::NavButton => rsx! { NavButtonSpecimens {} },
            Self::ListRow => rsx! { ListRowSpecimens {} },
            Self::ListDetails => rsx! { ListDetailsSpecimens {} },
            Self::PanelPage => rsx! { PanelPageSpecimens {} },
            Self::Sections => rsx! { SectionsSpecimens {} },
            Self::Sheet => rsx! { SheetSpecimens {} },
            Self::Drawer => rsx! { DrawerSpecimens {} },
            // The strip sizes itself to a viewport; the canvas gives it a fixed one, as the
            // showcase does, since a micro does not know its mount's size yet.
            Self::ScrubStrip => rsx! { ScrubStripSpecimens { viewport: (720.0, 860.0) } },
        }
    }
}

/// The width the canvas renders at: the whole pane, or one of the kit's width classes, so a
/// sheet is judged at phone, tablet and desktop widths without resizing the window.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Frame {
    Fill,
    Compact,
    Medium,
    Expanded,
}

impl Frame {
    const ALL: [Frame; 4] = [Self::Fill, Self::Compact, Self::Medium, Self::Expanded];

    fn title(self) -> &'static str {
        match self {
            Self::Fill => "Fill",
            Self::Compact => "Compact · 390",
            Self::Medium => "Medium · 700",
            Self::Expanded => "Expanded · 960",
        }
    }

    /// The frame's width in CSS pixels, or `None` to fill the pane.
    fn width_px(self) -> Option<u32> {
        match self {
            Self::Fill => None,
            Self::Compact => Some(390),
            Self::Medium => Some(700),
            Self::Expanded => Some(960),
        }
    }
}

/// The navigator: every section of the lab, grouped as the stack, the foundations and the
/// components. One entry is current; the canvas shows it.
#[component]
fn Navigator(selection: Signal<Selection>) -> Element {
    let mut selection = selection;
    let entry = move |title: &'static str, target: Selection| {
        rsx! {
            Button {
                key: "{title}",
                variant: ButtonVariant::Ghost,
                current: selection() == target,
                onclick: move |()| selection.set(target),
                "{title}"
            }
        }
    };
    rsx! {
        nav { "data-part": "navigator", "aria-label": "Theme lab sections",
            h2 { "data-part": "group", "Sheets" }
            {entry("Your theme", Selection::Sheets)}
            h2 { "data-part": "group", "Tokens" }
            for foundation in Foundation::ALL {
                {entry(foundation.title(), Selection::Foundation(foundation))}
            }
            h2 { "data-part": "group", "Components" }
            for component in Component::ALL {
                {entry(component.title(), Selection::Component(component))}
            }
        }
    }
}

/// The canvas: the toolbar, then the current section inside the chosen frame. It renders inside
/// the lab's mount, so the draft previewing as the lab's stylesheet restyles every cell before
/// anything is applied. One section at a time: everything in one render would be one oversized
/// batch.
#[component]
fn Canvas(selection: Selection, frame: Signal<Frame>) -> Element {
    let mut frame = frame;
    let width = frame()
        .width_px()
        .map(|px| format!("max-width:{px}px;"))
        .unwrap_or_default();
    rsx! {
        main { "data-part": "canvas",
            div { "data-part": "toolbar", role: "toolbar", "aria-label": "Canvas",
                span { "data-part": "label", "Width" }
                for each in Frame::ALL {
                    Button {
                        key: "{each.title()}",
                        variant: ButtonVariant::Outline,
                        pressed: Some(frame() == each),
                        onclick: move |()| frame.set(each),
                        "{each.title()}"
                    }
                }
            }
            div { "data-part": "frame", style: "{width}",
                match selection {
                    Selection::Sheets => rsx! {
                        section { "data-part": "sheets",
                            h2 { "Sheets" }
                            p {
                                "One sheet today: your theme, edited in the inspector. Installed themes "
                                "and your own sheets stacked in order come with the stylesheet stack."
                            }
                        }
                    },
                    Selection::Foundation(foundation) => rsx! { FoundationPage { foundation } },
                    Selection::Component(component) => component.render(),
                }
            }
        }
    }
}
