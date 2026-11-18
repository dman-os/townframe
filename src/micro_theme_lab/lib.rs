//! The theme lab, authored once against the MDK's [`Context`]: the end user's theme editor, the
//! home of iterating on the design language. A theme is one stylesheet of CSS text. The lab shows
//! it in a plain monospace textarea, previews the draft on its own surface (sample kit
//! components) before anything is applied, and Apply sends it to the host, which applies it to
//! the shell and every micro live and keeps it across restarts. Revert returns the draft to what
//! is applied.
//!
//! The preview is the draft as this micro's own stylesheet (`document::Style`), which is the
//! cascade every micro has: the host's applied theme sits in `@layer user`, beneath a micro's own
//! rules, so the draft (not the applied sheet) decides what the lab shows. The host refuses an
//! applied theme that could make it fetch anything (`url(`, `@import`, `image-set(`, ...); the
//! preview is sanitized by the same rule on its way to the mount, so a draft's fetch paths are
//! silently absent from the preview and Apply reports the refusal.
//!
//! [`clay_mdk::export_micro!`] exports the lab as the `micro-client` guest on wasm32-wasip3 and as
//! the native `ACTIVATE` entry everywhere else.
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::PanelPage;
use clay_mdk::Capability;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::hooks::PresentationSlot;
use dioxus::prelude::*;
use micro_shell_manifest::ThemeSheet;
use micro_theme_lab_manifest::SHELL_GET_THEME;
use micro_theme_lab_manifest::SHELL_NAV_BUTTON;
use micro_theme_lab_manifest::SHELL_SET_THEME;
use micro_theme_lab_manifest::ThemeLabView;

/// What the view asks of the host: the shell's sidebar/back button, and the two theme calls.
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

/// The editor's own classes: one place for the monospace field's look.
const FIELD: &str = "box-border min-h-64 w-full resize-y rounded-md border border-border bg-surface-sunken p-3 font-mono text-sm leading-relaxed text-foreground focus-visible:outline-2";
const BUTTON: &str = "min-h-12 cursor-pointer rounded-md border border-border px-4 py-2 focus-visible:outline-2 disabled:cursor-default disabled:opacity-50";

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
    // `document::Style` creates its element once per mount, so each edit remounts it through its
    // key; a counter, because the text itself is long.
    let mut revision = use_signal(|| 0_u64);

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
        move |_| {
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

    page(
        leading,
        rsx! {
            // The draft is this micro's stylesheet while it previews, which the host adopts for
            // this mount alone.
            document::Style { key: "{revision}", "{preview_css}" }
            div { class: "mx-4 flex flex-col gap-6 pb-6",
                section { class: "flex flex-col gap-2", "aria-label": "Theme CSS",
                    label { class: "font-medium", r#for: "theme-css", "Theme CSS" }
                    textarea {
                        id: "theme-css",
                        class: FIELD,
                        spellcheck: "false",
                        rows: "14",
                        value: "{text}",
                        oninput: move |event| {
                            draft.set(Some(event.value()));
                            status.set(Status::Idle);
                            revision += 1;
                        },
                    }
                    div { class: "flex flex-wrap items-center gap-2",
                        button {
                            class: BUTTON,
                            "aria-label": "Apply theme",
                            disabled: !dirty || applying(),
                            onclick: apply,
                            "Apply"
                        }
                        button {
                            class: BUTTON,
                            "aria-label": "Revert to the applied theme",
                            disabled: !dirty,
                            onclick: move |_| {
                                draft.set(None);
                                status.set(Status::Idle);
                                revision += 1;
                            },
                            "Revert"
                        }
                        label { class: "flex min-h-12 items-center gap-2",
                            input {
                                r#type: "checkbox",
                                checked: preview(),
                                onchange: move |event| {
                                    preview.set(event.checked());
                                    revision += 1;
                                },
                            }
                            "Preview the draft here"
                        }
                    }
                    match status() {
                        Status::Idle => rsx! {},
                        Status::Applied => rsx! {
                            p { role: "status", "Applied to the whole app." }
                        },
                        Status::Failed(reason) => rsx! {
                            p { role: "alert", class: "text-destructive", "Not applied: {reason}" }
                        },
                    }
                }
                Specimens {}
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

/// Sample surfaces the draft is judged on: kit components and the semantic roles they are made
/// of. They are the lab's own surfaces, not the host's: the preview restyles these (and the
/// editor around them) before anything is applied.
#[component]
fn Specimens() -> Element {
    rsx! {
        section { class: "flex flex-col gap-4", "aria-label": "Preview",
            h2 { class: "m-0 text-lg font-semibold", "Preview" }
            div { class: "overflow-hidden rounded-lg border border-border bg-background",
                AppBar { title: "App bar" }
                div { class: "flex flex-col gap-3 p-4",
                    p { class: "m-0 text-foreground", "Body text in the foreground role." }
                    p { class: "m-0 text-sm text-foreground-muted", "Muted text, for secondary detail." }
                    div { class: "flex flex-wrap gap-2",
                        button { class: "{BUTTON} bg-primary text-primary-foreground", "Primary" }
                        button { class: "{BUTTON} bg-secondary text-secondary-foreground", "Secondary" }
                        button { class: "{BUTTON} bg-destructive text-destructive-foreground", "Destructive" }
                    }
                    ul { class: "m-0 flex list-none flex-col gap-1 p-0",
                        for (index , name) in ["Inbox", "Today", "Archive"].into_iter().enumerate() {
                            li {
                                key: "{name}",
                                class: if index == 0 { "rounded-md bg-selected px-4 py-3 text-on-selected" } else { "rounded-md bg-surface-sunken px-4 py-3" },
                                "{name}"
                            }
                        }
                    }
                    div { class: "rounded-lg bg-card p-4 text-card-foreground shadow-sm",
                        p { class: "m-0 font-medium", "A card" }
                        p { class: "m-0 text-sm text-foreground-subtle", "On the card role, with subtle text." }
                    }
                }
            }
        }
    }
}
