//! The shell's presentations: the frame that arranges the panels, the home panel and the
//! sidebar/back button.
use crate::HostInputs;
use crate::Shell;
use crate::State;
use crate::navigation::Entry;
use crate::navigation::Overlay;
use clay_dioxus_kit::ListDetails;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonMode;
use clay_dioxus_kit::SectionPane;
use clay_dioxus_kit::Sections;
use clay_dioxus_kit::Sheet;
use clay_dioxus_kit::SizeRule;
use clay_dioxus_kit::Split;
use clay_dioxus_kit::WidthClass;
use clay_mdk::FacetSnapshot;
use clay_mdk::FacetState;
use clay_mdk::Row;
use clay_mdk::hooks::PresentationSlot;
use clay_mdk::hooks::use_facet;
use clay_mdk::hooks::use_rows;
use dioxus::prelude::*;
use micro_shell_manifest::NavigateRequest;
use micro_shell_manifest::Panel;
use micro_shell_manifest::ShellConfig;
use micro_shell_manifest::Socket;
use std::cell::Cell;
use std::rc::Rc;

/// Limits of the sidebar pane and the list/details panes, in logical px (`KIT-042`).
const SIDEBAR_RULE: SizeRule = SizeRule {
    min: 160.0,
    max: 480.0,
    snap: None,
};
const LIST_RULE: SizeRule = SizeRule {
    min: 200.0,
    max: f64::INFINITY,
    snap: None,
};
const DETAILS_RULE: SizeRule = SizeRule {
    min: 240.0,
    max: f64::INFINITY,
    snap: None,
};

const BUTTON: &str = "rounded-md border border-border px-4 py-2 text-left focus-visible:outline";

/// Where in the arrangement a mounted panel sits. The sidebar/back button reads it: a details
/// panel beside its list has no use for a second back button, but one over a compact list is the
/// only way back that is not covered.
#[derive(Clone, Copy, PartialEq)]
enum Placement {
    Main,
    Details,
}

/// Mounts a panel's presentation and provides its [`Placement`] to whatever the panel's tree
/// puts the button in. It takes the reference, not children: a component that only forwards an
/// `Element` prop does not re-render when the element inside it changes.
#[component]
fn PlacedSlot(placement: Placement, reference: clay_mdk::Capability) -> Element {
    use_context_provider(|| placement);
    rsx! { PresentationSlot { reference } }
}

pub(crate) fn frame(shell: Shell) -> Element {
    rsx! { Frame { shell } }
}

pub(crate) fn home(shell: Shell) -> Element {
    rsx! { Home { shell } }
}

pub(crate) fn navigation_button(shell: Shell) -> Element {
    rsx! { NavigationButton { shell } }
}

/// The shell's device-local state at this render; the calling component re-renders whenever it
/// changes.
fn use_state(shell: &Shell) -> State {
    let mut tick = use_signal(|| 0_u64);
    let changes = use_hook(|| shell.state.subscribe());
    use_future(move || {
        let mut changes = changes.clone();
        async move {
            // The sender lives in the shell, which every presentation holds.
            while changes.changed().await.is_ok() {
                tick += 1;
            }
        }
    });
    let _observed: u64 = tick();
    shell.state.borrow().clone()
}

/// The arrangement weights in `snapshot`. A document that has none yet, or one a sync wrote
/// with weights a split cannot lay out (external input, unlike the weights this shell writes),
/// reads as the defaults.
fn arrangement(snapshot: &FacetSnapshot<ShellConfig>) -> ShellConfig {
    match &snapshot.state {
        FacetState::Present(config)
            if [config.sidebar, config.list, config.details]
                .iter()
                .all(|weight| weight.is_finite() && *weight > 0.0) =>
        {
            *config
        }
        _ => ShellConfig::default(),
    }
}

/// `config` after the user dragged the handle between the sidebar and the content: the content
/// keeps its list/details ratio and takes the weight the drag left it.
fn with_outer_weights(config: ShellConfig, weights: &[f64]) -> ShellConfig {
    let [sidebar, content] = weights else {
        unreachable!("the outer split has a sidebar and the content")
    };
    let list = content * config.list / (config.list + config.details);
    ShellConfig {
        sidebar: *sidebar,
        list,
        details: content - list,
    }
}

/// `config` after the user dragged the handle between the list and the details.
fn with_inner_weights(config: ShellConfig, weights: &[f64]) -> ShellConfig {
    let [list, details] = weights else {
        unreachable!("the inner split has a list and details")
    };
    ShellConfig {
        list: *list,
        details: *details,
        ..config
    }
}

fn panel_slot(row: &Row<String, Panel>, attachment: clay_mdk::Attachment) -> Element {
    rsx! { PresentationSlot { reference: row.attachments[attachment.index()].clone() } }
}

/// The panels that are destinations: the home panel first, then every other list panel by
/// title.
fn destinations(panels: &[Row<String, Panel>]) -> Vec<&Row<String, Panel>> {
    let mut found: Vec<_> = panels
        .iter()
        .filter(|row| row.value.home || row.value.sockets.contains(&Socket::List))
        .collect();
    found.sort_by(|left, right| {
        (!left.value.home, &left.value.title, &left.key).cmp(&(
            !right.value.home,
            &right.value.title,
            &right.key,
        ))
    });
    found
}

/// Ask the shell to open `panel`, showing a refusal (the panel vanished since it was listed)
/// where `failure` is rendered.
fn open_panel(shell: &Shell, panel: String, mut failure: Signal<Option<String>>) {
    let shell = shell.clone();
    spawn(async move {
        let result = shell.navigate(NavigateRequest { panel, row: None }).await;
        failure.set(result.err().map(|problem| problem.to_string()));
    });
}

#[component]
fn Frame(shell: Shell) -> Element {
    let host = use_context::<HostInputs>();
    let width_px = *host.width.read();
    let width = WidthClass::from_px(width_px);
    let state = use_state(&shell);
    let panels = use_rows(&shell.panels);
    let config = arrangement(&use_facet(&shell.config));
    let failure = use_signal(|| None::<String>);

    // The platform back gesture lands on the stack (ADR 032 §3). `seen` is what this frame has
    // already acted on, so remounting the frame does not replay old presses.
    let seen = use_hook(|| Rc::new(Cell::new(*host.platform_back.peek())));
    let back = shell.clone();
    use_effect(move || {
        let presses = (host.platform_back)();
        if presses != seen.get() {
            seen.set(presses);
            back.back();
        }
    });
    let mut can_back = host.can_back;
    let can_go_back = state.navigation.can_back();
    use_effect(use_reactive!(|(can_go_back,)| can_back.set(can_go_back)));

    let frame = state.navigation.frame();
    let find = |key: &str| panels.iter().find(|row| row.key == key);
    let Some(main) = frame
        .main
        .and_then(find)
        .or_else(|| panels.iter().find(|row| row.value.home))
    else {
        return if shell.panels.is_bootstrapped() {
            rsx! { p { role: "alert", "No home panel is installed." } }
        } else {
            rsx! { p { role: "status", "Loading the shell…" } }
        };
    };
    // A details entry whose panel is gone, or that no longer opens rows, shows nothing until
    // the shell prunes it.
    let details = frame.details.and_then(|(panel, row)| {
        let row_panel = find(panel)?;
        let attachment = match row {
            Some(_) => row_panel.value.details?,
            None => row_panel.value.content,
        };
        Some((row_panel, row, attachment))
    });

    let on_open = {
        let shell = shell.clone();
        let panel = main.key.clone();
        move |row: String| {
            let shell = shell.clone();
            let panel = panel.clone();
            let mut failure = failure;
            spawn(async move {
                let result = shell
                    .navigate(NavigateRequest {
                        panel,
                        row: Some(row),
                    })
                    .await;
                failure.set(result.err().map(|problem| problem.to_string()));
            });
        }
    };
    let on_back = {
        let shell = shell.clone();
        move |()| {
            shell.back();
        }
    };

    let sidebar_visible = width.fits_side_by_side() && !state.sidebar_hidden;
    let outer = Split::new(vec![config.sidebar, config.list + config.details], width_px)
        .rule(0, SIDEBAR_RULE)
        .keyboard(host.keyboard);
    // The list/details split lays out inside the content pane, so its extent is that pane's.
    let content_px = if sidebar_visible {
        outer.sizes()[1]
    } else {
        width_px
    };
    let inner = Split::new(vec![config.list, config.details], content_px)
        .rule(0, LIST_RULE)
        .rule(1, DETAILS_RULE)
        .keyboard(host.keyboard);

    let main_slot = rsx! {
        PlacedSlot {
            key: "{main.key}",
            placement: Placement::Main,
            reference: main.attachments[main.value.content.index()].clone(),
        }
    };
    let details_slot = details.map(|(row_panel, row, attachment)| {
        rsx! {
            PlacedSlot {
                key: "{row_panel.key}/{row:?}",
                placement: Placement::Details,
                reference: row_panel.attachments[attachment.index()].clone(),
            }
        }
    });
    let details_name = details.map_or_else(
        || "Details".to_owned(),
        |(row_panel, ..)| format!("{} details", row_panel.value.title),
    );
    let write = {
        let config_handle = shell.config.clone();
        move |next: ShellConfig| {
            config_handle
                .write(next)
                .expect("the shell may write its own configuration");
        }
    };
    let on_resize_inner = {
        let write = write.clone();
        move |weights: Vec<f64>| write(with_inner_weights(config, &weights))
    };
    let on_resize_outer = move |weights: Vec<f64>| write(with_outer_weights(config, &weights));

    let list_details = rsx! {
        ListDetails {
            width,
            list_name: main.value.title.clone(),
            list: main_slot,
            details_name,
            details: details_slot,
            split: inner,
            on_resize: on_resize_inner,
            on_open,
            on_back,
        }
    };
    let overlay = match frame.overlay {
        Some(Overlay::Sheet(key)) => find(key).map(|row| {
            rsx! {
                Sheet {
                    width,
                    name: row.value.title.clone(),
                    on_dismiss: {
                        let shell = shell.clone();
                        move |()| {
                            shell.close_sheet();
                        }
                    },
                    {panel_slot(row, row.value.content)}
                }
            }
        }),
        // A drawer is for a host with no room for the sidebar beside the content; a window that
        // grew wide while it was open shows the sidebar instead.
        Some(Overlay::Sidebar) if !width.fits_side_by_side() => Some(rsx! {
            Sheet {
                width,
                name: "Sidebar",
                on_dismiss: {
                    let shell = shell.clone();
                    move |()| {
                        shell.back();
                    }
                },
                Sidebar { shell: shell.clone() }
            }
        }),
        Some(Overlay::Sidebar) | None => None,
    };

    rsx! {
        div {
            style: "position:relative;width:100%;height:100%;min-width:0;min-height:0",
            if let Some(problem) = failure() {
                p { role: "alert", "{problem}" }
            }
            if sidebar_visible {
                Sections {
                    label: "Shell",
                    panes: vec![
                        SectionPane {
                            name: "Sidebar".to_owned(),
                            content: rsx! { Sidebar { shell: shell.clone() } },
                        },
                        SectionPane {
                            name: "Content".to_owned(),
                            content: list_details,
                        },
                    ],
                    split: outer,
                    on_resize: on_resize_outer,
                }
            } else {
                {list_details}
            }
            {overlay}
        }
    }
}

/// The sidebar (FDR 006): navigation across micros. It offers every destination and mounts the
/// panels that fill the sidebar socket.
#[component]
fn Sidebar(shell: Shell) -> Element {
    let host = use_context::<HostInputs>();
    let wide = WidthClass::from_px(*host.width.read()).fits_side_by_side();
    let state = use_state(&shell);
    let panels = use_rows(&shell.panels);
    let failure = use_signal(|| None::<String>);
    let current = state
        .navigation
        .frame()
        .main
        .map(str::to_owned)
        .or_else(|| {
            panels
                .iter()
                .find(|row| row.value.home)
                .map(|row| row.key.clone())
        });
    rsx! {
        nav { class: "flex flex-col gap-2 p-4", "aria-label": "Sidebar",
            if wide {
                button {
                    class: BUTTON,
                    r#type: "button",
                    "aria-label": "Hide sidebar",
                    onclick: {
                        let shell = shell.clone();
                        move |_| {
                            shell.edit(|state| {
                                state.sidebar_hidden = true;
                                true
                            });
                        }
                    },
                    "Hide sidebar"
                }
            }
            for row in destinations(&panels) {
                button {
                    key: "{row.key}",
                    class: BUTTON,
                    r#type: "button",
                    "aria-label": "Show {row.value.title}",
                    "aria-current": (current.as_deref() == Some(row.key.as_str())).then_some("page"),
                    onclick: {
                        let shell = shell.clone();
                        let key = row.key.clone();
                        move |_| open_panel(&shell, key.clone(), failure)
                    },
                    "{row.value.title}"
                }
            }
            if let Some(problem) = failure() {
                p { role: "alert", "{problem}" }
            }
            for row in panels.iter().filter(|row| row.value.sockets.contains(&Socket::Sidebar)) {
                div { key: "sidebar-{row.key}", {panel_slot(row, row.value.content)} }
            }
        }
    }
}

/// The home panel: a list of entry points, one row per destination other than itself. The
/// tile grammar replaces the plain rows later (FDR 005).
#[component]
fn Home(shell: Shell) -> Element {
    let panels = use_rows(&shell.panels);
    let failure = use_signal(|| None::<String>);
    rsx! {
        // The frame's region already carries the panel's title as its accessible name.
        div {
            class: "flex flex-col gap-4 p-4",
            header { class: "flex items-center gap-2",
                NavigationButton { shell: shell.clone() }
                h2 { class: "text-xl font-semibold", "Home" }
            }
            nav { class: "flex flex-col gap-2", "aria-label": "Entry points",
                for row in destinations(&panels).into_iter().filter(|row| !row.value.home) {
                    button {
                        key: "{row.key}",
                        class: BUTTON,
                        r#type: "button",
                        "aria-label": "Open {row.value.title}",
                        onclick: {
                            let shell = shell.clone();
                            let key = row.key.clone();
                            move |_| open_panel(&shell, key.clone(), failure)
                        },
                        "{row.value.title}"
                    }
                }
            }
            if let Some(problem) = failure() {
                p { role: "alert", "{problem}" }
            }
        }
    }
}

/// The sidebar/back button (ADR 032 §5). **Back** while the current context can go back,
/// **open the sidebar** otherwise. On a host wide enough to keep the sidebar beside the content
/// there is nothing to open until the user hides it, so the button is absent rather than
/// offering a no-op, and a details panel beside its list leaves the button to the list.
#[component]
fn NavigationButton(shell: Shell) -> Element {
    let host = use_context::<HostInputs>();
    let state = use_state(&shell);
    let wide = WidthClass::from_px(*host.width.read()).fits_side_by_side();
    // Outside a frame's mount (a button mounted somewhere the shell did not place) the button
    // belongs to the main position.
    let placement = try_use_context::<Placement>().unwrap_or(Placement::Main);
    if placement == Placement::Details && wide {
        return rsx! {};
    }
    let mode = if state.navigation.can_back() {
        NavButtonMode::Back
    } else {
        NavButtonMode::Sidebar
    };
    if mode == NavButtonMode::Sidebar && wide && !state.sidebar_hidden {
        return rsx! {};
    }
    rsx! {
        NavButton {
            mode,
            onclick: move |()| {
                shell.edit(|state| match mode {
                    NavButtonMode::Back => state.navigation.back(),
                    // The host's width is read when pressed, not captured: the window may have
                    // been resized since this rendered.
                    NavButtonMode::Sidebar if wide => {
                        state.sidebar_hidden = false;
                        true
                    }
                    NavButtonMode::Sidebar => {
                        state.navigation.push(Entry::Sidebar);
                        true
                    }
                });
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dragging_the_sidebar_handle_keeps_the_list_to_details_ratio() {
        let config = ShellConfig {
            sidebar: 1.0,
            list: 2.0,
            details: 3.0,
        };
        let next = with_outer_weights(config, &[2.0, 10.0]);
        assert_eq!(next.sidebar, 2.0);
        assert_eq!(next.list / next.details, 2.0 / 3.0);
        assert_eq!(next.list + next.details, 10.0);
    }

    #[test]
    fn dragging_the_details_handle_leaves_the_sidebar_alone() {
        let config = ShellConfig::default();
        let next = with_inner_weights(config, &[4.0, 1.0]);
        assert_eq!((next.sidebar, next.list, next.details), (1.0, 4.0, 1.0));
    }

    #[test]
    fn weights_a_split_cannot_lay_out_read_as_the_defaults() {
        let present = |config| FacetSnapshot {
            state: FacetState::Present(config),
            refreshing: false,
            revision: 0,
            pending: 0,
        };
        let broken = ShellConfig {
            sidebar: 0.0,
            ..ShellConfig::default()
        };
        assert_eq!(arrangement(&present(broken)), ShellConfig::default());
        let fine = ShellConfig {
            sidebar: 2.0,
            list: 1.0,
            details: 1.0,
        };
        assert_eq!(arrangement(&present(fine)), fine);
    }
}
