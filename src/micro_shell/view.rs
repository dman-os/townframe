//! The shell's presentations: the frame that arranges the panels, the home panel and the
//! navigation rack (ADR 058). The frame is the whole app: sidebar, list and details edge to edge, with
//! sheets and (on a compact host) the sidebar drawer over them.
use crate::HostInputs;
use crate::Shell;
use crate::State;
use crate::navigation::Entry;
use crate::navigation::Overlay;
use clay_dioxus_kit::AppBar;
use clay_dioxus_kit::Button;
use clay_dioxus_kit::ButtonVariant;
use clay_dioxus_kit::Drawer;
use clay_dioxus_kit::ListDetails;
use clay_dioxus_kit::NavButton;
use clay_dioxus_kit::NavButtonKind;
use clay_dioxus_kit::NavRack;
use clay_dioxus_kit::PanelPage;
use clay_dioxus_kit::Renderer;
use clay_dioxus_kit::SectionPane;
use clay_dioxus_kit::Sections;
use clay_dioxus_kit::Sheet;
use clay_dioxus_kit::SizeRule;
use clay_dioxus_kit::Split;
use clay_dioxus_kit::WidthClass;
use clay_dioxus_kit::use_pane_rack;
use clay_mdk::FacetSnapshot;
use clay_mdk::FacetState;
use clay_mdk::Row;
use clay_mdk::hooks::PresentationSlot;
use clay_mdk::hooks::Stylesheet;
use clay_mdk::hooks::use_facet;
use clay_mdk::hooks::use_rows;
use clay_mdk::host::UserStylesheet;
use dioxus::prelude::*;
use micro_shell_manifest::NavigateRequest;
use micro_shell_manifest::Panel;
use micro_shell_manifest::ShellConfig;
use micro_shell_manifest::Socket;
use std::cell::Cell;
use std::rc::Rc;

/// Limits of the sidebar pane and the list/details panes, in logical px (`KIT-042`). Dragging the
/// sidebar 48 px below its minimum hides it, as the "Hide sidebar" button does.
const SIDEBAR_RULE: SizeRule = SizeRule {
    min: 160.0,
    max: 480.0,
    snap: None,
    collapse_past: Some(48.0),
};
const LIST_RULE: SizeRule = SizeRule {
    min: 200.0,
    max: f64::INFINITY,
    snap: None,
    collapse_past: None,
};
const DETAILS_RULE: SizeRule = SizeRule {
    min: 240.0,
    max: f64::INFINITY,
    snap: None,
    collapse_past: None,
};

/// The shell's own stylesheet: the layout of its frame, sidebar and home, which the kit has no
/// component for. It names no colour, so a theme restyles the shell through the kit's tokens and
/// parts.
const STYLES: &str = include_str!("styles.css");

/// Where in the arrangement a mounted panel sits. The navigation rack reads it: a details panel
/// beside its list has no use for a second rack, but one over a compact list is the only way
/// back that is not covered.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Placement {
    Main,
    Details,
}

/// Mounts a panel's presentation and provides its [`Placement`] to whatever the panel's tree
/// puts the button in. It takes the reference, not children: a component that only forwards an
/// `Element` prop does not re-render when the element inside it changes.
///
/// The panel's own tree carries the navigation rack, so while the panel has no presentation
/// (its micro is disabled or still starting) the shell supplies a page with the panel's title and
/// the rack beside its title; otherwise a compact window would show a placeholder with no way
/// out of it.
#[component]
fn PlacedSlot(
    placement: Placement,
    reference: clay_mdk::Capability,
    title: String,
    shell: Shell,
) -> Element {
    use_context_provider(|| placement);
    let placeholder_chrome = rsx! {
        PanelPage {
            rack: rsx! { Rack { shell } },
            app_bar: rsx! { AppBar { title } },
            ""
        }
    };
    rsx! { PresentationSlot { reference, placeholder_chrome } }
}

/// `row`'s `link` mounted in `placement`, keyed by `key` (a different key is a fresh mount).
fn placed_slot(
    shell: &Shell,
    placement: Placement,
    key: String,
    row: &Row<String, Panel>,
    link: clay_mdk::Link,
) -> Element {
    rsx! {
        PlacedSlot {
            key: "{key}",
            placement,
            reference: row.links[link.index()].clone(),
            title: row.value.title.clone(),
            shell: shell.clone(),
        }
    }
}

pub(crate) fn frame(shell: Shell) -> Element {
    rsx! { Frame { shell } }
}

pub(crate) fn home(shell: Shell) -> Element {
    rsx! { Home { shell } }
}

pub(crate) fn navigation_rack(shell: Shell) -> Element {
    rsx! { Rack { shell } }
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

fn panel_slot(row: &Row<String, Panel>, link: clay_mdk::Link) -> Element {
    rsx! { PresentationSlot { reference: row.links[link.index()].clone() } }
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

/// Ask the shell to open `panel`, or one `row` of it, showing a refusal (the panel vanished since
/// it was listed) where `failure` is rendered.
fn open_panel(
    shell: &Shell,
    panel: String,
    row: Option<String>,
    mut failure: Signal<Option<String>>,
) {
    let shell = shell.clone();
    spawn(async move {
        let result = shell.navigate(NavigateRequest { panel, row }).await;
        failure.set(result.err().map(|problem| problem.to_string()));
    });
}

/// Wire the frame to the host (ADR 032 §3): the platform back gesture lands on the stack, and the
/// host is told whether back has somewhere to go.
fn use_host_wiring(shell: &Shell, host: &HostInputs, can_go_back: bool) {
    // `seen` is what this frame has already acted on, so remounting the frame does not replay
    // old presses.
    let seen = use_hook(|| Rc::new(Cell::new(*host.platform_back.peek())));
    let shell = shell.clone();
    let platform_back = host.platform_back;
    use_effect(move || {
        let presses = platform_back();
        if presses != seen.get() {
            seen.set(presses);
            shell.back();
        }
    });
    let mut can_back = host.can_back;
    use_effect(use_reactive!(|(can_go_back,)| can_back.set(can_go_back)));
}

/// A development seed (`HostInputs::start`) opens its stack once the panels have loaded; a step
/// the shell refuses is shown through `failure`.
fn use_start(shell: &Shell, host: &HostInputs, failure: Signal<Option<String>>) {
    let shell = shell.clone();
    let start = host.start.clone();
    let mut failure = failure;
    use_future(move || {
        let shell = shell.clone();
        let start = start.clone();
        async move {
            if let Some(spec) = start
                && let Err(problem) = shell.start_on(&spec).await
            {
                failure.set(Some(format!("DAYTXT_SHELL_OPEN: {problem}")));
            }
        }
    });
}

/// The sheet over the arrangement, if one is open.
fn sheet_element(
    shell: &Shell,
    width: WidthClass,
    overlay: Option<Overlay<'_>>,
    panels: &[Row<String, Panel>],
) -> Option<Element> {
    let Some(Overlay::Sheet(key)) = overlay else {
        return None;
    };
    panels.iter().find(|row| row.key == key).map(|row| {
        let shell = shell.clone();
        rsx! {
            Sheet {
                width,
                name: row.value.title.clone(),
                on_dismiss: move |()| {
                    shell.close_sheet();
                },
                {panel_slot(row, row.value.content)}
            }
        }
    })
}

/// The details position's mounted panel and its accessible name.
fn details_pane(
    shell: &Shell,
    details: Option<(&Row<String, Panel>, Option<&str>, clay_mdk::Link)>,
) -> (Option<Element>, String) {
    let slot = details.map(|(row_panel, row, link)| {
        placed_slot(
            shell,
            Placement::Details,
            format!("{}/{row:?}", row_panel.key),
            row_panel,
            link,
        )
    });
    let name = details.map_or_else(
        || "Details".to_owned(),
        |(row_panel, ..)| format!("{} details", row_panel.value.title),
    );
    (slot, name)
}

/// What the frame applies of the end user's theme, checked once per change.
struct AppliedTheme {
    /// The CSS the facet held when it was last checked.
    raw: String,
    /// `raw` in `@layer user`, or empty when it is empty or was refused: the frame's copy.
    layered: Rc<str>,
}

/// Applies the end user's theme (see [`Shell::set_theme`]) host-wide and live: returns the
/// `@layer user` sheet the frame adds to its own mount's stylesheet (the frame is a mount like
/// any other, so no `<style>` reaches the host's document), and provides every mount below it the same
/// CSS as [`UserStylesheet`], which a mount adopts as the last layer of its shadow root. The
/// facet is document-backed and so can arrive from another device, so what it holds is
/// checked again here, once per change rather than per render, and a sheet that could make the
/// host fetch is not applied.
fn use_theme(shell: &Shell) -> Rc<str> {
    let snapshot = use_facet(&shell.theme);
    let raw = match &snapshot.state {
        FacetState::Present(sheet) => sheet.css.as_str(),
        FacetState::Loading | FacetState::Absent => "",
    };
    let mut mounts = use_signal(String::new);
    use_context_provider(|| UserStylesheet(mounts.into()));
    let applied = use_hook(|| {
        Rc::new(std::cell::RefCell::new(AppliedTheme {
            raw: String::new(),
            layered: Rc::from(""),
        }))
    });
    let mut applied = applied.borrow_mut();
    if applied.raw != raw {
        let accepted = clay_mdk::host::refuse_fetching_stylesheet(raw).is_ok() && !raw.is_empty();
        applied.raw = raw.to_owned();
        applied.layered = if accepted {
            Rc::from(format!("@layer user {{\n{raw}\n}}"))
        } else {
            Rc::from("")
        };
        mounts.set(if accepted {
            raw.to_owned()
        } else {
            String::new()
        });
    }
    Rc::clone(&applied.layered)
}

/// The frame's two splits: the sidebar against the content, and, inside the content pane, the
/// list against the details.
fn splits(
    config: &ShellConfig,
    renderer: Renderer,
    width_px: f64,
    sidebar_visible: bool,
) -> (Split, Split) {
    let outer = Split::new(vec![config.sidebar, config.list + config.details], width_px)
        .rule(0, SIDEBAR_RULE)
        .renderer(renderer);
    // The list/details split lays out inside the content pane, so its extent is that pane's.
    let content_px = if sidebar_visible {
        outer.sizes()[1]
    } else {
        width_px
    };
    let inner = Split::new(vec![config.list, config.details], content_px)
        .rule(0, LIST_RULE)
        .rule(1, DETAILS_RULE)
        .renderer(renderer);
    (outer, inner)
}

#[component]
fn Frame(shell: Shell) -> Element {
    let host = use_context::<HostInputs>();
    let theme = use_theme(&shell);
    // The frame's own sheet, then the theme in `@layer user`. No mount is above the frame to
    // supply the theme (the frame is where it comes from), so the frame's mount takes it here.
    let own_sheet = if theme.is_empty() {
        std::borrow::Cow::Borrowed(STYLES)
    } else {
        std::borrow::Cow::Owned(format!("{STYLES}\n{theme}"))
    };
    let width_px = *host.width.read();
    let width = WidthClass::from_px(width_px);
    let state = use_state(&shell);
    let panels = use_rows(&shell.panels);
    let config = arrangement(&use_facet(&shell.config));
    let failure = use_signal(|| None::<String>);

    use_host_wiring(&shell, &host, state.navigation.can_back());
    use_start(&shell, &host, failure);

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
        let link = match row {
            Some(_) => row_panel.value.details?,
            None => row_panel.value.content,
        };
        Some((row_panel, row, link))
    });

    let on_open = {
        let shell = shell.clone();
        let panel = main.key.clone();
        move |row: String| open_panel(&shell, panel.clone(), Some(row), failure)
    };
    let on_back = {
        let shell = shell.clone();
        move |()| {
            shell.back();
        }
    };

    let wide = width.fits_side_by_side();
    let sidebar_visible = wide && !state.sidebar_hidden;
    let (outer, inner) = splits(&config, host.renderer, width_px, sidebar_visible);

    let main_slot = placed_slot(
        &shell,
        Placement::Main,
        main.key.clone(),
        main,
        main.value.content,
    );
    let (details_slot, details_name) = details_pane(&shell, details);
    let on_resize_inner = {
        let shell = shell.clone();
        move |weights: Vec<f64>| shell.write_config(with_inner_weights(config, &weights))
    };
    let on_resize_outer = {
        let shell = shell.clone();
        move |weights: Vec<f64>| shell.write_config(with_outer_weights(config, &weights))
    };
    // Dragging the sidebar shut and reopening it from the edge strip set the same state as the
    // "Hide sidebar" button and the navigation rack's toggle; the weights stay, so it reopens at the
    // width it had.
    let on_collapse_outer = {
        let shell = shell.clone();
        move |_pane: usize| shell.set_sidebar_hidden(true)
    };
    let on_expand_outer = {
        let shell = shell.clone();
        move |()| shell.set_sidebar_hidden(false)
    };

    let current = state.navigation.opened(&main.key).map(str::to_owned);
    let list_details = rsx! {
        ListDetails {
            width,
            list_name: main.value.title.clone(),
            list: main_slot,
            details_name,
            details: details_slot,
            current,
            split: inner,
            on_resize: on_resize_inner,
            on_open,
            on_back,
        }
    };
    let sheet = sheet_element(&shell, width, frame.overlay, &panels);
    // The drawer shows itself only on a compact host (`Drawer` checks `width`): a window that grew
    // wide while it was open shows the sidebar instead.
    let drawer_open = matches!(frame.overlay, Some(Overlay::Sidebar));

    rsx! {
        div { class: "ct-shell",
            Stylesheet { css: own_sheet }
            if let Some(problem) = failure() {
                p { role: "alert", "{problem}" }
            }
            if wide {
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
                    folded: state.sidebar_hidden,
                    on_resize: on_resize_outer,
                    on_collapse: on_collapse_outer,
                    on_expand: on_expand_outer,
                }
            } else {
                {list_details}
            }
            Drawer {
                width,
                name: "Sidebar",
                open: drawer_open,
                on_dismiss: {
                    let shell = shell.clone();
                    move |()| {
                        shell.back();
                    }
                },
                Sidebar { shell: shell.clone() }
            }
            {sheet}
        }
    }
}

/// The sidebar's header while it is shown (ADR 058 §2.3): the rack's controls at the window's
/// top-left, where the floating rack was, then the app's mark. Docked, the rack's three: hide,
/// back and forward (the last two disabled when there is nowhere to go, as in the floating rack).
/// As the compact drawer, hide alone: back would pop the same entry, and forward is the footer's.
/// The mark is a plain image, not a control, and there is no menu behind it yet.
#[component]
fn SidebarHeader(
    wide: bool,
    can_back: bool,
    can_forward: bool,
    on_hide: EventHandler<()>,
    on_back: EventHandler<()>,
    on_forward: EventHandler<()>,
) -> Element {
    rsx! {
        header { "data-part": "header",
            NavRack {
                NavButton { kind: NavButtonKind::HideSidebar, onclick: on_hide }
                if wide {
                    NavButton { kind: NavButtonKind::Back, disabled: !can_back, onclick: on_back }
                    NavButton { kind: NavButtonKind::Forward, disabled: !can_forward, onclick: on_forward }
                }
            }
            span { "data-part": "mark", role: "img", "aria-label": "daytxt", "🌞" }
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
        nav { "data-part": "sidebar", "aria-label": "Sidebar",
            SidebarHeader {
                wide,
                can_back: state.navigation.can_back(),
                can_forward: state.navigation.can_forward(),
                on_hide: {
                    let shell = shell.clone();
                    move |()| {
                        // Docked, hiding is the setting; as the compact drawer it is the
                        // navigation entry that opened it.
                        if wide {
                            shell.set_sidebar_hidden(true);
                        } else {
                            shell.back();
                        }
                    }
                },
                on_back: {
                    let shell = shell.clone();
                    move |()| {
                        shell.back();
                    }
                },
                on_forward: {
                    let shell = shell.clone();
                    move |()| {
                        shell.forward();
                    }
                },
            }
            for row in destinations(&panels) {
                Button {
                    key: "{row.key}",
                    variant: ButtonVariant::Outline,
                    label: format!("Show {}", row.value.title),
                    current: current.as_deref() == Some(row.key.as_str()),
                    onclick: {
                        let shell = shell.clone();
                        let key = row.key.clone();
                        move |()| open_panel(&shell, key.clone(), None, failure)
                    },
                    "{row.value.title}"
                }
            }
            if let Some(problem) = failure() {
                p { "data-part": "problem", role: "alert", "{problem}" }
            }
            for row in panels.iter().filter(|row| row.value.sockets.contains(&Socket::Sidebar)) {
                div { key: "sidebar-{row.key}", "data-part": "slot", {panel_slot(row, row.value.content)} }
            }
            // Forward is the drawer's footer on a compact host; docked, the header holds it.
            if !wide {
                footer { "data-part": "footer",
                    NavButton {
                        kind: NavButtonKind::Forward,
                        disabled: !state.navigation.can_forward(),
                        onclick: {
                            let shell = shell.clone();
                            move |()| {
                                shell.forward();
                            }
                        },
                    }
                }
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
    use_pane_rack(rsx! { Rack { shell: shell.clone() } });
    rsx! {
        // The frame's region already carries the panel's title as its accessible name.
        PanelPage {
            app_bar: rsx! { AppBar { title: "Home" } },
        div { "data-part": "home",
            nav { "data-part": "entries", "aria-label": "Entry points",
                for row in destinations(&panels).into_iter().filter(|row| !row.value.home) {
                    Button {
                        key: "{row.key}",
                        variant: ButtonVariant::Outline,
                        label: format!("Open {}", row.value.title),
                        onclick: {
                            let shell = shell.clone();
                            let key = row.key.clone();
                            move |()| open_panel(&shell, key.clone(), None, failure)
                        },
                        "{row.value.title}"
                    }
                }
            }
            if let Some(problem) = failure() {
                p { "data-part": "problem", role: "alert", "{problem}" }
            }
        }
        }
    }
}

/// What the rack is drawn from: the width class, the position it is mounted in, whether the
/// sidebar is shown and what the stack can do.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Situation {
    wide: bool,
    placement: Placement,
    /// The sidebar is docked (wide, not folded) or open as the drawer (compact): its header
    /// holds the controls, so no mounted rack draws any.
    sidebar_shown: bool,
    can_back: bool,
    can_forward: bool,
}

/// The rack's buttons for one [`Situation`], the table of ADR 058 §2.1. A button's `Some` says
/// whether it is enabled.
#[derive(Debug, PartialEq)]
struct Buttons {
    toggle: bool,
    back: Option<bool>,
    forward: Option<bool>,
}

impl Buttons {
    /// `None` when the rack draws nothing here.
    fn of(situation: Situation) -> Option<Self> {
        let Situation {
            wide,
            placement,
            sidebar_shown,
            can_back,
            can_forward,
        } = situation;
        match (wide, placement) {
            _ if sidebar_shown => None,
            // The rack is the window's top-left corner, which is the main position's: a details
            // pane beside its list has no use for a second one.
            (true, Placement::Details) => None,
            // Back and forward stay, disabled, when there is nowhere to go, so the rack keeps
            // its shape.
            (true, Placement::Main) => Some(Self {
                toggle: true,
                back: Some(can_back),
                forward: Some(can_forward),
            }),
            // Forward is the sidebar's footer's on a compact host (ADR 058 §2.3).
            (false, Placement::Main) => Some(Self {
                toggle: true,
                back: can_back.then_some(true),
                forward: None,
            }),
            // A details page does not offer the sidebar: back leaves it for its list first.
            (false, Placement::Details) => Some(Self {
                toggle: false,
                back: Some(true),
                forward: None,
            }),
        }
    }
}

/// The rack's presentation: [`Buttons`] for `situation`, reporting presses.
#[component]
fn RackButtons(
    situation: Situation,
    on_toggle: EventHandler<()>,
    on_back: EventHandler<()>,
    on_forward: EventHandler<()>,
) -> Element {
    let Some(buttons) = Buttons::of(situation) else {
        return rsx! {};
    };
    rsx! {
        NavRack {
            if buttons.toggle {
                NavButton { kind: NavButtonKind::Sidebar, onclick: on_toggle }
            }
            if let Some(enabled) = buttons.back {
                NavButton {
                    kind: NavButtonKind::Back,
                    disabled: !enabled,
                    onclick: on_back,
                }
            }
            if let Some(enabled) = buttons.forward {
                NavButton {
                    kind: NavButtonKind::Forward,
                    disabled: !enabled,
                    onclick: on_forward,
                }
            }
        }
    }
}

/// The navigation rack (ADR 058): separate buttons for the sidebar, back and forward, which ones
/// depending on the width class and the position it is mounted in (see [`Buttons::of`]), acting
/// on the shell's state.
#[component]
fn Rack(shell: Shell) -> Element {
    let host = use_context::<HostInputs>();
    let state = use_state(&shell);
    let wide = WidthClass::from_px(*host.width.read()).fits_side_by_side();
    // Outside a frame's mount (a rack mounted somewhere the shell did not place) it belongs to
    // the main position.
    let placement = try_use_context::<Placement>().unwrap_or(Placement::Main);
    let situation = Situation {
        wide,
        placement,
        sidebar_shown: if wide {
            !state.sidebar_hidden
        } else {
            matches!(state.navigation.frame().overlay, Some(Overlay::Sidebar))
        },
        can_back: state.navigation.can_back(),
        can_forward: state.navigation.can_forward(),
    };
    rsx! {
        RackButtons {
            situation,
            on_toggle: {
                let shell = shell.clone();
                let width = host.width;
                move |()| {
                    // The host's width is read when pressed, not captured: the window may have
                    // been resized since this rendered.
                    let wide = WidthClass::from_px(*width.peek()).fits_side_by_side();
                    shell.edit(|state| {
                        if wide {
                            state.sidebar_hidden = false;
                        } else {
                            state.navigation.push(Entry::Sidebar);
                        }
                        true
                    });
                }
            },
            on_back: {
                let shell = shell.clone();
                move |()| {
                    shell.back();
                }
            },
            on_forward: move |()| {
                shell.forward();
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIDEBAR: &str = "Open sidebar";
    const BACK: &str = "Back";
    const FORWARD: &str = "Forward";

    /// The rack's buttons as the page renders them, in order: each button's accessible name,
    /// with a `!` after it when it is disabled. `None` when no rack is drawn at all.
    fn rack(situation: Situation) -> Option<Vec<String>> {
        fn app(situation: Situation) -> Element {
            rsx! {
                RackButtons {
                    situation,
                    on_toggle: |()| {},
                    on_back: |()| {},
                    on_forward: |()| {},
                }
            }
        }
        let mut dom = VirtualDom::new_with_props(app, situation);
        dom.rebuild_in_place();
        let html = dioxus_ssr::render(&dom);
        if !html.contains("ct-nav-rack") {
            assert_eq!(html, "", "no rack means nothing at all");
            return None;
        }
        let mut names = vec![];
        for button in html.split("<button").skip(1) {
            let tag = &button[..button.find('>').unwrap()];
            let name = tag
                .split("aria-label=\"")
                .nth(1)
                .and_then(|rest| rest.split('"').next())
                .unwrap();
            let disabled = tag.contains("data-disabled");
            names.push(format!("{name}{}", if disabled { "!" } else { "" }));
        }
        Some(names)
    }

    fn situation(
        wide: bool,
        placement: Placement,
        sidebar_shown: bool,
        can_back: bool,
        can_forward: bool,
    ) -> Situation {
        Situation {
            wide,
            placement,
            sidebar_shown,
            can_back,
            can_forward,
        }
    }

    #[test]
    fn a_wide_rack_with_the_sidebar_hidden_holds_the_toggle_back_and_forward() {
        assert_eq!(
            rack(situation(true, Placement::Main, false, true, true)),
            Some(vec![SIDEBAR.into(), BACK.into(), FORWARD.into()])
        );
    }

    #[test]
    fn a_wide_rack_disables_back_and_forward_instead_of_hiding_them() {
        assert_eq!(
            rack(situation(true, Placement::Main, false, false, false)),
            Some(vec![SIDEBAR.into(), "Back!".into(), "Forward!".into()])
        );
        assert_eq!(
            rack(situation(true, Placement::Main, false, true, false)),
            Some(vec![SIDEBAR.into(), BACK.into(), "Forward!".into()])
        );
        assert_eq!(
            rack(situation(true, Placement::Main, false, false, true)),
            Some(vec![SIDEBAR.into(), "Back!".into(), FORWARD.into()])
        );
    }

    #[test]
    fn a_wide_details_position_has_no_rack_of_its_own() {
        // The window's top-left corner is the main position's.
        assert_eq!(
            rack(situation(true, Placement::Details, false, true, true)),
            None
        );
    }

    #[test]
    fn every_rack_hides_while_the_sidebar_is_shown() {
        for wide in [true, false] {
            for placement in [Placement::Main, Placement::Details] {
                for (can_back, can_forward) in
                    [(false, false), (true, false), (false, true), (true, true)]
                {
                    assert_eq!(
                        rack(situation(wide, placement, true, can_back, can_forward)),
                        None,
                        "{wide} {placement:?} {can_back} {can_forward}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_compact_main_rack_holds_the_toggle_and_back_when_there_is_somewhere_to_go_back_to() {
        assert_eq!(
            rack(situation(false, Placement::Main, false, false, false)),
            Some(vec![SIDEBAR.into()])
        );
        assert_eq!(
            rack(situation(false, Placement::Main, false, true, false)),
            Some(vec![SIDEBAR.into(), BACK.into()])
        );
    }

    #[test]
    fn a_compact_rack_never_holds_forward() {
        for placement in [Placement::Main, Placement::Details] {
            let names = rack(situation(false, placement, false, true, true)).unwrap();
            assert!(!names.iter().any(|name| name.starts_with(FORWARD)));
        }
    }

    #[test]
    fn a_compact_details_rack_is_back_only() {
        assert_eq!(
            rack(situation(false, Placement::Details, false, true, false)),
            Some(vec![BACK.into()])
        );
    }

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

    /// The header's buttons as the page renders them, in order, `!` after a disabled one.
    fn header(wide: bool, can_back: bool, can_forward: bool) -> Vec<String> {
        fn app(props: (bool, bool, bool)) -> Element {
            rsx! {
                SidebarHeader {
                    wide: props.0,
                    can_back: props.1,
                    can_forward: props.2,
                    on_hide: |()| {},
                    on_back: |()| {},
                    on_forward: |()| {},
                }
            }
        }
        let mut dom = VirtualDom::new_with_props(app, (wide, can_back, can_forward));
        dom.rebuild_in_place();
        let html = dioxus_ssr::render(&dom);
        assert!(
            html.contains("aria-label=\"daytxt\""),
            "the mark is named: {html}"
        );
        html.split("<button")
            .skip(1)
            .map(|button| {
                let tag = &button[..button.find('>').unwrap()];
                let name = tag
                    .split("aria-label=\"")
                    .nth(1)
                    .and_then(|rest| rest.split('"').next())
                    .unwrap();
                format!(
                    "{name}{}",
                    if tag.contains("data-disabled") {
                        "!"
                    } else {
                        ""
                    }
                )
            })
            .collect()
    }

    #[test]
    fn the_docked_header_holds_the_racks_three_and_the_drawers_only_hide() {
        assert_eq!(
            header(true, true, false),
            ["Hide sidebar", BACK, "Forward!"]
        );
        assert_eq!(
            header(true, false, false),
            ["Hide sidebar", "Back!", "Forward!"]
        );
        assert_eq!(header(false, true, true), ["Hide sidebar"]);
    }
}
