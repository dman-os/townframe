//! The shell (ADR 032): a native micro that owns the frame, the navigation stack and the
//! sidebar/back button, and arranges the panels other micros contribute. It is native because it
//! is on every startup path and owns the frame, not because it needs a platform API; its
//! contract ([`micro_shell_manifest`]) is backend-free so any micro can contribute to it and an
//! alternative shell can implement it.
//!
//! The mechanism: the `shell.panels` collection holds one row per panel, a title, the sockets it
//! may fill and lazy presentation references. The frame reads rows without starting any
//! contributor and mounts exactly the panels the navigation stack shows, each through a
//! `PresentationSlot`, so a panel that is listed but not shown never activates its provider.
//! The stack ([`navigation::Navigation`]) is device-local state held in the shell's
//! incarnation: it is never written to a document, so it is never synced or applied from
//! another device (FDR 006 "Sync and restore"); only the arrangement weights live in the
//! shell's configuration document. Because it lives in the incarnation, it starts over at home
//! when the shell restarts; persisting it through a store that never syncs is future work.
//!
//! The host tells the shell what only it knows through [`HostInputs`], a Dioxus context the
//! composition provides around the frame: the width available to the frame (the shell never
//! measures the DOM, `KIT-040`) and the platform back gesture.
pub use micro_shell_manifest as contract;
mod navigation;
mod view;

use clay_mdk::Collection;
use clay_mdk::Context;
use clay_mdk::Error;
use clay_mdk::Facet;
use contract::NavigateRequest;
use contract::OpenSheetRequest;
use contract::Panel;
use contract::ShellConfig;
use contract::Socket;
use dioxus::prelude::*;
use navigation::Entry;
use navigation::Navigation;
use std::rc::Rc;
use tokio::sync::watch;

/// What the host tells the native shell. The composition provides it as a Dioxus context around
/// every presentation of the shell, which are all mounted in-process.
#[derive(Clone, Copy, PartialEq)]
pub struct HostInputs {
    /// The logical px width available to the frame. The host owns the window, so it classifies
    /// nothing: the shell derives the width class from this and passes the px down as the
    /// extent a drag converts pointer travel with.
    pub width: Signal<f64>,
    /// Incremented by the host for every platform back gesture (Android `KEYCODE_BACK`, which
    /// winit reports as `BrowserBack`, a mouse back button, a browser's back).
    pub platform_back: Signal<u64>,
    /// Written by the shell: whether back has somewhere to go, so a host that must decide
    /// whether to claim the platform gesture (the browser's history) can.
    pub can_back: Signal<bool>,
}

/// The device-local part of the shell: the stack, and whether a wide host's sidebar is folded.
#[derive(Clone, Default, PartialEq)]
struct State {
    navigation: Navigation,
    sidebar_hidden: bool,
}

/// The shell's handles, shared by every presentation and call it publishes. Equal when they are
/// the same activation.
#[derive(Clone)]
struct Shell {
    state: Rc<watch::Sender<State>>,
    panels: Collection<String, Panel>,
    config: Facet<ShellConfig>,
}

impl PartialEq for Shell {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.state, &other.state)
    }
}

impl Shell {
    /// Change the stack, and tell watchers only if it changed.
    fn edit(&self, edit: impl FnOnce(&mut State) -> bool) -> bool {
        let mut moved = false;
        self.state.send_if_modified(|state| {
            moved = edit(state);
            moved
        });
        moved
    }

    /// The panels once the collection has loaded: a call made before that would otherwise find
    /// no panel at all.
    async fn loaded_panels(&self) -> Result<std::rc::Rc<[clay_mdk::Row<String, Panel>]>, Error> {
        self.panels.bootstrapped().await;
        self.panels.rows()
    }

    async fn navigate(&self, request: NavigateRequest) -> Result<(), Error> {
        let rows = self.loaded_panels().await?;
        let panel = rows
            .iter()
            .find(|row| row.key == request.panel)
            .ok_or_else(|| Error::Rejected(format!("no panel {:?}", request.panel)))?;
        let sockets = &panel.value.sockets;
        let entry = match request.row {
            Some(row) if panel.value.details.is_some() => Entry::Details {
                panel: request.panel,
                row: Some(row),
            },
            Some(_) => {
                return Err(Error::Rejected(format!(
                    "panel {:?} opens no rows",
                    request.panel
                )));
            }
            None if sockets.contains(&Socket::List) => Entry::Main(request.panel),
            None if sockets.contains(&Socket::Details) => Entry::Details {
                panel: request.panel,
                row: None,
            },
            None => {
                return Err(Error::Rejected(format!(
                    "panel {:?} fills neither the list nor the details socket",
                    request.panel
                )));
            }
        };
        self.edit(|state| {
            state.navigation.push(entry);
            true
        });
        Ok(())
    }

    async fn open_sheet(&self, request: OpenSheetRequest) -> Result<(), Error> {
        let rows = self.loaded_panels().await?;
        let panel = rows
            .iter()
            .find(|row| row.key == request.panel)
            .ok_or_else(|| Error::Rejected(format!("no panel {:?}", request.panel)))?;
        if !panel.value.sockets.contains(&Socket::Sheet) {
            return Err(Error::Rejected(format!(
                "panel {:?} does not fill the sheet socket",
                request.panel
            )));
        }
        self.edit(|state| {
            state.navigation.push(Entry::Sheet(request.panel));
            true
        });
        Ok(())
    }

    fn back(&self) -> bool {
        self.edit(|state| state.navigation.back())
    }

    fn forward(&self) -> bool {
        self.edit(|state| state.navigation.forward())
    }

    fn close_sheet(&self) -> bool {
        self.edit(|state| state.navigation.close_sheet())
    }

    fn opened(&self, panel: &str) -> Option<String> {
        self.state
            .borrow()
            .navigation
            .opened(panel)
            .map(str::to_owned)
    }

    /// Drop stack entries whose panel is gone (a sheet goes with its panel, ADR 032 §6).
    fn prune(&self) {
        let rows = self.panels.rows().expect("the panels mirror outlives the shell");
        self.edit(|state| {
            let before = state.navigation.clone();
            state
                .navigation
                .retain_panels(|panel| rows.iter().any(|row| row.key == panel));
            before != state.navigation
        });
    }
}

/// The shell reads its panels and its arrangement, and publishes its frame, home panel, button
/// and navigation calls. The home panel and the frame's root row are declared in the manifest,
/// so they exist before this runs.
pub async fn activate(context: Context) -> Result<(), Error> {
    let panels: Collection<String, Panel> = context
        .collection(contract::PANELS_BINDING)
        .expect("configuration binds the panels collection");
    let config = context
        .config(contract::CONFIG_NAME)
        .expect("manifest declares the shell configuration")
        .facet::<ShellConfig>(contract::CONFIG_TAG, contract::CONFIG_KEY)?;
    let (state, _) = watch::channel(State::default());
    let shell = Shell {
        state: Rc::new(state),
        panels,
        config,
    };

    for (name, view) in [
        (contract::FRAME, view::frame as fn(Shell) -> Element),
        (contract::HOME, view::home),
        (contract::NAV_BUTTON, view::navigation_button),
    ] {
        let shell = shell.clone();
        context.publish_component(name, move || view(shell.clone()))?;
    }

    context.publish_call(contract::NAVIGATE, {
        let shell = shell.clone();
        move |request: NavigateRequest| {
            let shell = shell.clone();
            async move { shell.navigate(request).await }
        }
    })?;
    context.publish_call(contract::OPEN_SHEET, {
        let shell = shell.clone();
        move |request: OpenSheetRequest| {
            let shell = shell.clone();
            async move { shell.open_sheet(request).await }
        }
    })?;
    let simple = |name: &str, act: fn(&Shell) -> bool| {
        let shell = shell.clone();
        context.publish_call(name, move |(): ()| {
            let shell = shell.clone();
            async move { Ok(act(&shell)) }
        })
    };
    simple(contract::BACK, Shell::back)?;
    simple(contract::FORWARD, Shell::forward)?;
    simple(contract::CLOSE_SHEET, Shell::close_sheet)?;
    context.publish_call(contract::OPENED, {
        let shell = shell.clone();
        move |panel: String| {
            let shell = shell.clone();
            async move { Ok(shell.opened(&panel)) }
        }
    })?;

    context.spawn({
        let mut changes = shell.panels.clone();
        async move {
            // The feed ends with the incarnation, which ends the wait.
            while changes.changed().await.is_ok() {
                shell.prune();
            }
        }
    });
    Ok(())
}
