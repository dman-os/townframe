//! The navigation stack (ADR 032 §3): a pure model with no panels, no Dioxus and no calls, so
//! its semantics are tested as plain data. Entries name what is shown, never a rendered
//! surface; the shell validates a name against its panels before it pushes.
//!
//! The base of every stack is implicit: with nothing pushed the shell shows the home panel, so
//! going home is an ordinary push of its panel and back from it returns to where you were.
//!
//! The model is a browser's history: `entries` plus how many of them are active. Back and
//! forward move that count; pushing discards the forward entries. Two things differ from a
//! browser, both because they are not places to return to:
//! * an **overlay** (a sheet, the compact sidebar drawer) sits only on top of the stack, and
//!   anything that leaves it (back, a push, another overlay) discards it for good, so forward
//!   never resurrects a dismissed sheet;
//! * opening another row of the list whose row is already open *replaces* that row's entry
//!   instead of stacking a second one, so back leaves the list rather than stepping through
//!   every row you glanced at.

/// What one stack entry shows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Entry {
    /// A panel in the main position.
    Main(String),
    /// A panel in the details position over the main one: one `row` of a list panel, or (with
    /// no `row`) a panel that only fills the details socket.
    Details { panel: String, row: Option<String> },
    /// A sheet: the [`Socket::Sheet`](micro_shell_manifest::Socket::Sheet) panel `.0`.
    Sheet(String),
    /// The sidebar drawer of a compact host.
    Sidebar,
}

impl Entry {
    fn is_overlay(&self) -> bool {
        matches!(self, Self::Sheet(_) | Self::Sidebar)
    }

    fn panel(&self) -> Option<&str> {
        match self {
            Self::Main(panel) | Self::Sheet(panel) | Self::Details { panel, .. } => Some(panel),
            Self::Sidebar => None,
        }
    }
}

/// What is on top of everything else.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Overlay<'a> {
    Sheet(&'a str),
    Sidebar,
}

/// What the active entries add up to: the arrangement the frame draws.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Frame<'a> {
    /// The panel in the main position; `None` is the home panel.
    pub main: Option<&'a str>,
    /// The panel in the details position and, for a list, the row it shows.
    pub details: Option<(&'a str, Option<&'a str>)>,
    pub overlay: Option<Overlay<'a>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Navigation {
    entries: Vec<Entry>,
    /// How many of `entries` are active: the rest are forward entries.
    active: usize,
}

impl Navigation {
    /// Show `entry`, discarding the forward entries (and any overlay, see the module docs).
    pub fn push(&mut self, entry: Entry) {
        self.entries.truncate(self.active);
        if self.entries.last().is_some_and(Entry::is_overlay) {
            self.entries.pop();
        }
        match (self.entries.last_mut(), &entry) {
            (Some(Entry::Details { panel, row }), Entry::Details { panel: next, row: next_row })
                if panel == next && row.is_some() && next_row.is_some() =>
            {
                row.clone_from(next_row);
            }
            _ => self.entries.push(entry),
        }
        self.active = self.entries.len();
    }

    /// Whether back has somewhere to go: this is what makes the sidebar/back button read
    /// **back**.
    pub fn can_back(&self) -> bool {
        self.active > 0
    }

    /// Step back, or dismiss the overlay on top. `false` when there is nothing to go back to.
    pub fn back(&mut self) -> bool {
        if self.active == 0 {
            return false;
        }
        if self.entries[self.active - 1].is_overlay() {
            self.entries.truncate(self.active - 1);
            self.active -= 1;
        } else {
            self.active -= 1;
        }
        true
    }

    /// Step forward over an entry back left. `false` when there is none.
    pub fn forward(&mut self) -> bool {
        if self.active == self.entries.len() {
            return false;
        }
        self.active += 1;
        true
    }

    /// Dismiss the overlay on top, if it is a sheet. `false` when none is.
    pub fn close_sheet(&mut self) -> bool {
        if matches!(self.active_entries().last(), Some(Entry::Sheet(_))) {
            return self.back();
        }
        false
    }

    /// Drop every entry that names a panel `exists` rejects (a sheet goes with its panel), then
    /// keep the active/forward split where it was for the entries that remain.
    pub fn retain_panels(&mut self, exists: impl Fn(&str) -> bool) {
        let mut position = 0;
        let mut active = 0;
        self.entries.retain(|entry| {
            let keep = entry.panel().is_none_or(&exists);
            if position < self.active && keep {
                active += 1;
            }
            position += 1;
            keep
        });
        self.active = active;
    }

    fn active_entries(&self) -> &[Entry] {
        &self.entries[..self.active]
    }

    /// The arrangement the active entries add up to.
    pub fn frame(&self) -> Frame<'_> {
        let mut frame = Frame::default();
        for entry in self.active_entries() {
            match entry {
                Entry::Main(panel) => {
                    frame.main = Some(panel);
                    frame.details = None;
                }
                Entry::Details { panel, row } => frame.details = Some((panel, row.as_deref())),
                Entry::Sheet(panel) => frame.overlay = Some(Overlay::Sheet(panel)),
                Entry::Sidebar => frame.overlay = Some(Overlay::Sidebar),
            }
        }
        frame
    }

    /// The row `panel` has open in the details position, if it has one.
    pub fn opened(&self, panel: &str) -> Option<&str> {
        match self.frame().details {
            Some((open, row)) if open == panel => row,
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn main(panel: &str) -> Entry {
        Entry::Main(panel.into())
    }

    fn row(panel: &str, row: &str) -> Entry {
        Entry::Details {
            panel: panel.into(),
            row: Some(row.into()),
        }
    }

    #[test]
    fn an_empty_stack_shows_home_and_has_nowhere_to_go_back() {
        let mut navigation = Navigation::default();
        assert_eq!(navigation.frame(), Frame::default());
        assert!(!navigation.can_back());
        assert!(!navigation.back());
        assert!(!navigation.forward());
    }

    #[test]
    fn back_and_forward_walk_the_pushed_entries() {
        let mut navigation = Navigation::default();
        navigation.push(main("settings"));
        navigation.push(row("settings", "counter"));
        assert_eq!(navigation.opened("settings"), Some("counter"));
        assert_eq!(navigation.opened("files"), None);
        assert!(navigation.back());
        assert_eq!(navigation.frame().main, Some("settings"));
        assert_eq!(navigation.frame().details, None);
        assert!(navigation.back());
        assert_eq!(navigation.frame(), Frame::default());
        assert!(!navigation.can_back());
        assert!(navigation.forward());
        assert!(navigation.forward());
        assert_eq!(navigation.opened("settings"), Some("counter"));
        assert!(!navigation.forward());
    }

    #[test]
    fn a_push_discards_the_forward_entries() {
        let mut navigation = Navigation::default();
        navigation.push(main("a"));
        navigation.push(main("b"));
        navigation.back();
        navigation.push(main("c"));
        assert!(!navigation.forward());
        navigation.back();
        assert_eq!(navigation.frame().main, Some("a"));
    }

    #[test]
    fn opening_another_row_of_the_open_list_replaces_the_row() {
        let mut navigation = Navigation::default();
        navigation.push(main("settings"));
        navigation.push(row("settings", "a"));
        navigation.push(row("settings", "b"));
        assert_eq!(navigation.opened("settings"), Some("b"));
        // One back leaves the details, not the previous row.
        navigation.back();
        assert_eq!(navigation.frame().details, None);
        assert_eq!(navigation.frame().main, Some("settings"));
    }

    #[test]
    fn a_new_main_panel_clears_the_details_of_the_old_one() {
        let mut navigation = Navigation::default();
        navigation.push(main("settings"));
        navigation.push(row("settings", "a"));
        navigation.push(main("files"));
        assert_eq!(navigation.frame().main, Some("files"));
        assert_eq!(navigation.frame().details, None);
        navigation.back();
        assert_eq!(navigation.opened("settings"), Some("a"));
    }

    #[test]
    fn back_dismisses_a_sheet_and_forward_does_not_bring_it_back() {
        let mut navigation = Navigation::default();
        navigation.push(main("editor"));
        navigation.push(Entry::Sheet("picker".into()));
        assert_eq!(navigation.frame().overlay, Some(Overlay::Sheet("picker")));
        assert!(navigation.can_back());
        assert!(navigation.back());
        assert_eq!(navigation.frame().overlay, None);
        assert_eq!(navigation.frame().main, Some("editor"));
        assert!(!navigation.forward());
    }

    #[test]
    fn closing_a_sheet_is_a_back_that_only_a_sheet_answers() {
        let mut navigation = Navigation::default();
        navigation.push(main("editor"));
        assert!(!navigation.close_sheet());
        assert_eq!(navigation.frame().main, Some("editor"));
        navigation.push(Entry::Sheet("picker".into()));
        assert!(navigation.close_sheet());
        assert_eq!(navigation.frame().overlay, None);
        navigation.push(Entry::Sidebar);
        assert!(!navigation.close_sheet());
        assert_eq!(navigation.frame().overlay, Some(Overlay::Sidebar));
    }

    #[test]
    fn leaving_for_another_panel_dismisses_the_overlay() {
        let mut navigation = Navigation::default();
        navigation.push(Entry::Sidebar);
        navigation.push(main("settings"));
        assert_eq!(navigation.frame().overlay, None);
        navigation.back();
        // Back from the panel returns to home, not to the drawer.
        assert_eq!(navigation.frame(), Frame::default());
        assert!(!navigation.can_back());
        navigation.push(Entry::Sheet("a".into()));
        navigation.push(Entry::Sheet("b".into()));
        assert_eq!(navigation.frame().overlay, Some(Overlay::Sheet("b")));
        navigation.back();
        assert_eq!(navigation.frame().overlay, None);
    }

    #[test]
    fn a_vanished_panel_takes_its_entries_and_sheets_with_it() {
        let mut navigation = Navigation::default();
        navigation.push(main("settings"));
        navigation.push(row("settings", "a"));
        navigation.push(Entry::Sheet("picker".into()));
        navigation.retain_panels(|panel| panel != "picker");
        assert_eq!(navigation.frame().overlay, None);
        assert_eq!(navigation.opened("settings"), Some("a"));
        navigation.back();
        navigation.retain_panels(|panel| panel != "settings");
        // The forward entry named the vanished panel too.
        assert!(!navigation.forward());
        assert_eq!(navigation.frame(), Frame::default());
    }
}
