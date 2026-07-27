use crate::interlude::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ThemeMode {
    #[default]
    Light,
    Dark,
}

impl ThemeMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Light => "light",
            Self::Dark => "dark",
        }
    }
}

#[derive(Clone, Copy)]
pub struct ThemeController {
    mode: RwSignal<ThemeMode>,
}

impl ThemeController {
    pub fn init() -> Self {
        let controller = Self {
            mode: RwSignal::new(initial_mode()),
        };
        provide_context(controller);

        #[cfg(feature = "hydrate")]
        Effect::new(move |_| {
            let mode = controller.mode.get();
            apply_root_theme(mode);
            persist_mode(mode);
        });

        controller
    }

    pub fn toggle(self) {
        self.mode.update(|mode| {
            *mode = match *mode {
                ThemeMode::Light => ThemeMode::Dark,
                ThemeMode::Dark => ThemeMode::Light,
            };
        });
    }

    pub fn is_dark(self) -> bool {
        self.mode.get() == ThemeMode::Dark
    }
}

pub const STORAGE_KEY: &str = "sysadmin:theme";

pub fn init_script() -> &'static str {
    r#"(() => {
  const key = 'sysadmin:theme';
  const root = document.documentElement;
  try {
    const stored = localStorage.getItem(key);
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    const isDark = stored === 'dark' || (stored !== 'light' && prefersDark);
    root.classList.toggle('dark', isDark);
    root.style.colorScheme = isDark ? 'dark' : 'light';
  } catch (_) {
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    root.classList.toggle('dark', !!prefersDark);
    root.style.colorScheme = prefersDark ? 'dark' : 'light';
  }
})();"#
}

fn initial_mode() -> ThemeMode {
    #[cfg(feature = "hydrate")]
    {
        if let Some(mode) = read_mode_from_storage() {
            return mode;
        }
        if prefers_dark_media() {
            return ThemeMode::Dark;
        }
    }

    ThemeMode::Light
}

#[cfg(feature = "hydrate")]
fn apply_root_theme(mode: ThemeMode) {
    use wasm_bindgen::JsCast as _;

    let document = document();
    if let Some(root) = document.document_element() {
        match mode {
            ThemeMode::Dark => {
                let _ = root.class_list().add_1("dark");
            }
            ThemeMode::Light => {
                let _ = root.class_list().remove_1("dark");
            }
        }

        if let Some(el) = root.dyn_ref::<leptos::web_sys::HtmlElement>() {
            let _ = el.style().set_property("color-scheme", mode.as_str());
        }
    }
}

#[cfg(feature = "hydrate")]
fn read_mode_from_storage() -> Option<ThemeMode> {
    let window = leptos::prelude::window();
    let storage = window.local_storage().ok().flatten()?;
    let raw = storage.get_item(STORAGE_KEY).ok().flatten()?;
    match raw.as_str() {
        "dark" => Some(ThemeMode::Dark),
        "light" => Some(ThemeMode::Light),
        _ => None,
    }
}

#[cfg(feature = "hydrate")]
fn prefers_dark_media() -> bool {
    window()
        .match_media("(prefers-color-scheme: dark)")
        .ok()
        .flatten()
        .map(|m| m.matches())
        .unwrap_or(false)
}

#[cfg(feature = "hydrate")]
fn persist_mode(mode: ThemeMode) {
    if let Ok(Some(storage)) = leptos::prelude::window().local_storage() {
        let _ = storage.set_item(STORAGE_KEY, mode.as_str());
    }
}
