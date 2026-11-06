// Ported from @pierre/theming (Apache-2.0, Pierre Computer Company): the
// `createThemeResolver` module (dist/modules/createThemeResolver.js), the only
// part of the package the diffs highlighter needs.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: synchronous. Upstream loaders return promises, so it tracks
// `inflight` loads and a `cacheGeneration` that stops a load started before
// `clearResolvedThemes` from repopulating the cache; with synchronous loaders
// there is nothing in flight, so both are dropped, and `resolveThemes`/
// `getResolvedOrResolveTheme` return values instead of promises. Errors are
// [`ThemeError`] values; `unwrapDefault` (ES module `default` unwrapping) is a
// JavaScript module detail and is dropped.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThemeError {
    /// `Theme "<name>" is already registered`
    Duplicate(String),
    /// `No loader registered for theme "<name>"`
    Unregistered(String),
    /// `Theme "<name>" has not been resolved`
    Unresolved(String),
    /// A loaded theme failed `normalizeTheme`.
    Invalid(String),
}

impl std::fmt::Display for ThemeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duplicate(name) => write!(formatter, "Theme \"{name}\" is already registered"),
            Self::Unregistered(name) => {
                write!(formatter, "No loader registered for theme \"{name}\"")
            }
            Self::Unresolved(name) => write!(formatter, "Theme \"{name}\" has not been resolved"),
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ThemeError {}

/// A loader returns `None` for a theme it does not have.
pub type ThemeLoader<Theme> = Box<dyn Fn() -> Option<Theme>>;

/// `fallbackLoader`: asked when no loader is registered for a name.
pub type FallbackLoader<Theme> = Box<dyn Fn(&str) -> Option<Theme>>;

/// `normalizeTheme`: validates (or rewrites) a loaded theme.
pub type NormalizeTheme<Theme> = Box<dyn Fn(Theme, &str) -> Result<Theme, ThemeError>>;

pub struct ThemeResolver<Theme> {
    loaders: HashMap<String, ThemeLoader<Theme>>,
    resolved: RefCell<HashMap<String, Rc<Theme>>>,
    fallback_loader: Option<FallbackLoader<Theme>>,
    normalize_theme: Option<NormalizeTheme<Theme>>,
}

impl<Theme> ThemeResolver<Theme> {
    pub fn new(
        fallback_loader: Option<FallbackLoader<Theme>>,
        normalize_theme: Option<NormalizeTheme<Theme>>,
    ) -> Self {
        Self {
            loaders: HashMap::new(),
            resolved: RefCell::new(HashMap::new()),
            fallback_loader,
            normalize_theme,
        }
    }

    pub fn register_theme(
        &mut self,
        name: &str,
        loader: ThemeLoader<Theme>,
    ) -> Result<(), ThemeError> {
        if self.loaders.contains_key(name) {
            return Err(ThemeError::Duplicate(name.to_owned()));
        }
        self.loaders.insert(name.to_owned(), loader);
        Ok(())
    }

    /// Returns whether the loader was registered.
    pub fn register_theme_if_absent(&mut self, name: &str, loader: ThemeLoader<Theme>) -> bool {
        self.register_theme(name, loader).is_ok()
    }

    pub fn has_registered_theme(&self, name: &str) -> bool {
        self.loaders.contains_key(name)
    }

    /// Loads (or returns the cached) theme.
    pub fn resolve_theme(&self, name: &str) -> Result<Rc<Theme>, ThemeError> {
        if let Some(cached) = self.resolved.borrow().get(name) {
            return Ok(Rc::clone(cached));
        }
        let loaded = match self.loaders.get(name) {
            Some(loader) => loader(),
            None => self.fallback_loader.as_ref().and_then(|load| load(name)),
        }
        .ok_or_else(|| ThemeError::Unregistered(name.to_owned()))?;
        let theme = match &self.normalize_theme {
            Some(normalize) => normalize(loaded, name)?,
            None => loaded,
        };
        let theme = Rc::new(theme);
        self.resolved
            .borrow_mut()
            .insert(name.to_owned(), Rc::clone(&theme));
        Ok(theme)
    }

    pub fn resolve_themes(&self, names: &[&str]) -> Result<Vec<Rc<Theme>>, ThemeError> {
        names.iter().map(|name| self.resolve_theme(name)).collect()
    }

    pub fn seed_resolved_theme(&self, name: &str, theme: Theme) {
        self.resolved
            .borrow_mut()
            .insert(name.to_owned(), Rc::new(theme));
    }

    pub fn get_resolved_theme(&self, name: &str) -> Option<Rc<Theme>> {
        self.resolved.borrow().get(name).cloned()
    }

    pub fn get_resolved_themes(&self, names: &[&str]) -> Result<Vec<Rc<Theme>>, ThemeError> {
        names
            .iter()
            .map(|name| {
                self.get_resolved_theme(name)
                    .ok_or_else(|| ThemeError::Unresolved((*name).to_owned()))
            })
            .collect()
    }

    pub fn has_resolved_theme(&self, name: &str) -> bool {
        self.resolved.borrow().contains_key(name)
    }

    pub fn has_resolved_themes(&self, names: &[&str]) -> bool {
        names.iter().all(|name| self.has_resolved_theme(name))
    }

    pub fn clear_resolved_themes(&self) {
        self.resolved.borrow_mut().clear();
    }
}
