//! The kit's z-order: one scale, so no component picks a `z-index` of its own (`KIT-102`).
//!
//! The values are the `layer.*` semantic tokens (`semantic.tokens.json`), emitted as
//! `--df-layer-*` custom properties by `x/design-tokens.ts`. A component names a [`Layer`] and
//! writes its [`css`](Layer::css) (`var(--df-layer-sheet)`) into its inline `style`; the number
//! lives only in the token file, where a theme or a host can read it, and the test below keeps the
//! enum and the file in step.
//!
//! Overlay surfaces (`Drawer`, `Sheet`) are one element on their layer whose first child is the
//! scrim and whose second is the panel. Neither child has a `z-index`: both are positioned, so
//! tree order stacks the panel over the scrim inside the overlay's own stacking context, and
//! nothing a panel's content does can climb above the overlay or fall below its scrim.

/// What a surface is stacked as, lowest first. Declaration order is the z-order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Layer {
    /// A pane or panel. Giving it a layer makes it a stacking context, so whatever a panel's
    /// content does (an app bar, a popup) stays inside it and a resize handle can overlap its edge.
    Content,
    /// Something that must paint over its siblings inside the content: the details covering the
    /// list on a compact host, a resize handle overlapping the panes beside it.
    Raised,
    /// A panel's app bar, over the content that scrolls beneath it.
    AppBar,
    /// The compact host's sidebar and its scrim.
    Drawer,
    /// A sheet and its scrim. Above the drawer: a sheet opened from the sidebar covers it.
    Sheet,
    /// A transient message over everything a user can dismiss. No kit component yet; the shell's
    /// toast takes this layer.
    Toast,
    /// The window-sized overlay that carries a drag past the edge of its handle (`KIT-041`). It
    /// is the topmost layer so it also receives a drag begun in a sheet.
    Gesture,
}

impl Layer {
    /// The token's custom property as a CSS value, for an inline `z-index`.
    pub const fn css(self) -> &'static str {
        match self {
            Self::Content => "var(--df-layer-content)",
            Self::Raised => "var(--df-layer-raised)",
            Self::AppBar => "var(--df-layer-app-bar)",
            Self::Drawer => "var(--df-layer-drawer)",
            Self::Sheet => "var(--df-layer-sheet)",
            Self::Toast => "var(--df-layer-toast)",
            Self::Gesture => "var(--df-layer-gesture)",
        }
    }
}

/// The dim layer behind an overlay's panel: the `scrim` colour role, which is a fixed black at
/// 32% (Material's value) and so darkens a light and a dark theme alike. It fades in with the
/// overlay (`KIT-108`). Shared by every overlay so they cannot disagree.
pub(crate) const SCRIM: &str = "cursor-default bg-scrim animate-fade-in motion-reduce:animate-none";

#[cfg(test)]
mod tests {
    use super::*;

    const LAYERS: [(Layer, &str); 7] = [
        (Layer::Content, "content"),
        (Layer::Raised, "raised"),
        (Layer::AppBar, "app-bar"),
        (Layer::Drawer, "drawer"),
        (Layer::Sheet, "sheet"),
        (Layer::Toast, "toast"),
        (Layer::Gesture, "gesture"),
    ];

    #[test]
    fn the_enum_is_the_token_scale() {
        let tokens: serde_json::Value =
            serde_json::from_str(include_str!("../ui/styles/tokens/semantic.tokens.json")).unwrap();
        let tokens = tokens["layer"].as_object().unwrap();
        let mut previous = None;
        for (layer, name) in LAYERS {
            assert_eq!(layer.css(), format!("var(--df-layer-{name})"));
            let z = tokens[name]["$value"].as_i64().unwrap();
            assert!(
                previous < Some(z),
                "{name} is not above the layer before it"
            );
            previous = Some(z);
        }
        assert_eq!(
            tokens.keys().filter(|key| !key.starts_with('$')).count(),
            LAYERS.len(),
            "a token without a Layer, or the reverse"
        );
        assert!(LAYERS.windows(2).all(|pair| pair[0].0 < pair[1].0));
    }

    #[test]
    fn no_headless_part_names_a_z_index_of_its_own() {
        for (module, source) in [
            ("app_bar", include_str!("headless/app_bar.rs")),
            ("drawer", include_str!("headless/drawer.rs")),
            ("list_details", include_str!("headless/list_details.rs")),
            ("sections", include_str!("headless/sections.rs")),
            ("sheet", include_str!("headless/sheet.rs")),
        ] {
            // Outside the module's own tests, every `z-index:` is followed by a `{` that
            // interpolates a `Layer`, never a number.
            let source = source.split("#[cfg(test)]").next().unwrap();
            for (at, _) in source.match_indices("z-index:") {
                let after = &source[at + "z-index:".len()..];
                assert!(
                    after.starts_with('{'),
                    "headless/{module}.rs hard-codes a z-index: {}",
                    &after[..after.len().min(24)]
                );
            }
        }
    }
}
