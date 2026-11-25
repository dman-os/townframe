//! Engine-neutral declarations of the theme lab micro: the end user's theme editor. It publishes
//! one presentation, contributes it as a list panel the shell's home offers, and requires three
//! of the shell's publications: the navigation rack for its app bar, and the `get-theme` and
//! `set-theme` calls it edits the host's theme through. It holds no configuration of its own:
//! the applied theme is stored by the shell (`micro_shell_manifest::ShellTheme`). The wasm guest
//! reads only the constants; the composition root reads `manifest()`.
use clay_mdk::ComponentDecl;
use clay_mdk::ManifestBuilder;
use clay_mdk::MicroManifest;
use clay_mdk::Necessity;
use clay_mdk::PackageDecl;
use clay_mdk::Sidecar;
use micro_shell_manifest::ShellPanels;
use micro_shell_manifest::Socket;

pub const ID: &str = "dayframe.theme-lab";
pub const PACKAGE_VERSION: u32 = 1;
/// The NSID prefix the theme lab holds. It declares no contracts: everything it calls is the
/// shell's.
pub const AUTHORITY: &str = "org.example.dayframe.themelab";
/// The theme lab's one presentation: the CSS editor, its preview and Apply.
pub const VIEW: &str = "view";
/// The local binding name under which the theme lab contributes its shell panel.
pub const PANELS_BINDING: &str = "panels";
/// The local binding names under which it requires what it uses of the shell. They are local
/// names: the catalog resolves each requirement by the contract it names to the micro serving
/// it.
pub const SHELL_NAVIGATION_RACK: &str = micro_shell_manifest::NAVIGATION_RACK;
pub const SHELL_GET_THEME: &str = micro_shell_manifest::GET_THEME;
pub const SHELL_SET_THEME: &str = micro_shell_manifest::SET_THEME;

/// The theme lab package's identity.
pub struct ThemeLab;

impl PackageDecl for ThemeLab {
    const ID: &'static str = ID;
    const VERSION: u32 = PACKAGE_VERSION;
    const AUTHORITY: &'static str = AUTHORITY;
}

/// The theme lab's presentation, published as [`VIEW`].
pub struct ThemeLabView;

impl ComponentDecl for ThemeLabView {
    const NAME: &'static str = VIEW;
}

/// The theme lab's package declaration. Its panel row is declared, so the shell lists it on home
/// while it is stopped, and opening it is what starts it.
pub fn manifest() -> MicroManifest {
    ManifestBuilder::new()
        .view::<ThemeLabView>()
        .requires(
            SHELL_NAVIGATION_RACK,
            micro_shell_manifest::NAVIGATION_RACK_CONTRACT,
            Necessity::Required,
        )
        .requires(
            SHELL_GET_THEME,
            micro_shell_manifest::GET_THEME_CONTRACT,
            Necessity::Required,
        )
        .requires(
            SHELL_SET_THEME,
            micro_shell_manifest::SET_THEME_CONTRACT,
            Necessity::Required,
        )
        .writes::<ShellPanels>(PANELS_BINDING, Necessity::Required)
        .contributes::<ShellPanels>("panel", ID, |at| micro_shell_manifest::Panel {
            title: "Theme lab".into(),
            sockets: vec![Socket::List],
            content: at.own::<ThemeLabView>(),
            details: None,
            home: false,
        })
        .build()
}

/// The theme lab package as its sidecar carries it: identity, authority and manifest.
pub fn sidecar() -> Sidecar {
    Sidecar::of::<ThemeLab>(manifest())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clay_mdk::host::declared_rows;

    // The manifest is wire data (ADR 030 §11); the snapshot follows the current shape so drift
    // is a reviewable diff.
    #[test]
    fn manifest_wire_shape_is_reviewable() {
        insta::assert_yaml_snapshot!(sidecar());
    }

    #[test]
    fn the_manifest_declares_a_shell_panel_row_and_is_served_by_the_shell() {
        sidecar().validate().unwrap();
        let rows = declared_rows(&clay_mdk::MicroId::local(ID), &sidecar()).unwrap();
        assert_eq!(rows.keys().collect::<Vec<_>>(), [PANELS_BINDING]);
        // Every requirement names a contract the shell serves, as a method or as a view, which
        // is what the catalog resolves it by.
        let shell = micro_shell_manifest::sidecar();
        for request in &sidecar().manifest.requires {
            assert!(
                shell
                    .manifest
                    .endpoints
                    .values()
                    .any(|endpoint| match endpoint {
                        clay_mdk::Endpoint::Method { nsid } => nsid == request.contract.nsid(),
                        clay_mdk::Endpoint::View { implements } => {
                            implements.as_ref() == Some(request.contract.nsid())
                        }
                        _ => false,
                    }),
                "the shell serves no {}",
                request.contract
            );
        }
    }
}
