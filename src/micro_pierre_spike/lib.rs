//! Style spike for the Pierre diff viewer port (docs/scratch/pierre-spikes.md, "P1 style"): one
//! guest micro that renders the rows real Pierre 1.5.2 emitted for a 217-row unified diff, as
//! `Hast` nodes the guest turns into a Dioxus tree, one presentation per resolution of the
//! stylesheet-delivery blocker. The rows are data (`assets/rows.*.json`, derived by
//! `x/pierre-spike-assets.ts`); this crate holds no Pierre logic, only the node-to-element walk.
use clay_mdk::Context;
use clay_mdk::Error;
use dioxus::prelude::*;
use micro_settings_manifest::SECTIONS;
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;

pub const ID: &str = "dayframe.pierrespike";
pub const PACKAGE_VERSION: u32 = 1;
pub const AUTHORITY: &str = "org.example.dayframe.pierrespike";
pub const SECTIONS_BINDING: &str = "sections";

/// A node of Pierre's rendered `<pre>`: an element (tag, attributes, children) or a text node.
/// Attribute names `style:<property>` are one inline property in the `style` attribute
/// namespace; every other name is an attribute of its own.
#[derive(Deserialize)]
#[serde(untagged)]
enum Hast {
    Text(String),
    Element {
        t: String,
        a: BTreeMap<String, String>,
        c: Vec<Hast>,
    },
}

/// One presentation of the rows per resolution under test.
struct Mode {
    publication: &'static str,
    title: &'static str,
    rows: &'static str,
    /// The class of the wrapper the guest emits around the rows, standing in for the shadow host
    /// Pierre's stylesheet is written against (`:host`); `None` when the host supplies it.
    host_class: Option<&'static str>,
}

const MODES: &[Mode] = &[];

pub fn manifest() -> clay_mdk::MicroManifest {
    use clay_mdk::Access;
    use clay_mdk::AttachmentDeclaration;
    use clay_mdk::BindingResolution;
    use clay_mdk::ContributionDeclaration;
    use clay_mdk::Entrypoints;
    use clay_mdk::MicroManifest;
    use clay_mdk::Necessity;
    use clay_mdk::Nsid;
    use clay_mdk::PublicationDeclaration;
    use clay_mdk::PublicationKind;
    use clay_mdk::Request;
    MicroManifest {
        entrypoints: Entrypoints::micro_client(),
        publications: MODES
            .iter()
            .map(|mode| PublicationDeclaration {
                name: mode.publication.into(),
                kind: PublicationKind::Presentation,
                contract: None,
            })
            .collect(),
        requires: vec![],
        configuration_requests: vec![],
        collection_contracts: vec![],
        collection_requests: vec![Request {
            binding: SECTIONS_BINDING.into(),
            contract: Nsid::literal(SECTIONS),
            access: Some(Access::Write),
            necessity: Necessity::Optional,
            resolution: BindingResolution::Configured,
            min_revision: None,
        }],
        contributions: MODES
            .iter()
            .map(|mode| ContributionDeclaration {
                id: format!("settings-section-{}", mode.publication),
                binding: SECTIONS_BINDING.into(),
                key: format!("{ID}.{}", mode.publication),
                payload: serde_json::json!({
                    "title": mode.title,
                    "detail": { "$attachment": 0 },
                }),
                attachments: vec![AttachmentDeclaration {
                    index: 0,
                    publication: mode.publication.into(),
                }],
            })
            .collect(),
        extra: Default::default(),
    }
}

pub fn sidecar() -> clay_mdk::Sidecar {
    clay_mdk::Sidecar::new(ID, PACKAGE_VERSION, manifest()).with_authority(AUTHORITY)
}

async fn activate(context: Context) -> Result<(), Error> {
    for mode in MODES {
        let rows: Rc<Hast> =
            Rc::new(serde_json::from_str(mode.rows).expect("the rows fixture is a hast tree"));
        let host_class = mode.host_class;
        context.publish_component(mode.publication, move || view(rows.clone(), host_class))?;
    }
    Ok(())
}

clay_mdk::export_micro!(activate, manifest);

fn view(rows: Rc<Hast>, host_class: Option<&'static str>) -> Element {
    let rows = element(&rows, 0);
    match host_class {
        Some(class) => rsx! { div { class: "{class}", {rows} } },
        None => rows,
    }
}

thread_local! {
    /// Attribute and property names the dynamic tree names. `dioxus_core::Attribute` takes
    /// `&'static str`; the vocabulary is a few dozen names, leaked once each.
    static NAMES: RefCell<BTreeSet<&'static str>> = const { RefCell::new(BTreeSet::new()) };
}

fn interned(name: &str) -> &'static str {
    NAMES.with_borrow_mut(|names| match names.get(name) {
        Some(known) => known,
        None => {
            let leaked: &'static str = Box::leak(name.to_owned().into_boxed_str());
            names.insert(leaked);
            leaked
        }
    })
}

fn attribute(name: &str, value: &str) -> Attribute {
    let value = dioxus::core::AttributeValue::Text(value.to_owned());
    match name.strip_prefix("style:") {
        Some(property) => Attribute::new(interned(property), value, Some("style"), false),
        None => Attribute::new(interned(name), value, None, false),
    }
}

/// The element for one node. The tag set is the four tags the rows use; like the host's
/// `render_element_tag`, a tag is a macro site because `rsx!` names tags statically.
fn element(node: &Hast, key: usize) -> Element {
    match node {
        Hast::Text(text) => rsx! { "{text}" },
        Hast::Element { t, a, c } => {
            let attrs: Vec<Attribute> = a
                .iter()
                .map(|(name, value)| attribute(name, value))
                .collect();
            let children = c
                .iter()
                .enumerate()
                .map(|(index, child)| element(child, index));
            match t.as_str() {
                "pre" => rsx! { pre { key: "{key}", ..attrs, {children} } },
                "code" => rsx! { code { key: "{key}", ..attrs, {children} } },
                "div" => rsx! { div { key: "{key}", ..attrs, {children} } },
                "span" => rsx! { span { key: "{key}", ..attrs, {children} } },
                other => panic!("the rows fixture names an unexpected tag {other}"),
            }
        }
    }
}
