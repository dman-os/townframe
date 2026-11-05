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

/// A hostile guest's probe of what the wire already lets through: one element whose raw `style`
/// attribute (allowed, see `clay_surface::render::static_attribute`) covers the viewport and
/// fetches a URL. Nothing in the style-property list is involved.
const PROBE: &str = r#"{"t":"div","a":{"id":"probe","style":"position:fixed;top:0;left:0;width:100vw;height:100vh;pointer-events:none;background:url(http://127.0.0.1:9/pierre-probe-beacon)"},"c":["raw style probe"]}"#;

const MODES: &[Mode] = &[Mode {
    publication: "probe-raw-style",
    title: "Pierre probe (raw style)",
    rows: PROBE,
    host_class: None,
}];

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

/// What one initial render of each presentation costs on the wire: the `WriteMutations` calls the
/// guest adapter turns one-to-one into a batch, recorded as `clay_surface` mutations and offered
/// to a fresh `SurfaceTree`, which applies its own limits and vocabulary to them.
#[cfg(all(test, not(target_os = "wasi")))]
mod tests {
    use super::*;
    use clay_surface::AttributeValue as Wire;
    use clay_surface::Mutation;
    use clay_surface::SurfaceTree;
    use dioxus::core::BorrowedAttributeValue;
    use dioxus::core::ElementId;
    use dioxus::core::VirtualDom;
    use dioxus::core::WriteMutations;

    #[derive(Default)]
    struct Record(Vec<Mutation<()>>);

    impl WriteMutations for Record {
        fn push_id(&mut self, id: ElementId) {
            self.0.push(Mutation::PushId(id.raw() as u32));
        }
        fn set_id(&mut self, id: ElementId) {
            self.0.push(Mutation::SetId(id.raw() as u32));
        }
        fn child(&mut self, index: usize) {
            self.0.push(Mutation::Child(index as u32));
        }
        fn pop(&mut self) {
            self.0.push(Mutation::Pop);
        }
        fn create_element(&mut self, tag: &str, ns: Option<&str>) {
            self.0
                .push(Mutation::CreateElement((tag.into(), ns.map(Into::into))));
        }
        fn create_text(&mut self, value: &str) {
            self.0.push(Mutation::CreateText(value.into()));
        }
        fn clone(&mut self) {
            self.0.push(Mutation::CloneNode);
        }
        fn append_children(&mut self, m: usize) {
            self.0.push(Mutation::AppendChildren(m as u32));
        }
        fn replace_with(&mut self, m: usize) {
            self.0.push(Mutation::ReplaceWith(m as u32));
        }
        fn insert_after(&mut self, m: usize) {
            self.0.push(Mutation::InsertAfter(m as u32));
        }
        fn insert_before(&mut self, m: usize) {
            self.0.push(Mutation::InsertBefore(m as u32));
        }
        fn set_attribute(
            &mut self,
            name: &str,
            ns: Option<&str>,
            value: BorrowedAttributeValue<'_>,
        ) {
            let BorrowedAttributeValue::Text(text) = value else {
                panic!("the rows set only text attributes");
            };
            self.0.push(Mutation::SetAttribute((
                name.into(),
                ns.map(Into::into),
                Wire::Text(text.into()),
            )));
        }
        fn set_text(&mut self, value: &str) {
            self.0.push(Mutation::SetText(value.into()));
        }
        fn add_event_listener(&mut self, name: &str) {
            self.0.push(Mutation::AddEventListener(name.into()));
        }
        fn remove_event_listener(&mut self, name: &str) {
            self.0.push(Mutation::RemoveEventListener(name.into()));
        }
        fn remove(&mut self) {
            self.0.push(Mutation::Remove);
        }
    }

    fn batch(mode: &Mode) -> Vec<Mutation<()>> {
        let rows: Rc<Hast> = Rc::new(serde_json::from_str(mode.rows).unwrap());
        let mut dom = VirtualDom::new_with_props(
            |(rows, host_class): (Rc<Hast>, Option<&'static str>)| view(rows, host_class),
            (rows, mode.host_class),
        );
        let mut record = Record::default();
        dom.rebuild(&mut record);
        record.0
    }

    #[test]
    fn the_rows_cost_what_the_report_says() {
        for mode in MODES {
            let batch = batch(mode);
            let (mut elements, mut texts, mut attributes, mut bytes) = (0, 0, 0, 0);
            for mutation in &batch {
                match mutation {
                    Mutation::CreateElement((tag, _)) => {
                        elements += 1;
                        bytes += tag.len();
                    }
                    Mutation::CreateText(text) => {
                        texts += 1;
                        bytes += text.len();
                    }
                    Mutation::SetAttribute((name, ns, Wire::Text(text))) => {
                        attributes += 1;
                        bytes += name.len() + ns.as_deref().map_or(0, str::len) + text.len();
                    }
                    _ => {}
                }
            }
            let applied = SurfaceTree::<()>::new().apply(&batch);
            eprintln!(
                "{}: mutations={} elements={elements} texts={texts} attributes={attributes} string_bytes={bytes} apply={applied:?}",
                mode.publication,
                batch.len(),
            );
        }
    }
}
