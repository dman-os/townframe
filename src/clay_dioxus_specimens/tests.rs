//! Every page renders inside a micro on the theme lab's specimen board, so every page's first
//! render must be admitted by a Clay surface: an edit outside the vocabulary (an attribute, a
//! style property) withdraws the whole mount.

use dioxus::prelude::*;
use dioxus_core::AttributeValue;
use dioxus_core::Mutation;
use dioxus_core::Mutations;
use dioxus_core::VirtualDom;
use std::collections::BTreeMap;
use std::rc::Rc;

/// One Dioxus edit as the wire edit a micro sends for it. Specimen renders carry text, numbers,
/// booleans and removals only; anything else is a new kind of value this mapping must learn.
fn wire_edit(edit: &Mutation) -> clay_surface::Mutation {
    use clay_surface::AttributeValue as Wire;
    use clay_surface::Mutation as W;
    let id =
        |id: &dioxus_core::ElementId| u32::try_from(id.raw()).expect("element ids fit the wire");
    let count = |m: &usize| u32::try_from(*m).expect("counts fit the wire");
    match edit {
        Mutation::PushId { id: element } => W::PushId(id(element)),
        Mutation::SetId { id: element } => W::SetId(id(element)),
        Mutation::Child { index } => W::Child(count(index)),
        Mutation::Pop => W::Pop,
        Mutation::CreateElement { tag, ns } => W::CreateElement((tag.clone(), ns.clone())),
        Mutation::CreateText { value } => W::CreateText(value.clone()),
        Mutation::Clone => W::CloneNode,
        Mutation::AppendChildren { m } => W::AppendChildren(count(m)),
        Mutation::ReplaceWith { m } => W::ReplaceWith(count(m)),
        Mutation::InsertAfter { m } => W::InsertAfter(count(m)),
        Mutation::InsertBefore { m } => W::InsertBefore(count(m)),
        Mutation::SetAttribute { name, ns, value } => W::SetAttribute((
            name.clone(),
            ns.clone(),
            match value {
                AttributeValue::Text(text) => Wire::Text(text.clone()),
                AttributeValue::Float(float) => Wire::Float(*float),
                AttributeValue::Int(int) => Wire::Int(*int),
                AttributeValue::Bool(bool) => Wire::Boolean(*bool),
                AttributeValue::None => Wire::None,
                other => panic!("no wire form for attribute value {other:?}"),
            },
        )),
        Mutation::SetText { value } => W::SetText(value.clone()),
        Mutation::NewEventListener { name } => W::AddEventListener(name.clone()),
        Mutation::RemoveEventListener { name } => W::RemoveEventListener(name.clone()),
        Mutation::Remove => W::Remove,
    }
}

/// One specimen page, as the board renders it.
type PageFn = fn() -> Element;

#[derive(Props, Clone)]
struct PageProps {
    page: PageFn,
}

// Props must compare for Dioxus to skip a re-render; a page's props never change in this test,
// and comparing function pointers means nothing, so they are always equal.
impl PartialEq for PageProps {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

/// The contexts the board provides, around one page.
fn page_root(props: PageProps) -> Element {
    use_context_provider(|| Signal::new(BTreeMap::<String, Rc<MountedData>>::new()));
    use_context_provider(|| clay_dioxus_kit::Renderer::Browser);
    (props.page)()
}

#[test]
fn every_page_is_admitted_by_a_clay_surface() {
    let pages: [(&str, PageFn); 17] = [
        ("foundations", || rsx! { crate::TokenFoundations {} }),
        (
            "colours",
            || rsx! { crate::FoundationPage { foundation: crate::Foundation::Colour } },
        ),
        ("button", || rsx! { crate::ButtonSpecimens {} }),
        ("card", || rsx! { crate::CardSpecimens {} }),
        ("alert", || rsx! { crate::AlertSpecimens {} }),
        ("chip", || rsx! { crate::ChipSpecimens {} }),
        ("tabs", || rsx! { crate::TabsSpecimens {} }),
        ("separator", || rsx! { crate::SeparatorSpecimens {} }),
        ("app bar", || rsx! { crate::AppBarSpecimens {} }),
        ("nav button", || rsx! { crate::NavButtonSpecimens {} }),
        ("list row", || rsx! { crate::ListRowSpecimens {} }),
        ("list details", || rsx! { crate::ListDetailsSpecimens {} }),
        ("panel page", || rsx! { crate::PanelPageSpecimens {} }),
        ("sections", || rsx! { crate::SectionsSpecimens {} }),
        ("sheet", || rsx! { crate::SheetSpecimens {} }),
        ("drawer", || rsx! { crate::DrawerSpecimens {} }),
        (
            "scrub strip",
            || rsx! { crate::ScrubStripSpecimens { viewport: (720.0, 860.0) } },
        ),
    ];
    let mut refused = Vec::new();
    for (name, page) in pages {
        let mut dom = VirtualDom::new_with_props(page_root, PageProps { page });
        let mut mutations = Mutations::default();
        dom.rebuild(&mut mutations);
        let batch: Vec<clay_surface::Mutation> = mutations.edits.iter().map(wire_edit).collect();
        let mut surface = clay_surface::SurfaceTree::new();
        if let Err(error) = surface.apply(&batch) {
            refused.push(format!("{name}: {error}"));
        }
    }
    assert!(
        refused.is_empty(),
        "pages Clay refuses:\n{}",
        refused.join("\n")
    );
}
