use super::*;
use crate::tree::SurfaceTree;
use crate::wire::EventResponse;
use crate::wire::Mutation;
use dioxus_html::Code;
use dioxus_html::EventData;
use dioxus_html::Key;
use dioxus_html::Location;
use dioxus_html::Modifiers;
use dioxus_html::SerializedKeyboardData;
use dioxus_html::SerializedMouseData;
use std::cell::RefCell;
use std::rc::Rc;

const SVG_NS: &str = "http://www.w3.org/2000/svg";

// ── vocabulary snapshots: the probe lists must name every currently-supported entry
//    (extending vocabulary means extending the probes); a removal or rename then shows
//    up as a reviewable snapshot diff, and the known-unknowns pin the closed domain ────

// The snapshot grew by `style`, `slot`, `bdi` and the SVG `symbol` and `use`: the markup Pierre's
// diff view emits that the vocabulary lacked. None can fetch or run anything on the host:
// `style` text and `use` hrefs are policed at ingress (tree tests), and `slot`/`bdi` are inert
// without scripting. Autonomous custom elements are admitted by name, not listed here.
#[test]
fn supported_tags_are_reviewable() {
    let probes: Vec<(&str, Option<&str>)> = vec![
        ("div", None),
        ("section", None),
        ("article", None),
        ("aside", None),
        ("header", None),
        ("footer", None),
        ("main", None),
        ("nav", None),
        ("h1", None),
        ("h2", None),
        ("h3", None),
        ("h4", None),
        ("h5", None),
        ("h6", None),
        ("p", None),
        ("span", None),
        ("pre", None),
        ("code", None),
        ("blockquote", None),
        ("strong", None),
        ("em", None),
        ("small", None),
        ("b", None),
        ("i", None),
        ("u", None),
        ("s", None),
        ("mark", None),
        ("kbd", None),
        ("sub", None),
        ("sup", None),
        ("br", None),
        ("hr", None),
        ("ul", None),
        ("ol", None),
        ("li", None),
        ("dl", None),
        ("dt", None),
        ("dd", None),
        ("table", None),
        ("thead", None),
        ("tbody", None),
        ("tfoot", None),
        ("tr", None),
        ("th", None),
        ("td", None),
        ("caption", None),
        ("form", None),
        ("label", None),
        ("input", None),
        ("textarea", None),
        ("button", None),
        ("select", None),
        ("option", None),
        ("optgroup", None),
        ("fieldset", None),
        ("legend", None),
        ("details", None),
        ("summary", None),
        ("dialog", None),
        ("a", None),
        ("img", None),
        ("figure", None),
        ("figcaption", None),
        ("progress", None),
        ("meter", None),
        ("output", None),
        ("time", None),
        ("data", None),
        ("style", None),
        ("slot", None),
        ("bdi", None),
        ("svg", Some(SVG_NS)),
        ("g", Some(SVG_NS)),
        ("path", Some(SVG_NS)),
        ("circle", Some(SVG_NS)),
        ("rect", Some(SVG_NS)),
        ("line", Some(SVG_NS)),
        ("polyline", Some(SVG_NS)),
        ("polygon", Some(SVG_NS)),
        ("defs", Some(SVG_NS)),
        ("symbol", Some(SVG_NS)),
        ("use", Some(SVG_NS)),
    ];
    // Custom element names are not in this static vocabulary; they are admitted by
    // `is_custom_element_name` and interned (see the custom element tests).
    for unknown in [
        ("unknown-widget", None),
        ("unknownwidget", None),
        ("svg", None),
        ("symbol", None),
        ("use", None),
        ("script", None),
        ("link", None),
        ("iframe", None),
        ("image", Some(SVG_NS)),
        ("foreignObject", Some(SVG_NS)),
        ("style", Some(SVG_NS)),
        ("div", Some(SVG_NS)),
        ("div", Some("unknown")),
    ] {
        assert!(
            !supported_tag(unknown.0, unknown.1),
            "{unknown:?} must stay outside the tag vocabulary"
        );
    }
    let supported: Vec<String> = probes
        .into_iter()
        .filter(|&(tag, namespace)| supported_tag(tag, namespace))
        .map(|(tag, namespace)| match namespace {
            Some(namespace) => format!("{namespace}:{tag}"),
            None => tag.to_owned(),
        })
        .collect();
    insta::assert_yaml_snapshot!(supported);
}

// Vocabulary here is names only; `attribute_value_policy` decides values. `xlink:href` is in the
// snapshot for the sake of SVG `<use>`, and only `<use>` (with a `#fragment`) accepts it: the
// policy refuses it on every other element. The raw `style` attribute, `src`, `srcset`, `action`
// and the other names that exist only to carry a URL are absent on purpose (egress closure).
#[test]
fn supported_attributes_are_reviewable() {
    let plain = [
        "id",
        "class",
        "style",
        "title",
        "role",
        "tabindex",
        "hidden",
        "lang",
        "dir",
        "accesskey",
        "contenteditable",
        "draggable",
        "spellcheck",
        "translate",
        "value",
        "checked",
        "disabled",
        "readonly",
        "required",
        "multiple",
        "selected",
        "name",
        "type",
        "placeholder",
        "autocomplete",
        "autofocus",
        "inputmode",
        "pattern",
        "min",
        "max",
        "step",
        "minlength",
        "maxlength",
        "size",
        "rows",
        "cols",
        "wrap",
        "for",
        "form",
        "action",
        "method",
        "enctype",
        "novalidate",
        "accept",
        "accept-charset",
        "href",
        "target",
        "rel",
        "download",
        "src",
        "alt",
        "width",
        "height",
        "loading",
        "srcset",
        "sizes",
        "crossorigin",
        "referrerpolicy",
        "open",
        "datetime",
        "start",
        "reversed",
        "colspan",
        "rowspan",
        "scope",
        "headers",
        "label",
        "low",
        "high",
        "optimum",
        "media",
        "data-testid",
        "data-state",
        "data-value",
        "aria-label",
        "aria-labelledby",
        "aria-describedby",
        "aria-hidden",
        "aria-live",
        "aria-atomic",
        "aria-busy",
        "aria-controls",
        "aria-current",
        "aria-disabled",
        "aria-expanded",
        "aria-haspopup",
        "aria-invalid",
        "aria-modal",
        "aria-multiline",
        "aria-multiselectable",
        "aria-orientation",
        "aria-pressed",
        "aria-readonly",
        "aria-required",
        "aria-selected",
        "aria-checked",
        "aria-valuemin",
        "aria-valuemax",
        "aria-valuenow",
        "aria-valuetext",
        "aria-level",
        "aria-posinset",
        "aria-setsize",
        "viewBox",
        "preserveAspectRatio",
        "d",
        "fill",
        "fill-rule",
        "fill-opacity",
        "stroke",
        "stroke-width",
        "stroke-linecap",
        "stroke-linejoin",
        "stroke-dasharray",
        "stroke-dashoffset",
        "stroke-opacity",
        "opacity",
        "transform",
        "cx",
        "cy",
        "r",
        "rx",
        "ry",
        "x",
        "y",
        "x1",
        "y1",
        "x2",
        "y2",
        "points",
        "xmlns",
        "clip-path",
    ];
    let style = [
        "display",
        "position",
        "top",
        "right",
        "bottom",
        "left",
        "inset",
        "z-index",
        "width",
        "height",
        "min-width",
        "max-width",
        "min-height",
        "max-height",
        "margin",
        "margin-top",
        "margin-right",
        "margin-bottom",
        "margin-left",
        "padding",
        "padding-top",
        "padding-right",
        "padding-bottom",
        "padding-left",
        "gap",
        "row-gap",
        "column-gap",
        "flex",
        "flex-direction",
        "flex-wrap",
        "flex-grow",
        "flex-shrink",
        "flex-basis",
        "align-items",
        "align-self",
        "justify-content",
        "grid-template-columns",
        "grid-template-rows",
        "grid-column",
        "grid-row",
        "overflow",
        "overflow-x",
        "overflow-y",
        "overflow-anchor",
        "color",
        "background",
        "background-color",
        "border",
        "border-color",
        "border-width",
        "border-style",
        "border-radius",
        "font",
        "font-family",
        "font-size",
        "font-weight",
        "font-style",
        "line-height",
        "text-align",
        "text-decoration",
        "text-overflow",
        "white-space",
        "word-break",
        "cursor",
        "pointer-events",
        "touch-action",
        "user-select",
        "opacity",
        "visibility",
        "box-sizing",
        "box-shadow",
        "transform",
        "transform-origin",
        "transition",
        "object-fit",
    ];
    let probes: Vec<(String, Option<String>)> = plain
        .iter()
        .map(|name| (name.to_string(), None))
        .chain(
            style
                .iter()
                .map(|name| (name.to_string(), Some("style".to_owned()))),
        )
        .chain(std::iter::once((
            "href".to_owned(),
            Some("http://www.w3.org/1999/xlink".to_owned()),
        )))
        .collect();
    let lookup = |name: &str, namespace: Option<&str>| {
        static_attribute(&DATA_ATTRIBUTES, name, namespace).expect("snapshot probes stay in bound")
    };
    assert!(lookup("onclick", None).is_none());
    assert!(lookup("unknown-property", None).is_none());
    assert!(lookup("class", Some("unknown")).is_none());
    let supported: Vec<String> = probes
        .into_iter()
        .filter(|(name, namespace)| lookup(name, namespace.as_deref()).is_some())
        .map(|(name, namespace)| match namespace {
            Some(namespace) => format!("{namespace}:{name}"),
            None => name,
        })
        .collect();
    insta::assert_yaml_snapshot!(supported);
}

#[test]
fn data_attribute_names_are_open_within_the_documented_form() {
    let interner = NameInterner::new(DATA_ATTRIBUTE_NAME_LIMIT);
    let pierre_style = [
        "data-line",
        "data-line-index",
        "data-line-type",
        "data-line-annotation",
        "data-diff-span",
        "data-diff-type",
        "data-code",
        "data-gutter",
        "data-gutter-buffer",
        "data-content",
        "data-content-buffer",
        "data-column-number",
        "data-alt-line",
        "data-alt-line-index",
        "data-expand-button",
        "data-expand-index",
        "data-separator",
        "data-separator-wrapper",
        "data-separator-content",
        "data-hunk",
        "data-hunk-index",
        "data-file",
        "data-file-name",
        "data-file-header",
        "data-change-type",
        "data-no-newline",
        "data-indicators",
        "data-background",
        "data-overflow",
        "data-theme",
        "data-theme-type",
        "data-title",
        "data-header-content",
        "data-metadata",
        "data-additions",
        "data-deletions",
        "data-diffs",
        "data-diffs-header",
        "data-annotation-slot",
        "data-line-number-content",
        "data-merge-conflict",
        "data-unmodified-lines",
        "data-collapsed",
        "data-selected-line",
        "data-selected-line-first",
        "data-selected-line-last",
        "data-rename",
        "data-pure",
        "data-testid",
        "data-state",
        "data-value",
        "data-a.b",
        "data-a_b",
        "data-0",
        "data--",
        "data-x-1.2_3",
        "data-z",
        "data-row",
        "data-cell",
        "data-col",
        "data-split",
        "data-unified",
        "data-9-lines",
        "data-v1",
    ];
    assert!(pierre_style.len() >= 60);
    for name in pierre_style {
        let (resolved, namespace) = static_attribute(&interner, name, None)
            .unwrap_or_else(|_| panic!("{name} is within the bound"))
            .unwrap_or_else(|| panic!("{name} must be accepted"));
        assert_eq!(resolved, name);
        assert_eq!(namespace, None);
    }
    for bad in [
        "data-",
        "data-Line",
        "data-LINE",
        "data-a b",
        "data-a:b",
        "data- ",
        "data-a\n",
        "data-é",
        "Data-line",
        "data",
        "datum-line",
        "xdata-line",
        "data-a/b",
        "data-a=b",
    ] {
        assert_eq!(
            static_attribute(&interner, bad, None),
            Ok(None),
            "{bad:?} must be refused"
        );
    }
    assert_eq!(
        static_attribute(&interner, "data-line", Some("style")),
        Ok(None),
        "a data-* name never takes a namespace"
    );
}

#[test]
fn data_attribute_interner_is_bounded_and_deduplicates() {
    let interner = NameInterner::new(4096);
    for index in 0..4096 {
        interner
            .intern(&format!("data-n{index}"), |_| ())
            .expect("within bound");
    }
    let (first, ()) = interner
        .intern("data-n0", |_| ())
        .expect("repeat does not grow");
    let (again, ()) = interner
        .intern("data-n0", |_| ())
        .expect("repeat does not grow");
    assert!(
        std::ptr::eq(first, again),
        "a repeated name is the same leaked str"
    );
    assert_eq!(
        static_attribute(&interner, "data-n4095", None),
        Ok(Some(("data-n4095", None)))
    );
    assert_eq!(
        interner.intern("data-one-too-many", |_| ()),
        Err(NamesExhausted)
    );
    assert_eq!(
        static_attribute(&interner, "data-one-too-many", None),
        Err(NamesExhausted)
    );
    // The allowlist is not charged against the bound.
    assert_eq!(
        static_attribute(&interner, "class", None),
        Ok(Some(("class", None)))
    );
}

#[test]
fn overflow_anchor_is_an_allowed_style_property() {
    assert_eq!(
        static_attribute(&DATA_ATTRIBUTES, "overflow-anchor", Some("style")),
        Ok(Some(("overflow-anchor", Some("style"))))
    );
}

#[test]
fn supported_listeners_are_reviewable() {
    let probes = [
        // clipboard
        "copy",
        "cut",
        "paste",
        // composition
        "compositionstart",
        "compositionupdate",
        "compositionend",
        // keyboard
        "keydown",
        "keypress",
        "keyup",
        // focus
        "focus",
        "blur",
        "focusin",
        "focusout",
        // form
        "change",
        "input",
        "beforeinput",
        "invalid",
        "reset",
        "submit",
        "formdata",
        "select",
        // mouse
        "click",
        "contextmenu",
        "dblclick",
        "mousedown",
        "mouseenter",
        "mouseleave",
        "mousemove",
        "mouseout",
        "mouseover",
        "mouseup",
        "auxclick",
        // drag
        "drag",
        "dragend",
        "dragenter",
        "dragexit",
        "dragleave",
        "dragover",
        "dragstart",
        "drop",
        // pointer
        "pointerdown",
        "pointermove",
        "pointerup",
        "pointercancel",
        "gotpointercapture",
        "lostpointercapture",
        "pointerenter",
        "pointerleave",
        "pointerover",
        "pointerout",
        // touch
        "touchstart",
        "touchend",
        "touchmove",
        "touchcancel",
        // scroll and wheel
        "scroll",
        "scrollend",
        "wheel",
        // media
        "play",
        "pause",
        "ended",
        "volumechange",
        "timeupdate",
        "loadedmetadata",
        "canplay",
        "canplaythrough",
        "durationchange",
        "emptied",
        "loadstart",
        "loadeddata",
        "playing",
        "progress",
        "ratechange",
        "seeked",
        "seeking",
        "stalled",
        "suspend",
        "waiting",
        // animation and transition
        "animationstart",
        "animationend",
        "animationiteration",
        "animationcancel",
        "transitionstart",
        "transitionend",
        "transitionrun",
        "transitioncancel",
        // misc
        "toggle",
        "load",
        "error",
        "mounted",
        "resize",
        "visible",
        "visibilitychange",
    ];
    assert!(!supported_listener("unknown-event"));
    let supported: Vec<&str> = probes
        .into_iter()
        .filter(|name| supported_listener(name))
        .collect();
    insta::assert_yaml_snapshot!(supported);
}

// ── listener translation: a host event reaching a surface listener carries the guest's
//    surface-local element id, the surface revision at render time, and the event group's
//    serialized data; `prevent_default`/`stop_propagation` honoring is fixed by the macro
//    expansion and not observable through synthetic dispatch ────────────────────────────

#[derive(Clone, PartialEq)]
struct ListenerProps {
    tree: SurfaceTree,
    events: Rc<RefCell<Vec<Event>>>,
}

fn listener_app(props: ListenerProps) -> Element {
    let captured = props.events.clone();
    let dispatch: SurfaceEventHandler = Rc::new(move |event| {
        captured.borrow_mut().push(event);
        EventResponse {
            prevent_default: true,
            stop_propagation: true,
        }
    });
    props.tree.render(
        &dispatch,
        &|_| unreachable!("test has no component slots"),
        &crate::ElementHandles::default(),
        false,
    )
}

#[test]
fn listener_events_carry_guest_identity_revision_and_serialized_data() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut tree = SurfaceTree::new();
    tree.apply(&[
        Mutation::CreateElement(("button".into(), None)),
        Mutation::SetId(1),
        Mutation::AddEventListener("click".into()),
        Mutation::AddEventListener("keydown".into()),
        Mutation::AppendChildren(1),
    ])
    .unwrap();
    let revision = tree.revision();
    let mut dom = VirtualDom::new_with_props(
        listener_app,
        ListenerProps {
            tree,
            events: events.clone(),
        },
    );
    let mut host_tree: SurfaceTree = SurfaceTree::new();
    dom.rebuild(&mut host_tree);
    let host_element = host_tree
        .listener_id("button", "click")
        .expect("host listener mounted");
    assert_eq!(
        host_tree.listener_id("button", "keydown"),
        Some(host_element)
    );

    dom.runtime().handle_event_for_target(
        dioxus_core::RenderTargetId::ROOT,
        "click",
        dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        ),
        host_element,
    );
    dom.runtime().handle_event_for_target(
        dioxus_core::RenderTargetId::ROOT,
        "keydown",
        dioxus_core::Event::new(
            EventData::Keyboard(SerializedKeyboardData::new(
                Key::Character("a".into()),
                Code::KeyA,
                Location::Standard,
                false,
                Modifiers::empty(),
                false,
            ))
            .into_any(),
            true,
        ),
        host_element,
    );

    let events = events.borrow();
    let [click, keydown] = &events[..] else {
        panic!("expected click and keydown events, got {:?}", events.len())
    };
    assert_eq!(click.name, "click");
    assert_eq!(click.element, 1);
    assert_eq!(click.revision, revision);
    assert!(click.bubbles);
    assert_eq!(keydown.name, "keydown");
    assert_eq!(keydown.element, 1);
    assert_eq!(keydown.revision, revision);
    insta::assert_yaml_snapshot!(
        "click_data_json",
        serde_json::from_str::<serde_json::Value>(&click.data_json).unwrap()
    );
    insta::assert_yaml_snapshot!(
        "keydown_data_json",
        serde_json::from_str::<serde_json::Value>(&keydown.data_json).unwrap()
    );
}

/// `visible` crosses the whole boundary: the host serializes the platform's visible data as the
/// event group's `data_json`, and the guest rebuilds a `VisibleData` from that JSON through
/// the same `HtmlEvent` envelope `clay_mdk`'s `GuestDom::dispatch` uses, so a standard
/// `onvisible` handler sees `is_intersecting`, the ratio, and the rects.
#[test]
fn visible_events_reach_a_guest_onvisible_handler() {
    let rect = |x: f64, y: f64, width: f64, height: f64| {
        serde_json::json!({
            "bottom": y + height, "height": height, "left": x, "right": x + width,
            "top": y, "width": width, "x": x, "y": y,
        })
    };
    let platform: dioxus_html::SerializedVisibleData = serde_json::from_value(serde_json::json!({
        "bounding_client_rect": rect(0.0, 10.0, 100.0, 40.0),
        "intersection_ratio": 0.5,
        "intersection_rect": rect(0.0, 10.0, 100.0, 20.0),
        "is_intersecting": true,
        "root_bounds": rect(0.0, 0.0, 200.0, 30.0),
        "time_ms": 7,
    }))
    .unwrap();

    // Host half: a surface element with a `visible` listener.
    let events = Rc::new(RefCell::new(Vec::new()));
    let mut tree = SurfaceTree::new();
    tree.apply(&[
        Mutation::CreateElement(("div".into(), None)),
        Mutation::SetId(1),
        Mutation::AddEventListener("visible".into()),
        Mutation::AppendChildren(1),
    ])
    .unwrap();
    let mut dom = VirtualDom::new_with_props(
        listener_app,
        ListenerProps {
            tree,
            events: events.clone(),
        },
    );
    let mut host_tree: SurfaceTree = SurfaceTree::new();
    dom.rebuild(&mut host_tree);
    let host_element = host_tree
        .listener_id("div", "visible")
        .expect("host listener mounted");
    dom.runtime().handle_event_for_target(
        dioxus_core::RenderTargetId::ROOT,
        "visible",
        dioxus_core::Event::new(EventData::Visible(platform).into_any(), true),
        host_element,
    );
    let wire_event = events.borrow()[0].clone();
    assert_eq!(wire_event.name, "visible");

    // Guest half: a plain Dioxus `onvisible` handler fed by the envelope the guest builds.
    dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
    let seen = Rc::new(RefCell::new(None));
    #[derive(Clone)]
    struct GuestProps(Rc<RefCell<Option<Rc<dioxus_html::VisibleData>>>>);
    impl PartialEq for GuestProps {
        fn eq(&self, other: &Self) -> bool {
            Rc::ptr_eq(&self.0, &other.0)
        }
    }
    fn guest_app(props: GuestProps) -> Element {
        rsx! {
            div { onvisible: move |event| *props.0.borrow_mut() = Some(event.data()) }
        }
    }
    let mut guest = VirtualDom::new_with_props(guest_app, GuestProps(seen.clone()));
    let mut guest_edits: SurfaceTree = SurfaceTree::new();
    guest.rebuild(&mut guest_edits);
    let guest_element = guest_edits
        .listener_id("div", "visible")
        .expect("guest listener mounted");
    let html_event: dioxus_html::HtmlEvent = serde_json::from_value(serde_json::json!({
        "element": guest_element.raw(),
        "name": wire_event.name,
        "bubbles": wire_event.bubbles,
        "data": serde_json::from_str::<serde_json::Value>(&wire_event.data_json).unwrap(),
    }))
    .unwrap();
    guest.runtime().handle_event_for_target(
        dioxus_core::RenderTargetId::ROOT,
        &html_event.name,
        dioxus_core::Event::new(html_event.data.into_any(), html_event.bubbles),
        guest_element,
    );
    let visible = seen.borrow_mut().take().expect("onvisible handler ran");
    assert!(visible.is_intersecting().unwrap());
    assert_eq!(visible.get_intersection_ratio().unwrap(), 0.5);
    let bounding = visible.get_bounding_client_rect().unwrap();
    assert_eq!((bounding.origin.y, bounding.size.height), (10.0, 40.0));
    let root = visible.get_root_bounds().unwrap();
    assert_eq!(root.size.width, 200.0);
}

#[test]
fn custom_element_names_follow_the_html_rule_narrowed_to_ascii() {
    for good in [
        "diffs-container",
        "a-b",
        "x-1",
        "my-el.name_2",
        "a-",
        &format!("a-{}", "b".repeat(MAX_CUSTOM_ELEMENT_NAME_BYTES - 2)),
    ] {
        assert!(is_custom_element_name(good), "{good:?} is a valid name");
    }
    for bad in [
        "",
        "-",
        "a",
        "div",
        "1-a",
        "-a",
        "A-b",
        "a-B",
        "a b-c",
        "a-b:c",
        "a-b>",
        "é-a",
        "a-é",
        "annotation-xml",
        "color-profile",
        "font-face",
        "font-face-src",
        "font-face-uri",
        "font-face-format",
        "font-face-name",
        "missing-glyph",
        &format!("a-{}", "b".repeat(MAX_CUSTOM_ELEMENT_NAME_BYTES - 1)),
    ] {
        assert!(!is_custom_element_name(bad), "{bad:?} is not admitted");
    }
    // `font-facet` merely shares a prefix with the reserved names.
    assert!(is_custom_element_name("font-facet"));
}

#[test]
fn custom_element_interner_is_bounded_deduplicates_and_builds_one_template_per_name() {
    let interner = NameInterner::<Template>::new(3);
    let first = intern_custom_element(&interner, "x-one").expect("within bound");
    let again = intern_custom_element(&interner, "x-one").expect("repeat does not grow");
    assert!(std::ptr::eq(first, again));
    intern_custom_element(&interner, "x-two").unwrap();
    intern_custom_element(&interner, "x-three").unwrap();
    assert_eq!(
        intern_custom_element(&interner, "x-four"),
        Err(NamesExhausted)
    );
    intern_custom_element(&interner, "x-two").expect("a held name stays available when full");
    let template = |name: &str| {
        interner
            .intern(name, |_| unreachable!("already interned"))
            .unwrap()
            .1
    };
    assert_eq!(
        template("x-one"),
        template("x-one"),
        "one template per name, not one per render"
    );
    assert_ne!(template("x-one"), template("x-two"));
}
