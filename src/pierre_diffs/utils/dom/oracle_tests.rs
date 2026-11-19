//! Differential tests for the S11a DOM-building utils, CSS wrappers, platform
//! detection and scrollbar probe (`oracle/golden/s11a.json`: `dom`, `css`,
//! `platform`, `scrollbar`). The dist ran against a minimal fake DOM whose
//! snapshot (attribute list in insertion order, `style` as `name:value;`
//! declarations, children) is compared with the hast `Element` the port
//! returns. Test-only, original to this crate.

use std::cell::RefCell;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::PropValue;
use crate::hast::Properties;
use crate::oracle_testing::assert_json_eq;
use crate::oracle_testing::load_golden;
use crate::types::lines::CodeColumnType;
use crate::types::options::PrePropertiesConfig;
use crate::types::options::ThemeTypes;
use crate::utils::dom::create_annotation_wrapper_node::create_annotation_wrapper_node;
use crate::utils::dom::create_gutter_utility_content_node::create_gutter_utility_content_node;
use crate::utils::dom::create_unsafe_css_style_node::create_unsafe_css_style_node;
use crate::utils::dom::css_wrappers::patch_scrollbar_gutter_size;
use crate::utils::dom::css_wrappers::wrap_core_css;
use crate::utils::dom::css_wrappers::wrap_theme_css;
use crate::utils::dom::css_wrappers::wrap_unsafe_css;
use crate::utils::dom::get_or_create_code_node::CreateCodeNodeProps;
use crate::utils::dom::get_or_create_code_node::get_or_create_code_node;
use crate::utils::dom::host_theme::create_host_theme_style_node;
use crate::utils::dom::host_theme::upsert_host_theme_style;
use crate::utils::dom::is_style_node::is_style_node;
use crate::utils::dom::platform::PlatformInfo;
use crate::utils::dom::platform::is_firefox;
use crate::utils::dom::platform::is_linux;
use crate::utils::dom::platform::is_mac_like;
use crate::utils::dom::platform::is_safari;
use crate::utils::dom::scrollbar_gutter::ScrollbarGutterHost;
use crate::utils::dom::scrollbar_gutter::create_measured_scrollbar_gutter_declaration;
use crate::utils::dom::scrollbar_gutter::get_measured_scrollbar_gutter;
use crate::utils::dom::set_wrapper_node_props::set_pre_node_properties;

fn snapshot(element: &Element) -> Value {
    let attrs: Vec<Value> = element
        .properties
        .defined()
        .map(|(key, value)| {
            let text = match value {
                PropValue::Str(text) => text.clone(),
                PropValue::Int(number) => number.to_string(),
                PropValue::Float(number) => number.to_string(),
                PropValue::Bool(_) | PropValue::Undefined => String::new(),
            };
            json!([key, text])
        })
        .collect();
    let children: Vec<Value> = element
        .children
        .iter()
        .map(|child| match child {
            ElementContent::Element(element) => snapshot(element),
            ElementContent::Text(text) => json!({ "text": text.value }),
        })
        .collect();
    json!({ "tag": element.tag_name, "attrs": attrs, "children": children })
}

fn element_from(tag: &str, attrs: &[(String, String)]) -> Element {
    let mut properties = Properties::new();
    for (key, value) in attrs {
        properties.set(key, value.as_str());
    }
    Element::new(tag, properties, Vec::new())
}

#[derive(Deserialize)]
struct SlotCase {
    slot: String,
    expected: Value,
}

#[derive(Deserialize)]
struct Created {
    #[serde(rename = "annotationWrappers")]
    annotation_wrappers: Vec<SlotCase>,
    #[serde(rename = "gutterUtility")]
    gutter_utility: Value,
    #[serde(rename = "unsafeStyle")]
    unsafe_style: Value,
    #[serde(rename = "hostThemeStyle")]
    host_theme_style: Value,
}

#[derive(Deserialize)]
struct StyleNodeCase {
    tag: String,
    expected: bool,
}

#[derive(Deserialize)]
struct PreCase {
    initial: Vec<(String, String)>,
    config: PrePropertiesConfig,
    expected: Value,
}

#[derive(Deserialize)]
struct CodeCase {
    initial: Option<Vec<(String, String)>>,
    #[serde(rename = "columnType")]
    column_type: Option<CodeColumnType>,
    #[serde(rename = "rowSpan")]
    row_span: Option<usize>,
    #[serde(rename = "containerSize")]
    container_size: bool,
    expected: Value,
}

#[derive(Deserialize)]
struct HostThemeCase {
    css: String,
    #[serde(rename = "hasCurrent")]
    has_current: bool,
    expected: Value,
    attached: bool,
}

#[derive(Deserialize)]
struct Dom {
    created: Created,
    #[serde(rename = "styleNodes")]
    style_nodes: Vec<StyleNodeCase>,
    #[serde(rename = "preNodes")]
    pre_nodes: Vec<PreCase>,
    #[serde(rename = "codeNodes")]
    code_nodes: Vec<CodeCase>,
    #[serde(rename = "hostTheme")]
    host_theme: Vec<HostThemeCase>,
}

#[derive(Deserialize)]
struct StringCase {
    css: String,
    expected: String,
}

#[derive(Deserialize)]
struct ThemeCase {
    css: String,
    #[serde(rename = "themeType")]
    theme_type: Option<ThemeTypes>,
    gutter: Option<f64>,
    expected: String,
}

#[derive(Deserialize)]
struct PatchCase {
    css: String,
    gutter: Option<f64>,
    expected: String,
}

#[derive(Deserialize)]
struct DeclarationCase {
    gutter: Option<f64>,
    expected: String,
}

#[derive(Deserialize)]
struct Css {
    #[serde(rename = "wrapCore")]
    wrap_core: Vec<StringCase>,
    #[serde(rename = "wrapUnsafe")]
    wrap_unsafe: Vec<StringCase>,
    #[serde(rename = "wrapTheme")]
    wrap_theme: Vec<ThemeCase>,
    patch: Vec<PatchCase>,
    declarations: Vec<DeclarationCase>,
}

#[derive(Deserialize)]
struct PlatformInfoJson {
    platform: Option<String>,
    #[serde(rename = "userAgentDataPlatform")]
    user_agent_data_platform: Option<String>,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(rename = "hasSafariPushNotification")]
    has_safari_push_notification: bool,
}

#[derive(Deserialize)]
struct PlatformCase {
    info: PlatformInfoJson,
    #[serde(rename = "isMacLike")]
    is_mac_like: bool,
    #[serde(rename = "isLinux")]
    is_linux: bool,
    #[serde(rename = "isSafari")]
    is_safari: bool,
    #[serde(rename = "isFirefox")]
    is_firefox: bool,
}

#[derive(Deserialize)]
struct ScrollbarStep {
    connected: bool,
    #[serde(rename = "offsetHeight")]
    offset_height: f64,
    #[serde(rename = "clientHeight")]
    client_height: f64,
    value: Option<f64>,
    probes: Vec<Value>,
}

#[derive(Deserialize)]
struct Golden {
    dom: Dom,
    css: Css,
    platform: Vec<PlatformCase>,
    scrollbar: Vec<ScrollbarStep>,
}

#[test]
fn created_nodes_and_style_check_match_the_published_functions() {
    let golden: Golden = load_golden("s11a.json");
    let created = golden.dom.created;
    for case in &created.annotation_wrappers {
        assert_json_eq(
            &format!("annotation wrapper `{}`", case.slot),
            &case.expected,
            &snapshot(&create_annotation_wrapper_node(&case.slot)),
        );
    }
    assert_json_eq(
        "gutter utility",
        &created.gutter_utility,
        &snapshot(&create_gutter_utility_content_node()),
    );
    assert_json_eq(
        "unsafe style",
        &created.unsafe_style,
        &snapshot(&create_unsafe_css_style_node()),
    );
    assert_json_eq(
        "host theme style",
        &created.host_theme_style,
        &snapshot(&create_host_theme_style_node()),
    );
    for case in &golden.dom.style_nodes {
        let element = Element::new(&case.tag, Properties::new(), Vec::new());
        assert_eq!(is_style_node(&element), case.expected, "{}", case.tag);
    }
}

#[test]
fn pre_and_code_nodes_match_the_published_functions() {
    let golden: Golden = load_golden("s11a.json");
    assert!(golden.dom.pre_nodes.len() >= 80);
    for case in &golden.dom.pre_nodes {
        let mut pre = element_from("pre", &case.initial);
        set_pre_node_properties(&mut pre, &case.config);
        assert_json_eq(
            &format!("pre {:?} + {:?}", case.initial, case.config),
            &case.expected,
            &snapshot(&pre),
        );
    }
    for case in &golden.dom.code_nodes {
        let code = get_or_create_code_node(CreateCodeNodeProps {
            code: case
                .initial
                .as_ref()
                .map(|attrs| element_from("code", attrs)),
            column_type: case.column_type,
            row_span: case.row_span,
            container_size: case.container_size,
        });
        assert_json_eq(
            &format!("code {:?}", case.initial),
            &case.expected,
            &snapshot(&code),
        );
    }
}

#[test]
fn host_theme_upsert_matches_the_published_function() {
    let golden: Golden = load_golden("s11a.json");
    for case in &golden.dom.host_theme {
        let current = case.has_current.then(|| {
            let mut node = create_host_theme_style_node();
            node.children = vec![ElementContent::Text(crate::hast::Text::new("old"))];
            node
        });
        let result = upsert_host_theme_style(current, &case.css);
        assert_eq!(result.is_some(), case.attached, "{:?}", case.css);
        let actual = result.as_ref().map_or(Value::Null, snapshot);
        assert_json_eq(
            &format!("host theme css {:?}", case.css),
            &case.expected,
            &actual,
        );
    }
}

#[test]
fn css_wrappers_match_the_published_functions() {
    let golden: Golden = load_golden("s11a.json");
    for case in &golden.css.wrap_core {
        assert_eq!(wrap_core_css(&case.css), case.expected);
    }
    for case in &golden.css.wrap_unsafe {
        assert_eq!(wrap_unsafe_css(&case.css), case.expected);
    }
    for case in &golden.css.wrap_theme {
        let theme_type = case.theme_type.unwrap_or(ThemeTypes::System);
        assert_eq!(
            wrap_theme_css(&case.css, theme_type, case.gutter),
            case.expected
        );
    }
    for case in &golden.css.patch {
        assert_eq!(
            patch_scrollbar_gutter_size(&case.css, case.gutter),
            case.expected,
            "{:?}",
            case.css
        );
    }
    for case in &golden.css.declarations {
        assert_eq!(
            create_measured_scrollbar_gutter_declaration(case.gutter),
            case.expected
        );
    }
}

#[test]
fn platform_detection_matches_the_published_functions() {
    let golden: Golden = load_golden("s11a.json");
    assert!(golden.platform.len() >= 100);
    for case in &golden.platform {
        let info = PlatformInfo {
            platform: case.info.platform.clone(),
            user_agent_data_platform: case.info.user_agent_data_platform.clone(),
            user_agent: case.info.user_agent.clone(),
            has_safari_push_notification: case.info.has_safari_push_notification,
        };
        let label = format!("{:?}", case.info.user_agent);
        assert_eq!(is_mac_like(&info), case.is_mac_like, "mac {label}");
        assert_eq!(is_linux(&info), case.is_linux, "linux {label}");
        assert_eq!(is_safari(&info), case.is_safari, "safari {label}");
        assert_eq!(is_firefox(&info), case.is_firefox, "firefox {label}");
    }
}

struct ProbeHost {
    connected: bool,
    gutter: f64,
    probes: RefCell<Vec<Value>>,
}

impl ScrollbarGutterHost for ProbeHost {
    fn is_host_connected(&self) -> bool {
        self.connected
    }

    fn measure_probe(&self, probe: &Element) -> f64 {
        self.probes.borrow_mut().push(snapshot(probe));
        self.gutter
    }
}

// The measurement is cached for the process, so the golden is one ordered
// sequence: not connected, measured, cached, not connected.
#[test]
fn scrollbar_probe_matches_the_published_function() {
    let golden: Golden = load_golden("s11a.json");
    for step in &golden.scrollbar {
        let host = ProbeHost {
            connected: step.connected,
            gutter: step.offset_height - step.client_height,
            probes: RefCell::default(),
        };
        let value = get_measured_scrollbar_gutter(&host);
        assert_eq!(value, step.value, "{:?}", step.offset_height);
        assert_json_eq(
            "probe",
            &Value::Array(step.probes.clone()),
            &Value::Array(host.probes.borrow().clone()),
        );
    }
}

#[test]
fn a_negative_probe_is_clamped_to_zero() {
    let host = ProbeHost {
        connected: true,
        gutter: -3.0,
        probes: RefCell::default(),
    };
    assert_eq!(get_measured_scrollbar_gutter(&host), Some(0.0));
}
