//! The hast subset `@pierre/diffs` builds (`Element` and `Text` nodes with
//! `Properties`), with `JSON.stringify`-compatible serialization so the
//! oracle can compare trees with the published package.
//!
//! Original to this crate: upstream uses the `hast` type package. Design
//! notes:
//! * `Properties` keeps insertion order and replaces in place, like a
//!   JavaScript object spread, because `hastToHtml` serializes attributes in
//!   that order.
//! * A property set to [`PropValue::Undefined`] stays in the map (a later
//!   spread can override an earlier value with `undefined`) but is omitted
//!   from JSON and HTML, exactly as `JSON.stringify` and the HTML serializer
//!   drop it.

use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum PropValue {
    Undefined,
    Str(String),
    Int(i64),
    Float(f64),
    /// `true`, e.g. a bare attribute.
    Bool(bool),
}

impl From<&str> for PropValue {
    fn from(value: &str) -> Self {
        Self::Str(value.to_owned())
    }
}

impl From<String> for PropValue {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}

impl From<usize> for PropValue {
    fn from(value: usize) -> Self {
        Self::Int(i64::try_from(value).expect("line numbers fit in i64"))
    }
}

impl From<i64> for PropValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl<Value: Into<PropValue>> From<Option<Value>> for PropValue {
    fn from(value: Option<Value>) -> Self {
        value.map_or(Self::Undefined, Into::into)
    }
}

/// An insertion-ordered property bag.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Properties(Vec<(String, PropValue)>);

impl Properties {
    pub fn new() -> Self {
        Self::default()
    }

    /// `{ ...this, [key]: value }`: replaces in place or appends.
    pub fn set(&mut self, key: &str, value: impl Into<PropValue>) {
        let value = value.into();
        match self.0.iter_mut().find(|(existing, _)| existing == key) {
            Some((_, slot)) => *slot = value,
            None => self.0.push((key.to_owned(), value)),
        }
    }

    /// Builder form of [`Properties::set`].
    pub fn with(mut self, key: &str, value: impl Into<PropValue>) -> Self {
        self.set(key, value);
        self
    }

    /// `{ ...this, ...other }`.
    pub fn extend(&mut self, other: &Properties) {
        for (key, value) in &other.0 {
            self.set(key, value.clone());
        }
    }

    pub fn get(&self, key: &str) -> Option<&PropValue> {
        self.0
            .iter()
            .find(|(existing, _)| existing == key)
            .map(|(_, value)| value)
    }

    /// Entries whose value is not `undefined`, in order.
    pub fn defined(&self) -> impl Iterator<Item = (&str, &PropValue)> {
        self.0
            .iter()
            .filter(|(_, value)| *value != PropValue::Undefined)
            .map(|(key, value)| (key.as_str(), value))
    }
}

impl Serialize for PropValue {
    fn serialize<Out: Serializer>(&self, serializer: Out) -> Result<Out::Ok, Out::Error> {
        match self {
            Self::Undefined => serializer.serialize_none(),
            Self::Str(value) => serializer.serialize_str(value),
            Self::Int(value) => serializer.serialize_i64(*value),
            Self::Float(value) => serializer.serialize_f64(*value),
            Self::Bool(value) => serializer.serialize_bool(*value),
        }
    }
}

impl Serialize for Properties {
    fn serialize<Out: Serializer>(&self, serializer: Out) -> Result<Out::Ok, Out::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (key, value) in self.defined() {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Element {
    #[serde(rename = "type")]
    node_type: ElementTag,
    pub tag_name: String,
    pub properties: Properties,
    pub children: Vec<ElementContent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum ElementTag {
    Element,
}

impl Element {
    pub fn new(tag_name: &str, properties: Properties, children: Vec<ElementContent>) -> Self {
        Self {
            node_type: ElementTag::Element,
            tag_name: tag_name.to_owned(),
            properties,
            children,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Text {
    #[serde(rename = "type")]
    node_type: TextTag,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum TextTag {
    Text,
}

impl Text {
    pub fn new(value: &str) -> Self {
        Self {
            node_type: TextTag::Text,
            value: value.to_owned(),
        }
    }
}

/// hast `ElementContent`: an element or a text node.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ElementContent {
    Element(Element),
    Text(Text),
}

impl ElementContent {
    /// The concatenated text of this node and its descendants.
    pub fn text_content(&self) -> String {
        match self {
            Self::Text(text) => text.value.clone(),
            Self::Element(element) => element
                .children
                .iter()
                .map(ElementContent::text_content)
                .collect(),
        }
    }

    pub fn as_element(&self) -> Option<&Element> {
        match self {
            Self::Element(element) => Some(element),
            Self::Text(_) => None,
        }
    }

    pub fn as_element_mut(&mut self) -> Option<&mut Element> {
        match self {
            Self::Element(element) => Some(element),
            Self::Text(_) => None,
        }
    }
}

impl From<Element> for ElementContent {
    fn from(element: Element) -> Self {
        Self::Element(element)
    }
}

impl From<Text> for ElementContent {
    fn from(text: Text) -> Self {
        Self::Text(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_replace_in_place_and_drop_undefined_on_serialize() {
        let properties = Properties::new()
            .with("data-a", "1")
            .with("data-b", PropValue::Undefined)
            .with("data-c", 3usize)
            .with("data-a", "2");
        let json = serde_json::to_string(&properties).expect("serializes");
        assert_eq!(json, r#"{"data-a":"2","data-c":3}"#);
        assert_eq!(properties.get("data-b"), Some(&PropValue::Undefined));
    }

    #[test]
    fn elements_serialize_like_hast() {
        let element = Element::new(
            "div",
            Properties::new().with("data-x", ""),
            vec![Text::new("hi").into()],
        );
        assert_eq!(
            serde_json::to_string(&element).expect("serializes"),
            r#"{"type":"element","tagName":"div","properties":{"data-x":""},"children":[{"type":"text","value":"hi"}]}"#
        );
        assert_eq!(ElementContent::from(element).text_content(), "hi");
    }
}
