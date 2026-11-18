//! A bounded group of related content (`ct-card`).
//!
//! Parts: `header`, `content`, `footer`, each optional and in that order. Axis: `data-variant`
//! (`outline`, `raised`, `plain`) and `data-status` (`info`, `success`, `warning`, `danger`, the
//! same axis and colour roles as `Alert`; absent is neutral). A card has no role of its own: it is a visual grouping, and a
//! caller who wants it announced names it with a heading inside the header.
use super::Status;
use dioxus::prelude::*;

/// How a card sets itself off from its surroundings.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum CardVariant {
    /// A hairline border.
    #[default]
    Outline,
    /// A raised surface with elevation.
    Raised,
    /// No boundary of its own: only its padding and parts.
    Plain,
}

impl CardVariant {
    fn as_str(self) -> &'static str {
        match self {
            Self::Outline => "outline",
            Self::Raised => "raised",
            Self::Plain => "plain",
        }
    }
}

#[component]
pub fn Card(
    #[props(default)] variant: CardVariant,
    /// What the card says about its content; a neutral card has none.
    #[props(default)]
    status: Option<Status>,
    children: Element,
) -> Element {
    rsx! {
        div {
            class: "ct-card",
            "data-variant": variant.as_str(),
            "data-status": status.map(Status::as_str),
            {children}
        }
    }
}

#[component]
pub fn CardHeader(children: Element) -> Element {
    rsx! {
        div { "data-part": "header", {children} }
    }
}

#[component]
pub fn CardContent(children: Element) -> Element {
    rsx! {
        div { "data-part": "content", {children} }
    }
}

#[component]
pub fn CardFooter(children: Element) -> Element {
    rsx! {
        div { "data-part": "footer", {children} }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Page;

    fn app(variant: CardVariant) -> Element {
        rsx! {
            Card { variant, status: None,
                CardHeader { h3 { "Title" } }
                CardContent { "Body" }
                CardFooter { button { "Act" } }
            }
        }
    }

    #[test]
    fn a_card_is_a_root_with_its_parts_in_order() {
        let page = Page::with_props(app, CardVariant::Raised);
        let heading = page.by_role("heading", "Title");
        let header = page.parent(heading);
        let card = page.parent(header);
        assert_eq!(page.attr(card, "class").as_deref(), Some("ct-card"));
        assert_eq!(page.attr(card, "data-variant").as_deref(), Some("raised"));
        let parts: Vec<_> = page
            .children(card)
            .into_iter()
            .map(|part| page.attr(part, "data-part").expect("every child is a part"))
            .collect();
        assert_eq!(parts, ["header", "content", "footer"]);
    }

    #[test]
    fn a_status_names_itself_and_a_neutral_card_has_none() {
        fn with_status(status: Option<Status>) -> Element {
            rsx! { Card { status, CardContent { "Body" } } }
        }
        let page = Page::with_props(with_status, None);
        assert_eq!(page.attr(page.by_class("ct-card"), "data-status"), None);
        for status in [
            Status::Info,
            Status::Success,
            Status::Warning,
            Status::Danger,
        ] {
            let page = Page::with_props(with_status, Some(status));
            let card = page.by_class("ct-card");
            assert_eq!(
                page.attr(card, "data-status").as_deref(),
                Some(status.as_str())
            );
        }
    }

    #[test]
    fn every_variant_names_itself_for_the_stylesheet() {
        for (variant, name) in [
            (CardVariant::Outline, "outline"),
            (CardVariant::Raised, "raised"),
            (CardVariant::Plain, "plain"),
        ] {
            let page = Page::with_props(app, variant);
            let card = page.parent(page.parent(page.by_role("heading", "Title")));
            assert_eq!(page.attr(card, "data-variant").as_deref(), Some(name));
        }
    }
}
