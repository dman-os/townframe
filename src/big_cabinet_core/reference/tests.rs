use super::*;
use serde_json::json;

fn doc() -> DocId {
    DocId::from_str("z11111111111111111111111111111111").unwrap()
}

const DOC: &str = "z11111111111111111111111111111111";

fn key(slot: &[u8]) -> DatumKey {
    DatumKey::new("org.example.note", slot.to_vec()).unwrap()
}

fn self_reference(slot: &[u8]) -> DatumReference {
    DatumReference::new(ReferenceDocId::SelfDoc, key(slot))
}

fn head(byte: u8) -> ChangeHash {
    ChangeHash([byte; 32])
}

fn hex_bytes(text: &str) -> [u8; 32] {
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).unwrap();
    }
    bytes
}

fn node_key() -> MultibaseKey {
    MultibaseKey::try_from([7u8; 32].as_slice()).unwrap()
}

#[test]
fn go_quote_vectors_are_independent_and_round_trip() {
    let cases: &[(&[u8], &str)] = &[
        ("café/東京".as_bytes(), "café/東京"),
        (b"quote\" slash\\ percent%", "quote\\\" slash\\\\ percent%"),
        (
            b"\0\x01\x07\x08\x0c\n\r\t\x0b\x1f\x7f",
            "\\x00\\x01\\a\\b\\f\\n\\r\\t\\v\\x1f\\x7f",
        ),
        (&[0x80, 0xff], "\\x80\\xff"),
    ];
    for (bytes, expected) in cases {
        assert_eq!(go_string_escape(bytes), *expected);
        assert_eq!(go_string_unescape(expected.as_bytes()).unwrap(), *bytes);
    }
}

#[test]
fn go_quote_matches_published_go_quote_edge_table() {
    // Provenance: Go 1.23.0 src/strconv/quote.go (Quote/IsPrint) and
    // src/strconv/quote_test.go (quotetests), fetched from the published Go
    // source at https://raw.githubusercontent.com/golang/go/go1.23.0/src/strconv/.
    let cases = [
        ("\u{0085}".as_bytes(), r"\u0085"),
        ("\u{00a0}".as_bytes(), r"\u00a0"),
        ("\u{200b}".as_bytes(), r"\u200b"),
        ("\u{2028}".as_bytes(), r"\u2028"),
        ("\u{2029}".as_bytes(), r"\u2029"),
        // U+088F was unassigned in Unicode 15.0 (Go 1.23's data) and
        // must remain escaped even though later Unicode data assigns it.
        ("\u{088f}".as_bytes(), r"\u088f"),
        ("é東京".as_bytes(), "é東京"),
    ];
    for (bytes, expected) in cases {
        assert_eq!(go_string_escape(bytes), expected);
        assert_eq!(go_string_unescape(expected.as_bytes()).unwrap(), bytes);
    }
}

#[test]
fn canonical_unicode_data_version_is_pinned() {
    assert_eq!(
        unicode_properties::UNICODE_VERSION,
        CANONICAL_REFERENCE_UNICODE_VERSION
    );
    assert!(!go_is_print('\u{088f}'));
    assert_eq!(go_string_escape("\u{088f}".as_bytes()), r"\u088f");
}

#[test]
fn every_byte_round_trips_through_go_escapes() {
    let bytes = (0..=u8::MAX).collect::<Vec<_>>();
    let escaped = go_string_escape(&bytes);
    assert_eq!(go_string_unescape(escaped.as_bytes()).unwrap(), bytes);
}

#[test]
fn malformed_go_escape_width_and_value_are_structured_errors() {
    for value in [
        br"\1".as_slice(),
        br"\12".as_slice(),
        br"\777".as_slice(),
        br"\400".as_slice(),
        br"\x0".as_slice(),
        br"\u123".as_slice(),
        br"\U1234567".as_slice(),
        br"\uD800".as_slice(),
        br"\U00110000".as_slice(),
        br"\'".as_slice(),
        b"\"".as_slice(),
        b"\n".as_slice(),
    ] {
        assert!(
            matches!(
                go_string_unescape(value),
                Err(ReferenceError::InvalidGoEscape)
            ),
            "{value:?}"
        );
    }
}

#[test]
fn slots_are_go_escaped_then_percent_encoded_with_slash_raw() {
    let cases: &[(&[u8], &str)] = &[
        // `/` stays raw so dpath-like slots read as paths.
        (
            b"/photos/beach.jpg",
            "dtxt:datum:self/org.example.note//photos/beach.jpg",
        ),
        (
            b"main/notes/v1.2",
            "dtxt:datum:self/org.example.note/main/notes/v1.2",
        ),
        // `%` is the escape character, so it is always encoded.
        (
            b"100%/literal",
            "dtxt:datum:self/org.example.note/100%25/literal",
        ),
        (b"a b", "dtxt:datum:self/org.example.note/a%20b"),
        (b"what?x#y", "dtxt:datum:self/org.example.note/what%3Fx%23y"),
        (
            "café/東京".as_bytes(),
            "dtxt:datum:self/org.example.note/caf%C3%A9/%E6%9D%B1%E4%BA%AC",
        ),
        // A Go-escaped quote is a backslash and a quote, each percent-encoded.
        (
            b"quote\" slash\\ percent%",
            "dtxt:datum:self/org.example.note/quote%5C%22%20slash%5C%5C%20percent%25",
        ),
        (
            &[0x80, 0xff],
            "dtxt:datum:self/org.example.note/%5Cx80%5Cxff",
        ),
        (
            "\u{0085}\u{00a0}\u{200b}\u{2028}\u{2029}".as_bytes(),
            "dtxt:datum:self/org.example.note/%5Cu0085%5Cu00a0%5Cu200b%5Cu2028%5Cu2029",
        ),
        // Nothing is escaped that the grammar does not need escaped.
        (b"a:b@c,d+e", "dtxt:datum:self/org.example.note/a:b@c,d+e"),
    ];
    for (slot, expected) in cases {
        let reference = self_reference(slot);
        assert_eq!(reference.to_string(), *expected);
        assert_eq!(DatumReference::parse(expected).unwrap(), reference);
        assert_eq!(reference.key().slot_bytes(), *slot);
    }
}

#[test]
fn parsers_accept_noncanonical_spelling_and_canonicalize() {
    // A capitalized scheme, lowercase percent escapes, an encoded `/` and a Go `\x41` escape.
    let reference =
        DatumReference::parse("DTXT:datum:self/org.example.note/caf%c3%a9/a%2fb%5cx41").unwrap();
    assert_eq!(reference.key().slot_bytes(), "café/a/bA".as_bytes());
    assert_eq!(
        reference.to_string(),
        "dtxt:datum:self/org.example.note/caf%C3%A9/a/bA"
    );
}

#[test]
fn a_literal_percent_escape_in_a_slot_is_data_not_a_decoded_dot() {
    let percent = DatumReference::parse("dtxt:datum:self/org.example.note/100%25/v1").unwrap();
    assert_eq!(percent.key().slot_bytes(), b"100%/v1");
    let literal = DatumReference::parse("dtxt:datum:self/org.example.note/%252E/v1").unwrap();
    assert_eq!(literal.key().slot_bytes(), b"%2E/v1");
    assert_eq!(
        literal.to_string(),
        "dtxt:datum:self/org.example.note/%252E/v1"
    );
}

#[test]
fn dot_segments_are_ordinary_slots() {
    // The path is opaque, so no URL parser can normalize `..` away (ADR 035 §4).
    for slot in ["./v1", "../v1", "a/../b", ".", ".."] {
        let reference = self_reference(slot.as_bytes());
        assert_eq!(
            reference.to_string(),
            format!("dtxt:datum:self/org.example.note/{slot}")
        );
        assert_eq!(
            DatumReference::parse(&reference.to_string()).unwrap(),
            reference
        );
    }
    assert!(DatumTag::new("..").is_ok());
}

#[test]
fn datum_references_round_trip_without_heads() {
    for text in [
        format!("dtxt:datum:{DOC}/org.example.dpath//photos/beach.jpg"),
        "dtxt:datum:self/org.example.note/main".to_owned(),
    ] {
        let reference = DatumReference::parse(&text).unwrap();
        assert!(reference.heads().is_empty());
        assert_eq!(reference.to_string(), text);
    }
    let concrete =
        DatumReference::parse(&format!("dtxt:datum:{DOC}/org.example.note/main")).unwrap();
    assert_eq!(concrete.document(), &ReferenceDocId::Concrete(doc()));
    let reference = DatumReference::parse("dtxt:datum:self/org.example.note/main").unwrap();
    assert_eq!(reference.document(), &ReferenceDocId::SelfDoc);
    assert_eq!(reference.document().resolve(&doc()), doc());
}

#[test]
fn heads_are_multibase_in_hash_byte_order_and_pipe_joined() {
    let reference = self_reference(b"main").pinned([head(9), head(2), head(9)]);
    assert_eq!(reference.heads(), &[head(2), head(9)]);
    let texts = [head_text(&head(2)), head_text(&head(9))];
    assert!(texts.iter().all(|text| text.starts_with('z')));
    let expected = format!(
        "dtxt:datum:self/org.example.note/main?heads={}|{}",
        texts[0], texts[1]
    );
    assert_eq!(reference.to_string(), expected);
    assert_eq!(DatumReference::parse(&expected).unwrap(), reference);

    // Unsorted and repeated input canonicalizes.
    let shuffled = format!(
        "dtxt:datum:self/org.example.note/main?heads={}|{}|{}",
        texts[1], texts[0], texts[1]
    );
    assert_eq!(DatumReference::parse(&shuffled).unwrap(), reference);
    assert_eq!(
        DatumReference::parse(&shuffled).unwrap().to_string(),
        expected
    );
}

#[test]
fn heads_are_spelled_in_hash_byte_order_even_where_their_text_sorts_otherwise() {
    // 32-byte values spell as 43 or 44 base58 digits, so string order and byte order disagree
    // across that boundary: 58^43 - 1 spells as 43 `z`s and 58^43 as `2` and 43 `1`s. The
    // canonical order is the bytes', which is also a stored pin's `heads` order.
    let low = ChangeHash(hex_bytes(
        "0edbafda67ca37188cf28263571f03b9716879e4acc9c514ab6727ffffffffff",
    ));
    let high = ChangeHash(hex_bytes(
        "0edbafda67ca37188cf28263571f03b9716879e4acc9c514ab67280000000000",
    ));
    assert!(low < high);
    assert!(head_text(&low) > head_text(&high));
    let reference = self_reference(b"main").pinned([high, low]);
    assert_eq!(
        reference.to_string(),
        format!(
            "dtxt:datum:self/org.example.note/main?heads={}|{}",
            head_text(&low),
            head_text(&high)
        )
    );
}

#[test]
fn head_text_round_trips_and_rejects_non_heads() {
    let text = head_text(&head(5));
    assert_eq!(parse_head(&text).unwrap(), head(5));
    for bad in [
        "",
        "z",
        "zshort",
        &"a".repeat(64),
        "Qm123",
        "z0OIl",
        "é",
        &utils_rs::hash::encode_base58_multibase([1u8; 16]),
    ] {
        assert!(
            matches!(parse_head(bad), Err(ReferenceError::InvalidHead(_))),
            "{bad}"
        );
    }
}

#[test]
fn empty_and_unsupported_queries_are_rejected() {
    let prefix = "dtxt:datum:self/org.example.note/main";
    assert!(matches!(
        DatumReference::parse(&format!("{prefix}?heads=")),
        Err(ReferenceError::EmptyHeads)
    ));
    assert!(matches!(
        DatumReference::parse(&format!("{prefix}?")),
        Err(ReferenceError::UnsupportedQuery(_))
    ));
    let one = head_text(&head(1));
    for query in [
        "at=head".to_owned(),
        "branch=main".to_owned(),
        format!("heads={one}&branch=main"),
        format!("branch=main&heads={one}"),
        format!("heads={one}|"),
        format!("heads=|{one}"),
    ] {
        assert!(
            DatumReference::parse(&format!("{prefix}?{query}")).is_err(),
            "{query}"
        );
    }
}

#[test]
fn locations_carry_a_fragment_and_datum_references_refuse_it() {
    let text = format!(
        "dtxt:datum:{DOC}/org.example.dpath//photos/beach.jpg?heads={}#meta/@0",
        head_text(&head(3))
    );
    let location = Location::parse(&text).unwrap();
    assert_eq!(location.fragment(), Some("meta/@0"));
    assert_eq!(location.reference().heads(), &[head(3)]);
    assert_eq!(
        location.reference().key().slot_bytes(),
        b"/photos/beach.jpg"
    );
    assert_eq!(location.to_string(), text);

    // A location without a fragment is the whole datum.
    let whole = Location::parse("dtxt:datum:self/org.example.note/main").unwrap();
    assert_eq!(whole.fragment(), None);
    assert_eq!(whole.to_string(), "dtxt:datum:self/org.example.note/main");

    // A `?` after the fragment's `#` belongs to the fragment, and a slot's encoded `#` does not
    // start one.
    let with_question = Location::parse("dtxt:datum:self/org.example.note/a%23b#x?y").unwrap();
    assert_eq!(with_question.fragment(), Some("x?y"));
    assert_eq!(with_question.reference().key().slot_bytes(), b"a#b");

    assert!(matches!(
        DatumReference::parse(&text),
        Err(ReferenceError::FragmentNotAllowed)
    ));
    assert!(matches!(
        DatumReference::parse("dtxt:datum:self/org.example.note/main#frag"),
        Err(ReferenceError::FragmentNotAllowed)
    ));
    for bad in ["#", "a#b", "a b", "100%", "%zz", "é"] {
        assert!(
            Location::new(self_reference(b"main"), Some(bad.to_owned())).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn the_stored_form_holds_no_pin() {
    let pinned = self_reference(b"main").pinned([head(1)]);
    assert!(matches!(
        DatumReference::parse_stored(&pinned.to_string()),
        Err(ReferenceError::PinInStoredReference)
    ));
    assert!(serde_json::to_string(&pinned).is_err());
    assert!(serde_json::from_str::<DatumReference>(&format!("\"{pinned}\"")).is_err());

    let plain = self_reference(b"main");
    let encoded = serde_json::to_string(&plain).unwrap();
    assert_eq!(encoded, "\"dtxt:datum:self/org.example.note/main\"");
    assert_eq!(
        serde_json::from_str::<DatumReference>(&encoded).unwrap(),
        plain
    );

    // A location stores the same way and keeps its fragment: the fragment moves with the
    // position, the pin moves on its own (ADR 035 §2.8).
    let pinned_location = Location::new(pinned, Some("meta/@0".to_owned())).unwrap();
    assert!(matches!(
        Location::parse_stored(&pinned_location.to_string()),
        Err(ReferenceError::PinInStoredReference)
    ));
    assert!(serde_json::to_string(&pinned_location).is_err());
    let location = Location::new(plain.clone(), Some("meta/@0".to_owned())).unwrap();
    let encoded = serde_json::to_string(&location).unwrap();
    assert_eq!(encoded, "\"dtxt:datum:self/org.example.note/main#meta/@0\"");
    let decoded = serde_json::from_str::<Location>(&encoded).unwrap();
    assert_eq!(decoded, location);
    assert_eq!(decoded.into_reference(), plain);
    // A whole-datum position refuses what a location accepts.
    assert!(serde_json::from_str::<DatumReference>(&encoded).is_err());
}

#[test]
fn malformed_datum_references_are_refused() {
    for value in [
        "",
        "dtxt",
        "dtxt:datum",
        "dtxt:datum:",
        "dtxt:datum:self",
        "dtxt:datum:self/org.example.note",
        "dtxt:datum:self//main",
        "dtxt:datum:self/org.example.note/",
        "dtxt:datum://self/org.example.note/main",
        "dtxt:datum:///self/org.example.note/main",
        "dtxt:datum:/self/org.example.note/main",
        "dtxt:datum:nonsense/org.example.note/main",
        "dtxt:datum:self/org.example.note/main%",
        "dtxt:datum:self/org.example.note/main%2",
        "dtxt:datum:self/org.example.note/main%GG",
        "dtxt:datum:self/org.example.note/main%FF",
    ] {
        assert!(DatumReference::parse(value).is_err(), "{value}");
    }
}

#[test]
fn dropped_schemes_and_other_kinds_are_refused() {
    for value in [
        "db+datum:/self/org.example.note/main",
        "db+facet:///self/org.example.note/main",
        "db+blob:///zHash",
        "db+actor://node/path",
        "db+command:///@scope/plug/command",
        "db+iroh-clone:ticket",
        "future+blob:unrecognized",
    ] {
        assert!(
            matches!(
                DatumReference::parse(value),
                Err(ReferenceError::WrongScheme(_))
            ),
            "{value}"
        );
        assert!(Location::parse(value).is_err(), "{value}");
        assert!(ActorUrl::parse(value).is_err(), "{value}");
    }
    assert!(matches!(
        DatumReference::parse("dtxt:facet:self/org.example.note/main"),
        Err(ReferenceError::UnexpectedKind { .. })
    ));
    assert!(matches!(
        DatumReference::parse("dtxt:blob:zHash"),
        Err(ReferenceError::UnexpectedKind { .. })
    ));
    assert!(matches!(
        ActorUrl::parse("dtxt:datum:self/org.example.note/main"),
        Err(ReferenceError::UnexpectedKind { .. })
    ));
}

#[test]
fn actor_urls_round_trip_with_and_without_a_path() {
    let operator = ActorUrl::new(node_key(), "").unwrap();
    assert_eq!(operator.to_string(), format!("dtxt:actor:{}", node_key()));
    assert_eq!(ActorUrl::parse(&operator.to_string()).unwrap(), operator);
    assert_eq!(operator.path(), "");

    let nested = ActorUrl::new(node_key(), "agents/session-1.a_b").unwrap();
    let text = format!("dtxt:actor:{}/agents/session-1.a_b", node_key());
    assert_eq!(nested.to_string(), text);
    assert_eq!(ActorUrl::parse(&text).unwrap(), nested);
    assert_eq!(nested.node_key(), &node_key());
    assert_eq!(nested.path(), "agents/session-1.a_b");

    let encoded = serde_json::to_string(&nested).unwrap();
    assert_eq!(encoded, format!("\"{text}\""));
    assert_eq!(serde_json::from_str::<ActorUrl>(&encoded).unwrap(), nested);
    assert!(serde_json::from_str::<ActorUrl>("\"dtxt:actor:\"").is_err());
}

#[test]
fn malformed_actor_urls_are_refused() {
    let key = node_key();
    for value in [
        "dtxt:actor:".to_owned(),
        "dtxt:actor:/path".to_owned(),
        "dtxt:actor:notakey".to_owned(),
        "dtxt:actor:é".to_owned(),
        format!("dtxt:actor:{key}/"),
        format!("dtxt:actor:{key}//a"),
        format!("dtxt:actor:{key}/a/"),
        format!("dtxt:actor:{key}/."),
        format!("dtxt:actor:{key}/a/../b"),
        format!("dtxt:actor:{key}/a b"),
        format!("dtxt:actor:{key}/%70rocess"),
        format!("dtxt:actor:{key}/a?x=1"),
        format!("dtxt:actor:{key}#frag"),
        format!("dtxt:actor://{key}/a"),
    ] {
        assert!(ActorUrl::parse(&value).is_err(), "{value}");
    }
}

#[test]
fn constructors_and_serde_enforce_invariants() {
    assert!(DatumTag::new("").is_err());
    assert!(DatumTag::new("tag/with/slash").is_err());
    assert!(DatumKey::new("tag", Vec::<u8>::new()).is_err());
    assert!(serde_json::from_str::<DatumTag>("\"\"").is_err());
    assert!(serde_json::from_str::<DatumKey>("\"tag/\"").is_err());
}

fn pin_text(suffix: &str) -> String {
    format!("dtxt:datum:self/org.example.note/{suffix}")
}

#[test]
fn a_pin_round_trips_with_and_without_heads() {
    let unpinned = json!({"ref": pin_text("main")});
    let pin: DatumPin = serde_json::from_value(unpinned.clone()).unwrap();
    assert!(pin.location().reference().heads().is_empty());
    assert_eq!(pin.location().fragment(), None);
    assert_eq!(serde_json::to_value(&pin).unwrap(), unpinned);

    let pinned = json!({"ref": pin_text("main"), "heads": [head_text(&head(1))]});
    let pin: DatumPin = serde_json::from_value(pinned.clone()).unwrap();
    assert_eq!(pin.location().reference().heads(), [head(1)]);
    // The address inside carries no query: the heads are the pin's own member.
    assert_eq!(pin.location().reference().key(), &key(b"main"));
    assert_eq!(serde_json::to_value(&pin).unwrap(), pinned);

    // A member a newer writer added survives a round trip through this one.
    let extended = json!({"ref": pin_text("main"), "heads": [head_text(&head(1))], "label": "v2"});
    let pin: DatumPin = serde_json::from_value(extended.clone()).unwrap();
    assert_eq!(serde_json::to_value(&pin).unwrap(), extended);
}

#[test]
fn a_pin_writes_its_heads_in_hash_byte_order_as_multibase() {
    let reference = self_reference(b"main").pinned([head(9), head(2), head(5), head(2)]);
    let pin = DatumPin::new(Location::new(reference, None).unwrap());
    // The location written to `ref` is unpinned; the pin is only in `heads`.
    assert_eq!(
        serde_json::to_value(&pin).unwrap(),
        json!({
            "ref": pin_text("main"),
            "heads": [head_text(&head(2)), head_text(&head(5)), head_text(&head(9))],
        })
    );
    // Read back out of order and with a repeat, it is the same pin.
    let read: DatumPin = serde_json::from_value(json!({
        "ref": pin_text("main"),
        "heads": [head_text(&head(9)), head_text(&head(2)), head_text(&head(5)), head_text(&head(2))],
    }))
    .unwrap();
    assert_eq!(read, pin);
}

#[test]
fn a_pin_refuses_empty_heads_and_a_query_on_its_address() {
    // Absent heads is the one spelling of "current", so a present empty list is refused.
    let error = serde_json::from_value::<DatumPin>(json!({"ref": pin_text("main"), "heads": []}))
        .unwrap_err()
        .to_string();
    assert!(error.contains("never empty"), "{error}");
    // The pin is the object's `heads`, never a query on the address.
    let queried = format!("{}?heads={}", pin_text("main"), head_text(&head(1)));
    assert!(serde_json::from_value::<DatumPin>(json!({"ref": queried})).is_err());
    assert!(serde_json::from_value::<DatumPin>(json!({"heads": [head_text(&head(1))]})).is_err());
    assert!(
        serde_json::from_value::<DatumPin>(json!({"ref": pin_text("main"), "heads": ["nope"]}))
            .is_err()
    );
}

#[test]
fn a_pin_keeps_its_locations_fragment() {
    let stated = json!({
        "ref": format!("{}#text/@3", pin_text("main")),
        "heads": [head_text(&head(1))],
    });
    let pin: DatumPin = serde_json::from_value(stated.clone()).unwrap();
    assert_eq!(pin.location().fragment(), Some("text/@3"));
    assert_eq!(pin.location().reference().heads(), [head(1)]);
    assert_eq!(serde_json::to_value(&pin).unwrap(), stated);
}
