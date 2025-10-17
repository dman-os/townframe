use super::*;

mod create;

pub fn feature(reg: &TypeReg) -> Feature {
    let mime_ty = reg.add_type(Type::Alias(
        Alias::builder("MimeType", reg.string()).build(),
    ));
    let multihash = reg.add_type(Type::Alias(
        Alias::builder("Multihash", reg.string()).build(),
    ));
    let doc_id = reg.add_type(Type::Alias(Alias::builder("DocId", reg.string()).build()));

    let doc_blob = reg.add_type(Type::Record(
        Record::builder("DocBlob")
            .with_fields([
                ("length_octets", RecordField::builder(reg.u64()).build()),
                ("hash", RecordField::builder(multihash).build()),
            ])
            .build(),
    ));
    let doc_image = reg.add_type(Type::Record(
        Record::builder("DocImage")
            .with_fields([
                ("mime", RecordField::builder(mime_ty).build()),
                ("width_px", RecordField::builder(reg.u64()).build()),
                ("height_px", RecordField::builder(reg.u64()).build()),
                (
                    // FIXME: find something better than blurhash
                    "blurhash",
                    RecordField::builder(doc_id).optional(reg).build(),
                ),
                ("blob", RecordField::builder(doc_id).build()),
            ])
            .build(),
    ));
    // NOTE:
    //  - If breaking changes are needed on the schema of contents and tags
    //    declare v2 like `text2`
    let doc_content_variants = vec![
        ("text", reg.string()),
        ("blob", doc_blob),
        ("image", doc_image),
    ];
    let doc_content_kind = reg.add_type(Type::Enum(
        Enum::builder("DocKind")
            .with_variants(
                doc_content_variants
                    .iter()
                    .map(|(key, _)| (*key, EnumVariant::builder().build())),
            )
            .build(),
    ));
    let doc_content = reg.add_type(Type::Variant(
        Variant::builder("DocContent")
            .with_variants(doc_content_variants.into_iter().map(|(key, val)| {
                (
                    key,
                    VariantVariant::builder(VariantVariantType::Wrapped(val)).build(),
                )
            }))
            .build(),
    ));
    let doc_ref = reg.add_type(Type::Alias(Alias::builder("DocRef", doc_id).build()));
    let doc_tag_variants = vec![
        (
            "ref_generic",
            VariantVariant::builder(VariantVariantType::Wrapped(doc_ref))
                .desc("A link to another document.")
                .build(),
        ),
        (
            "label_generic",
            VariantVariant::builder(VariantVariantType::Wrapped(reg.string())).build(),
        ),
        // path_generic
        // version_branch
    ];
    let doc_tag_kind = reg.add_type(Type::Enum(
        Enum::builder("DocTagKind")
            .with_variants(
                doc_tag_variants
                    .iter()
                    .map(|(key, _)| (*key, EnumVariant::builder().build())),
            )
            .build(),
    ));
    let doc_tag = reg.add_type(Type::Variant(
        Variant::builder("DocTag")
            .with_variants(doc_tag_variants)
            .build(),
    ));
    let doc = reg.add_type(Type::Record(
        Record::builder("Doc")
            .with_fields([
                ("id", RecordField::builder(doc_id).build()),
                ("created_at", RecordField::date_time(reg).build()),
                ("updated_at", RecordField::date_time(reg).build()),
                // (
                //     "kind",
                //     RecordField::builder(doc_content_kind).build(),
                // ),
                ("content", RecordField::builder(doc_content).build()),
                ("tags", RecordField::builder(reg.list(doc_tag)).build()),
            ])
            .build(),
    ));

    let doc_added_event = reg.add_type(Type::Record(
        Record::builder("DocAddedEvent")
            .with_fields([
                ("id", RecordField::builder(doc_id).build()),
                (
                    "heads",
                    RecordField::builder(reg.list(reg.string())).build(),
                ),
            ])
            .build(),
    ));

    Feature {
        tag: Tag {
            name: "doc".into(),
            desc: "Doc mgmt.".into(),
        },
        schema_types: vec![
            mime_ty,
            multihash,
            doc_image,
            doc_blob,
            doc_id,
            doc_content_kind,
            doc_content,
            doc_ref,
            doc_tag_kind,
            doc_tag,
            doc,
            doc_added_event,
        ],
        endpoints: vec![create::epoint_type(reg, doc)],
        wit_module: "townframe:daybook-api".into(),
    }
}
