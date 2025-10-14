use super::*;

mod create;

pub fn feature(reg: &TypeReg) -> Feature {
    let schema_mime_ty = reg.add_type(Type::Alias(
        Alias::builder("MimeType", reg.string()).build(),
    ));
    let schema_multihash = reg.add_type(Type::Alias(
        Alias::builder("Multihash", reg.string()).build(),
    ));
    let schema_doc_id = reg.add_type(Type::Alias(Alias::builder("DocId", reg.string()).build()));

    let schema_doc_blob = reg.add_type(Type::Record(
        Record::builder("DocBlob")
            .with_fields([
                ("length_octets", RecordField::builder(reg.u64()).build()),
                ("hash", RecordField::builder(schema_multihash).build()),
            ])
            .build(),
    ));
    let schema_doc_image = reg.add_type(Type::Record(
        Record::builder("DocImage")
            .with_fields([
                ("mime", RecordField::builder(schema_mime_ty).build()),
                ("width_px", RecordField::builder(reg.u64()).build()),
                ("height_px", RecordField::builder(reg.u64()).build()),
                (
                    // FIXME: find something better than blurhash
                    "blurhash",
                    RecordField::builder(schema_doc_id).optional(reg).build(),
                ),
                ("blob", RecordField::builder(schema_doc_id).build()),
            ])
            .build(),
    ));
    // NOTE:
    //  - If breaking changes are needed on the schema of contents and tags
    //    declare v2 like `text2`
    let schema_doc_content_variants = vec![
        ("text", reg.string()),
        ("blob", schema_doc_blob),
        ("image", schema_doc_image),
    ];
    let schema_doc_content_kind = reg.add_type(Type::Enum(
        Enum::builder("DocKind")
            .with_variants(
                schema_doc_content_variants
                    .iter()
                    .map(|(key, _)| (*key, EnumVariant::builder().build())),
            )
            .build(),
    ));
    let schema_doc_content = reg.add_type(Type::Variant(
        Variant::builder("DocContent")
            .with_variants(schema_doc_content_variants.into_iter().map(|(key, val)| {
                (
                    key,
                    VariantVariant::builder(VariantVariantType::Wrapped(val)).build(),
                )
            }))
            .build(),
    ));
    let schema_doc_ref = reg.add_type(Type::Alias(Alias::builder("DocRef", schema_doc_id).build()));
    let schema_doc_tag_variants = vec![
        (
            "ref_generic",
            VariantVariant::builder(VariantVariantType::Wrapped(schema_doc_ref))
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
    let schema_doc_tag_kind = reg.add_type(Type::Enum(
        Enum::builder("DocTagKind")
            .with_variants(
                schema_doc_tag_variants
                    .iter()
                    .map(|(key, _)| (*key, EnumVariant::builder().build())),
            )
            .build(),
    ));
    let schema_doc_tag = reg.add_type(Type::Variant(
        Variant::builder("DocTag")
            .with_variants(schema_doc_tag_variants)
            .build(),
    ));
    let schema_doc = reg.add_type(Type::Record(
        Record::builder("Doc")
            .with_fields([
                ("id", RecordField::builder(schema_doc_id).build()),
                ("created_at", RecordField::date_time(&reg).build()),
                ("updated_at", RecordField::date_time(&reg).build()),
                // (
                //     "kind",
                //     RecordField::builder(schema_doc_content_kind).build(),
                // ),
                ("content", RecordField::builder(schema_doc_content).build()),
                (
                    "tags",
                    RecordField::builder(reg.list(schema_doc_tag)).build(),
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
            schema_mime_ty,
            schema_multihash,
            schema_doc_image,
            schema_doc_blob,
            schema_doc_id,
            schema_doc_content_kind,
            schema_doc_content,
            schema_doc_ref,
            schema_doc_tag_kind,
            schema_doc_tag,
            schema_doc,
        ],
        endpoints: vec![create::epoint_type(reg, schema_doc)],
        wit_module: "townframe:daybook-api".into(),
    }
}
