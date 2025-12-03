use super::*;

pub fn feature(reg: &TypeReg) -> Feature {
    let wasmcloud_wflow_service_meta = reg.add_type(Type::Record(
        Record::builder("WasmcloudWflowServiceMeta")
            .with_fields([("workload_id", RecordField::builder(reg.string()).build())])
            .build(),
    ));

    let wflow_service_meta = reg.add_type(Type::Variant(
        Variant::builder("WflowServiceMeta")
            .with_variants(vec![
                //
                (
                    "wasmcloud",
                    VariantVariant::builder(VariantVariantType::Wrapped(
                        wasmcloud_wflow_service_meta,
                    ))
                    .build(),
                ),
                (
                    "local_native",
                    VariantVariant::builder(VariantVariantType::Unit).build(),
                ),
            ])
            .build(),
    ));

    let wflow_meta = reg.add_type(Type::Record(
        Record::builder("WflowMeta")
            .with_fields([
                ("key", RecordField::builder(reg.string()).build()),
                ("service", RecordField::builder(wflow_service_meta).build()),
            ])
            .build(),
    ));

    let partitions_meta = reg.add_type(Type::Record(
        Record::builder("PartitionsMeta")
            .with_fields([
                ("version", RecordField::builder(reg.string()).build()),
                ("partition_count", RecordField::builder(reg.u64()).build()),
            ])
            .build(),
    ));

    Feature {
        tag: Tag {
            name: "metastore".into(),
            desc: "Wflow metadata store types.".into(),
        },
        schema_types: vec![
            wasmcloud_wflow_service_meta,
            wflow_service_meta,
            wflow_meta,
            partitions_meta,
        ],
        endpoints: vec![],
        wit_module: "townframe:wflow".into(),
    }
}
