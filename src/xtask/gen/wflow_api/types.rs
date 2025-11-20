use super::*;

pub fn feature(reg: &TypeReg) -> Feature {
    let job_id = reg.add_type(Type::Alias(Alias::builder("JobId", reg.string()).build()));

    let partition_id = reg.add_type(Type::Alias(
        Alias::builder("PartitionId", reg.u64()).build(),
    ));

    Feature {
        tag: Tag {
            name: "types".into(),
            desc: "Wflow common types.".into(),
        },
        schema_types: vec![job_id, partition_id],
        endpoints: vec![],
        wit_module: "townframe:wflow".into(),
    }
}
