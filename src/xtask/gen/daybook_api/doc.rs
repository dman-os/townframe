use super::*;

mod create;

pub fn feature(reg: &TypeReg) -> Feature {
    let schema_user = reg.add_type(Type::Record(
        Record::builder()
            .name("Doc")
            .with_fields([
                ("id", RecordField::uuid(&reg).build()),
                ("created_at", RecordField::date_time(&reg).build()),
                ("updated_at", RecordField::date_time(&reg).build()),
            ])
            .build(),
    ));
    Feature {
        tag: Tag {
            name: "doc".into(),
            desc: "Doc mgmt.".into(),
        },
        schema_types: vec![schema_user],
        endpoints: vec![create::epoint_type(reg, schema_user)],
        wit_module: "townframe:daybook-api".into(),
    }
}
