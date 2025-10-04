use super::*;
pub fn feature(reg: &TypeReg) -> Feature {
    let schema_user = reg.add_type(Type::Record(
        Record::builder()
            .name("User")
            .with_fields([
                ("id", RecordField::uuid(&reg).build()),
                ("created_at", RecordField::date_time(&reg).build()),
                ("updated_at", RecordField::date_time(&reg).build()),
                ("email", RecordField::email(&reg).optional(&reg).build()),
                ("username", RecordField::builder(reg.string()).build()),
            ])
            .build(),
    ));
    Feature {
        tag: Tag {
            name: "user".into(),
            desc: "User mgmt.".into(),
        },
        schema_types: vec![schema_user],
        endpoints: vec![create::epoint_type(reg, schema_user)],
        wit_module: "townframe:btress-api".into(),
    }
}
mod create;
