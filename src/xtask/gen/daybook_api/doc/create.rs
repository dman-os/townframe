use super::*;

fn input(reg: &TypeReg) -> InputType {
    InputType::builder()
        .desc("Create doc request")
        .with_fields([(
            "id",
            InputField::builder()
                .inner(RecordField::uuid(reg).build())
                .with_validations([
                    FieldValidations::MinLength(1),
                    FieldValidations::MaxLength(1024),
                ])
                .build(),
        )])
        .build()
}

fn error(reg: &TypeReg) -> ErrorType {
    ErrorType::builder()
        .with_variants([
            (
                "idOccupied",
                ErrorVariant::builder()
                    .http_code(StatusCode::BAD_REQUEST)
                    .message("Id occupied")
                    .message_with_fields(": {id}")
                    // .rust_attrs(RustAttrs { emit_serde: true, emit_autosurgeon: true, emit_uniffi: true, emit_utoipa: true })
                    .with_field(
                        "id",
                        ErrorField::builder()
                            .inner(RecordField::builder(reg.string()).build())
                            .build(),
                    )
                    .build(),
            ),
            ErrorVariant::invalid_input(reg),
            ErrorVariant::internal(reg),
        ])
        .build()
}

pub fn epoint_type(reg: &TypeReg, user_schema: TypeId) -> EndpointType {
    let output = OutputType::Ref(user_schema);

    EndpointType::builder("DocCreate")
        .path("/id")
        .method(Method::POST)
        .success(StatusCode::CREATED)
        .input(input(reg))
        .output(output)
        .error(error(reg))
        .build()
}
