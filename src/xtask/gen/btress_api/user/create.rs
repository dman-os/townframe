use super::*;

fn input(reg: &TypeReg) -> InputType {
    InputType::builder()
        .desc("Create user request")
        .with_fields([
            (
                "username",
                InputField::builder()
                    .inner(RecordField::builder(reg.string()).build())
                    .with_validations([
                        FieldValidations::Ascii,
                        FieldValidations::MinLength(3),
                        FieldValidations::MaxLength(25),
                        FieldValidations::Pattern("USERNAME_REGEX".into()),
                    ])
                    .build(),
            ),
            (
                "email",
                InputField::builder()
                    .inner(RecordField::email(reg).optional(reg).build())
                    .with_validation(FieldValidations::Email)
                    .build(),
            ),
            (
                "password",
                InputField::builder()
                    .inner(
                        RecordField::builder(reg.string())
                            .example("hunter2")
                            .build(),
                    )
                    .with_validations([
                        FieldValidations::MinLength(8),
                        FieldValidations::MaxLength(1024),
                    ])
                    .build(),
            ),
        ])
        .build()
}

fn error(reg: &TypeReg) -> ErrorType {
    ErrorType::builder()
        .with_variants([
            (
                "usernameOccupied",
                ErrorVariant::builder()
                    .http_code(StatusCode::BAD_REQUEST)
                    .message("Username occupied")
                    .message_with_fields(": {username}")
                    .with_field(
                        "username",
                        ErrorField::builder()
                            .inner(RecordField::builder(reg.string()).build())
                            .build(),
                    )
                    .build(),
            ),
            (
                "emailOccupied",
                ErrorVariant::builder()
                    .http_code(StatusCode::BAD_REQUEST)
                    .message("Email occupied")
                    .message_with_fields(": {email:?}")
                    .with_field(
                        "email",
                        ErrorField::builder()
                            .inner(RecordField::email(reg).optional(reg).build())
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

    EndpointType::builder("UserCreate")
        .path("/users")
        .method(Method::POST)
        .success(StatusCode::CREATED)
        .input(input(reg))
        .output(output)
        .error(error(reg))
        .build()
}
