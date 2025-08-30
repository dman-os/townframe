use crate::interlude::*;

use http::{Method, StatusCode};

use std::hash::{Hash, Hasher};

use heck::*;
mod component_wit;
mod features;
mod handler_rust;

use std::fmt::Write;

pub fn cli() -> Res<()> {
    // use std::io::Write as WriteIo;
    let reg = TypeReg::new();

    let features = features::btress_api_features(&reg);

    let mut out = String::new();
    let buf = &mut out;
    write!(
        buf,
        r#"use super::*;   

"#
    )?;
    for feature in &features {
        handler_rust::feature_module(&reg, buf, &feature)?;
    }

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../btress_api/gen/");
    std::fs::create_dir_all(&path)?;
    std::fs::write(path.join("mod.rs"), &out)?;

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../btress_api/wit/");
    std::fs::create_dir_all(&path)?;
    for feature in &features {
        let mut out = String::new();
        let buf = &mut out;
        writeln!(buf, "package townframe:btress-api;")?;
        component_wit::feature_file(&reg, buf, &feature)?;
        let path = path.join(format!("{}.wit", feature.tag.name.to_kebab_case()));
        std::fs::write(path, &out)?;
    }
    Ok(())
}

pub type TypeId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Type {
    Primitives(Primitives),
    Record(Record),
    List(TypeId),
    Map(TypeId, TypeId),
    Option(TypeId),
    Tuple(Vec<TypeId>),
    Alias(CHeapStr, TypeId),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Primitives {
    String,
    U64,
    F64,
    Bool,
    Uuid,
    DateTime,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, bon::Builder)]
#[builder(on(CHeapStr, into))]
pub struct Record {
    #[builder(field)]
    fields: Vec<(CHeapStr, RecordField)>,
    name: CHeapStr,
}

impl<S: record_builder::State> RecordBuilder<S> {
    pub fn with_field(mut self, name: impl Into<CHeapStr>, value: RecordField) -> Self {
        self.fields.push((name.into(), value));
        self
    }

    pub fn with_fields(
        mut self,
        fields: impl IntoIterator<Item = (impl Into<CHeapStr>, RecordField)>,
    ) -> Self {
        for (name, value) in fields {
            self = self.with_field(name, value);
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, bon::Builder)]
#[builder(on(String, into))]
pub struct RecordField {
    #[builder(start_fn)]
    ty: TypeId,
    desc: Option<String>,
    example: Option<String>,
}

impl<S: record_field_builder::State> RecordFieldBuilder<S> {
    pub fn optional(mut self, reg: &TypeReg) -> Self {
        self.ty = reg.option(self.ty);
        self
    }
}

impl RecordField {
    pub fn email(reg: &TypeReg) -> RecordFieldBuilder<record_field_builder::SetExample> {
        Self::builder(reg.string()).example("alice@example.com")
    }
    pub fn date_time(reg: &TypeReg) -> RecordFieldBuilder {
        Self::builder(reg.date_time())
    }

    pub fn uuid(reg: &TypeReg) -> RecordFieldBuilder {
        Self::builder(reg.uuid())
    }
}

pub struct TypeReg {
    types: DHashMap<TypeId, Type>,
    validation_errors_id: TypeId,
}

impl TypeReg {
    pub fn new() -> Self {
        let mut this = Self {
            types: default(),
            validation_errors_id: 0,
        };
        this.validation_errors_id = this.add_type(Type::Alias(
            "ErrorsValidation".into(),
            this.add_type(Type::List(
                this.add_type(Type::Tuple(vec![this.string(), this.string()])),
            )),
        ));
        this
    }
    pub fn add_type(&self, ty: Type) -> TypeId {
        let mut hasher = std::hash::DefaultHasher::new();
        ty.hash(&mut hasher);
        let id = hasher.finish();

        self.types.insert(id, ty);

        id
    }

    pub fn string(&self) -> TypeId {
        self.add_type(Type::Primitives(Primitives::String))
    }

    pub fn u64(&self) -> TypeId {
        self.add_type(Type::Primitives(Primitives::U64))
    }

    pub fn f64(&self) -> TypeId {
        self.add_type(Type::Primitives(Primitives::F64))
    }

    pub fn bool(&self) -> TypeId {
        self.add_type(Type::Primitives(Primitives::Bool))
    }

    pub fn uuid(&self) -> TypeId {
        self.add_type(Type::Primitives(Primitives::Uuid))
    }

    pub fn date_time(&self) -> TypeId {
        self.add_type(Type::Primitives(Primitives::DateTime))
    }

    pub fn json(&self) -> TypeId {
        self.add_type(Type::Primitives(Primitives::Json))
    }

    pub fn map(&self, key: TypeId, value: TypeId) -> TypeId {
        self.add_type(Type::Map(key, value))
    }

    pub fn list(&self, ty: TypeId) -> TypeId {
        self.add_type(Type::List(ty))
    }

    pub fn option(&self, ty: TypeId) -> TypeId {
        self.add_type(Type::Option(ty))
    }

    pub fn validation_errors(&self) -> TypeId {
        self.validation_errors_id
    }
}

pub struct Tag {
    pub name: String,
    pub desc: String,
}

pub struct Feature {
    pub tag: Tag,
    pub schema_types: Vec<TypeId>,
    pub endpoints: Vec<EndpointType>,
}

#[derive(bon::Builder)]
#[builder(on(String, into))]
#[builder(on(CHeapStr, into))]
pub struct EndpointType {
    #[builder(start_fn)]
    id: CHeapStr,
    input: InputType,
    output: OutputType,
    error: ErrorType,
    path: String,
    method: Method,
    success: StatusCode,
}

#[derive(Default, PartialEq, Eq, Hash, Clone, Copy)]
enum InputFieldSource {
    #[default]
    JsonBody,
    Query,
}

#[derive(bon::Builder)]
#[builder(on(CHeapStr, into))]
pub struct InputType {
    #[builder(field)]
    fields: IndexMap<CHeapStr, InputField>,
    desc: CHeapStr,
    #[builder(default)]
    main_source: InputFieldSource,
}

impl<S: input_type_builder::State> InputTypeBuilder<S> {
    pub fn with_field(mut self, name: impl Into<CHeapStr>, value: InputField) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    pub fn with_fields(
        mut self,
        fields: impl IntoIterator<Item = (impl Into<CHeapStr>, InputField)>,
    ) -> Self {
        for (name, value) in fields {
            self = self.with_field(name, value);
        }
        self
    }
}

#[derive(bon::Builder)]
pub struct InputField {
    #[builder(field)]
    validations: Vec<FieldValidations>,
    inner: RecordField,
    #[builder(default)]
    source: InputFieldSource,
}

impl<S: input_field_builder::State> InputFieldBuilder<S> {
    pub fn with_validation(mut self, validation: FieldValidations) -> Self {
        self.validations.push(validation);
        self
    }

    pub fn with_validations(
        mut self,
        validations: impl IntoIterator<Item = FieldValidations>,
    ) -> Self {
        for validation in validations {
            self = self.with_validation(validation);
        }
        self
    }
}

pub enum FieldValidations {
    Ascii,
    MinLength(u32),
    MaxLength(u32),
    Pattern(CHeapStr),
    Email,
    Range(u32, u32),
    Regex(CHeapStr),
    Enum(Vec<CHeapStr>),
}

pub enum OutputType {
    Ref(TypeId),
    Record(Record),
}

#[derive(bon::Builder)]
#[builder(on(String, into))]
#[builder(on(CHeapStr, into))]
pub struct ErrorType {
    #[builder(start_fn)]
    id: CHeapStr,
    #[builder(field)]
    variants: IndexMap<CHeapStr, ErrorVariant>,
}

impl<S: error_type_builder::State> ErrorTypeBuilder<S> {
    pub fn with_variant(mut self, name: impl Into<CHeapStr>, value: ErrorVariant) -> Self {
        self.variants.insert(name.into(), value);
        self
    }
    pub fn with_variants(
        mut self,
        variants: impl IntoIterator<Item = (impl Into<CHeapStr>, ErrorVariant)>,
    ) -> Self {
        for (name, value) in variants {
            self = self.with_variant(name, value);
        }
        self
    }
}

#[derive(bon::Builder)]
#[builder(on(String, into))]
pub struct ErrorVariant {
    #[builder(field)]
    fields: IndexMap<CHeapStr, ErrorField>,
    http_code: StatusCode,
    message: String,
    message_with_fields: Option<String>,
}

impl<S: error_variant_builder::State> ErrorVariantBuilder<S> {
    pub fn with_field(mut self, name: impl Into<CHeapStr>, value: ErrorField) -> Self {
        self.fields.insert(name.into(), value);
        self
    }
    pub fn with_fields(
        mut self,
        fields: impl IntoIterator<Item = (impl Into<CHeapStr>, ErrorField)>,
    ) -> Self {
        for (name, value) in fields {
            self = self.with_field(name, value);
        }
        self
    }
}

#[derive(bon::Builder)]
pub struct ErrorField {
    inner: RecordField,
    #[builder(default)]
    thiserror_from: bool,
}

impl ErrorVariant {
    const ERROR_INVALID_INPUT_NAME: &str = "invalidInput";
    const ERROR_INTERNAL_NAME: &str = "internal";
    pub fn invalid_input(reg: &TypeReg) -> (&'static str, Self) {
        (
            Self::ERROR_INVALID_INPUT_NAME,
            Self::builder()
                .http_code(StatusCode::BAD_REQUEST)
                .message("Invalid input")
                .with_field(
                    "issues",
                    ErrorField::builder()
                        .inner(RecordField::builder(reg.validation_errors()).build())
                        .thiserror_from(true)
                        .build(),
                )
                .build(),
        )
    }

    pub fn internal(reg: &TypeReg) -> (&'static str, Self) {
        (
            Self::ERROR_INTERNAL_NAME,
            Self::builder()
                .http_code(StatusCode::INTERNAL_SERVER_ERROR)
                .message("Internal server error")
                .with_field(
                    "message",
                    ErrorField::builder()
                        .inner(RecordField::builder(reg.string()).build())
                        .build(),
                )
                .build(),
        )
    }
}

fn http_status_code_name(code: StatusCode) -> &'static str {
    match code {
        StatusCode::BAD_REQUEST => "BAD_REQUEST",
        StatusCode::INTERNAL_SERVER_ERROR => "INTERNAL_SERVER_ERROR",
        StatusCode::NOT_FOUND => "NOT_FOUND",
        StatusCode::UNAUTHORIZED => "UNAUTHORIZED",
        StatusCode::FORBIDDEN => "FORBIDDEN",
        StatusCode::CONFLICT => "CONFLICT",
        StatusCode::UNPROCESSABLE_ENTITY => "UNPROCESSABLE_ENTITY",
        StatusCode::OK => "OK",
        StatusCode::CREATED => "CREATED",
        StatusCode::NO_CONTENT => "NO_CONTENT",
        StatusCode::MOVED_PERMANENTLY => "MOVED_PERMANENTLY",
        StatusCode::FOUND => "FOUND",
        StatusCode::SEE_OTHER => "SEE_OTHER",
        StatusCode::TEMPORARY_REDIRECT => "TEMPORARY_REDIRECT",
        StatusCode::PERMANENT_REDIRECT => "PERMANENT_REDIRECT",
        StatusCode::NOT_IMPLEMENTED => "NOT_IMPLEMENTED",
        StatusCode::SERVICE_UNAVAILABLE => "SERVICE_UNAVAILABLE",
        StatusCode::GATEWAY_TIMEOUT => "GATEWAY_TIMEOUT",
        StatusCode::HTTP_VERSION_NOT_SUPPORTED => "HTTP_VERSION_NOT_SUPPORTED",
        StatusCode::VARIANT_ALSO_NEGOTIATES => "VARIANT_ALSO_NEGOTIATES",
        _ => panic!("unsupported status code: {code}"),
    }
}
