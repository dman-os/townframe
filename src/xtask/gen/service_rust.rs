use super::*;

use std::fmt::Write;

impl TypeReg {
    pub fn rust_name(&self, ty: TypeId) -> Option<CHeapStr> {
        Some(match self.types.get(&ty)?.value() {
            Type::Record(record) => record.name.to_pascal_case().into(),
            Type::Primitives(Primitives::String) => "String".into(),
            Type::Primitives(Primitives::U64) => "u64".into(),
            Type::Primitives(Primitives::F64) => "f64".into(),
            Type::Primitives(Primitives::Bool) => "bool".into(),
            Type::Primitives(Primitives::Uuid) => "String".into(),
            // Type::Primitives(Primitives::Uuid) => "Uuid".into(),
            Type::Primitives(Primitives::DateTime) => "Datetime".into(),
            Type::Primitives(Primitives::Json) => "serde_json::Value".into(),
            Type::List(ty) => format!(
                "Vec<{}>",
                self.rust_name(*ty).expect("unregistered inner type")
            )
            .into(),
            Type::Map(key, value) => format!(
                "HashMap<{}, {}>",
                self.rust_name(*key).expect("unregistered key type"),
                self.rust_name(*value).expect("unregistered value type")
            )
            .into(),
            Type::Option(ty) => format!(
                "Option<{}>",
                self.rust_name(*ty).expect("unregistered inner type")
            )
            .into(),
            Type::Tuple(items) => {
                let joined = items
                    .into_iter()
                    .map(|id| self.rust_name(*id).expect("unregistered inner type"))
                    .fold(")".to_string(), |acc, curr| format!("{acc},{curr}"));
                format!("({joined}").into()
            }
            Type::Alias(alias, _) => alias.to_pascal_case().into(),
        })
    }
}

type ExportedTypes = Arc<DHashMap<String, String>>;

struct ExportedTypesAppender {
    into: ExportedTypes,
    wit_prefix: Option<String>,
    rust_prefix: Option<String>,
}

impl ExportedTypesAppender {
    fn append(&self, wit: impl std::fmt::Display, rust: impl std::fmt::Display) {
        self.into.insert(
            if let Some(prefix) = &self.wit_prefix {
                format!("{prefix}/{wit}")
            } else {
                format!("{wit}")
            },
            if let Some(prefix) = &self.rust_prefix {
                format!("{prefix}::{rust}")
            } else {
                format!("{rust}")
            },
        );
    }

    fn with_prefix(
        &self,
        wit: impl std::fmt::Display,
        rust: impl std::fmt::Display,
    ) -> ExportedTypesAppender {
        Self {
            into: self.into.clone(),
            wit_prefix: Some(if let Some(prefix) = &self.wit_prefix {
                format!("{prefix}/{wit}")
            } else {
                format!("{wit}")
            }),
            rust_prefix: Some(if let Some(prefix) = &self.rust_prefix {
                format!("{prefix}::{rust}")
            } else {
                format!("{rust}")
            }),
        }
    }
}

pub fn feature_module(
    reg: &TypeReg,
    buf: &mut impl Write,
    Feature {
        tag,
        schema_types,
        endpoints,
    }: &Feature,
) -> Res<()> {
    writeln!(
        buf,
        "pub mod {module_name} {{",
        module_name = AsSnekCase(&tag.name[..])
    )?;
    {
        let exp_root = ExportedTypes::default();

        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        writeln!(
            buf,
            r#"use super::*;

pub const TAG: api::Tag = api::Tag {{
    name: "{tag_name}",
    desc: "{tag_desc}",
}};"#,
            tag_name = tag.name,
            tag_desc = tag.desc,
        )?;

        {
            let mut exp = ExportedTypesAppender {
                into: exp_root.clone(),
                wit_prefix: Some(tag.name.to_kebab_case()),
                rust_prefix: Some(tag.name.to_snek_case()),
            };
            for id in schema_types {
                writeln!(buf)?;
                schema_type(reg, buf, &mut exp, *id)?;
            }
        }
        /*{
                writeln!(
                    buf,
                    r#"pub fn router() -> axum::Router<SharedContext> {{
        axum::Router::new()"#
                )?;
                for epoint in endpoints {
                    writeln!(
                        buf,
                        "    .merge(EndpointWrapper::new({module}::{epoint}))",
                        module = heck::AsSnekCase(&epoint.id[..]),
                        epoint = heck::AsPascalCase(&epoint.id[..])
                    )?;
                }
                writeln!(buf, "}}")?;
            }*/
        {
            let mut exp = ExportedTypesAppender {
                into: exp_root.clone(),
                wit_prefix: default(),
                rust_prefix: Some(tag.name.to_snek_case()),
            };
            for epoint in endpoints {
                writeln!(buf)?;
                endpoint_module(reg, buf, &mut exp, epoint)?;
            }
        }
        writeln!(buf)?;
        writeln!(
            buf,
            "pub mod wit {{
    wit_bindgen::generate!({{"
        )?;
        {
            let mut out = indenter::indented(buf).with_str("        ");
            let buf = &mut out;
            writeln!(
                buf,
                r#"world: "feat-{world}",
async: true,
additional_derives: [serde::Serialize, serde::Deserialize],
with: {{
    "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
    "townframe:api-utils/utils": api_utils_rs::wit::utils,"#,
                world = AsKebabCase(&tag.name[..])
            )?;
            {
                let mut out = indenter::indented(buf).with_str("    ");
                let buf = &mut out;
                for (wit_path, rust_path) in Arc::try_unwrap(exp_root).expect("arc was held") {
                    writeln!(
                        buf,
                        "\"townframe:btress-api/{wit_path}\": crate::gen::{rust_path},",
                    )?;
                }
            }
            writeln!(buf, "}}")?;
        }
        writeln!(
            buf,
            r#"    }});
}}"#,
        )?;
    }
    writeln!(buf, "}}")?;
    Ok(())
}

fn endpoint_module(
    reg: &TypeReg,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    epoint: &EndpointType,
) -> Res<()> {
    writeln!(
        buf,
        "pub mod {module_name} {{",
        module_name = AsSnekCase(&epoint.id[..])
    )?;
    {
        let mut exp = exp.with_prefix(AsKebabCase(&epoint.id[..]), AsSnekCase(&epoint.id[..]));

        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        writeln!(buf, "use super::*;")?;
        writeln!(buf)?;
        writeln!(
            buf,
            r#"#[derive(Debug, Clone)]
pub struct {id};"#,
            id = AsPascalCase(&epoint.id[..]),
        )?;
        writeln!(buf)?;
        output_type(reg, buf, &mut exp, &epoint.output)?;
        writeln!(buf)?;
        input_type(reg, buf, &mut exp, &epoint.input)?;
        writeln!(buf)?;
        error_type(reg, buf, &mut exp, &epoint.error)?;
        /*
                // http_impl(&epoint, reg, buf)?;
                writeln!(
                    buf,
                    r#"impl HttpEndpoint for {id} {{
            const SUCCESS_CODE: StatusCode = StatusCode::{success_code};
            const METHOD: Method = Method::{method};
            const PATH: &'static str = "{path}";

            type SharedCx = SharedContext;"#,
                    id = AsPascalCase(&epoint.id[..]),
                    success_code = http_status_code_name(epoint.success),
                    method = AsPascalCase(epoint.method.as_str()),
                    path = epoint.path
                )?;
                fn http_input_source_type(source: &InputFieldSource) -> &str {
                    match source {
                        InputFieldSource::JsonBody => "Json",
                        InputFieldSource::Query => "Query",
                    }
                }
                let mut discard_body = epoint.input.main_source != InputFieldSource::JsonBody;
                let mut http_input_types = vec![format!(
                    "{wrapper}<Request>",
                    wrapper = http_input_source_type(&epoint.input.main_source)
                )];
                let mut http_input_destructure = vec![
                    (format!(
                        "{wrapper}(req)",
                        wrapper = http_input_source_type(&epoint.input.main_source)
                    )),
                ];
                for (name, field) in &epoint.input.fields {
                    if field.source == epoint.input.main_source {
                        continue;
                    }
                    discard_body = discard_body && field.source != InputFieldSource::JsonBody;
                    let wrapper = http_input_source_type(&field.source);
                    http_input_types.push(format!("{wrapper}<{inner}>"));
                    http_input_destructure.push(format!("{wrapper}({name})"));
                }
                if discard_body {
                    http_input_types.push("DiscardBody");
                    http_input_destructure.push("_");
                }
                let body_type = http_input_types.join(", ");
                let desctructure_type = http_input_destructure.join(", ");
                writeln!(
                    buf,
                    r#"type HttpRequest = ({body_type},);

            fn request({http_input_destructure},): Self::HttpRequest) -> Result<Self::Request, Self::Error> {{
                Ok(req)
            }}

            fn response(resp: Self::Response) -> HttpResponse {{
                Json(resp).into_response()
            }}
        }}"#,
                )?; */
    }
    writeln!(buf, "}}")?;
    Ok(())
}

// pub fn http_impl(reg: &TypeReg, buf: &mut impl Write, epoint: &EndpointType) -> Res<()> {}

fn schema_type(
    reg: &TypeReg,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    id: TypeId,
) -> Res<()> {
    let borrow = reg.types.get(&id).unwrap();
    match borrow.value() {
        Type::Record(record) => schema_record(&reg, buf, exp, record)?,
        ty => eyre::bail!("found unsupported schema type: {ty:?}"),
    };
    Ok(())
}

fn schema_record(
    reg: &TypeReg,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    this: &Record,
) -> Res<()> {
    exp.append(
        AsKebabCase(&this.name[..]).to_string(),
        AsPascalCase(&this.name[..]).to_string(),
    );
    write!(
        buf,
        r#"#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct {name} {{
"#,
        name = heck::AsPascalCase(&this.name[..]),
    )?;
    for (field_name, field) in &this.fields {
        record_field(
            &field,
            reg,
            &mut indenter::indented(buf).with_str("    "),
            &field_name,
            true,
        )?;
        writeln!(buf, ",")?;
    }
    writeln!(buf, "}}")?;
    Ok(())
}

fn output_type(
    reg: &TypeReg,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    this: &OutputType,
) -> Res<()> {
    let rust_name: String = match this {
        OutputType::Ref(ty_id) => reg
            .rust_name(*ty_id)
            .expect("unregistered field type")
            .into(),
        OutputType::Record(record) => {
            exp.append("output".to_string(), "Output".to_string());
            schema_record(reg, buf, exp, record)?;
            if record.name.eq_ignore_ascii_case("Output") {
                return Ok(());
            }
            record.name.to_pascal_case()
        }
    };
    writeln!(buf, "pub type Output = SchemaRef<{rust_name}>;",)?;
    Ok(())
}

fn input_type(
    reg: &TypeReg,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    this: &InputType,
) -> Res<()> {
    exp.append("input".to_string(), "Input".to_string());
    writeln!(
        buf,
        r#"#[derive(Debug, Clone, Serialize, Deserialize, garde::Validate, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Input {{"#
    )?;

    {
        let mut out = &mut indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        for (field_name, field) in &this.fields {
            // Add field documentation if description exists
            if let Some(desc) = &field.inner.desc {
                writeln!(buf, "/// {}", desc)?;
            }

            // Add utoipa schema attributes for validations
            let mut schema_attrs = Vec::new();
            for validation in &field.validations {
                match validation {
                    FieldValidations::MinLength(len) => {
                        schema_attrs.push(format!("min_length = {len}"))
                    }
                    FieldValidations::MaxLength(len) => {
                        schema_attrs.push(format!("max_length = {len}"))
                    }
                    FieldValidations::Pattern(pattern) => {
                        schema_attrs.push(format!(r#"pattern = "{}""#, pattern.as_str()))
                    }
                    _ => {} // Other validations don't have direct utoipa schema equivalents
                }
            }

            if !schema_attrs.is_empty() {
                writeln!(buf, "#[schema({})]", schema_attrs.join(", "))?;
            }

            // Add garde validation attributes
            let mut garde_attrs = Vec::new();
            let mut length_validations = (None, None);
            for validation in &field.validations {
                match validation {
                    FieldValidations::Ascii => garde_attrs.push("ascii".to_string()),
                    FieldValidations::Email => garde_attrs.push("email".to_string()),
                    FieldValidations::MinLength(len) => {
                        if length_validations.0.is_some() {
                            eyre::bail!(
                                "duplicate min length validations: {len} && {length_validations:?}"
                            )
                        }
                        length_validations.0 = Some(len);
                    }
                    FieldValidations::MaxLength(len) => {
                        if length_validations.1.is_some() {
                            eyre::bail!(
                                "duplicate max length validations: {len} && {length_validations:?}"
                            )
                        }
                        length_validations.1 = Some(len);
                    }
                    FieldValidations::Pattern(pattern) => {
                        garde_attrs.push(format!(r#"pattern({})"#, pattern.as_str()))
                    }
                    _ => {} // Handle other validation types as needed
                }
            }
            match length_validations {
                (None, None) => {}
                (None, Some(max)) => garde_attrs.push(format!("length(max = {max})")),
                (Some(min), None) => garde_attrs.push(format!("length(min = {min})")),
                (Some(min), Some(max)) => {
                    garde_attrs.push(format!("length(min = {min}, max = {max})"))
                }
            }

            if !garde_attrs.is_empty() {
                writeln!(buf, "#[garde({})]", garde_attrs.join(", "))?;
            }
            if field.source != this.main_source {
                writeln!(buf, "#[serde(skip)]")?;
            }

            // Write the field definition
            record_field(&field.inner, reg, buf, &field_name, true)?;
            writeln!(buf, ",")?;
        }
    }

    writeln!(buf, "}}")?;
    Ok(())
}

fn error_type(
    reg: &TypeReg,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    this: &ErrorType,
) -> Res<()> {
    for (
        name,
        ErrorVariant {
            fields,
            http_code: _,
            message,
            message_with_fields,
        },
    ) in &this.variants
    {
        if fields.is_empty() {
            continue;
        }
        if &name[..] == ErrorVariant::ERROR_INTERNAL_NAME
            || &name[..] == ErrorVariant::ERROR_INVALID_INPUT_NAME
        {
            continue;
        }
        let message_with_fields = message_with_fields.as_deref().unwrap_or_default();
        exp.append(
            format!("error-{}", AsKebabCase(&name[..])),
            format!("Error{}", AsPascalCase(&name[..])),
        );
        writeln!(
            buf,
            r#"#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error, displaydoc::Display, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", tag = "error")]
/// {message}{message_with_fields}
pub struct Error{name} {{"#,
            name = AsPascalCase(&name[..])
        )?;
        {
            let mut out = &mut indenter::indented(buf).with_str("    ");
            let buf = &mut out;
            for (
                field_name,
                ErrorField {
                    inner,
                    thiserror_from,
                },
            ) in fields
            {
                if let Some(desc) = &inner.desc {
                    writeln!(buf, "/// {desc}",)?;
                }
                if let Some(example) = &inner.example {
                    writeln!(buf, "/// example: {example}",)?;
                }
                if *thiserror_from {
                    writeln!(buf, "#[from]")?;
                }
                record_field(inner, reg, buf, &field_name, true)?;
                writeln!(buf, ",")?;
            }
        }
        writeln!(buf, "}}")?;
    }
    exp.append("error".to_string(), "Error".to_string());
    writeln!(
        buf,
        r#"#[derive(
    Debug,
    Serialize,
    thiserror::Error,
    displaydoc::Display,
    macros::HttpError,
    utoipa::ToSchema,
)]
#[serde(rename_all = "camelCase", tag = "error")]
pub enum Error {{"#
    )?;
    for (
        name,
        ErrorVariant {
            fields,
            http_code,
            message,
            message_with_fields: _,
        },
    ) in &this.variants
    {
        let mut out = &mut indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        write!(
            buf,
            r#"/// {message} {{0}}
#[http(code(StatusCode::{code_name}), desc("{message}"))]
{name}"#,
            name = AsPascalCase(&name[..]),
            code_name = http_status_code_name(*http_code),
        )?;
        if &name[..] == ErrorVariant::ERROR_INTERNAL_NAME {
            writeln!(buf, "(#[from] ErrorInternal),")?;
        } else if &name[..] == ErrorVariant::ERROR_INVALID_INPUT_NAME {
            writeln!(buf, "(#[from] ErrorsValidation),",)?;
        } else if fields.is_empty() {
            writeln!(buf, ",")?;
        } else {
            writeln!(
                buf,
                "(#[from] Error{name}),",
                name = AsPascalCase(&name[..])
            )?;
        }
    }
    writeln!(buf, "}}")?;
    Ok(())
}

fn record_field(
    this: &RecordField,
    reg: &TypeReg,
    buf: &mut impl Write,
    name: &str,
    pub_visibility: bool,
) -> std::fmt::Result {
    match reg
        .types
        .get(&this.ty)
        .as_deref()
        .expect("unregistered field type")
    {
        Type::Primitives(Primitives::DateTime) => {
            // writeln!(
            //     buf,
            //     r#"#[serde(with = "api_utils_rs::codecs::sane_iso8601")]"#
            // )?;
        }
        _ => {}
    }
    write!(
        buf,
        "{vis}{field_name}: {ty_name}",
        vis = if pub_visibility { "pub " } else { "" },
        field_name = heck::AsSnekCase(name),
        ty_name = reg.rust_name(this.ty).expect("unregistered field type"),
    )
}
