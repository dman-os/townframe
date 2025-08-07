use super::*;

use std::fmt::Write;

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
        "mod {module_name} {{",
        module_name = AsSnekCase(tag.name)
    )?;
    {
        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        write!(
            buf,
            r#"
use super::*;   

pub const TAG: api::Tag = api::Tag {{
    name: "{tag_name}",
    desc: "{tag_desc}",
}};
"#,
            tag_name = tag.name,
            tag_desc = tag.desc,
        )?;
        for id in schema_types {
            writeln!(buf)?;
            schema_type(reg, buf, *id)?;
        }
        writeln!(buf)?;
        {
            writeln!(
                buf,
                r#"pub fn router() -> axum::Router<SharedContext> {{
    axum::Router::new()"#
            )?;
            for epoint in endpoints {
                writeln!(
                    buf,
                    "    .merge(EndpointWrapper::new({}))",
                    heck::AsPascalCase(&epoint.id[..])
                )?;
            }
            writeln!(buf, "}}")?;
        }
        writeln!(buf)?;
        for epoint in endpoints {
            handler_rust::endpoint_module(reg, buf, epoint)?;
        }
        writeln!(buf)?;
    }
    writeln!(buf, "}}")?;
    Ok(())
}

pub fn endpoint_module(reg: &TypeReg, buf: &mut impl Write, epoint: &EndpointType) -> Res<()> {
    writeln!(
        buf,
        "mod {module_name} {{",
        module_name = AsSnekCase(&epoint.id[..])
    )?;
    {
        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        writeln!(buf, "use super::*;")?;
        writeln!(buf)?;
        write!(
            buf,
            r#"
#[derive(Debug, Clone)]
pub struct {id};
"#,
            id = AsPascalCase(&epoint.id[..]),
        )?;

        input_type(&epoint.input, reg, buf)?;
        writeln!(buf)?;
        error_type(&epoint.error, reg, buf)?;
    }
    writeln!(buf, "}}")?;
    Ok(())
}

pub fn schema_type(reg: &TypeReg, buf: &mut impl Write, id: TypeId) -> Res<()> {
    let borrow = reg.types.get(&id).unwrap();
    match borrow.value() {
        Type::Record(record) => schema_record(record, &reg, buf)?,
        ty => eyre::bail!("found unsupported schema type: {ty:?}"),
    };
    Ok(())
}

fn schema_record(this: &Record, reg: &TypeReg, buf: &mut impl Write) -> Res<()> {
    write!(
        buf,
        r#"
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
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

fn input_type(this: &InputType, reg: &TypeReg, buf: &mut impl Write) -> Res<()> {
    write!(
        buf,
        r#"
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Request {{
"#
    )?;

    for (field_name, field) in &this.fields {
        // Add field documentation if description exists
        if let Some(desc) = &field.inner.desc {
            writeln!(buf, "    /// {}", desc)?;
        }

        // Add utoipa schema attributes for validations
        let mut schema_attrs = Vec::new();
        for validation in &field.validations {
            match validation {
                FieldValidations::MinLength(len) => {
                    schema_attrs.push(format!("min_length = {}", len))
                }
                FieldValidations::MaxLength(len) => {
                    schema_attrs.push(format!("max_length = {}", len))
                }
                FieldValidations::Pattern(pattern) => {
                    schema_attrs.push(format!(r#"pattern = "{}""#, pattern.as_str()))
                }
                _ => {} // Other validations don't have direct utoipa schema equivalents
            }
        }

        if !schema_attrs.is_empty() {
            writeln!(buf, "    #[schema({})]", schema_attrs.join(", "))?;
        }

        // Add garde validation attributes
        let mut garde_attrs = Vec::new();
        for validation in &field.validations {
            match validation {
                FieldValidations::Ascii => garde_attrs.push("ascii".to_string()),
                FieldValidations::Email => garde_attrs.push("email".to_string()),
                FieldValidations::MinLength(len) => {
                    garde_attrs.push(format!("length(min = {})", len))
                }
                FieldValidations::MaxLength(len) => {
                    garde_attrs.push(format!("length(max = {})", len))
                }
                FieldValidations::Length(min, max) => {
                    garde_attrs.push(format!("length(min = {}, max = {})", min, max))
                }
                FieldValidations::Pattern(pattern) => {
                    garde_attrs.push(format!(r#"pattern("{}")"#, pattern.as_str()))
                }
                _ => {} // Handle other validation types as needed
            }
        }

        if !garde_attrs.is_empty() {
            writeln!(buf, "    #[garde({})]", garde_attrs.join(", "))?;
        }

        // Write the field definition
        record_field(
            &field.inner,
            reg,
            &mut indenter::indented(buf).with_str("    "),
            &field_name,
            true,
        )?;
        writeln!(buf, ", ")?;
    }

    writeln!(buf, "}}")?;
    Ok(())
}

fn error_type(this: &ErrorType, reg: &TypeReg, buf: &mut impl Write) -> Res<()> {
    write!(
        buf,
        r#"
#[derive(
    Debug, Serialize, thiserror::Error, displaydoc::Display, macros::HttpError, utoipa::ToSchema,
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
            message_with_fields,
        },
    ) in &this.variants
    {
        let mut out = &mut indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        let message_with_fields = message_with_fields.as_deref().unwrap_or_default();
        write!(
            buf,
            r#"    
/// {message}{message_with_fields}
#[http(code(StatusCode::{code_name}), desc("{message}"))]
{name}"#,
            name = AsPascalCase(&name[..]),
            code_name = http_status_code_name(*http_code),
        )?;
        if fields.is_empty() {
            writeln!(buf, ",")?;
            continue;
        }
        writeln!(buf, " {{ ")?;
        for (
            field_name,
            ErrorField {
                inner,
                thiserror_from,
            },
        ) in fields
        {
            let mut out = &mut indenter::indented(buf).with_str("    ");
            let buf = &mut out;
            if let Some(desc) = &inner.desc {
                writeln!(buf, "/// {desc}",)?;
            }
            if let Some(example) = &inner.example {
                writeln!(buf, "/// example: {example}",)?;
            }
            if *thiserror_from {
                writeln!(buf, "#[from]")?;
            }
            record_field(inner, reg, buf, &field_name, false)?;
            writeln!(buf, ",")?;
        }
        write!(buf, "}},")?;
    }
    writeln!(
        buf,
        "
}}"
    )?;
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
            writeln!(
                buf,
                r#"#[serde(with = "api_utils_rs::codecs::sane_iso8601")]"#
            )?;
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
