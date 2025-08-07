use super::*;

pub fn endpoint_interface(reg: &TypeReg, buf: &mut impl Write, endpoint: &EndpointType) -> Res<()> {
    error_type(&endpoint.error, reg, buf)?;
    Ok(())
}

fn error_type(this: &ErrorType, reg: &TypeReg, buf: &mut impl Write) -> Res<()> {
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
        if fields.is_empty() {
            continue;
        }
        writeln!(buf)?;
        write!(
            buf,
            r#"/// {message}
/// http error code: {code_name}
record error-{name} {{
"#,
            name = AsKebabCase(&name[..]),
            code_name = http_status_code_name(*http_code),
        )?;
        for (
            field_name,
            ErrorField {
                inner,
                thiserror_from: _,
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
            record_field(inner, reg, buf, &field_name[..])?;
            writeln!(buf, ",")?;
        }
        writeln!(buf, "}}")?;
    }
    writeln!(buf)?;
    writeln!(buf, r#"variant error {{"#)?;
    for (name, ErrorVariant { fields, .. }) in &this.variants {
        write!(buf, r#"    {name}"#, name = AsKebabCase(&name[..]),)?;
        if fields.is_empty() {
            writeln!(buf, ",")?;
            continue;
        } else {
            writeln!(buf, "(error-{name}),", name = AsKebabCase(&name[..]))?;
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
) -> std::fmt::Result {
    write!(
        buf,
        "{field_name}: {ty_name}",
        field_name = heck::AsSnekCase(name),
        ty_name = reg.wit_name(this.ty).expect("unregistered field type"),
    )
}
