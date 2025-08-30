use super::*;

impl TypeReg {
    pub fn wit_name(&self, ty: TypeId) -> Option<CHeapStr> {
        Some(match self.types.get(&ty)?.value() {
            Type::Record(record) => record.name.to_kebab_case().into(),
            Type::Primitives(Primitives::String) => "string".into(),
            Type::Primitives(Primitives::U64) => "u64".into(),
            Type::Primitives(Primitives::F64) => "f64".into(),
            Type::Primitives(Primitives::Bool) => "bool".into(),
            Type::Primitives(Primitives::Uuid) => "uuid".into(),
            Type::Primitives(Primitives::DateTime) => "datetime".into(),
            Type::Primitives(Primitives::Json) => "string".into(),
            Type::List(ty) => format!(
                "list<{}>",
                self.wit_name(*ty).expect("unregistered inner type")
            )
            .into(),
            Type::Map(key, value) => format!(
                "list<tuple<{}, {}>>",
                self.wit_name(*key).expect("unregistered key type"),
                self.wit_name(*value).expect("unregistered value type")
            )
            .into(),
            Type::Option(ty) => format!(
                "option<{}>",
                self.wit_name(*ty).expect("unregistered inner type")
            )
            .into(),
            Type::Tuple(items) => {
                let joined = items
                    .into_iter()
                    .map(|id| self.rust_name(*id).expect("unregistered inner type"))
                    .fold(">".to_string(), |acc, curr| format!("{acc},{curr}"));
                format!("tuple<{joined}").into()
            }
            Type::Alias(alias, _) => alias.to_kebab_case().into(),
        })
    }
}

pub(crate) fn feature_file(
    reg: &TypeReg,
    buf: &mut impl Write,
    Feature {
        tag,
        schema_types,
        endpoints,
    }: &Feature,
) -> Res<()> {
    let mut imports = vec![
        "townframe:api-utils/utils.{errors-validation}".to_string(),
        "townframe:api-utils/utils.{error-internal}".to_string(),
        "townframe:api-utils/utils.{uuid}".to_string(),
        "townframe:api-utils/utils.{datetime}".to_string(),
    ];
    writeln!(
        buf,
        "interface {name} {{",
        name = AsKebabCase(&tag.name[..])
    )?;
    {
        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        for import in &imports {
            writeln!(buf, "use {import};")?;
        }
        // schema types go here
        for id in schema_types {
            writeln!(buf)?;
            schema_type(reg, buf, *id)?;
            imports.push(format!(
                "{interface}.{{{ty}}}",
                interface = AsKebabCase(&tag.name[..]),
                ty = reg.wit_name(*id).expect("unregistered schema type")
            ));
        }
    }
    writeln!(buf, "}}")?;
    for epoint in endpoints {
        endpoint_interface(reg, buf, epoint, &imports)?;
    }
    writeln!(
        buf,
        "world feat-{name} {{",
        name = AsKebabCase(&tag.name[..])
    )?;
    {
        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        // writeln!(buf, "export {name};", name = AsKebabCase(&tag.name[..]))?;
        for epoint in endpoints {
            writeln!(buf, "export {name};", name = AsKebabCase(&epoint.id[..]))?;
        }
    }
    writeln!(buf, "}}")?;
    Ok(())
}

pub fn endpoint_interface(
    reg: &TypeReg,
    buf: &mut impl Write,
    endpoint: &EndpointType,
    imports: &[String],
) -> Res<()> {
    writeln!(
        buf,
        "interface {name} {{",
        name = AsKebabCase(&endpoint.id[..])
    )?;
    {
        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        for import in imports {
            writeln!(buf, "use {import};")?;
        }
        error_type(&endpoint.error, reg, buf)?;
        writeln!(buf)?;
        input_type(&endpoint.input, reg, buf)?;
        output_type(&endpoint.output, reg, buf)?;
        writeln!(
            buf,
            r#"resource service {{
    serve: func(inp: input) -> result<output, error>;
}}"#
        )?;
        // writeln!(buf, "call: func(inp: input) -> result<output, error>;")?;
    }
    writeln!(buf, "}}")?;
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
        if &name[..] == ErrorVariant::ERROR_INTERNAL_NAME
            || &name[..] == ErrorVariant::ERROR_INVALID_INPUT_NAME
        {
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
        if &name[..] == ErrorVariant::ERROR_INTERNAL_NAME {
            writeln!(buf, "(error-internal),")?;
        } else if &name[..] == ErrorVariant::ERROR_INVALID_INPUT_NAME {
            writeln!(buf, "(errors-validation),")?;
        } else if fields.is_empty() {
            writeln!(buf, ",")?;
        } else {
            writeln!(buf, "(error-{name}),", name = AsKebabCase(&name[..]))?;
        }
    }
    writeln!(buf, "}}")?;
    Ok(())
}

fn output_type(this: &OutputType, reg: &TypeReg, buf: &mut impl Write) -> Res<()> {
    match this {
        OutputType::Ref(ty_id) => {
            let wit_name = reg.wit_name(*ty_id).expect("unregistered field type");
            writeln!(buf, "type output = {wit_name};")?;
        }
        OutputType::Record(record) => {
            writeln!(buf, "record output {{")?;
            {
                let mut out = indenter::indented(buf).with_str("    ");
                let buf = &mut out;
                for (field_name, field) in &record.fields {
                    record_field(field, reg, buf, &field_name[..])?;
                    writeln!(buf, ",")?;
                }
            }
            writeln!(buf, "}}")?;
        }
    }
    Ok(())
}

fn input_type(this: &InputType, reg: &TypeReg, buf: &mut impl Write) -> Res<()> {
    writeln!(buf, "record input {{")?;
    {
        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        for (field_name, field) in &this.fields {
            if field.source != this.main_source {
                continue;
            }
            if let Some(desc) = &field.inner.desc {
                writeln!(buf, "/// {desc}")?;
            }
            record_field(&field.inner, reg, buf, &field_name[..])?;
            writeln!(buf, ",")?;
        }
    }
    writeln!(buf, "}}")?;
    Ok(())
}

fn schema_type(reg: &TypeReg, buf: &mut impl Write, id: TypeId) -> Res<()> {
    let borrow = reg.types.get(&id).unwrap();
    match borrow.value() {
        Type::Record(record) => schema_record(record, reg, buf)?,
        ty => eyre::bail!("found unsupported schema type: {ty:?}"),
    };
    Ok(())
}

fn schema_record(this: &Record, reg: &TypeReg, buf: &mut impl Write) -> Res<()> {
    writeln!(
        buf,
        r#"record {name} {{"#,
        name = heck::AsKebabCase(&this.name[..]),
    )?;
    for (field_name, field) in &this.fields {
        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        if let Some(desc) = &field.desc {
            writeln!(buf, "/// {desc}")?;
        }
        record_field(field, reg, buf, &field_name[..])?;
        writeln!(buf, ",")?;
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
        field_name = heck::AsKebabCase(name),
        ty_name = reg.wit_name(this.ty).expect("unregistered field type"),
    )
}
