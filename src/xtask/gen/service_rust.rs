use super::*;

use std::fmt::Write;

pub struct RustGenCtx<'a> {
    pub reg: &'a TypeReg,
    pub attrs: RustAttrs,
}

impl RustGenCtx<'_> {
    pub fn rust_name(&self, ty: TypeId) -> Option<CHeapStr> {
        Some(match self.reg.types.get(&ty)?.value() {
            Type::Record(record) => record.name.to_pascal_case().into(),
            Type::Enum(r#enum) => r#enum.name.to_pascal_case().into(),
            Type::Variant(variant) => variant.name.to_pascal_case().into(),
            Type::Primitives(Primitives::String) => "String".into(),
            Type::Primitives(Primitives::U64) => "u64".into(),
            Type::Primitives(Primitives::F64) => "f64".into(),
            Type::Primitives(Primitives::Bool) => "bool".into(),
            Type::Primitives(Primitives::Uuid) => {
                if self.attrs.automerge {
                    "Uuid".into()
                } else {
                    "String".into()
                }
            }
            Type::Primitives(Primitives::DateTime) => {
                if self.attrs.automerge {
                    "OffsetDateTime".into()
                } else {
                    "Datetime".into()
                }
            }
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
                    .iter()
                    .map(|id| self.rust_name(*id).expect("unregistered inner type"))
                    .fold(")".to_string(), |acc, curr| format!("{acc},{curr}"));
                format!("({joined}").into()
            }
            Type::Alias(alias) => alias.name.to_pascal_case().into(),
        })
    }
}

// we use a arced dhasmap since we're cloning this multiple times
// in with_prefix
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
    cx: &RustGenCtx,
    buf: &mut impl Write,
    Feature {
        tag,
        schema_types,
        endpoints,
        wit_module,
    }: &Feature,
) -> Res<ExportedTypes> {
    let exp_root = ExportedTypes::default();
    writeln!(
        buf,
        "pub mod {module_name} {{",
        module_name = AsSnekCase(&tag.name[..])
    )?;
    {
        let mut out = indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        writeln!(buf, "use super::*;")?;
        if cx.attrs.utoipa {
            writeln!(
                buf,
                r#"
pub const TAG: api::Tag = api::Tag {{
    name: "{tag_name}",
    desc: "{tag_desc}",
}};"#,
                tag_name = tag.name,
                tag_desc = tag.desc,
            )?;
        }

        {
            let mut exp = ExportedTypesAppender {
                into: exp_root.clone(),
                wit_prefix: Some(format!(
                    "{wit_module}/{tag}",
                    tag = tag.name.to_kebab_case()
                )),
                rust_prefix: Some(tag.name.to_snek_case()),
            };
            for id in schema_types {
                writeln!(buf)?;
                schema_type(cx, buf, &mut exp, *id)?;
            }
        }
        {
            let mut exp = ExportedTypesAppender {
                into: exp_root.clone(),
                wit_prefix: Some(wit_module.clone()),
                rust_prefix: Some(tag.name.to_snek_case()),
            };
            for epoint in endpoints {
                writeln!(buf)?;
                endpoint_module(cx, buf, &mut exp, epoint)?;
            }
        }
        writeln!(buf)?;
    }
    writeln!(buf, "}}")?;
    Ok(exp_root)
}

pub fn _wit_bindgen_module(
    buf: &mut impl Write,
    Feature { tag, .. }: &Feature,
    rust_root_module: &str,
    exp_root: ExportedTypes,
) -> Res<()> {
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
                writeln!(buf, "\"{wit_path}\": {rust_root_module}::{rust_path},",)?;
            }
        }
        writeln!(buf, "}}")?;
    }
    writeln!(
        buf,
        r#"    }});
}}"#,
    )?;
    Ok(())
}

fn endpoint_module(
    cx: &RustGenCtx,
    buf: &mut impl Write,
    exp: &mut ExportedTypesAppender,
    epoint: &EndpointType,
) -> Res<()> {
    writeln!(
        buf,
        "pub mod {module_name} {{",
        module_name = AsSnekCase(&epoint.id[..])
    )?;
    {
        let exp = exp.with_prefix(AsKebabCase(&epoint.id[..]), AsSnekCase(&epoint.id[..]));

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
        output_type(cx, buf, &exp, &epoint.output)?;
        writeln!(buf)?;
        input_type(cx, buf, &exp, &epoint.input)?;
        writeln!(buf)?;
        error_type(cx, buf, &exp, &epoint.error)?;
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
    cx: &RustGenCtx,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    id: TypeId,
) -> Res<()> {
    let borrow = cx.reg.types.get(&id).unwrap();
    match borrow.value() {
        Type::Record(record) => schema_record(cx, buf, exp, record)?,
        Type::Enum(r#enum) => schema_enum(cx, buf, exp, r#enum)?,
        Type::Variant(variant) => schema_variant(cx, buf, exp, variant)?,
        Type::Alias(Alias { name, ty }) => writeln!(
            buf,
            "pub type {alias} = {other};",
            alias = AsPascalCase(&name[..]),
            other = cx.rust_name(*ty).expect("unregistered inner type")
        )?,
        ty => eyre::bail!("found unsupported schema type: {ty:?}"),
    };
    Ok(())
}

fn schema_record(
    cx: &RustGenCtx,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    this: &Record,
) -> Res<()> {
    exp.append(
        AsKebabCase(&this.name[..]).to_string(),
        AsPascalCase(&this.name[..]).to_string(),
    );

    // Build derives according to generation-time flags
    let mut derives: Vec<&str> = vec!["Debug", "Clone"];
    if cx.attrs.automerge {
        derives.push("Hydrate");
        derives.push("Reconcile");
    }
    if cx.attrs.patch {
        derives.push("Patch");
        derives.push("PartialEq");
    }
    if cx.attrs.utoipa {
        derives.push("utoipa::ToSchema");
    }
    if cx.attrs.serde {
        derives.push("Serialize");
        derives.push("Deserialize");
    }
    // only records get autosurgeon/automerge derives
    writeln!(buf, "#[derive({})]", derives.join(", "))?;
    if cx.attrs.uniffi {
        writeln!(
            buf,
            "#[cfg_attr(feature = \"uniffi\", derive(uniffi::Record))]"
        )?;
    }
    if cx.attrs.serde {
        writeln!(buf, "#[serde(rename_all = \"camelCase\")]")?;
    }
    if cx.attrs.patch {
        // emit patch attribute so generated Patch types derive the desired attributes for patches
        let mut patch_parts: Vec<&str> = vec!["Debug", "Default"];
        writeln!(
            buf,
            "#[patch(attribute(derive({})))]",
            patch_parts.join(", ")
        )?;
        if cx.attrs.uniffi {
            writeln!(
                buf,
                "#[cfg_attr(feature = \"uniffi\", patch(attribute(derive(uniffi::Record))))]"
            )?;
        }
    }
    writeln!(
        buf,
        "pub struct {name} {{",
        name = heck::AsPascalCase(&this.name[..])
    )?;
    for (field_name, field) in &this.fields {
        record_field(
            field,
            cx,
            &mut indenter::indented(buf).with_str("    "),
            field_name,
            true,
        )?;
        writeln!(buf, ",")?;
    }
    writeln!(buf, "}}")?;
    Ok(())
}

fn schema_enum(
    cx: &RustGenCtx,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    this: &Enum,
) -> Res<()> {
    exp.append(
        AsKebabCase(&this.name[..]).to_string(),
        AsPascalCase(&this.name[..]).to_string(),
    );

    // Emit derives according to ctx flags (generate attributes directly)
    let mut derives: Vec<&str> = vec!["Debug", "Clone"];
    if cx.attrs.automerge {
        derives.push("Hydrate");
        derives.push("Reconcile");
    }
    if cx.attrs.utoipa {
        derives.push("utoipa::ToSchema");
    }
    if cx.attrs.serde {
        derives.push("Serialize");
        derives.push("Deserialize");
    }
    if cx.attrs.patch {
        derives.push("PartialEq");
    }
    writeln!(buf, "#[derive({})]", derives.join(", "))?;
    if cx.attrs.uniffi {
        writeln!(
            buf,
            "#[cfg_attr(feature = \"uniffi\", derive(uniffi::Enum))]"
        )?;
    }
    if cx.attrs.serde {
        writeln!(buf, "#[serde(rename_all = \"camelCase\", untagged)]")?;
    }
    // For enums we keep serde derives only; autosurgeon derive is only emitted for records
    writeln!(
        buf,
        "pub enum {name} {{",
        name = heck::AsPascalCase(&this.name[..])
    )?;
    for (name, EnumVariant { desc }) in &this.variants {
        let mut out = &mut indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        if let Some(desc) = desc {
            writeln!(buf, "/// {desc}")?;
        }
        writeln!(buf, "{name},", name = AsPascalCase(&name[..]))?;
    }
    writeln!(buf, "}}")?;
    writeln!(
        buf,
        r#"impl {rust_name} {{
    pub unsafe fn _lift(val:u8) -> {rust_name} {{
        if !cfg!(debug_assertions){{
            return unsafe {{
                ::core::mem::transmute::<u8, {rust_name}>(val)
            }};
        }}
        match val {{
"#,
        rust_name = heck::AsPascalCase(&this.name[..])
    )?;
    for (ii, (name, _)) in this.variants.iter().enumerate() {
        writeln!(
            buf,
            "            {ii} => {enum_name}::{var_name},",
            enum_name = heck::AsPascalCase(&this.name[..]),
            var_name = heck::AsPascalCase(&name[..]),
        )?;
    }
    writeln!(
        buf,
        r#"
            _ => panic!("invalid enum discriminant"),
        }}
    }}
}}
"#,
    )?;
    Ok(())
}

fn schema_variant(
    cx: &RustGenCtx,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    this: &Variant,
) -> Res<()> {
    exp.append(
        AsKebabCase(&this.name[..]).to_string(),
        AsPascalCase(&this.name[..]).to_string(),
    );

    let mut derives: Vec<&str> = vec!["Debug", "Clone"];
    if cx.attrs.automerge {
        derives.push("Hydrate");
        derives.push("Reconcile");
    }
    if cx.attrs.utoipa {
        derives.push("utoipa::ToSchema");
    }
    if cx.attrs.serde {
        derives.push("Serialize");
        derives.push("Deserialize");
    }
    if cx.attrs.patch {
        derives.push("PartialEq");
    }
    writeln!(buf, "#[derive({})]", derives.join(", "))?;
    if cx.attrs.uniffi {
        writeln!(
            buf,
            "#[cfg_attr(feature = \"uniffi\", derive(uniffi::Enum))]"
        )?;
    }
    if cx.attrs.serde {
        writeln!(buf, "#[serde(rename_all = \"camelCase\", untagged)]")?;
    }
    // autosurgeon derives are only emitted for records — enums are intentionally left with serde only
    writeln!(
        buf,
        "pub enum {name} {{",
        name = heck::AsPascalCase(&this.name[..])
    )?;
    for (name, VariantVariant { ty, desc }) in &this.variants {
        let mut out = &mut indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        if let Some(desc) = desc {
            writeln!(buf, "/// {desc}")?;
        }
        write!(buf, "{name}", name = AsPascalCase(&name[..]))?;
        match ty {
            VariantVariantType::Unit => writeln!(buf, ",")?,
            VariantVariantType::Wrapped(ty) => writeln!(
                buf,
                "({ty_name}),",
                ty_name = cx.rust_name(*ty).expect("unregistered variant type")
            )?,
        }
    }
    writeln!(buf, "}}")?;
    Ok(())
}

fn output_type(
    cx: &RustGenCtx,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    this: &OutputType,
) -> Res<()> {
    let rust_name: String = match this {
        OutputType::Ref(ty_id) => cx
            .rust_name(*ty_id)
            .expect("unregistered field type")
            .into(),
        OutputType::Record(record) => {
            exp.append("output".to_string(), "Output".to_string());
            schema_record(cx, buf, exp, record)?;
            if record.name.eq_ignore_ascii_case("Output") {
                return Ok(());
            }
            record.name.to_pascal_case()
        }
    };
    if cx.attrs.utoipa {
        writeln!(buf, r#"pub type Output = SchemaRef<{rust_name}>;"#,)?;
    } else {
        writeln!(buf, r#"pub type Output = {rust_name};"#,)?;
    }
    Ok(())
}

fn input_type(
    cx: &RustGenCtx,
    buf: &mut impl Write,
    exp: &ExportedTypesAppender,
    this: &InputType,
) -> Res<()> {
    exp.append("input".to_string(), "Input".to_string());
    // Include garde::Validate derive only when garde emission is enabled
    let mut derives = vec!["Debug", "Clone"];
    if cx.attrs.automerge {
        derives.push("Hydrate");
        derives.push("Reconcile");
    }
    if cx.attrs.garde {
        derives.push("garde::Validate");
    }
    if cx.attrs.utoipa {
        derives.push("utoipa::ToSchema");
    }
    if cx.attrs.serde {
        derives.push("Serialize");
        derives.push("Deserialize");
    }
    writeln!(buf, "#[derive({})]", derives.join(", "))?;
    if cx.attrs.serde {
        writeln!(buf, "#[serde(rename_all = \"camelCase\")]")?;
    }
    writeln!(buf, "pub struct Input {{")?;

    {
        let mut out = &mut indenter::indented(buf).with_str("    ");
        let buf = &mut out;
        for (field_name, field) in &this.fields {
            // Add field documentation if description exists
            if let Some(desc) = &field.inner.desc {
                writeln!(buf, "/// {}", desc)?;
            }

            if cx.attrs.utoipa {
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

                if !schema_attrs.is_empty() && cx.attrs.utoipa {
                    writeln!(buf, "#[schema({})]", schema_attrs.join(", "))?;
                }
            }
            if cx.attrs.garde {
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
            }

            if cx.attrs.serde && field.source != this.main_source {
                writeln!(buf, "#[serde(skip)]")?;
            }

            // Write the field definition
            record_field(&field.inner, cx, buf, field_name, true)?;
            writeln!(buf, ",")?;
        }
    }

    writeln!(buf, "}}")?;
    Ok(())
}

fn error_type(
    cx: &RustGenCtx,
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
        // Build derives for error struct
        let mut err_derives = vec!["Debug", "Clone", "thiserror::Error", "displaydoc::Display"];
        if cx.attrs.utoipa {
            err_derives.push("utoipa::ToSchema");
        }
        if cx.attrs.serde {
            err_derives.push("Serialize");
            err_derives.push("Deserialize");
        }
        if cx.attrs.automerge {
            err_derives.push("Hydrate");
            err_derives.push("Reconcile");
        }
        writeln!(buf, "#[derive({})]", err_derives.join(", "))?;
        // Optional serde derives and serde attributes when enabled for this ErrorType
        if cx.attrs.serde {
            writeln!(buf, "#[serde(rename_all = \"camelCase\", tag = \"error\")]")?;
        }
        writeln!(buf, "/// {message}{message_with_fields}",)?;
        writeln!(
            buf,
            "pub struct Error{name} {{",
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
                record_field(inner, cx, buf, field_name, true)?;
                writeln!(buf, ",")?;
            }
        }
        writeln!(buf, "}}")?;
        writeln!(buf)?;
    }
    exp.append("error".to_string(), "Error".to_string());
    // enum-level derives will be composed below; avoid duplicate derive block here
    // Build derives for the error enum
    let mut enum_derives = vec!["Debug", "thiserror::Error", "displaydoc::Display"];
    if cx.attrs.utoipa {
        enum_derives.push("utoipa::ToSchema");
        // macros::HttpError is an auxiliary derive macro; emit when utoipa is requested
        enum_derives.push("macros::HttpError");
    }
    if cx.attrs.serde {
        enum_derives.push("Serialize");
        enum_derives.push("Deserialize");
    }
    if cx.attrs.automerge {
        enum_derives.push("Hydrate");
        enum_derives.push("Reconcile");
    }
    writeln!(buf, "#[derive({})]", enum_derives.join(", "))?;
    if cx.attrs.serde {
        writeln!(buf, "#[serde(rename_all = \"camelCase\", tag = \"error\")]")?;
    }

    writeln!(buf, "pub enum Error {{")?;
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
        writeln!(buf, r#"/// {message} {{0}}"#,)?;
        if cx.attrs.utoipa {
            writeln!(
                buf,
                r#"#[http(code(StatusCode::{code_name}), desc("{message}"))]"#,
                code_name = http_status_code_name(*http_code),
            )?;
        }
        write!(buf, r#"{name}"#, name = AsPascalCase(&name[..]),)?;
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
    cx: &RustGenCtx,
    buf: &mut impl Write,
    name: &str,
    pub_visibility: bool,
) -> std::fmt::Result {
    // Emit autosurgeon key attribute for fields when requested
    if cx.attrs.automerge && this.autosurgeon_key {
        writeln!(buf, "#[autosurgeon(key)]")?;
    }

    if let Type::Primitives(Primitives::DateTime) = cx
        .reg
        .types
        .get(&this.ty)
        .as_deref()
        .expect("unregistered field type")
    {
        // Emit autosurgeon date helper at field-level when the parent record requested autosurgeon
        if cx.attrs.automerge {
            if cx.attrs.serde {
                // Emit serde codec helper for sane ISO8601 on datetime fields
                writeln!(
                    buf,
                    "#[serde(with = \"api_utils_rs::codecs::sane_iso8601\")]",
                )?;
            }
            writeln!(buf, "#[autosurgeon(with = \"utils_rs::am::codecs::date\")]",)?;
        }
    }
    write!(
        buf,
        "{vis}{field_name}: {ty_name}",
        vis = if pub_visibility { "pub " } else { "" },
        field_name = heck::AsSnekCase(name),
        ty_name = cx.rust_name(this.ty).expect("unregistered field type"),
    )
}
