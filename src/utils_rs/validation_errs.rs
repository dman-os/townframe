use serde::Serialize;

#[derive(Default, Debug, Serialize, Clone, PartialEq, utoipa::ToSchema)]
#[serde(crate = "serde", rename_all = "camelCase")]
pub struct ValidationErrors(pub Vec<(String, String)>);

impl std::fmt::Display for ValidationErrors {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, (path, err)) in self.0.iter().enumerate() {
            write!(fmt, "{path}: {err}")?;
            if idx + 1 < self.0.len() {
                // writeln!(fmt)?;
                write!(fmt, ",")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {
    fn description(&self) -> &str {
        "Validation failed"
    }
    fn cause(&self) -> Option<&dyn std::error::Error> {
        None
    }
}

impl From<garde::Report> for ValidationErrors {
    fn from(value: garde::Report) -> Self {
        Self(
            value
                .into_inner()
                .into_iter()
                .map(|(path, err)| (format!("{path}"), err.message().to_string()))
                .collect(),
        )
    }
}
