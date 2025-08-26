use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, utoipa::ToSchema)]
#[serde(crate = "serde", rename_all = "camelCase")]
pub struct ValidationErrors {
    pub issues: Vec<(String, String)>,
}

impl std::ops::Deref for ValidationErrors {
    type Target = Vec<(String, String)>;

    fn deref(&self) -> &Self::Target {
        &self.issues
    }
}

impl std::fmt::Display for ValidationErrors {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, (path, err)) in self.issues.iter().enumerate() {
            write!(fmt, "{path}: {err}")?;
            if idx + 1 < self.issues.len() {
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
        Self {
            issues: value
                .into_inner()
                .into_iter()
                .map(|(path, err)| (format!("{path}"), err.message().to_string()))
                .collect(),
        }
    }
}
