use crate::interlude::*;

#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, utoipa::ToSchema)]
#[serde(crate = "serde", rename_all = "camelCase")]
#[cfg_attr(
    feature = "automerge",
    derive(autosurgeon::Hydrate, autosurgeon::Reconcile)
)]
pub struct ErrorsValidation {
    pub issues: Vec<(String, String)>,
}

impl std::ops::Deref for ErrorsValidation {
    type Target = Vec<(String, String)>;

    fn deref(&self) -> &Self::Target {
        &self.issues
    }
}

impl std::fmt::Display for ErrorsValidation {
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

impl std::error::Error for ErrorsValidation {
    fn description(&self) -> &str {
        "Validation failed"
    }
    fn cause(&self) -> Option<&dyn std::error::Error> {
        None
    }
}

impl From<garde::Report> for ErrorsValidation {
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

#[derive(
    Default,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    utoipa::ToSchema,
    thiserror::Error,
    displaydoc::Display,
)]
#[serde(crate = "serde", rename_all = "camelCase")]
#[cfg_attr(
    feature = "automerge",
    derive(autosurgeon::Hydrate, autosurgeon::Reconcile)
)]
/// internal error: {message}
pub struct ErrorInternal {
    pub message: String,
}

impl From<String> for ErrorInternal {
    fn from(message: String) -> Self {
        Self { message }
    }
}

impl From<&str> for ErrorInternal {
    fn from(message: &str) -> Self {
        Self {
            message: message.into(),
        }
    }
}
