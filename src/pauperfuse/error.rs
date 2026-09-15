//! The crate's errors.
//!
//! **One concrete error type, deliberately.** Both of this crate's traits are
//! implemented by embedders, so their error type has to be nameable by everyone:
//! a generic `Result<T, Self::Error>` would make the traits object-unsafe and
//! force every caller to know whose error it is holding. An implementation whose
//! own failure fits none of these variants boxes it into [`Error::Backend`],
//! keeping it as the source.
//!
//! The failure classes, in the order an embedder usually cares about them:
//!
//! - [`Error::Fs`] — the environment said no, with the operation and the path
//!   attached, so a caller can point at the file that failed;
//! - [`Error::Path`] — something handed over a path a checkout cannot hold;
//! - [`Error::Stored`] — a recorded row cannot be read back: corrupt, or written
//!   by a version this build does not know;
//! - [`Error::Sql`] and [`Error::Migrate`] — the store's database;
//! - [`Error::Backend`] — an implementor's own error.
//!
//! Anything else is a programming error and panics: an unimplemented branch, a
//! shape that cannot exist, a `todo!()`.
//!
//! An error is a boundary's last resort, never a control path. A scan that
//! cannot read a path reports a failure instead of omitting it, because a
//! missing row and a lost row are indistinguishable downstream — the difference
//! between "the user deleted this" and "we could not look at this" is the
//! difference between a removal and silent data loss.

use std::path::PathBuf;

use crate::path::PathError;

#[cfg(feature = "sqlite")]
pub use crate::codec::StoredError;

/// What a fallible operation in this crate returns.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why an operation failed.
///
/// `#[non_exhaustive]`: new failure classes are additions, not breakage. Build
/// the variants with the constructors below rather than naming them, so that
/// stays true in both directions.
#[derive(Debug, thiserror::Error, displaydoc::Display)]
#[non_exhaustive]
pub enum Error {
    /// failed {op} {path}
    Fs {
        /// What was being attempted.
        op: FsOp,
        /// What it was attempted on.
        path: PathBuf,
        /// What the filesystem said.
        #[source]
        source: std::io::Error,
    },

    /// io error: {0}
    Io(#[from] std::io::Error),

    /// the root {path} is not a directory
    NotADirectory {
        /// The path that was checked.
        path: PathBuf,
    },

    /// invalid path: {0}
    Path(#[from] PathError),

    /// the recorded entry at "{path}" is not decodable: {reason}
    #[cfg(feature = "sqlite")]
    Stored {
        /// The row's path, rendered for a human: the decoded path when it
        /// decodes, the raw column when it does not.
        path: String,
        /// Why the row was refused.
        reason: StoredError,
    },

    /// store error: {0}
    #[cfg(feature = "sqlite")]
    Sql(#[from] sqlx::Error),

    /// migration error: {0}
    #[cfg(feature = "sqlite")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// a backend failed: {0}
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// {0}
    Message(Message),
}

impl Error {
    /// A filesystem operation on `path` failed.
    ///
    /// This is the shape an embedder's own filesystem work should report in:
    /// the operation and the path are what make the failure actionable, and the
    /// source keeps the errno-level detail.
    pub fn fs(op: FsOp, path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Fs {
            op,
            path: path.into(),
            source,
        }
    }

    /// A recorded row at `path` cannot be read back.
    #[cfg(feature = "sqlite")]
    pub fn stored(path: impl Into<String>, reason: StoredError) -> Self {
        Self::Stored {
            path: path.into(),
            reason,
        }
    }

    /// An implementor's own error.
    ///
    /// A backend whose failure is not one of the classes above wraps it here
    /// rather than flattening it into a string: the source chain survives, so
    /// the layer that knows what its error means still gets to say so.
    pub fn backend(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Backend(Box::new(source))
    }

    /// An error that can only be handed over as text.
    ///
    /// For a dependency that reports through `eyre::Report`, which deliberately
    /// does not implement [`std::error::Error`] — this workspace's `SqlCtx`, for
    /// one. There is nothing better to keep than the rendering, so keep the
    /// rendering and say so.
    pub fn message(text: impl std::fmt::Display) -> Self {
        Self::Message(Message {
            text: text.to_string(),
        })
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for Error {
    fn from(source: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::Backend(source)
    }
}

/// An error that arrived as text, because its source could not be kept as a
/// type.
#[derive(Debug, thiserror::Error)]
#[error("{text}")]
pub struct Message {
    text: String,
}

impl Message {
    /// The text, as it was rendered.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// What a filesystem operation was attempting.
///
/// The operation is separate from the [`std::io::Error`] because the two answer
/// different questions: the operation says what this crate was doing, and the io
/// error says what the operating system refused. "Failed writing" and "failed
/// statting" on the same path are very different bugs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, displaydoc::Display)]
#[non_exhaustive]
pub enum FsOp {
    /// opening
    Open,
    /// reading
    Read,
    /// seeking
    Seek,
    /// statting
    Stat,
    /// checking
    Check,
    /// listing
    ReadDir,
    /// reading link
    ReadLink,
    /// creating directory
    CreateDir,
    /// creating symlink
    CreateSymlink,
    /// writing
    Write,
    /// moving
    Rename,
    /// linking
    Link,
    /// setting the mode of
    SetMode,
    /// removing
    Remove,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Embedders move these across tasks, channels and threads, so both bounds
    /// are part of the contract rather than a coincidence of today's variants.
    #[test]
    fn errors_are_send_and_sync() {
        fn check<T: Send + Sync + 'static>() {}
        check::<Error>();
    }

    /// The operation and the path are in the message; the errno is in the
    /// source, where a caller can still match on it.
    #[test]
    fn a_filesystem_error_carries_its_operation_its_path_and_its_cause() {
        let error = Error::fs(
            FsOp::Open,
            "/checkout/notes.md",
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "nope"),
        );
        assert_eq!(error.to_string(), "failed opening /checkout/notes.md");

        let source = std::error::Error::source(&error).expect("the io error is the source");
        let io = source
            .downcast_ref::<std::io::Error>()
            .expect("and it is still an io error");
        assert_eq!(io.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn a_bare_io_error_is_kept_as_one() {
        let error: Error = std::io::Error::from(std::io::ErrorKind::NotFound).into();
        assert!(matches!(error, Error::Io(_)), "{error:?}");
    }

    /// A dependency that reports through `eyre` keeps its text and nothing more,
    /// and the message says so rather than implying a type survived.
    #[test]
    fn a_text_error_renders_its_text() {
        let error = Error::message("sqlite said no");
        assert_eq!(error.to_string(), "sqlite said no");
        match &error {
            Error::Message(message) => assert_eq!(message.text(), "sqlite said no"),
            other => panic!("expected a text error, got {other:?}"),
        }
    }
}
