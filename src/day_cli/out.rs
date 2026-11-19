//! The one way a command writes its output.
//!
//! A command's results go to stdout through [`Out`], and a write that fails is an error the
//! command returns, not a panic as `println!` would have it. Reading stdout can stop before the
//! command is done (`day exec ... | head`): the next write then fails with `EPIPE`, and `main`
//! ends the process with status 0 and no message, as a Unix tool does. Rust already ignores
//! `SIGPIPE`, which the server needs, so the write reports the broken pipe instead of killing the
//! process. Any other write error is a real error.

use crate::interlude::*;

pub struct Out(std::io::Stdout);

pub fn out() -> Out {
    Out(std::io::stdout())
}

/// What a broken pipe on stdout carries, so that `main` can tell it from the `EPIPE` of a socket
/// a command was talking to.
#[derive(Debug, thiserror::Error)]
#[error("stdout was closed by its reader")]
struct StdoutClosed;

fn tag(error: std::io::Error) -> std::io::Error {
    if error.kind() == std::io::ErrorKind::BrokenPipe {
        std::io::Error::new(std::io::ErrorKind::BrokenPipe, StdoutClosed)
    } else {
        error
    }
}

impl std::io::Write for Out {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf).map_err(tag)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush().map_err(tag)
    }
}

/// Whether `error` is stdout's reader having gone away, which ends the command quietly.
pub fn is_stdout_closed(error: &eyre::Report) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
        .is_some_and(|inner| inner.is::<StdoutClosed>())
}
