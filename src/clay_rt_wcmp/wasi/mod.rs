//! The `wasi:*` imports a dayframe guest pulls in when rustc targets
//! `wasm32-wasip3` (ADR 018 §Phase 1), defined on the polyfill
//! [`Linker`].
//!
//! This is not a full WASI host. It supplies only the interfaces
//! `clay_mdk` guests link against on the wasip3 target, at the versions
//! rustc's wasi-libc emits (`0.3.0`). A function of an interface that
//! our guests do not call answers with an error when it is called, so a
//! change that starts calling one shows as a failure rather than a
//! silent wrong answer.
//!
//! stdout/stderr write through the [`StreamConsumer`] /
//! [`FutureProducer`] pattern from `wcmp-demo`: lines the guest writes
//! go to the process stdio under a component label; the write future
//! resolves when the guest drops the stream.

mod cli;
mod clocks;
mod io;

use wcmp::Error;
use wcmp::InterfaceIdentifier;
use wcmp::Linker;

/// Define every `wasi:*` interface our wasip3 guests import on
/// `linker`. Guest writes to stdout/stderr go to process stdio under
/// `label`, which names the component.
///
/// # Errors
///
/// The polyfill's error when it refuses a definition.
pub fn define<T: 'static>(linker: &mut Linker<T>, label: &str) -> Result<(), Error> {
    cli::define_environment(linker)?;
    cli::define_exit(linker)?;
    linker.instance(&identifier(cli::CLI_TYPES));
    cli::define_stdin(linker)?;
    cli::define_output(linker, cli::CLI_STDOUT, label, io::Console::Stdout)?;
    cli::define_output(linker, cli::CLI_STDERR, label, io::Console::Stderr)?;
    cli::define_terminal_resources(linker)?;
    linker.instance(&identifier(clocks::CLOCK_TYPES));
    clocks::define_monotonic_clock(linker)?;
    clocks::define_system_clock(linker)?;
    clocks::define_insecure_seed(linker)?;
    Ok(())
}

fn identifier(interface: &str) -> InterfaceIdentifier {
    interface
        .parse()
        .unwrap_or_else(|error| panic!("`{interface}` is not an interface: {error}"))
}

fn unsupported(interface: &str, method: &str) -> Error {
    Error::Unsupported {
        feature: format!("`{interface}#{method}` in clay_rt_wcmp's WASI host"),
    }
}
