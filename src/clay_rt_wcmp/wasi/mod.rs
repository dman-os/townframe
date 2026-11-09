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

#[cfg(test)]
mod tests {
    /// A4's checking test (ADR 015 R5): enumerate the async-linked WASI imports.
    ///
    /// Every `func_wrap_concurrent` registration is a suspension point a guest can reach from
    /// inside a sync-typed presentation call, because the WASI imports are linked into the same
    /// instance that serves `mount`/`dispatch`/`unmount`. Today that is exactly the
    /// monotonic-clock pair — `wait-for` and `wait-until` (the world imports the whole
    /// interface, so both must be registered) — and the may-not-suspend rule makes blocking on
    /// them fail with `CannotBlock` instead of hanging (pinned behaviorally by the `sleeper`
    /// fixture on both engines). Growing this set widens the surface where that runtime net is
    /// the only thing between a guest and a stalled presentation call, so the durable fix is the
    /// link-time split (a presentation world with no async imports). This test is the trigger:
    /// adding a third async import fails here and forces that split, or a deliberate ADR
    /// amendment, rather than silently enlarging the contract's exception list.
    #[test]
    fn async_linked_wasi_imports_are_enumerated_and_pinned() {
        let wasi_directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("wasi");
        let mut registrations = Vec::new();
        let mut entries: Vec<_> = std::fs::read_dir(&wasi_directory)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", wasi_directory.display()))
            .map(|entry| entry.expect("wasi directory entry reads").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
            .collect();
        entries.sort();
        for path in entries {
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
            // This test module lives in `mod.rs`, so scan production code only — otherwise the
            // needle matches the scanner itself.
            let production = source
                .split_once("#[cfg(test)]")
                .map_or(source.as_str(), |(head, _)| head);
            let count = production.matches("func_wrap_concurrent").count();
            if count > 0 {
                registrations.push((
                    path.file_name()
                        .expect("wasi source has a file name")
                        .to_string_lossy()
                        .into_owned(),
                    count,
                ));
            }
        }
        assert_eq!(
            registrations,
            [("clocks.rs".to_string(), 2)],
            "the async-linked WASI import set changed; see the doc comment on this test"
        );
    }
}
