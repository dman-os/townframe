use crate::interlude::*;

/// Whether stderr gets ANSI colour: only on a terminal, and never when `NO_COLOR` is set
/// (https://no-color.org: set and not empty). tracing's formatter colours unconditionally
/// unless told otherwise, and so does color-eyre 0.6, so both are driven from here.
pub(crate) fn stderr_colour() -> bool {
    std::env::var_os("NO_COLOR").is_none_or(|value| value.is_empty())
        && std::io::IsTerminal::is_terminal(&std::io::stderr())
}

/// Filter string for a subscriber: `env` (the `RUST_LOG`/`RUST_LOG_TEST` value) or else
/// `default`, with `instr=off,` prepended. `INSTR` investigation probes log under the `instr`
/// target and stay silent even when the env filter is a plain `info` or `debug`; `EnvFilter`
/// lets a later directive for the same target win, so an explicit `instr=info` in the env value
/// turns them back on.
pub(crate) fn instr_off_filter(env: Option<String>, default: &str) -> String {
    format!("instr=off,{}", env.as_deref().unwrap_or(default))
}

// FIXME: why this take 7ms on debug builds?
/// color-eyre 0.6 applies its owo-colors styles unconditionally, ignoring
/// both `NO_COLOR` and TTY detection, so ANSI leaks into piped output (e.g.
/// the CLI e2e snapshots. A blank theme renders plain text instead, when stderr is not a
/// terminal or `NO_COLOR` is set.
pub(crate) fn eyre_hook_builder() -> color_eyre::config::HookBuilder {
    let builder = color_eyre::config::HookBuilder::default();
    if stderr_colour() {
        builder
    } else {
        builder.theme(color_eyre::config::Theme::new())
    }
}

pub fn setup_tracing() -> Res<()> {
    setup_tracing_at("info", None)
}

/// Where a process keeps its rolling log (docs/scratch/guest-faults-and-logs.md §1): JSON lines,
/// one file a day, `<directory>/<prefix>.<YYYY-MM-DD>.jsonl`, the newest [`LOG_FILES_KEPT`] kept.
/// The file takes the same filter as stderr, so it holds what a watched run would have shown;
/// `DAY_LOG_FILE=off` turns it off (tests, CI).
#[derive(Debug, Clone)]
pub struct LogFile {
    pub directory: std::path::PathBuf,
    pub prefix: &'static str,
}

/// Days of log a process keeps: the rolling writer deletes older files as it rotates.
pub const LOG_FILES_KEPT: usize = 7;

/// The log file's writer guard: dropping it flushes what the writer's worker still holds. `main`
/// cannot hold it, because the panic hook ends the process with `exit`, which runs no
/// destructors; the hook takes it out of here and drops it before exiting.
#[cfg(not(target_arch = "wasm32"))]
static LOG_FILE_GUARD: std::sync::Mutex<Option<tracing_appender::non_blocking::WorkerGuard>> =
    std::sync::Mutex::new(None);

/// [`setup_tracing`] with `default_level` as the level of everything the dependency overrides
/// below do not name, and with `file` as the process's log file. An explicit `RUST_LOG` still
/// replaces the whole default.
pub fn setup_tracing_at(default_level: &str, file: Option<LogFile>) -> Res<()> {
    let default_filter = &format!(
        "{default_level},ort::logging=warn,netlink_packet_route::link::buffer_tool=error,iroh_docs::store::fs::migrations=warn,subduction_core=warn,subduction_keyhive=warn,keyhive_core=warn,keyhive_crypto=warn,beekem=warn"
    );
    #[cfg(not(target_arch = "wasm32"))]
    let filter = {
        if std::env::var("RUST_BACKTRACE").is_err() {
            // std::env::set_var("RUST_BACKTRACE", "1");
        }
        instr_off_filter(std::env::var("RUST_LOG").ok(), default_filter)
    };

    #[cfg(target_arch = "wasm32")]
    let filter = instr_off_filter(None, default_filter);

    use ::tracing_subscriber::prelude::*;
    let registry = ::tracing_subscriber::registry()
        .with(::tracing_subscriber::EnvFilter::new(filter))
        .with(
            ::tracing_subscriber::fmt::layer()
                .compact()
                .with_writer(std::io::stderr)
                .with_ansi(stderr_colour())
                //.with_timer(::tracing_subscriber::fmt::time::time()),
                .with_timer(::tracing_subscriber::fmt::time::uptime()),
        )
        .with(tracing_error::ErrorLayer::default());

    #[cfg(target_os = "android")]
    let registry = registry.with(tracing_android::layer("org.example.daybook")?);

    #[cfg(not(target_arch = "wasm32"))]
    let registry = {
        let file = file.filter(|_| std::env::var("DAY_LOG_FILE").as_deref() != Ok("off"));
        let layer = match file {
            Some(LogFile { directory, prefix }) => {
                // The builder prunes old files before it creates the directory, and says so on
                // stderr when the directory is missing.
                std::fs::create_dir_all(&directory)
                    .wrap_err_with(|| format!("creating the log directory {}", directory.display()))?;
                let appender = tracing_appender::rolling::RollingFileAppender::builder()
                    .rotation(tracing_appender::rolling::Rotation::DAILY)
                    .filename_prefix(prefix)
                    .filename_suffix("jsonl")
                    .max_log_files(LOG_FILES_KEPT)
                    .build(&directory)
                    .wrap_err_with(|| {
                        format!("opening the log directory {}", directory.display())
                    })?;
                // Writes leave the logging thread for the writer's worker, so an event costs a
                // channel send, not a file write.
                let (writer, guard) = tracing_appender::non_blocking(appender);
                *LOG_FILE_GUARD.lock().unwrap() = Some(guard);
                Some(
                    ::tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(writer)
                        .with_ansi(false),
                )
            }
            None => None,
        };
        registry.with(layer)
    };
    // A wasm32 process has no file system to log to.
    #[cfg(target_arch = "wasm32")]
    drop(file);

    registry.try_init().map_err(|err| ferr!(err))?;

    // color_eyre::install()?;
    let (eyre_panic_hook, eyre_hook) = eyre_hook_builder().try_into_hooks()?;
    std::panic::set_hook(Box::new(move |panic_info| {
        let report = eyre_panic_hook.panic_report(panic_info);
        println!("{report}");
        // The panic is an event too, so the log file has it, and the file is flushed before the
        // exit below, which runs no destructors.
        ::tracing::error!(target: "panic", "{report}");
        #[cfg(not(target_arch = "wasm32"))]
        drop(
            LOG_FILE_GUARD
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take(),
        );

        // - Tokio does not exit the process when a task panics, so we define a custom
        //   panic hook to implement this behaviour.
        std::process::exit(1);
    }));
    eyre_hook.install()?;

    Ok(())
}

// Ensure that the `tracing` stack is only initialised once using `once_cell`
pub fn setup_tracing_once() {
    static TRACING: std::sync::Once = std::sync::Once::new();
    TRACING.call_once(|| {
        setup_tracing().expect("setup tracing error");
    });
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::prelude::*;

    struct Count(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl<S: ::tracing::Subscriber> tracing_subscriber::Layer<S> for Count {
        fn on_event(&self, _: &::tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Events admitted by `filter`: one `instr` target event, one ordinary info event.
    fn admitted(filter: &str) -> (usize, usize) {
        let run = |instr: bool| {
            let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let subscriber = tracing_subscriber::registry()
                .with(tracing_subscriber::EnvFilter::new(filter))
                .with(Count(std::sync::Arc::clone(&count)));
            ::tracing::subscriber::with_default(subscriber, || {
                if instr {
                    ::tracing::info!(target: "instr", "INSTR probe");
                } else {
                    ::tracing::info!("ordinary");
                }
            });
            count.load(std::sync::atomic::Ordering::Relaxed)
        };
        (run(true), run(false))
    }

    #[test]
    fn instr_target_is_off_by_default_and_on_by_filter() {
        let default = "info,h2=info";
        let built = |env: Option<&str>| super::instr_off_filter(env.map(str::to_owned), default);
        // No env value: the default applies, probes off.
        assert_eq!(admitted(&built(None)), (0, 1));
        // A plain env filter replaces the default but cannot re-enable the probes.
        assert_eq!(admitted(&built(Some("info"))), (0, 1));
        assert_eq!(admitted(&built(Some("debug"))), (0, 1));
        // An explicit `instr=info` in the env value turns them on.
        assert_eq!(admitted(&built(Some("info,instr=info"))), (1, 1));
        assert_eq!(admitted(&built(Some("warn,instr=info"))), (1, 0));
    }
}
