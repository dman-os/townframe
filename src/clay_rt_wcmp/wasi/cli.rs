//! `wasi:cli/*` interfaces our wasip3 guests import.

use std::sync::Arc;
use std::sync::Mutex;

use wcmp::Error;
use wcmp::FunctionParameter;
use wcmp::FunctionType;
use wcmp::FutureAny;
use wcmp::FutureReader;
use wcmp::FutureType;
use wcmp::HostCall;
use wcmp::Linker;
use wcmp::OptionType;
use wcmp::PrimitiveType;
use wcmp::ResourceType;
use wcmp::ResultType;
use wcmp::StreamReader;
use wcmp::StreamType;
use wcmp::TupleType;
use wcmp::Val;
use wcmp::ValueType;

use super::identifier;
use super::io;
use super::unsupported;

pub(super) const CLI_ENVIRONMENT: &str = "wasi:cli/environment@0.3.0";
pub(super) const CLI_EXIT: &str = "wasi:cli/exit@0.3.0";
pub(super) const CLI_TYPES: &str = "wasi:cli/types@0.3.0";
pub(super) const CLI_STDIN: &str = "wasi:cli/stdin@0.3.0";
pub(super) const CLI_STDOUT: &str = "wasi:cli/stdout@0.3.0";
pub(super) const CLI_STDERR: &str = "wasi:cli/stderr@0.3.0";
const CLI_TERMINAL_INPUT: &str = "wasi:cli/terminal-input@0.3.0";
const CLI_TERMINAL_OUTPUT: &str = "wasi:cli/terminal-output@0.3.0";
const CLI_TERMINAL_STDIN: &str = "wasi:cli/terminal-stdin@0.3.0";
const CLI_TERMINAL_STDOUT: &str = "wasi:cli/terminal-stdout@0.3.0";
const CLI_TERMINAL_STDERR: &str = "wasi:cli/terminal-stderr@0.3.0";

pub(super) fn define_environment<T: 'static>(linker: &mut Linker<T>) -> Result<(), Error> {
    linker
        .instance(&identifier(CLI_ENVIRONMENT))
        .func_wrap("get-environment", |_, (): ()| {
            // Deterministic empty environment: guests must not depend on
            // host process variables across the native/browser engines.
            Ok((Vec::<(String, String)>::new(),))
        })
}

pub(super) fn define_exit<T: 'static>(linker: &mut Linker<T>) -> Result<(), Error> {
    let mut exit = linker.instance(&identifier(CLI_EXIT));
    exit.func_wrap("exit", |_, (_status,): (Result<(), ()>,)| {
        Err::<(), _>(unsupported(CLI_EXIT, "exit"))
    })?;
    exit.func_wrap("exit-with-code", |_, (_code,): (u8,)| {
        Err::<(), _>(unsupported(CLI_EXIT, "exit-with-code"))
    })
}

pub(super) fn define_stdin<T: 'static>(linker: &mut Linker<T>) -> Result<(), Error> {
    linker.instance(&identifier(CLI_STDIN)).func_new(
        "read-via-stream",
        stdin_read_type(),
        |_call: HostCall<'_, T>, _args, _results| Err(unsupported(CLI_STDIN, "read-via-stream")),
    )
}

fn stdin_read_type() -> FunctionType {
    FunctionType {
        parameters: vec![],
        result: Some(ValueType::Tuple(TupleType::new([
            ValueType::Stream(StreamType::new(Some(ValueType::Primitive(
                PrimitiveType::U8,
            )))),
            ValueType::Future(FutureType::new(Some(write_result_type()))),
        ]))),
        async_: false,
    }
}

fn write_result_type() -> ValueType {
    let codes = io::error_codes();
    ValueType::Result(ResultType::new(None, Some(ValueType::Enum(codes))))
}

pub(super) fn define_output<T: 'static>(
    linker: &mut Linker<T>,
    interface: &str,
    label: &str,
    console: io::Console,
) -> Result<(), Error> {
    let label = format!("[{label}]");
    linker.instance(&identifier(interface)).func_new(
        "write-via-stream",
        write_via_stream_type(),
        move |mut call: HostCall<'_, T>, args, results| {
            let [Val::Stream(data)] = args else {
                return Err(Error::Internal {
                    message: format!("`write-via-stream` was given {args:?}"),
                });
            };
            let end = Arc::new(Mutex::new(io::End::default()));
            let consumer = io::Lines {
                label: label.clone(),
                console,
                pending: Vec::new(),
                end: Arc::clone(&end),
            };
            StreamReader::<u8>::try_from_stream_any(data.clone())?.pipe(call.store(), consumer)?;
            let finished = FutureReader::new(call.store(), io::Finishes(end))?;
            results[0] = Val::Future(FutureAny::try_from_future_reader(call.store(), finished)?);
            Ok(())
        },
    )
}

fn write_via_stream_type() -> FunctionType {
    FunctionType {
        parameters: vec![FunctionParameter {
            name: "data".to_string(),
            ty: ValueType::Stream(StreamType::new(Some(ValueType::Primitive(
                PrimitiveType::U8,
            )))),
        }],
        result: Some(ValueType::Future(FutureType::new(
            Some(write_result_type()),
        ))),
        async_: false,
    }
}

pub(super) fn define_terminal_resources<T: 'static>(linker: &mut Linker<T>) -> Result<(), Error> {
    linker.instance(&identifier(CLI_TERMINAL_INPUT)).resource(
        "terminal-input",
        ResourceType::host::<TerminalInput>(),
        |_store, _rep| Ok(()),
    )?;
    linker.instance(&identifier(CLI_TERMINAL_OUTPUT)).resource(
        "terminal-output",
        ResourceType::host::<TerminalOutput>(),
        |_store, _rep| Ok(()),
    )?;

    let mut terminal_stdin = linker.instance(&identifier(CLI_TERMINAL_STDIN));
    terminal_stdin.func_wrap("get-terminal-stdin", |_, (): ()| {
        Ok((Option::<ResourceMarker<TerminalInput>>::None,))
    })?;

    let mut terminal_stdout = linker.instance(&identifier(CLI_TERMINAL_STDOUT));
    terminal_stdout.func_wrap("get-terminal-stdout", |_, (): ()| {
        Ok((Option::<ResourceMarker<TerminalOutput>>::None,))
    })?;

    let mut terminal_stderr = linker.instance(&identifier(CLI_TERMINAL_STDERR));
    terminal_stderr.func_wrap("get-terminal-stderr", |_, (): ()| {
        Ok((Option::<ResourceMarker<TerminalOutput>>::None,))
    })
}

/// Marker type carrying the resource identity for `terminal-input`.
/// Never constructed: `get-terminal-*` always answers `none`.
struct TerminalInput;

/// Marker type carrying the resource identity for `terminal-output`.
struct TerminalOutput;

/// A resource handle lowered as `Resource<T>` would be, used only as a
/// phantom so `func_wrap` derives `option<terminal-*>` without the host
/// ever pushing a live resource.
type ResourceMarker<T> = wcmp::Resource<T>;

// Type constructors named only by #[derive(Lift, Lower)] on SystemInstant etc.
#[expect(
    unused_imports,
    reason = "option/list/result type constructors are named only by #[derive(Lift, Lower)]"
)]
use self::OptionType as _OptionType;
