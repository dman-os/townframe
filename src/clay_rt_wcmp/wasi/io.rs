//! stdout/stderr stream consumers and the `write-via-stream` future
//! producer: guest lines go to process stdio; the future resolves when
//! the guest drops the stream.

use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::Waker;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use wcmp::ComponentType;
use wcmp::EnumType;
use wcmp::Error;
use wcmp::FutureProducer;
use wcmp::Lift;
use wcmp::Lower;
use wcmp::Source;
use wcmp::StoreContext;
use wcmp::StreamConsumer;

const ERROR_CODES: [&str; 3] = ["io", "illegal-byte-sequence", "pipe"];

pub(super) fn error_codes() -> EnumType {
    EnumType::new(ERROR_CODES.map(str::to_string))
}

#[derive(Clone, Copy)]
pub(super) enum Console {
    Stdout,
    Stderr,
}

impl Console {
    fn write(self, label: &str, line: &str) {
        match self {
            Console::Stdout => println!("{label} {line}"),
            Console::Stderr => eprintln!("{label} {line}"),
        }
    }
}

#[derive(Default)]
pub(super) struct End {
    ended: bool,
    waker: Option<Waker>,
}

pub(super) struct Lines {
    pub(super) label: String,
    pub(super) console: Console,
    pub(super) pending: Vec<u8>,
    pub(super) end: Arc<Mutex<End>>,
}

impl Lines {
    fn flush_lines(&mut self) {
        while let Some(newline) = self.pending.iter().position(|&byte| byte == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=newline).collect();
            let text = String::from_utf8_lossy(&line[..line.len() - 1]);
            self.console.write(&self.label, &text);
        }
    }
}

impl<T: 'static> StreamConsumer<T> for Lines {
    type Item = u8;

    fn poll_consume(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        store: &mut StoreContext<'_, T>,
        mut source: Source<'_, u8>,
        _finish: bool,
    ) -> Poll<Result<wcmp::StreamResult, Error>> {
        let mut bytes = Vec::new();
        let count = source.remaining();
        source.read(store, &mut bytes, count)?;
        self.pending.extend(bytes);
        self.flush_lines();
        Poll::Ready(Ok(wcmp::StreamResult::Completed))
    }
}

impl Drop for Lines {
    fn drop(&mut self) {
        if !self.pending.is_empty() {
            let text = String::from_utf8_lossy(&self.pending).into_owned();
            self.console.write(&self.label, &text);
        }
        let waker = {
            let mut end = lock(&self.end);
            end.ended = true;
            end.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

pub(super) struct Finishes(pub(super) Arc<Mutex<End>>);

impl<T: 'static> FutureProducer<T> for Finishes {
    type Item = WriteResult;

    fn poll_produce(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _store: &mut StoreContext<'_, T>,
        finish: bool,
    ) -> Poll<Result<Option<WriteResult>, Error>> {
        let mut end = lock(&self.0);
        if end.ended {
            return Poll::Ready(Ok(Some(Ok(()))));
        }
        if finish {
            return Poll::Ready(Ok(None));
        }
        end.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

pub(super) type WriteResult = Result<(), ErrorCode>;

#[derive(ComponentType, Lift, Lower, Debug)]
#[component(enum)]
#[repr(u8)]
pub(super) enum ErrorCode {
    #[component(name = "io")]
    Io,
    #[component(name = "illegal-byte-sequence")]
    IllegalByteSequence,
    #[component(name = "pipe")]
    Pipe,
}

fn lock<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
    shared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
