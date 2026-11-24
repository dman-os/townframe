//! A workload's guest stdout and stderr, captured instead of inherited.
//!
//! wash-runtime builds a fresh WASI context for every request store and asks [`Stdio`] (its
//! `GuestStdio` hook) for that workload's two streams. Every store of a workload gets a writer
//! onto the same [`Pipe`] per stream, so the pipe is the workload's instance-wide stdio: it
//! turns what the guest writes into `tracing` events at target `guest` and keeps the last
//! [`TAIL_OCTETS`] written, which a fault carries (a Rust guest prints its panic message to
//! stderr just before it traps).
//!
//! The guest controls what is written, so everything here is bounded: a line is cut at
//! [`LINE_OCTETS`], at most [`LINES_PER_SECOND`] lines per second become events (the rest are
//! counted, and one event says how many were dropped), and the tail is a ring.

use big_cartridges_core::CartridgeId;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use tokio::io::AsyncWrite;
use wash_runtime::engine::GuestStdio;
use wasmtime_wasi::cli::IsTerminal;
use wasmtime_wasi::cli::StdoutStream;

/// How much of a stream's most recent output a fault carries.
const TAIL_OCTETS: usize = 16 * 1024;

/// A line longer than this is cut here; the rest of it is discarded.
const LINE_OCTETS: usize = 4 * 1024;

/// Lines per second of one stream that become events. A guest looping on `eprintln!` would
/// otherwise fill the log file and the disk.
const LINES_PER_SECOND: u32 = 100;

/// The pipes of the running workloads, by workload id. A workload's entry is made before it is
/// started and dropped once it is stopped, like the signers.
pub(crate) type Pipes = Arc<Mutex<BTreeMap<String, Arc<WorkloadPipes>>>>;

/// One workload's pair of streams.
pub(crate) struct WorkloadPipes {
    stdout: Arc<Pipe>,
    stderr: Arc<Pipe>,
}

impl WorkloadPipes {
    pub(crate) fn new(cartridge: &CartridgeId) -> Self {
        Self {
            stdout: Arc::new(Pipe::new(cartridge.clone(), "stdout")),
            stderr: Arc::new(Pipe::new(cartridge.clone(), "stderr")),
        }
    }

    /// The last output of the (stdout, stderr) streams, lossily decoded: a guest may write any
    /// octets, and the tail may start or end inside a character.
    pub(crate) fn tails(&self) -> (String, String) {
        (self.stdout.tail(), self.stderr.tail())
    }
}

/// wash-runtime's hook, handing each store of a workload writers onto that workload's pipes.
pub(crate) struct Stdio(pub(crate) Pipes);

impl Stdio {
    fn pipe(
        &self,
        workload_id: &str,
        stream: impl FnOnce(&WorkloadPipes) -> &Arc<Pipe>,
    ) -> Box<dyn StdoutStream + Sync> {
        let pipes = self.0.lock().expect("pipes lock");
        let workload = pipes
            .get(workload_id)
            .expect("a workload's pipes are registered before it starts");
        Box::new(PipeStream(Arc::clone(stream(workload))))
    }
}

impl GuestStdio for Stdio {
    fn stdout(
        &self,
        workload_id: &str,
        _component_id: &str,
    ) -> Option<Box<dyn StdoutStream + Sync>> {
        Some(self.pipe(workload_id, |workload| &workload.stdout))
    }

    fn stderr(
        &self,
        workload_id: &str,
        _component_id: &str,
    ) -> Option<Box<dyn StdoutStream + Sync>> {
        Some(self.pipe(workload_id, |workload| &workload.stderr))
    }
}

/// A stream as wasmtime-wasi sees it: it asks for as many writers as the guest opens handles,
/// all onto the one pipe.
struct PipeStream(Arc<Pipe>);

impl IsTerminal for PipeStream {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for PipeStream {
    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(PipeWriter(Arc::clone(&self.0)))
    }
}

struct PipeWriter(Arc<Pipe>);

impl AsyncWrite for PipeWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.write(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// One stream of one workload.
struct Pipe {
    cartridge: CartridgeId,
    stream: &'static str,
    state: Mutex<PipeState>,
}

struct PipeState {
    /// The octets of the line being written, up to [`LINE_OCTETS`].
    line: Vec<u8>,
    /// The current line is past [`LINE_OCTETS`]: its remainder is discarded up to the newline.
    overlong: bool,
    tail: VecDeque<u8>,
    window_start: Instant,
    lines_in_window: u32,
    dropped: u64,
}

impl Pipe {
    fn new(cartridge: CartridgeId, stream: &'static str) -> Self {
        Self {
            cartridge,
            stream,
            state: Mutex::new(PipeState {
                line: Vec::new(),
                overlong: false,
                tail: VecDeque::new(),
                window_start: Instant::now(),
                lines_in_window: 0,
                dropped: 0,
            }),
        }
    }

    fn tail(&self) -> String {
        let mut state = self.state.lock().expect("pipe lock");
        String::from_utf8_lossy(state.tail.make_contiguous()).into_owned()
    }

    fn write(&self, buf: &[u8]) {
        let mut state = self.state.lock().expect("pipe lock");
        state.tail.extend(buf);
        let excess = state.tail.len().saturating_sub(TAIL_OCTETS);
        state.tail.drain(..excess);
        for piece in buf.split_inclusive(|octet| *octet == b'\n') {
            let (text, ended) = match piece.strip_suffix(b"\n") {
                Some(text) => (text, true),
                None => (piece, false),
            };
            if !state.overlong {
                let room = LINE_OCTETS - state.line.len();
                state.line.extend_from_slice(&text[..text.len().min(room)]);
                state.overlong = text.len() > room;
            }
            if ended {
                self.emit(&mut state);
            }
        }
    }

    /// Ends the current line: an event if the stream is within its rate, else a count.
    fn emit(&self, state: &mut PipeState) {
        let line = std::mem::take(&mut state.line);
        let cut = std::mem::take(&mut state.overlong);
        if state.window_start.elapsed() >= Duration::from_secs(1) {
            self.report_dropped(std::mem::take(&mut state.dropped));
            state.window_start = Instant::now();
            state.lines_in_window = 0;
        }
        if state.lines_in_window >= LINES_PER_SECOND {
            state.dropped += 1;
            return;
        }
        state.lines_in_window += 1;
        tracing::info!(
            target: "guest",
            cartridge = %self.cartridge,
            stream = self.stream,
            cut,
            "{}",
            String::from_utf8_lossy(&line)
        );
    }

    fn report_dropped(&self, dropped: u64) {
        if dropped > 0 {
            tracing::warn!(
                target: "guest",
                cartridge = %self.cartridge,
                stream = self.stream,
                dropped,
                "guest output lines dropped over the rate limit"
            );
        }
    }
}

impl Drop for Pipe {
    /// A guest that went quiet after a burst still has its drop count reported.
    fn drop(&mut self) {
        let dropped = std::mem::take(&mut self.state.get_mut().expect("pipe lock").dropped);
        self.report_dropped(dropped);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipe() -> Pipe {
        Pipe::new(
            big_cartridges_core::example_cartridge!("stdio-test")
                .parse()
                .unwrap(),
            "stderr",
        )
    }

    #[test]
    fn the_tail_keeps_only_the_last_octets_written() {
        let pipe = pipe();
        pipe.write(&vec![b'a'; TAIL_OCTETS]);
        pipe.write(b"last\n");
        let tail = pipe.tail();
        assert_eq!(tail.len(), TAIL_OCTETS);
        assert!(tail.ends_with("aaaalast\n"));
    }

    #[test]
    fn an_overlong_line_is_cut_and_its_remainder_discarded_to_the_newline() {
        let pipe = pipe();
        pipe.write(&vec![b'x'; LINE_OCTETS * 3]);
        pipe.write(b"still the same line\nnext\n");
        let state = pipe.state.lock().unwrap();
        assert!(
            state.line.is_empty() && !state.overlong,
            "next line starts clean"
        );
        // Two lines ended; both were within the rate.
        assert_eq!(state.lines_in_window, 2);
    }

    #[test]
    fn lines_over_the_rate_are_counted_not_emitted() {
        let pipe = pipe();
        for _ in 0..LINES_PER_SECOND + 7 {
            pipe.write(b"spam\n");
        }
        let state = pipe.state.lock().unwrap();
        assert_eq!(state.lines_in_window, LINES_PER_SECOND);
        assert_eq!(state.dropped, 7);
    }
}
