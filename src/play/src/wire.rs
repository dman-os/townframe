//! Wire session - handles communication with the pglite WASM instance

use std::ffi::OsString;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use tracing::debug;
use wasmtime::{Engine, Instance, Memory, Module, Store, TypedFunc};
use wasmtime_wasi::p1::WasiP1Ctx;
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtxBuilder};

use crate::protocol;
use crate::Config;

/// WASI state held in the Store
pub struct WasiState {
    pub wasi: WasiP1Ctx,
}

/// Transport mode for communication with pglite
enum Transport {
    /// Contiguous Memory Area - direct memory access
    Cma { pending_len: usize },
    /// File-based transport using lock files
    File { paths: FilePaths },
}

/// Paths for file-based transport
struct FilePaths {
    /// Input to pglite (our queries)
    sinput: PathBuf,
    slock: PathBuf,
    /// Output from pglite (responses)
    cinput: PathBuf,
    #[allow(dead_code)]
    clock: PathBuf,
}

/// Wire session managing communication with pglite WASM instance
pub struct WireSession {
    config: Config,
    store: Store<WasiState>,
    #[allow(dead_code)]
    instance: Instance,
    memory: Memory,

    // WASM exports
    interactive_write: TypedFunc<i32, ()>,
    interactive_one: TypedFunc<(), ()>,
    interactive_read: TypedFunc<(), i32>,
    use_wire: Option<TypedFunc<i32, ()>>,
    #[allow(dead_code)]
    clear_error: Option<TypedFunc<(), ()>>,

    // Buffer info
    buffer_addr: usize,
    buffer_size: usize,

    // Transport state
    transport: Transport,
    handshake_done: bool,
}

impl WireSession {
    /// Create a new wire session
    pub fn new(config: &Config, engine: &Engine, module: &Module) -> Result<Self> {
        let wasi = build_wasi_ctx(config)?;
        let mut store = Store::new(engine, WasiState { wasi });

        let mut linker = wasmtime::Linker::<WasiState>::new(engine);
        wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state| &mut state.wasi)?;

        let instance = linker.instantiate(&mut store, module)?;

        // Run _start for initial setup
        if let Ok(start) = instance.get_typed_func::<(), ()>(&mut store, "_start") {
            let _ = start.call(&mut store, ());
        }

        // Run initdb if needed
        if let Ok(initdb) = instance.get_typed_func::<(), i32>(&mut store, "pgl_initdb") {
            let _ = initdb.call(&mut store, ());
        }

        // Start backend
        if let Ok(backend) = instance.get_typed_func::<(), ()>(&mut store, "pgl_backend") {
            let _ = backend.call(&mut store, ());
        }

        // Get memory export
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("missing 'memory' export")?;

        // Get required exports
        let interactive_write = instance
            .get_typed_func::<i32, ()>(&mut store, "interactive_write")
            .context("missing 'interactive_write' export")?;
        let interactive_one = instance
            .get_typed_func::<(), ()>(&mut store, "interactive_one")
            .context("missing 'interactive_one' export")?;
        let interactive_read = instance
            .get_typed_func::<(), i32>(&mut store, "interactive_read")
            .context("missing 'interactive_read' export")?;

        // Get channel info to determine transport mode
        let get_channel = instance
            .get_typed_func::<(), i32>(&mut store, "get_channel")
            .context("missing 'get_channel' export")?;
        let channel = get_channel.call(&mut store, ())?;

        let get_buffer_addr = instance
            .get_typed_func::<i32, i32>(&mut store, "get_buffer_addr")
            .context("missing 'get_buffer_addr' export")?;
        let buffer_addr = get_buffer_addr.call(&mut store, channel)? as usize;

        let get_buffer_size = instance
            .get_typed_func::<i32, i32>(&mut store, "get_buffer_size")
            .context("missing 'get_buffer_size' export")?;
        let buffer_size = get_buffer_size.call(&mut store, channel)? as usize;

        debug!(
            "transport channel={} addr={} size={}",
            channel, buffer_addr, buffer_size
        );

        // Determine transport based on channel
        let transport = if channel >= 0 {
            Transport::Cma { pending_len: 0 }
        } else {
            let io_socket = config.pgdata.join(".s.PGSQL.5432");
            Transport::File {
                paths: FilePaths {
                    sinput: append_suffix(&io_socket, ".in"),
                    slock: append_suffix(&io_socket, ".lock.in"),
                    cinput: append_suffix(&io_socket, ".out"),
                    clock: append_suffix(&io_socket, ".lock.out"),
                },
            }
        };

        // Optional exports
        let use_wire = instance
            .get_typed_func::<i32, ()>(&mut store, "use_wire")
            .ok();
        let clear_error = instance
            .get_typed_func::<(), ()>(&mut store, "clear_error")
            .ok();

        Ok(Self {
            config: config.clone(),
            store,
            instance,
            memory,
            interactive_write,
            interactive_one,
            interactive_read,
            use_wire,
            clear_error,
            buffer_addr,
            buffer_size,
            transport,
            handshake_done: false,
        })
    }

    /// Perform PostgreSQL wire protocol handshake
    pub fn handshake(&mut self) -> Result<()> {
        if self.handshake_done {
            return Ok(());
        }

        self.clear_pending()?;

        // Build and send startup message
        let startup = protocol::build_startup_message("postgres", "template1");
        let mut response = self.run_wire(&startup)?;

        // Process handshake messages until ReadyForQuery
        loop {
            let (next_payload, done) = self.process_handshake_response(&response)?;
            if done {
                self.handshake_done = true;
                return Ok(());
            }
            if let Some(payload) = next_payload {
                response = self.run_wire(&payload)?;
            } else {
                anyhow::bail!("handshake did not complete");
            }
        }
    }

    /// Process handshake response, returning next payload to send (if any) and whether done
    fn process_handshake_response(&self, data: &[u8]) -> Result<(Option<Vec<u8>>, bool)> {
        let mut next_payload: Option<Vec<u8>> = None;
        let mut done = false;

        for msg in protocol::parse_messages(data)? {
            match msg.tag {
                b'R' => {
                    // Authentication
                    ensure!(msg.body.len() >= 4, "auth response too short");
                    let code = u32::from_be_bytes(msg.body[0..4].try_into().unwrap());
                    match code {
                        0 => {} // AuthenticationOk
                        3 => {
                            // Cleartext password
                            let pw = self.read_password()?;
                            next_payload = Some(protocol::build_password_message(pw.as_bytes()));
                        }
                        5 => {
                            // MD5 password
                            ensure!(msg.body.len() >= 8, "MD5 auth missing salt");
                            let salt: [u8; 4] = msg.body[4..8].try_into().unwrap();
                            let pw = self.read_password()?;
                            let hashed = protocol::build_md5_password(&pw, "postgres", &salt);
                            next_payload = Some(protocol::build_password_message(hashed.as_bytes()));
                        }
                        other => anyhow::bail!("unsupported auth method: {}", other),
                    }
                }
                b'S' => {
                    // ParameterStatus - just log
                    if let Some((key, value)) = protocol::parse_parameter_status(msg.body) {
                        debug!("parameter: {}={}", key, value);
                    }
                }
                b'K' => {
                    debug!("backend key data received");
                }
                b'Z' => {
                    // ReadyForQuery
                    done = true;
                }
                b'E' => {
                    let error_msg = protocol::extract_error_message(data);
                    anyhow::bail!("handshake error: {}", error_msg);
                }
                b'N' => {
                    // Notice - just log
                    let notice = protocol::extract_error_message(msg.body);
                    debug!("notice: {}", notice);
                }
                _ => {}
            }
        }

        Ok((next_payload, done))
    }

    /// Read password from password file
    fn read_password(&self) -> Result<String> {
        let path = self.config.pgroot.join("pglite").join("password");
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("read password file {:?}", path))?;
        Ok(contents.trim_end_matches(['\n', '\r']).to_string())
    }

    /// Send wire protocol data
    pub fn send(&mut self, payload: &[u8]) -> Result<()> {
        self.set_wire_mode(true)?;

        match &mut self.transport {
            Transport::Cma { pending_len } => {
                ensure!(
                    payload.len() <= self.buffer_size,
                    "payload {} exceeds buffer {}",
                    payload.len(),
                    self.buffer_size
                );
                self.memory
                    .write(&mut self.store, self.buffer_addr, payload)
                    .context("write to WASM memory")?;
                self.interactive_write
                    .call(&mut self.store, payload.len() as i32)
                    .context("call interactive_write")?;
                *pending_len = payload.len();
            }
            Transport::File { paths } => {
                if let Some(parent) = paths.sinput.parent() {
                    fs::create_dir_all(parent)?;
                }
                let _ = fs::remove_file(&paths.slock);
                fs::write(&paths.slock, payload)?;
                fs::rename(&paths.slock, &paths.sinput)?;
            }
        }

        Ok(())
    }

    /// Tick the backend - process one iteration
    pub fn tick(&mut self) -> Result<()> {
        self.interactive_one
            .call(&mut self.store, ())
            .context("call interactive_one")
    }

    /// Try to receive response data (non-blocking)
    pub fn try_recv(&mut self) -> Result<Option<Vec<u8>>> {
        match &mut self.transport {
            Transport::Cma { pending_len } => {
                let reply_len = self
                    .interactive_read
                    .call(&mut self.store, ())
                    .context("call interactive_read")? as usize;

                if reply_len == 0 {
                    return Ok(None);
                }

                // Response is at buffer_addr + pending_len + 1
                let base = self.buffer_addr + *pending_len + 1;
                ensure!(
                    base + reply_len <= self.memory.data_size(&self.store),
                    "reply overflows memory"
                );

                let mut buf = vec![0u8; reply_len];
                self.memory
                    .read(&mut self.store, base, &mut buf)
                    .context("read from WASM memory")?;

                // Clear pending
                self.interactive_write
                    .call(&mut self.store, 0)
                    .context("clear interactive_write")?;
                *pending_len = 0;

                Ok(Some(buf))
            }
            Transport::File { paths } => match fs::read(&paths.cinput) {
                Ok(data) => {
                    let _ = fs::remove_file(&paths.cinput);
                    let _ = fs::remove_file(&paths.clock);
                    Ok(Some(data))
                }
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            },
        }
    }

    /// Clear any pending data
    fn clear_pending(&mut self) -> Result<()> {
        if let Transport::Cma { pending_len } = &mut self.transport {
            self.interactive_write.call(&mut self.store, 0)?;
            *pending_len = 0;
        }
        Ok(())
    }

    /// Set wire protocol mode
    fn set_wire_mode(&mut self, enable: bool) -> Result<()> {
        if let Some(use_wire) = &self.use_wire {
            use_wire.call(&mut self.store, if enable { 1 } else { 0 })?;
        }
        Ok(())
    }

    /// Send data and collect all responses until done
    fn run_wire(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        self.send(payload)?;

        let mut combined = Vec::new();
        const MAX_TICKS: usize = 256;

        for _ in 0..MAX_TICKS {
            self.tick()?;
            if let Some(data) = self.try_recv()? {
                combined.extend(data);
                // Check if we have a complete response
                if protocol::contains_ready_for_query(&combined)
                    || protocol::contains_error(&combined)
                {
                    break;
                }
            }
        }

        if combined.is_empty() {
            anyhow::bail!("no response received");
        }

        Ok(combined)
    }
}

/// Create a wasmtime Engine
pub fn create_engine() -> Result<Engine> {
    let mut cfg = wasmtime::Config::new();
    cfg.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
    Engine::new(&cfg).context("create wasmtime engine")
}

/// Load module with pre-compilation caching
pub fn load_module(engine: &Engine, config: &Config) -> Result<Module> {
    let cwasm_path = config.cwasm_path();
    let wasm_path = config.wasm_path();

    if cwasm_path.exists() {
        debug!("loading pre-compiled module from {:?}", cwasm_path);
        // SAFETY: We control the cwasm file and trust it
        unsafe {
            Module::deserialize_file(engine, &cwasm_path).context("deserialize cached module")
        }
    } else {
        debug!("compiling module from {:?}", wasm_path);
        let module = Module::from_file(engine, &wasm_path).context("compile module")?;

        // Cache for next time
        match module.serialize() {
            Ok(bytes) => {
                if let Err(e) = fs::write(&cwasm_path, bytes) {
                    tracing::warn!("failed to cache compiled module: {}", e);
                }
            }
            Err(e) => {
                tracing::warn!("failed to serialize module: {}", e);
            }
        }

        Ok(module)
    }
}

/// Build WASI context for pglite
pub fn build_wasi_ctx(config: &Config) -> Result<WasiP1Ctx> {
    // Ensure directories exist
    fs::create_dir_all(&config.pgroot)?;
    fs::create_dir_all(&config.pgdata)?;
    fs::create_dir_all(config.dev_path())?;

    let mut builder = WasiCtxBuilder::new();
    builder
        .inherit_stdin()
        .inherit_stdout()
        .inherit_stderr()
        .preopened_dir(&config.pgroot, "/tmp", DirPerms::all(), FilePerms::all())?
        .preopened_dir(
            &config.pgdata,
            "/tmp/pglite/base",
            DirPerms::all(),
            FilePerms::all(),
        )?
        .preopened_dir(
            config.dev_path(),
            "/dev",
            DirPerms::all(),
            FilePerms::all(),
        )?
        .env("ENVIRONMENT", "wasm32_wasi_preview1")
        .env("PREFIX", "/tmp/pglite")
        .env("PGDATA", "/tmp/pglite/base")
        .env("PGSYSCONFDIR", "/tmp/pglite")
        .env("PGUSER", "postgres")
        .env("PGDATABASE", "template1")
        .env("MODE", "REACT")
        .env("REPL", "N")
        .env("TZ", "UTC")
        .env("PGTZ", "UTC")
        .env("PATH", "/tmp/pglite/bin");

    Ok(builder.build_p1())
}

/// Append a suffix to a path
fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut os: OsString = path.as_os_str().to_os_string();
    os.push(suffix);
    PathBuf::from(os)
}
