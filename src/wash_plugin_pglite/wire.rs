//! Wire session - handles communication with the pglite WASM instance

use crate::interlude::*;

use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use tokio::fs;

use wasmtime::{Engine, Instance, Memory, Module, Store, TypedFunc};
use wasmtime_wasi::p1::WasiP1Ctx;
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtxBuilder};

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
    /// Create a new wire session (async)
    pub async fn new(config: &Config, engine: &Engine, module: &Module) -> Res<Self> {
        let wasi = build_wasi_ctx(config).await?;
        let mut store = Store::new(engine, WasiState { wasi });
        store.set_epoch_deadline(u64::MAX);

        let mut linker = wasmtime::Linker::<WasiState>::new(engine);
        wasmtime_wasi::p1::add_to_linker_async(&mut linker, |state| &mut state.wasi).to_eyre()?;

        let instance = linker
            .instantiate_async(&mut store, module)
            .await
            .to_eyre()?;

        // Run _start for initial setup
        if let Ok(start) = instance.get_typed_func::<(), ()>(&mut store, "_start") {
            let _ = start.call_async(&mut store, ()).await;
        }

        // Run initdb if needed
        if let Ok(initdb) = instance.get_typed_func::<(), i32>(&mut store, "pgl_initdb") {
            let _ = initdb.call_async(&mut store, ()).await;
        }

        // Start backend
        if let Ok(backend) = instance.get_typed_func::<(), ()>(&mut store, "pgl_backend") {
            let _ = backend.call_async(&mut store, ()).await;
        }

        // Get memory export
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| ferr!("missing 'memory' export"))?;

        // Get required exports
        let interactive_write = instance
            .get_typed_func::<i32, ()>(&mut store, "interactive_write")
            .to_eyre()
            .wrap_err("missing 'interactive_write' export")?;
        let interactive_one = instance
            .get_typed_func::<(), ()>(&mut store, "interactive_one")
            .to_eyre()
            .wrap_err("missing 'interactive_one' export")?;
        let interactive_read = instance
            .get_typed_func::<(), i32>(&mut store, "interactive_read")
            .to_eyre()
            .wrap_err("missing 'interactive_read' export")?;

        // Get channel info to determine transport mode
        let get_channel = instance
            .get_typed_func::<(), i32>(&mut store, "get_channel")
            .to_eyre()
            .wrap_err("missing 'get_channel' export")?;
        let channel = get_channel.call_async(&mut store, ()).await.to_eyre()?;

        let get_buffer_addr = instance
            .get_typed_func::<i32, i32>(&mut store, "get_buffer_addr")
            .to_eyre()
            .wrap_err("missing 'get_buffer_addr' export")?;
        let buffer_addr = get_buffer_addr
            .call_async(&mut store, channel)
            .await
            .to_eyre()? as usize;

        let get_buffer_size = instance
            .get_typed_func::<i32, i32>(&mut store, "get_buffer_size")
            .to_eyre()
            .wrap_err("missing 'get_buffer_size' export")?;
        let buffer_size = get_buffer_size
            .call_async(&mut store, channel)
            .await
            .to_eyre()? as usize;

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
    pub async fn handshake(&mut self) -> Res<()> {
        if self.handshake_done {
            return Ok(());
        }

        self.clear_pending().await?;

        // Build and send startup message
        use bytes::BytesMut;
        use postgres_protocol::message::frontend;
        let mut startup_buf = BytesMut::new();
        let params = [
            ("user", "postgres"),
            ("database", "template1"),
            ("client_encoding", "UTF8"),
            ("application_name", "wash-pglite"),
        ];
        frontend::startup_message(params.iter().map(|(k, v)| (*k, *v)), &mut startup_buf).unwrap();
        let startup = startup_buf.to_vec();
        let mut response = self.run_wire(&startup).await?;

        // Process handshake messages until ReadyForQuery
        loop {
            let (next_payload, done) = Self::process_handshake_response(&response, &self.config)?;
            if done {
                self.handshake_done = true;
                return Ok(());
            }
            if let Some(payload) = next_payload {
                response = self.run_wire(&payload).await?;
            } else {
                eyre::bail!("handshake did not complete");
            }
        }
    }

    /// Process handshake response, returning next payload to send (if any) and whether done
    fn process_handshake_response(data: &[u8], config: &Config) -> Res<(Option<Vec<u8>>, bool)> {
        use bytes::BytesMut;
        use postgres_protocol::message::backend::Message;

        let mut next_payload: Option<Vec<u8>> = None;
        let mut done = false;
        let mut buf = BytesMut::from(data);

        loop {
            match Message::parse(&mut buf) {
                Ok(Some(Message::AuthenticationOk)) => {
                    // Authentication successful
                }
                Ok(Some(Message::AuthenticationCleartextPassword)) => {
                    // Cleartext password
                    use postgres_protocol::message::frontend;
                    let pw = Self::read_password_sync(config)?;
                    let mut pw_buf = BytesMut::new();
                    frontend::password_message(pw.as_bytes(), &mut pw_buf).unwrap();
                    next_payload = Some(pw_buf.to_vec());
                }
                Ok(Some(Message::AuthenticationMd5Password(body))) => {
                    // MD5 password
                    use postgres_protocol::authentication::md5_hash;
                    use postgres_protocol::message::frontend;
                    let salt = body.salt();
                    let pw = Self::read_password_sync(config)?;
                    let hashed = md5_hash(b"postgres", pw.as_bytes(), salt);
                    let mut pw_buf = BytesMut::new();
                    frontend::password_message(hashed.as_bytes(), &mut pw_buf).unwrap();
                    next_payload = Some(pw_buf.to_vec());
                }
                Ok(Some(Message::ParameterStatus(ps))) => {
                    // ParameterStatus - just log
                    if let (Ok(name), Ok(value)) = (ps.name(), ps.value()) {
                        debug!("parameter: {}={}", name, value);
                    }
                }
                Ok(Some(Message::AuthenticationSasl(_)))
                | Ok(Some(Message::AuthenticationSaslContinue(_)))
                | Ok(Some(Message::AuthenticationSaslFinal(_))) => {
                    eyre::bail!("SASL authentication not supported");
                }
                Ok(Some(Message::BackendKeyData(_))) => {
                    debug!("backend key data received");
                }
                Ok(Some(Message::ReadyForQuery(_))) => {
                    // ReadyForQuery
                    done = true;
                    break;
                }
                Ok(Some(Message::ErrorResponse(err))) => {
                    // Extract error message
                    use fallible_iterator::FallibleIterator;
                    let mut fields = err.fields();
                    let mut error_msg = String::new();
                    while let Ok(Some(field)) = fields.next() {
                        if field.type_() == b'M' {
                            error_msg = String::from_utf8_lossy(field.value_bytes()).to_string();
                            break;
                        }
                    }
                    if error_msg.is_empty() {
                        error_msg = "handshake error".to_string();
                    }
                    eyre::bail!("handshake error: {}", error_msg);
                }
                Ok(Some(Message::NoticeResponse(notice))) => {
                    // Notice - just log
                    use fallible_iterator::FallibleIterator;
                    let mut fields = notice.fields();
                    while let Ok(Some(field)) = fields.next() {
                        if field.type_() == b'M' {
                            debug!("notice: {}", String::from_utf8_lossy(field.value_bytes()));
                            break;
                        }
                    }
                }
                Ok(Some(_)) => {
                    // Skip other messages
                    continue;
                }
                Ok(None) => {
                    // No more messages
                    break;
                }
                Err(_) => {
                    // Parse error - return what we have
                    break;
                }
            }
        }

        Ok((next_payload, done))
    }

    /// Read password from password file
    fn read_password_sync(config: &Config) -> Res<String> {
        use std::fs;
        let path = config.pgroot.join("pglite").join("password");
        let contents =
            fs::read_to_string(&path).wrap_err_with(|| format!("read password file {:?}", path))?;
        Ok(contents.trim_end_matches(['\n', '\r']).to_string())
    }

    /// Send wire protocol data (async)
    pub async fn send(&mut self, payload: &[u8]) -> Res<()> {
        self.set_wire_mode(true).await?;

        match &mut self.transport {
            Transport::Cma { pending_len } => {
                eyre::ensure!(
                    payload.len() <= self.buffer_size,
                    "payload {} exceeds buffer {}",
                    payload.len(),
                    self.buffer_size
                );
                self.memory
                    .write(&mut self.store, self.buffer_addr, payload)
                    .wrap_err("write to WASM memory")?;
                self.interactive_write
                    .call_async(&mut self.store, payload.len() as i32)
                    .await
                    .to_eyre()
                    .wrap_err("call interactive_write")?;
                *pending_len = payload.len();
            }
            Transport::File { paths } => {
                if let Some(parent) = paths.sinput.parent() {
                    fs::create_dir_all(parent).await?;
                }
                let _ = fs::remove_file(&paths.slock).await;
                fs::write(&paths.slock, payload).await?;
                fs::rename(&paths.slock, &paths.sinput).await?;
            }
        }

        Ok(())
    }

    /// Tick the backend - process one iteration (async)
    pub async fn tick(&mut self) -> Res<()> {
        self.interactive_one
            .call_async(&mut self.store, ())
            .await
            .map_err(|e| eyre::eyre!("call interactive_one failed: {}", e))
    }

    /// Try to receive response data (non-blocking, async)
    pub async fn try_recv(&mut self) -> Res<Option<Vec<u8>>> {
        match &mut self.transport {
            Transport::Cma { pending_len } => {
                let reply_len = self
                    .interactive_read
                    .call_async(&mut self.store, ())
                    .await
                    .to_eyre()
                    .wrap_err("call interactive_read")? as usize;

                if reply_len == 0 {
                    return Ok(None);
                }

                // Response is at buffer_addr + pending_len + 1
                let base = self.buffer_addr + *pending_len + 1;
                eyre::ensure!(
                    base + reply_len <= self.memory.data_size(&self.store),
                    "reply overflows memory"
                );

                let mut buf = vec![0u8; reply_len];
                self.memory
                    .read(&mut self.store, base, &mut buf)
                    .wrap_err("read from WASM memory")?;

                // Clear pending
                self.interactive_write
                    .call_async(&mut self.store, 0)
                    .await
                    .to_eyre()
                    .wrap_err("clear interactive_write")?;
                *pending_len = 0;

                Ok(Some(buf))
            }
            Transport::File { paths } => match fs::read(&paths.cinput).await {
                Ok(data) => {
                    let _ = fs::remove_file(&paths.cinput).await;
                    let _ = fs::remove_file(&paths.clock).await;
                    Ok(Some(data))
                }
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            },
        }
    }

    /// Clear any pending data (async)
    pub async fn clear_pending(&mut self) -> Res<()> {
        if let Transport::Cma { pending_len } = &mut self.transport {
            self.interactive_write
                .call_async(&mut self.store, 0)
                .await
                .to_eyre()?;
            *pending_len = 0;
        }
        Ok(())
    }

    /// Set wire protocol mode (async)
    async fn set_wire_mode(&mut self, enable: bool) -> Res<()> {
        if let Some(use_wire) = &self.use_wire {
            use_wire
                .call_async(&mut self.store, if enable { 1 } else { 0 })
                .await
                .to_eyre()?;
        }
        Ok(())
    }

    /// Send data and collect all responses until done (async)
    pub async fn run_wire(&mut self, payload: &[u8]) -> Res<Vec<u8>> {
        self.send(payload).await?;

        let mut combined = Vec::new();
        const MAX_TICKS: usize = 256;

        for _ in 0..MAX_TICKS {
            self.tick().await?;
            if let Some(data) = self.try_recv().await? {
                combined.extend(data);
                // Check if we have a complete response
                use bytes::BytesMut;
                use postgres_protocol::message::backend::Message;
                let mut check_buf = BytesMut::from(&combined[..]);
                let mut has_ready = false;
                let mut has_error = false;
                loop {
                    match Message::parse(&mut check_buf) {
                        Ok(Some(Message::ReadyForQuery(_))) => {
                            has_ready = true;
                            break;
                        }
                        Ok(Some(Message::ErrorResponse(_))) => {
                            has_error = true;
                            break;
                        }
                        Ok(Some(_)) => continue,
                        Ok(None) | Err(_) => break,
                    }
                }
                if has_ready || has_error {
                    break;
                }
            }
        }

        if combined.is_empty() {
            eyre::bail!("no response received");
        }

        Ok(combined)
    }
}

/// Load module with pre-compilation caching
pub async fn load_module(engine: &Engine, config: &Config) -> Res<Module> {
    let cwasm_path = config.cwasm_path();
    let wasm_path = config.wasm_path();

    if cwasm_path.exists() {
        debug!("loading pre-compiled module from {:?}", cwasm_path);
        // SAFETY: We control the cwasm file and trust it
        tokio::task::spawn_blocking({
            let engine = engine.clone();
            let cwasm_path = cwasm_path.clone();
            move || unsafe {
                Module::deserialize_file(&engine, &cwasm_path)
                    .to_eyre()
                    .wrap_err("deserialize cached module")
            }
        })
        .await
        .wrap_err("load module task panicked")?
    } else {
        debug!("compiling module from {:?}", wasm_path);
        let module = tokio::task::spawn_blocking({
            let engine = engine.clone();
            let wasm_path = wasm_path.clone();
            move || {
                Module::from_file(&engine, &wasm_path)
                    .to_eyre()
                    .wrap_err("compile module")
            }
        })
        .await
        .wrap_err("compile module task panicked")??;

        // Cache for next time
        match module.serialize() {
            Ok(bytes) => {
                if let Err(e) = fs::write(&cwasm_path, bytes).await {
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
pub async fn build_wasi_ctx(config: &Config) -> Res<WasiP1Ctx> {
    // Ensure directories exist
    fs::create_dir_all(&config.pgroot).await?;
    fs::create_dir_all(&config.pgdata).await?;
    fs::create_dir_all(config.dev_path()).await?;

    let mut builder = WasiCtxBuilder::new();
    builder.inherit_stdin();// .inherit_stdout().inherit_stderr();

    builder
        .preopened_dir(&config.pgroot, "/tmp", DirPerms::all(), FilePerms::all())
        .to_eyre()
        .wrap_err("preopened_dir /tmp")?;

    builder
        .preopened_dir(
            &config.pgdata,
            "/tmp/pglite/base",
            DirPerms::all(),
            FilePerms::all(),
        )
        .to_eyre()
        .wrap_err("preopened_dir /tmp/pglite/base")?;

    builder
        .preopened_dir(config.dev_path(), "/dev", DirPerms::all(), FilePerms::all())
        .to_eyre()
        .wrap_err("preopened_dir /dev")?;

    builder
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
