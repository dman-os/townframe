//! Host plugin implementation for townframe:pglite interfaces

use crate::interlude::*;

use std::collections::HashSet;

use wash_runtime::engine::ctx::Ctx as WashCtx;
use wash_runtime::wit::{WitInterface, WitWorld};

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::{Message, DataRowBody};
use postgres_protocol::types as pg_types;
use postgres_protocol::Oid;

use crate::{start_pglite, Config, PgResponse, PgliteHandle};

mod binds_guest {
    wash_runtime::wasmtime::component::bindgen!({
        world: "guest",
        path: "wit",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        additional_derives: [serde::Serialize, serde::Deserialize],
    });
}

pub use binds_guest::townframe::pglite::{query, types};

/// Host plugin providing pglite interfaces
pub struct PglitePlugin {
    handle: Arc<PgliteHandle>,
}

impl PglitePlugin {
    /// Create a new pglite plugin with the given configuration
    pub async fn new(config: Config) -> Res<Self> {
        info!("Initializing pglite plugin...");
        let handle = start_pglite(config).await?;
        Ok(Self {
            handle: Arc::new(handle),
        })
    }

    /// Create a new pglite plugin with default XDG paths
    pub async fn with_defaults() -> Res<Self> {
        let config = Config::new()?;
        Self::new(config).await
    }

    pub const ID: &'static str = "townframe:pglite";

    fn from_ctx(wcx: &WashCtx) -> Arc<Self> {
        let Some(this) = wcx.get_plugin::<Self>(Self::ID) else {
            panic!("pglite plugin not on ctx");
        };
        this
    }

    /// Get the pglite handle (already initialized)
    fn get_handle(&self) -> Arc<PgliteHandle> {
        Arc::clone(&self.handle)
    }
}

#[async_trait]
impl wash_runtime::plugin::HostPlugin for PglitePlugin {
    fn id(&self) -> &'static str {
        Self::ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: HashSet::new(),
            imports: HashSet::from([WitInterface::from("townframe:pglite/query")]),
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_bind(
        &self,
        _workload: &wash_runtime::engine::workload::UnresolvedWorkload,
        _interface_configs: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_component_bind(
        &self,
        component: &mut wash_runtime::engine::workload::WorkloadComponent,
        _interface_configs: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let world = component.world();
        for iface in world.imports {
            if iface.namespace == "townframe"
                && iface.package == "pglite"
                && iface.interfaces.contains("query")
            {
                query::add_to_linker::<_, wasmtime::component::HasSelf<WashCtx>>(
                    component.linker(),
                    |ctx| ctx,
                )?;
            }
        }
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        _resolved: &wash_runtime::engine::workload::ResolvedWorkload,
        _component_id: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        _workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        // Shutdown pglite
        let _ = self.handle.shutdown().await;
        Ok(())
    }
}

impl query::Host for WashCtx {
    async fn query(
        &mut self,
        query_str: String,
        _params: Vec<types::PgValue>,
    ) -> wasmtime::Result<Result<Vec<types::ResultRow>, types::QueryError>> {
        let plugin = PglitePlugin::from_ctx(self);

        // Get handle (already initialized)
        let handle = plugin.get_handle();

        // Execute query
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        if let Err(e) = handle.query(query_str.clone(), tx).await {
            return Ok(Err(types::QueryError::Unexpected(format!(
                "failed to send query: {}",
                e
            ))));
        }

        // Collect response data
        let mut response_data = Vec::new();
        let mut error_msg: Option<String> = None;

        while let Some(response) = rx.recv().await {
            match response {
                PgResponse::Data(data) => {
                    response_data.extend(data);
                }
                PgResponse::Done => {
                    break;
                }
                PgResponse::Error(e) => {
                    error_msg = Some(e);
                    break;
                }
            }
        }

        if let Some(err) = error_msg {
            return Ok(Err(types::QueryError::InvalidQuery(err)));
        }

        // Parse wire protocol response into result rows using postgres-protocol
        let rows = parse_wire_response(&response_data);

        Ok(Ok(rows))
    }

    async fn query_batch(
        &mut self,
        query_str: String,
    ) -> wasmtime::Result<Result<(), types::QueryError>> {
        let plugin = PglitePlugin::from_ctx(self);

        // Get handle (already initialized)
        let handle = plugin.get_handle();

        // Execute query
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        if let Err(e) = handle.query(query_str, tx).await {
            return Ok(Err(types::QueryError::Unexpected(format!(
                "failed to send query: {}",
                e
            ))));
        }

        // Wait for completion
        while let Some(response) = rx.recv().await {
            match response {
                PgResponse::Done => {
                    break;
                }
                PgResponse::Error(e) => {
                    return Ok(Err(types::QueryError::InvalidQuery(e)));
                }
                PgResponse::Data(_) => {
                    // Ignore data for batch queries
                }
            }
        }

        Ok(Ok(()))
    }
}

/// Column metadata from RowDescription
#[derive(Clone)]
struct ColumnInfo {
    name: String,
    type_oid: Oid,
}

/// Parse wire protocol response into result rows using postgres-protocol
fn parse_wire_response(data: &[u8]) -> Vec<types::ResultRow> {
    let mut buf = BytesMut::from(data);
    let mut columns: Vec<ColumnInfo> = Vec::new();
    let mut rows = Vec::new();

    loop {
        match Message::parse(&mut buf) {
            Ok(Some(Message::RowDescription(body))) => {
                // Extract column names and type OIDs
                columns.clear();
                let mut fields = body.fields();
                while let Ok(Some(field)) = fields.next() {
                    columns.push(ColumnInfo {
                        name: field.name().to_string(),
                        type_oid: field.type_oid(),
                    });
                }
            }
            Ok(Some(Message::DataRow(body))) => {
                // Parse row data
                if columns.is_empty() {
                    continue;
                }
                if let Some(row) = parse_data_row(&body, &columns) {
                    rows.push(row);
                }
            }
            Ok(Some(Message::ReadyForQuery(_))) => {
                // Done - ReadyForQuery indicates end of response
                break;
            }
            Ok(Some(_)) => {
                // Skip other messages (CommandComplete, etc.)
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

    rows
}

/// Parse DataRow message to get row values using postgres-protocol
fn parse_data_row(body: &DataRowBody, columns: &[ColumnInfo]) -> Option<types::ResultRow> {
    let mut row = Vec::new();
    let mut ranges = body.ranges();
    let buffer = body.buffer();

    let mut i = 0;
    while let Ok(Some(range_opt)) = ranges.next() {
        // FIXME: panic on unkown instead
        let column_info = columns.get(i).cloned().unwrap_or_else(|| ColumnInfo {
            name: format!("col{}", i),
            type_oid: 0, // Unknown type
        });

        let value = match range_opt {
            Some(range) => {
                // Non-null value - parse based on type OID
                let value_bytes = &buffer[range];
                parse_value_by_oid(value_bytes, column_info.type_oid)
            }
            None => {
                // NULL value
                types::PgValue::Null
            }
        };

        row.push(types::ResultRowEntry {
            column_name: column_info.name,
            value,
        });
        i += 1;
    }

    Some(row)
}

/// Parse a value based on PostgreSQL type OID
fn parse_value_by_oid(bytes: &[u8], oid: Oid) -> types::PgValue {
    // Common PostgreSQL type OIDs
    // See: https://www.postgresql.org/docs/current/datatype-oid.html
    match oid {
        // BOOL = 16
        16 => {
            if let Ok(b) = pg_types::bool_from_sql(bytes) {
                types::PgValue::Bool(b)
            } else {
                // Fallback to text
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // INT2 = 21
        21 => {
            if let Ok(i) = pg_types::int2_from_sql(bytes) {
                types::PgValue::Int2(i)
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // INT4 = 23
        23 => {
            match pg_types::int4_from_sql(bytes) {
                Ok(i) => types::PgValue::Int4(i),
                Err(_) => {
                    // If parsing fails, try manual parsing as fallback
                    // (postgres-protocol may fail on some text format values)
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        if let Ok(i) = s.parse::<i32>() {
                            types::PgValue::Int4(i)
                        } else {
                            types::PgValue::Text(s.to_string())
                        }
                    } else {
                        types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                    }
                }
            }
        }
        // INT8 = 20
        20 => {
            if let Ok(i) = pg_types::int8_from_sql(bytes) {
                types::PgValue::Int8(i)
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // TEXT = 25, VARCHAR = 1043, CHAR = 1042, NAME = 19
        25 | 1043 | 1042 | 19 => {
            if let Ok(s) = pg_types::text_from_sql(bytes) {
                types::PgValue::Text(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // BYTEA = 17
        17 => {
            // bytea_from_sql returns &[u8] directly (not a Result)
            let decoded = pg_types::bytea_from_sql(bytes);
            types::PgValue::Bytea(decoded.to_vec())
        }
        // FLOAT4 = 700
        700 => {
            if let Ok(f) = pg_types::float4_from_sql(bytes) {
                // Convert f32 to hashable-f64 format
                let bits = f.to_bits();
                types::PgValue::Float4((bits as u64, 0i16, 0i8))
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // FLOAT8 = 701
        701 => {
            if let Ok(f) = pg_types::float8_from_sql(bytes) {
                // Convert f64 to hashable-f64 format
                let bits = f.to_bits();
                types::PgValue::Float8((bits, 0i16, 0i8))
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        _ => {
            // Unknown type - try to parse as text, fallback to bytea
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Try parsing as integer if it looks like one
                if let Ok(i) = s.parse::<i32>() {
                    types::PgValue::Int4(i)
                } else if let Ok(i) = s.parse::<i64>() {
                    types::PgValue::Int8(i)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Bytea(bytes.to_vec())
            }
        }
    }
}

