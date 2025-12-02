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
use uuid::Uuid;
use time::{Date, Time, PrimitiveDateTime, OffsetDateTime};

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

/// Convert PostgreSQL date (days since 2000-01-01) to WIT Date
fn pg_date_to_wit_date(days: i32) -> types::Date {
    // PostgreSQL date represents days since 2000-01-01
    // Handle special values
    if days == i32::MAX {
        return types::Date::PositiveInfinity;
    }
    if days == i32::MIN {
        return types::Date::NegativeInfinity;
    }
    
    // Calculate date from days since 2000-01-01
    let base_date = Date::from_calendar_date(2000, time::Month::January, 1)
        .expect("invalid base date");
    let date = base_date + time::Duration::days(days as i64);
    
    types::Date::Ymd((
        date.year(),
        date.month() as u32,
        date.day() as u32,
    ))
}

/// Convert PostgreSQL time (microseconds since midnight) to WIT Time
fn pg_time_to_wit_time(microseconds: i64) -> types::Time {
    let seconds = microseconds / 1_000_000;
    let micros = (microseconds % 1_000_000) as u32;
    
    let hour = (seconds / 3600) as u32;
    let min = ((seconds % 3600) / 60) as u32;
    let sec = (seconds % 60) as u32;
    
    types::Time {
        hour,
        min,
        sec,
        micro: micros,
    }
}

/// Parse timezone offset string (e.g., "+05:30", "-04:00") to WIT Offset
fn parse_timezone_offset(offset_str: &str) -> types::Offset {
    // Parse offset like "+05:30" or "-04:00"
    if offset_str.is_empty() {
        return types::Offset::WesternHemisphereSecs(0);
    }
    
    let sign = if offset_str.starts_with('+') { 1 } else if offset_str.starts_with('-') { -1 } else { return types::Offset::WesternHemisphereSecs(0) };
    let parts: Vec<&str> = offset_str[1..].split(':').collect();
    
    if parts.len() == 2 {
        if let (Ok(hours), Ok(mins)) = (parts[0].parse::<i32>(), parts[1].parse::<i32>()) {
            let total_seconds = sign * (hours * 3600 + mins * 60);
            if total_seconds >= 0 {
                return types::Offset::EasternHemisphereSecs(total_seconds);
            } else {
                return types::Offset::WesternHemisphereSecs(-total_seconds);
            }
        }
    } else if parts.len() == 1 {
        // Try parsing as just hours (e.g., "+05")
        if let Ok(hours) = parts[0].parse::<i32>() {
            let total_seconds = sign * hours * 3600;
            if total_seconds >= 0 {
                return types::Offset::EasternHemisphereSecs(total_seconds);
            } else {
                return types::Offset::WesternHemisphereSecs(-total_seconds);
            }
        }
    }
    
    // Default to UTC if parsing fails
    types::Offset::WesternHemisphereSecs(0)
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
                // If parsing fails, try manual parsing as fallback
                // (postgres-protocol may fail on some text format values)
                if let Ok(s) = std::str::from_utf8(bytes) {
                    match s {
                        "t" | "true" | "TRUE" | "1" => types::PgValue::Bool(true),
                        "f" | "false" | "FALSE" | "0" => types::PgValue::Bool(false),
                        _ => types::PgValue::Text(s.to_string()),
                    }
                } else {
                    types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                }
            }
        }
        // INT2 = 21
        21 => {
            if let Ok(i) = pg_types::int2_from_sql(bytes) {
                types::PgValue::Int2(i)
            } else {
                // If parsing fails, try manual parsing as fallback
                // (postgres-protocol may fail on some text format values)
                if let Ok(s) = std::str::from_utf8(bytes) {
                    if let Ok(i) = s.parse::<i16>() {
                        types::PgValue::Int2(i)
                    } else {
                        types::PgValue::Text(s.to_string())
                    }
                } else {
                    types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                }
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
                // If parsing fails, try manual parsing as fallback
                // (postgres-protocol may fail on some text format values)
                if let Ok(s) = std::str::from_utf8(bytes) {
                    if let Ok(i) = s.parse::<i64>() {
                        types::PgValue::Int8(i)
                    } else {
                        types::PgValue::Text(s.to_string())
                    }
                } else {
                    types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                }
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
                // If parsing fails, try manual parsing as fallback
                // (postgres-protocol may fail on some text format values)
                if let Ok(s) = std::str::from_utf8(bytes) {
                    if let Ok(f) = s.parse::<f32>() {
                        let bits = f.to_bits();
                        types::PgValue::Float4((bits as u64, 0i16, 0i8))
                    } else {
                        types::PgValue::Text(s.to_string())
                    }
                } else {
                    types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                }
            }
        }
        // FLOAT8 = 701
        701 => {
            if let Ok(f) = pg_types::float8_from_sql(bytes) {
                // Convert f64 to hashable-f64 format
                let bits = f.to_bits();
                types::PgValue::Float8((bits, 0i16, 0i8))
            } else {
                // If parsing fails, try manual parsing as fallback
                // (postgres-protocol may fail on some text format values)
                if let Ok(s) = std::str::from_utf8(bytes) {
                    if let Ok(f) = s.parse::<f64>() {
                        let bits = f.to_bits();
                        types::PgValue::Float8((bits, 0i16, 0i8))
                    } else {
                        types::PgValue::Text(s.to_string())
                    }
                } else {
                    types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                }
            }
        }
        // DATE = 1082
        1082 => {
            match pg_types::date_from_sql(bytes) {
                Ok(days) => {
                    types::PgValue::Date(pg_date_to_wit_date(days))
                }
                Err(_) => {
                    // Fallback: try parsing text format (e.g., "2024-01-15")
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        if let Ok(date) = Date::parse(s, &time::format_description::well_known::Iso8601::DATE) {
                            types::PgValue::Date(types::Date::Ymd((
                                date.year(),
                                date.month() as u32,
                                date.day() as u32,
                            )))
                        } else {
                            types::PgValue::Text(s.to_string())
                        }
                    } else {
                        types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                    }
                }
            }
        }
        // TIME = 1083
        1083 => {
            match pg_types::time_from_sql(bytes) {
                Ok(microseconds) => {
                    types::PgValue::Time(pg_time_to_wit_time(microseconds))
                }
                Err(_) => {
                    // Fallback: try parsing text format (e.g., "12:34:56.789")
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        // Try parsing with microseconds
                        if let Ok(time) = Time::parse(s, &time::format_description::well_known::Iso8601::TIME) {
                            types::PgValue::Time(types::Time {
                                hour: time.hour() as u32,
                                min: time.minute() as u32,
                                sec: time.second() as u32,
                                micro: time.nanosecond() / 1000,
                            })
                        } else {
                            types::PgValue::Text(s.to_string())
                        }
                    } else {
                        types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                    }
                }
            }
        }
        // TIMESTAMP = 1114
        1114 => {
            match pg_types::timestamp_from_sql(bytes) {
                Ok(microseconds) => {
                    // PostgreSQL timestamp represents microseconds since 2000-01-01 00:00:00
                    let base_datetime = PrimitiveDateTime::new(
                        Date::from_calendar_date(2000, time::Month::January, 1).expect("invalid base date"),
                        Time::MIDNIGHT,
                    );
                    let datetime = base_datetime + time::Duration::microseconds(microseconds);
                    
                    types::PgValue::Timestamp(types::Timestamp {
                        date: types::Date::Ymd((
                            datetime.date().year(),
                            datetime.date().month() as u32,
                            datetime.date().day() as u32,
                        )),
                        time: types::Time {
                            hour: datetime.time().hour() as u32,
                            min: datetime.time().minute() as u32,
                            sec: datetime.time().second() as u32,
                            micro: datetime.time().nanosecond() / 1000,
                        },
                    })
                }
                Err(_) => {
                    // Fallback: try parsing text format (e.g., "2024-01-15 12:34:56")
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        if let Ok(datetime) = PrimitiveDateTime::parse(s, &time::format_description::well_known::Iso8601::DATE_TIME) {
                            types::PgValue::Timestamp(types::Timestamp {
                                date: types::Date::Ymd((
                                    datetime.date().year(),
                                    datetime.date().month() as u32,
                                    datetime.date().day() as u32,
                                )),
                                time: types::Time {
                                    hour: datetime.time().hour() as u32,
                                    min: datetime.time().minute() as u32,
                                    sec: datetime.time().second() as u32,
                                    micro: datetime.time().nanosecond() / 1000,
                                },
                            })
                        } else {
                            types::PgValue::Text(s.to_string())
                        }
                    } else {
                        types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                    }
                }
            }
        }
        // TIMESTAMPTZ = 1184
        1184 => {
            match pg_types::timestamp_from_sql(bytes) {
                Ok(microseconds) => {
                    // PostgreSQL timestamptz represents microseconds since 2000-01-01 00:00:00 UTC
                    let base_datetime = OffsetDateTime::new_utc(
                        Date::from_calendar_date(2000, time::Month::January, 1).expect("invalid base date"),
                        Time::MIDNIGHT,
                    );
                    let datetime = base_datetime + time::Duration::microseconds(microseconds);
                    
                    // Extract offset (default to UTC)
                    let offset = datetime.offset();
                    let offset_secs = offset.whole_seconds();
                    let offset_variant = if offset_secs >= 0 {
                        types::Offset::EasternHemisphereSecs(offset_secs)
                    } else {
                        types::Offset::WesternHemisphereSecs(-offset_secs)
                    };
                    
                    types::PgValue::TimestampTz(types::TimestampTz {
                        timestamp: types::Timestamp {
                            date: types::Date::Ymd((
                                datetime.date().year(),
                                datetime.date().month() as u32,
                                datetime.date().day() as u32,
                            )),
                            time: types::Time {
                                hour: datetime.time().hour() as u32,
                                min: datetime.time().minute() as u32,
                                sec: datetime.time().second() as u32,
                                micro: datetime.time().nanosecond() / 1000,
                            },
                        },
                        offset: offset_variant,
                    })
                }
                Err(_) => {
                    // Fallback: try parsing text format (e.g., "2024-01-15 12:34:56+00")
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        if let Ok(datetime) = OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DATE_TIME) {
                            let offset_secs = datetime.offset().whole_seconds();
                            let offset_variant = if offset_secs >= 0 {
                                types::Offset::EasternHemisphereSecs(offset_secs)
                            } else {
                                types::Offset::WesternHemisphereSecs(-offset_secs)
                            };
                            
                            types::PgValue::TimestampTz(types::TimestampTz {
                                timestamp: types::Timestamp {
                                    date: types::Date::Ymd((
                                        datetime.date().year(),
                                        datetime.date().month() as u32,
                                        datetime.date().day() as u32,
                                    )),
                                    time: types::Time {
                                        hour: datetime.time().hour() as u32,
                                        min: datetime.time().minute() as u32,
                                        sec: datetime.time().second() as u32,
                                        micro: datetime.time().nanosecond() / 1000,
                                    },
                                },
                                offset: offset_variant,
                            })
                        } else {
                            types::PgValue::Text(s.to_string())
                        }
                    } else {
                        types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                    }
                }
            }
        }
        // INTERVAL = 1186
        1186 => {
            // PostgreSQL interval is complex - for now, parse as text and try to extract components
            if let Ok(s) = std::str::from_utf8(bytes) {
                // PostgreSQL interval format: "1 day 2 hours 3 minutes" or "P1DT2H3M" (ISO 8601)
                // For now, return as text since parsing intervals is complex
                // TODO: Implement proper interval parsing
                types::PgValue::Text(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // TIMETZ = 1266
        1266 => {
            // TIME WITH TIME ZONE - parse time and timezone
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Format: "12:34:56+05:30" or "12:34:56.789+05:30" or "12:34:56-04:00"
                // Find the last occurrence of + or - (which should be the timezone separator)
                let mut split_pos = None;
                for (i, c) in s.char_indices().rev() {
                    if c == '+' || c == '-' {
                        // Make sure it's not part of the time itself (should be after a space or at start)
                        if i > 0 && (s.as_bytes()[i-1] == b' ' || s.as_bytes()[i-1] == b'T' || i == s.len() - 6 || i == s.len() - 3) {
                            split_pos = Some(i);
                            break;
                        }
                    }
                }
                
                if let Some(pos) = split_pos {
                    let time_str = &s[..pos];
                    let tz_str = &s[pos..];
                    
                    if let Ok(time) = Time::parse(time_str, &time::format_description::well_known::Iso8601::TIME) {
                        types::PgValue::TimeTz(types::TimeTz {
                            timesonze: tz_str.to_string(),
                            time: types::Time {
                                hour: time.hour() as u32,
                                min: time.minute() as u32,
                                sec: time.second() as u32,
                                micro: time.nanosecond() / 1000,
                            },
                        })
                    } else {
                        types::PgValue::Text(s.to_string())
                    }
                } else {
                    // No timezone separator found, try parsing as plain time (treat as UTC)
                    if let Ok(time) = Time::parse(s, &time::format_description::well_known::Iso8601::TIME) {
                        types::PgValue::TimeTz(types::TimeTz {
                            timesonze: "+00:00".to_string(),
                            time: types::Time {
                                hour: time.hour() as u32,
                                min: time.minute() as u32,
                                sec: time.second() as u32,
                                micro: time.nanosecond() / 1000,
                            },
                        })
                    } else {
                        types::PgValue::Text(s.to_string())
                    }
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // UUID = 2950
        2950 => {
            match pg_types::uuid_from_sql(bytes) {
                Ok(uuid_bytes) => {
                    // uuid_from_sql returns [u8; 16]
                    let uuid = Uuid::from_bytes(uuid_bytes);
                    types::PgValue::Uuid(uuid.to_string())
                }
                Err(_) => {
                    // Fallback: try parsing text format (e.g., "550e8400-e29b-41d4-a716-446655440000")
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        if Uuid::parse_str(s).is_ok() {
                            types::PgValue::Uuid(s.to_string())
                        } else {
                            types::PgValue::Text(s.to_string())
                        }
                    } else {
                        types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                    }
                }
            }
        }
        // JSON = 114
        114 => {
            // JSON is stored as text in PostgreSQL
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Validate it's valid JSON
                if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                    types::PgValue::Json(s.to_string())
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // JSONB = 3802
        3802 => {
            // JSONB is stored as text in PostgreSQL (binary format is internal)
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Validate it's valid JSON
                if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                    types::PgValue::Jsonb(s.to_string())
                } else {
                    types::PgValue::Text(s.to_string())
                }
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

