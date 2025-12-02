#[allow(unused)]
mod interlude {
    pub use api_utils_rs::prelude::*;

    pub use std::str::FromStr;
}

mod wit {
    wit_bindgen::generate!({
        world: "bundle",
        path: "wit",
        with: {
            "wasi:keyvalue/store@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::store,
            "wasmcloud:postgres/types@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::types,
            "wasmcloud:postgres/query@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::query,
            "wasi:keyvalue/atomics@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::atomics,
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
            "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
            "wasi:config/runtime@0.2.0-draft": api_utils_rs::wit::wasi::config::runtime,

            "townframe:api-utils/utils": api_utils_rs::wit::utils,

            "townframe:wflow/types": wflow_sdk::wit::townframe::wflow::types,
            "townframe:wflow/host": wflow_sdk::wit::townframe::wflow::host,
            "townframe:wflow/bundle": generate,
            "townframe:pglite/query": generate,
            "townframe:pglite/types": generate,
        }
    });
}

use crate::interlude::*;

use crate::wit::exports::townframe::wflow::bundle::JobResult;
use wflow_sdk::{JobErrorX, Json, WflowCtx};

wit::export!(Component with_types_in wit);

struct Component;

impl wit::exports::townframe::wflow::bundle::Guest for Component {
    fn run(args: wit::exports::townframe::wflow::bundle::RunArgs) -> JobResult {
        wflow_sdk::route_wflows!(args, {
            "pglite_select_one" => |cx, args: PgliteSelectOneArgs| pglite_select_one(cx, args),
        })
    }
}

#[derive(serde::Deserialize)]
struct PgliteSelectOneArgs {}

fn pglite_select_one(cx: WflowCtx, _args: PgliteSelectOneArgs) -> Result<(), JobErrorX> {
    use wit::townframe::pglite::{query, types};

    // Helper to create a result row entry
    fn entry(name: &str, value: types::PgValue) -> types::ResultRowEntry {
        types::ResultRowEntry {
            column_name: name.to_string(),
            value,
        }
    }

    // Helper to create a result row
    fn row(entries: Vec<types::ResultRowEntry>) -> types::ResultRow {
        entries
    }

    // Test cases: (SQL query, expected result rows)
    let test_cases: Vec<(&str, Vec<types::ResultRow>)> = vec![
        // NULL
        (
            "SELECT NULL AS null_val",
            vec![row(vec![entry("null_val", types::PgValue::Null)])],
        ),
        // Numeric types - Int2 (smallint)
        (
            "SELECT 32767::smallint AS int2_val",
            vec![row(vec![entry("int2_val", types::PgValue::Int2(32767))])],
        ),
        (
            "SELECT -32768::smallint AS int2_neg",
            vec![row(vec![entry("int2_neg", types::PgValue::Int2(-32768))])],
        ),
        // Numeric types - Int4 (integer)
        (
            "SELECT 1 AS int4_val",
            vec![row(vec![entry("int4_val", types::PgValue::Int4(1))])],
        ),
        (
            "SELECT -2147483648 AS int4_neg",
            vec![row(vec![entry("int4_neg", types::PgValue::Int4(-2147483648))])],
        ),
        (
            "SELECT 2147483647 AS int4_max",
            vec![row(vec![entry("int4_max", types::PgValue::Int4(2147483647))])],
        ),
        // Numeric types - Int8 (bigint)
        (
            "SELECT 9223372036854775807 AS int8_val",
            vec![row(vec![entry("int8_val", types::PgValue::Int8(9223372036854775807))])],
        ),
        (
            "SELECT -9223372036854775808 AS int8_neg",
            vec![row(vec![entry("int8_neg", types::PgValue::Int8(-9223372036854775808))])],
        ),
        // Numeric types - Float4 (real)
        (
            "SELECT 3.14159::real AS float4_val",
            vec![row(vec![entry("float4_val", {
                let f: f32 = 3.14159;
                types::PgValue::Float4((f.to_bits() as u64, 0i16, 0i8))
            })])],
        ),
        (
            "SELECT -42.5::real AS float4_neg",
            vec![row(vec![entry("float4_neg", {
                let f: f32 = -42.5;
                types::PgValue::Float4((f.to_bits() as u64, 0i16, 0i8))
            })])],
        ),
        // Numeric types - Float8 (double precision)
        (
            "SELECT 3.141592653589793 AS float8_val",
            vec![row(vec![entry("float8_val", {
                let f: f64 = 3.141592653589793;
                types::PgValue::Float8((f.to_bits(), 0i16, 0i8))
            })])],
        ),
        (
            "SELECT -42.5::double precision AS float8_neg",
            vec![row(vec![entry("float8_neg", {
                let f: f64 = -42.5;
                types::PgValue::Float8((f.to_bits(), 0i16, 0i8))
            })])],
        ),
        // Boolean
        (
            "SELECT true AS bool_true",
            vec![row(vec![entry("bool_true", types::PgValue::Bool(true))])],
        ),
        (
            "SELECT false AS bool_false",
            vec![row(vec![entry("bool_false", types::PgValue::Bool(false))])],
        ),
        // Text types
        (
            "SELECT 'hello world' AS text_val",
            vec![row(vec![entry("text_val", types::PgValue::Text("hello world".to_string()))])],
        ),
        (
            "SELECT '' AS text_empty",
            vec![row(vec![entry("text_empty", types::PgValue::Text("".to_string()))])],
        ),
        (
            "SELECT 'test''quote' AS text_quote",
            vec![row(vec![entry("text_quote", types::PgValue::Text("test'quote".to_string()))])],
        ),
        // Bytea
        (
            "SELECT '\\x48656c6c6f'::bytea AS bytea_val",
            vec![row(vec![entry("bytea_val", types::PgValue::Bytea(b"Hello".to_vec()))])],
        ),
        (
            "SELECT '\\x'::bytea AS bytea_empty",
            vec![row(vec![entry("bytea_empty", types::PgValue::Bytea(vec![]))])],
        ),
        // UUID (OID 2950)
        (
            "SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid AS uuid_val",
            vec![row(vec![entry("uuid_val", types::PgValue::Uuid("550e8400-e29b-41d4-a716-446655440000".to_string()))])],
        ),
        // Date (OID 1082) - Note: plugin may not support this yet, will fallback to Text
        (
            "SELECT '2024-01-15'::date AS date_val",
            vec![row(vec![entry("date_val", types::PgValue::Date(types::Date::Ymd((2024i32, 1u32, 15u32))))])],
        ),
        // Time (OID 1083) - Note: plugin may not support this yet
        (
            "SELECT '12:34:56'::time AS time_val",
            vec![row(vec![entry("time_val", types::PgValue::Time(types::Time {
                hour: 12u32,
                min: 34u32,
                sec: 56u32,
                micro: 0u32,
            }))])],
        ),
        // Timestamp (OID 1114) - Note: plugin may not support this yet
        (
            "SELECT '2024-01-15 12:34:56'::timestamp AS timestamp_val",
            vec![row(vec![entry("timestamp_val", types::PgValue::Timestamp(types::Timestamp {
                date: types::Date::Ymd((2024i32, 1u32, 15u32)),
                time: types::Time {
                    hour: 12u32,
                    min: 34u32,
                    sec: 56u32,
                    micro: 0u32,
                },
            }))])],
        ),
        // Timestamp with timezone (OID 1184) - Note: plugin may not support this yet
        (
            "SELECT '2024-01-15 12:34:56+00'::timestamptz AS timestamptz_val",
            vec![row(vec![entry("timestamptz_val", types::PgValue::TimestampTz(types::TimestampTz {
                timestamp: types::Timestamp {
                    date: types::Date::Ymd((2024i32, 1u32, 15u32)),
                    time: types::Time {
                        hour: 12u32,
                        min: 34u32,
                        sec: 56u32,
                        micro: 0u32,
                    },
                },
                offset: types::Offset::WesternHemisphereSecs(0i32),
            }))])],
        ),
        // INET (OID 869) - Note: plugin may not support this yet
        (
            "SELECT '192.168.1.1'::inet AS inet_val",
            vec![row(vec![entry("inet_val", types::PgValue::Inet("192.168.1.1".to_string()))])],
        ),
        (
            "SELECT '2001:db8::1'::inet AS inet_ipv6",
            vec![row(vec![entry("inet_ipv6", types::PgValue::Inet("2001:db8::1".to_string()))])],
        ),
        // CIDR (OID 650) - Note: plugin may not support this yet
        (
            "SELECT '192.168.1.0/24'::cidr AS cidr_val",
            vec![row(vec![entry("cidr_val", types::PgValue::Cidr("192.168.1.0/24".to_string()))])],
        ),
        // MACADDR (OID 829) - Note: plugin may not support this yet
        (
            "SELECT '08:00:2b:01:02:03'::macaddr AS macaddr_val",
            vec![row(vec![entry("macaddr_val", types::PgValue::Macaddr(types::MacAddressEui48 {
                bytes: (8u8, 0u8, 43u8, 1u8, 2u8, 3u8),
            }))])],
        ),
        // Point (OID 600) - Note: plugin may not support this yet
        (
            "SELECT '(1.5, 2.5)'::point AS point_val",
            vec![row(vec![entry("point_val", {
                let x: f64 = 1.5;
                let y: f64 = 2.5;
                types::PgValue::Point(((x.to_bits(), 0i16, 0i8), (y.to_bits(), 0i16, 0i8)))
            })])],
        ),
        // JSON (OID 114) - Note: plugin may not support this yet
        (
            "SELECT '{\"key\": \"value\"}'::json AS json_val",
            vec![row(vec![entry("json_val", types::PgValue::Json(r#"{"key": "value"}"#.to_string()))])],
        ),
        // JSONB (OID 3802) - Note: plugin may not support this yet
        (
            "SELECT '{\"key\": \"value\"}'::jsonb AS jsonb_val",
            vec![row(vec![entry("jsonb_val", types::PgValue::Jsonb(r#"{"key": "value"}"#.to_string()))])],
        ),
        // Numeric/Decimal (OID 1700) - Note: plugin may not support this yet
        (
            "SELECT 123.456::numeric AS numeric_val",
            vec![row(vec![entry("numeric_val", types::PgValue::Numeric("123.456".to_string()))])],
        ),
        (
            "SELECT 123.456::decimal AS decimal_val",
            vec![row(vec![entry("decimal_val", types::PgValue::Decimal("123.456".to_string()))])],
        ),
        // VARBIT/BIT (OID 1560/1562) - Note: plugin may not support this yet
        (
            "SELECT B'1010'::bit(4) AS bit_val",
            vec![row(vec![entry("bit_val", types::PgValue::Bit((4u32, vec![0b10100000u8])))])],
        ),
        (
            "SELECT B'1010'::varbit AS varbit_val",
            vec![row(vec![entry("varbit_val", types::PgValue::Varbit((Some(4u32), vec![0b10100000u8])))])],
        ),
        // HSTORE (OID extension) - Note: plugin may not support this yet
        (
            "SELECT 'a=>1, b=>2'::hstore AS hstore_val",
            vec![row(vec![entry("hstore_val", types::PgValue::Hstore(vec![
                ("a".to_string(), Some("1".to_string())),
                ("b".to_string(), Some("2".to_string())),
            ]))])],
        ),
        // Box (OID 603) - Note: plugin may not support this yet
        (
            "SELECT '((0,0),(1,1))'::box AS box_val",
            vec![row(vec![entry("box_val", {
                let ll_x: f64 = 0.0;
                let ll_y: f64 = 0.0;
                let ur_x: f64 = 1.0;
                let ur_y: f64 = 1.0;
                types::PgValue::Box((
                    ((ll_x.to_bits(), 0i16, 0i8), (ll_y.to_bits(), 0i16, 0i8)),
                    ((ur_x.to_bits(), 0i16, 0i8), (ur_y.to_bits(), 0i16, 0i8)),
                ))
            })])],
        ),
        // Circle (OID 718) - Note: plugin may not support this yet
        (
            "SELECT '((0,0),1)'::circle AS circle_val",
            vec![row(vec![entry("circle_val", {
                let cx: f64 = 0.0;
                let cy: f64 = 0.0;
                let r: f64 = 1.0;
                types::PgValue::Circle((
                    ((cx.to_bits(), 0i16, 0i8), (cy.to_bits(), 0i16, 0i8)),
                    (r.to_bits(), 0i16, 0i8),
                ))
            })])],
        ),
        // Path (OID 602) - Note: plugin may not support this yet
        (
            "SELECT '[(0,0),(1,1)]'::path AS path_val",
            vec![row(vec![entry("path_val", {
                let p1_x: f64 = 0.0;
                let p1_y: f64 = 0.0;
                let p2_x: f64 = 1.0;
                let p2_y: f64 = 1.0;
                types::PgValue::Path(vec![
                    ((p1_x.to_bits(), 0i16, 0i8), (p1_y.to_bits(), 0i16, 0i8)),
                    ((p2_x.to_bits(), 0i16, 0i8), (p2_y.to_bits(), 0i16, 0i8)),
                ])
            })])],
        ),
        // PG_LSN (OID 3220) - Note: plugin may not support this yet
        (
            "SELECT '0/12345678'::pg_lsn AS pg_lsn_val",
            vec![row(vec![entry("pg_lsn_val", types::PgValue::PgLsn(0x12345678))])],
        ),
        // Name (OID 19) - Note: plugin may support this as Text
        (
            "SELECT 'test_name'::name AS name_val",
            vec![row(vec![entry("name_val", types::PgValue::Name("test_name".to_string()))])],
        ),
        // XML (OID 142) - Note: plugin may not support this yet
        (
            "SELECT '<root>test</root>'::xml AS xml_val",
            vec![row(vec![entry("xml_val", types::PgValue::Xml("<root>test</root>".to_string()))])],
        ),
        // Money (OID 790) - Note: plugin may not support this yet
        (
            "SELECT '$123.45'::money AS money_val",
            vec![row(vec![entry("money_val", types::PgValue::Money("$123.45".to_string()))])],
        ),
        // Char (OID 18) - "char" type (single byte) - Note: plugin may not support this yet
        (
            "SELECT 'A'::\"char\" AS char_val",
            vec![row(vec![entry("char_val", types::PgValue::Char((1u32, vec![b'A'])))])],
        ),
        // Line (OID 628) - Note: plugin may not support this yet
        (
            "SELECT '{1,2,3}'::line AS line_val",
            vec![row(vec![entry("line_val", {
                let p1_x: f64 = 0.0;
                let p1_y: f64 = 0.0;
                let p2_x: f64 = 1.0;
                let p2_y: f64 = 1.0;
                types::PgValue::Line((
                    ((p1_x.to_bits(), 0i16, 0i8), (p1_y.to_bits(), 0i16, 0i8)),
                    ((p2_x.to_bits(), 0i16, 0i8), (p2_y.to_bits(), 0i16, 0i8)),
                ))
            })])],
        ),
        // Lseg (OID 601) - Note: plugin may not support this yet
        (
            "SELECT '[(0,0),(1,1)]'::lseg AS lseg_val",
            vec![row(vec![entry("lseg_val", {
                let p1_x: f64 = 0.0;
                let p1_y: f64 = 0.0;
                let p2_x: f64 = 1.0;
                let p2_y: f64 = 1.0;
                types::PgValue::Lseg((
                    ((p1_x.to_bits(), 0i16, 0i8), (p1_y.to_bits(), 0i16, 0i8)),
                    ((p2_x.to_bits(), 0i16, 0i8), (p2_y.to_bits(), 0i16, 0i8)),
                ))
            })])],
        ),
        // Polygon (OID 604) - Note: plugin may not support this yet
        (
            "SELECT '((0,0),(1,0),(1,1),(0,1))'::polygon AS polygon_val",
            vec![row(vec![entry("polygon_val", {
                let p1_x: f64 = 0.0;
                let p1_y: f64 = 0.0;
                let p2_x: f64 = 1.0;
                let p2_y: f64 = 0.0;
                let p3_x: f64 = 1.0;
                let p3_y: f64 = 1.0;
                let p4_x: f64 = 0.0;
                let p4_y: f64 = 1.0;
                types::PgValue::Polygon(vec![
                    ((p1_x.to_bits(), 0i16, 0i8), (p1_y.to_bits(), 0i16, 0i8)),
                    ((p2_x.to_bits(), 0i16, 0i8), (p2_y.to_bits(), 0i16, 0i8)),
                    ((p3_x.to_bits(), 0i16, 0i8), (p3_y.to_bits(), 0i16, 0i8)),
                    ((p4_x.to_bits(), 0i16, 0i8), (p4_y.to_bits(), 0i16, 0i8)),
                ])
            })])],
        ),
        // Interval (OID 1186) - Note: plugin may not support this yet
        (
            "SELECT '1 day 2 hours 3 minutes'::interval AS interval_val",
            vec![row(vec![entry("interval_val", types::PgValue::Interval(types::Interval {
                start: types::Date::Ymd((2000i32, 1u32, 1u32)),
                start_inclusive: true,
                end: types::Date::Ymd((2000i32, 1u32, 2u32)),
                end_inclusive: true,
            }))])],
        ),
        // Time with timezone (OID 1266) - Note: plugin may not support this yet
        (
            "SELECT '12:34:56+05:30'::timetz AS timetz_val",
            vec![row(vec![entry("timetz_val", types::PgValue::TimeTz(types::TimeTz {
                timesonze: "+05:30".to_string(),
                time: types::Time {
                    hour: 12u32,
                    min: 34u32,
                    sec: 56u32,
                    micro: 0u32,
                },
            }))])],
        ),
        // TS Query (OID 3615) - Note: plugin may not support this yet
        (
            "SELECT 'test & query'::tsquery AS tsquery_val",
            vec![row(vec![entry("tsquery_val", types::PgValue::TsQuery("test & query".to_string()))])],
        ),
        // TS Vector (OID 3614) - Note: plugin may not support this yet
        (
            "SELECT 'test query'::tsvector AS tsvector_val",
            vec![row(vec![entry("tsvector_val", types::PgValue::TsVector(vec![
                types::Lexeme {
                    position: Some(1u16),
                    weight: Some(types::LexemeWeight::A),
                    data: "test".to_string(),
                },
                types::Lexeme {
                    position: Some(2u16),
                    weight: Some(types::LexemeWeight::A),
                    data: "query".to_string(),
                },
            ]))])],
        ),
        // Multi-row query
        (
            "SELECT generate_series(1, 3) AS num",
            vec![
                row(vec![entry("num", types::PgValue::Int4(1))]),
                row(vec![entry("num", types::PgValue::Int4(2))]),
                row(vec![entry("num", types::PgValue::Int4(3))]),
            ],
        ),
        // Multi-column, single row
        (
            "SELECT 42 AS a, 'test' AS b, true AS c",
            vec![row(vec![
                entry("a", types::PgValue::Int4(42)),
                entry("b", types::PgValue::Text("test".to_string())),
                entry("c", types::PgValue::Bool(true)),
            ])],
        ),
        // Multi-row, multi-column
        (
            "SELECT 1 AS x, 'a' AS y UNION ALL SELECT 2, 'b'",
            vec![
                row(vec![
                    entry("x", types::PgValue::Int4(1)),
                    entry("y", types::PgValue::Text("a".to_string())),
                ]),
                row(vec![
                    entry("x", types::PgValue::Int4(2)),
                    entry("y", types::PgValue::Text("b".to_string())),
                ]),
            ],
        ),
    ];

    cx.effect(|| {
        for (sql, expected_rows) in test_cases {
            let actual_rows = query::query(sql, &[])
                .map_err(|err| JobErrorX::Terminal(ferr!("query error for '{}': {:?}", sql, err)))?;

            // Verify row count matches
            if actual_rows.len() != expected_rows.len() {
                return Err(JobErrorX::Terminal(ferr!(
                    "query '{}': expected {} rows, got {}",
                    sql,
                    expected_rows.len(),
                    actual_rows.len()
                )));
            }

            // Verify each row
            for (row_idx, (expected_row, actual_row)) in expected_rows.iter().zip(actual_rows.iter()).enumerate() {
                if actual_row.len() != expected_row.len() {
                    return Err(JobErrorX::Terminal(ferr!(
                        "query '{}', row {}: expected {} columns, got {}",
                        sql,
                        row_idx,
                        expected_row.len(),
                        actual_row.len()
                    )));
                }

                // Verify each column
                for (col_idx, (expected_entry, actual_entry)) in expected_row.iter().zip(actual_row.iter()).enumerate() {
                    if actual_entry.column_name != expected_entry.column_name {
                        return Err(JobErrorX::Terminal(ferr!(
                            "query '{}', row {}, col {}: expected column '{}', got '{}'",
                            sql,
                            row_idx,
                            col_idx,
                            expected_entry.column_name,
                            actual_entry.column_name
                        )));
                    }

                    // Compare values using pattern matching
                    match (&expected_entry.value, &actual_entry.value) {
                        (types::PgValue::Null, types::PgValue::Null) => {}
                        (types::PgValue::Int2(e), types::PgValue::Int2(a)) if e == a => {}
                        (types::PgValue::Int4(e), types::PgValue::Int4(a)) if e == a => {}
                        (types::PgValue::Int8(e), types::PgValue::Int8(a)) if e == a => {}
                        (types::PgValue::Text(e), types::PgValue::Text(a)) if e == a => {}
                        (types::PgValue::Bool(e), types::PgValue::Bool(a)) if e == a => {}
                        (types::PgValue::Bytea(e), types::PgValue::Bytea(a)) if e == a => {}
                        (types::PgValue::Float4(e), types::PgValue::Float4(a)) => {
                            // Compare hashable-f64 representation (u64, i16, i8)
                            if e.0 != a.0 || e.1 != a.1 || e.2 != a.2 {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Float4({:?}), got Float4({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::Float8(e), types::PgValue::Float8(a)) => {
                            // Compare hashable-f64 representation (u64, i16, i8)
                            if e.0 != a.0 || e.1 != a.1 || e.2 != a.2 {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Float8({:?}), got Float8({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::Uuid(e), types::PgValue::Uuid(a)) if e == a => {}
                        (types::PgValue::Date(e), types::PgValue::Date(a)) => {
                            // Compare date variants
                            match (e, a) {
                                (types::Date::PositiveInfinity, types::Date::PositiveInfinity) => {}
                                (types::Date::NegativeInfinity, types::Date::NegativeInfinity) => {}
                                (types::Date::Ymd(e_ymd), types::Date::Ymd(a_ymd)) if e_ymd == a_ymd => {}
                                _ => {
                                    return Err(JobErrorX::Terminal(ferr!(
                                        "query '{}', row {}, col {}: expected Date({:?}), got Date({:?})",
                                        sql, row_idx, col_idx, e, a
                                    )));
                                }
                            }
                        }
                        (types::PgValue::Time(e), types::PgValue::Time(a)) => {
                            if e.hour != a.hour || e.min != a.min || e.sec != a.sec || e.micro != a.micro {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Time({:?}), got Time({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::Timestamp(e), types::PgValue::Timestamp(a)) => {
                            // Compare timestamp by comparing date and time components
                            match (&e.date, &a.date) {
                                (types::Date::Ymd(e_ymd), types::Date::Ymd(a_ymd)) if e_ymd == a_ymd => {}
                                _ => {
                                    return Err(JobErrorX::Terminal(ferr!(
                                        "query '{}', row {}, col {}: expected Timestamp({:?}), got Timestamp({:?})",
                                        sql, row_idx, col_idx, e, a
                                    )));
                                }
                            }
                            if e.time.hour != a.time.hour || e.time.min != a.time.min || e.time.sec != a.time.sec || e.time.micro != a.time.micro {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Timestamp({:?}), got Timestamp({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::TimestampTz(e), types::PgValue::TimestampTz(a)) => {
                            // Compare timestamp-tz by comparing timestamp and offset
                            match (&e.timestamp.date, &a.timestamp.date) {
                                (types::Date::Ymd(e_ymd), types::Date::Ymd(a_ymd)) if e_ymd == a_ymd => {}
                                _ => {
                                    return Err(JobErrorX::Terminal(ferr!(
                                        "query '{}', row {}, col {}: expected TimestampTz({:?}), got TimestampTz({:?})",
                                        sql, row_idx, col_idx, e, a
                                    )));
                                }
                            }
                            if e.timestamp.time.hour != a.timestamp.time.hour || e.timestamp.time.min != a.timestamp.time.min || e.timestamp.time.sec != a.timestamp.time.sec || e.timestamp.time.micro != a.timestamp.time.micro {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected TimestampTz({:?}), got TimestampTz({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                            // Compare offsets
                            match (&e.offset, &a.offset) {
                                (types::Offset::EasternHemisphereSecs(e_secs), types::Offset::EasternHemisphereSecs(a_secs)) if e_secs == a_secs => {}
                                (types::Offset::WesternHemisphereSecs(e_secs), types::Offset::WesternHemisphereSecs(a_secs)) if e_secs == a_secs => {}
                                _ => {
                                    return Err(JobErrorX::Terminal(ferr!(
                                        "query '{}', row {}, col {}: expected TimestampTz({:?}), got TimestampTz({:?})",
                                        sql, row_idx, col_idx, e, a
                                    )));
                                }
                            }
                        }
                        (types::PgValue::Inet(e), types::PgValue::Inet(a)) if e == a => {}
                        (types::PgValue::Cidr(e), types::PgValue::Cidr(a)) if e == a => {}
                        (types::PgValue::Macaddr(e), types::PgValue::Macaddr(a)) => {
                            if e.bytes != a.bytes {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Macaddr({:?}), got Macaddr({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::Point(e), types::PgValue::Point(a)) => {
                            // Compare point by comparing hashable-f64 tuples
                            if e.0 .0 != a.0 .0 || e.0 .1 != a.0 .1 || e.0 .2 != a.0 .2 || e.1 .0 != a.1 .0 || e.1 .1 != a.1 .1 || e.1 .2 != a.1 .2 {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Point({:?}), got Point({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::Json(e), types::PgValue::Json(a)) if e == a => {}
                        (types::PgValue::Jsonb(e), types::PgValue::Jsonb(a)) if e == a => {}
                        (types::PgValue::Numeric(e), types::PgValue::Numeric(a)) if e == a => {}
                        (types::PgValue::Decimal(e), types::PgValue::Decimal(a)) if e == a => {}
                        (types::PgValue::Bit(e), types::PgValue::Bit(a)) if e == a => {}
                        (types::PgValue::Varbit(e), types::PgValue::Varbit(a)) if e == a => {}
                        (types::PgValue::Hstore(e), types::PgValue::Hstore(a)) if e == a => {}
                        (types::PgValue::Box(e), types::PgValue::Box(a)) => {
                            // Compare box by comparing lower-left and upper-right points
                            if e.0 .0 .0 != a.0 .0 .0 || e.0 .0 .1 != a.0 .0 .1 || e.0 .0 .2 != a.0 .0 .2
                                || e.0 .1 .0 != a.0 .1 .0 || e.0 .1 .1 != a.0 .1 .1 || e.0 .1 .2 != a.0 .1 .2
                                || e.1 .0 .0 != a.1 .0 .0 || e.1 .0 .1 != a.1 .0 .1 || e.1 .0 .2 != a.1 .0 .2
                                || e.1 .1 .0 != a.1 .1 .0 || e.1 .1 .1 != a.1 .1 .1 || e.1 .1 .2 != a.1 .1 .2
                            {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Box({:?}), got Box({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::Circle(e), types::PgValue::Circle(a)) => {
                            // Compare circle by comparing center point and radius
                            if e.0 .0 .0 != a.0 .0 .0 || e.0 .0 .1 != a.0 .0 .1 || e.0 .0 .2 != a.0 .0 .2
                                || e.0 .1 .0 != a.0 .1 .0 || e.0 .1 .1 != a.0 .1 .1 || e.0 .1 .2 != a.0 .1 .2
                                || e.1 .0 != a.1 .0 || e.1 .1 != a.1 .1 || e.1 .2 != a.1 .2
                            {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Circle({:?}), got Circle({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::Path(e), types::PgValue::Path(a)) => {
                            if e.len() != a.len() {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Path({:?}), got Path({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                            for (ep, ap) in e.iter().zip(a.iter()) {
                                if ep.0 .0 != ap.0 .0 || ep.0 .1 != ap.0 .1 || ep.0 .2 != ap.0 .2
                                    || ep.1 .0 != ap.1 .0 || ep.1 .1 != ap.1 .1 || ep.1 .2 != ap.1 .2
                                {
                                    return Err(JobErrorX::Terminal(ferr!(
                                        "query '{}', row {}, col {}: expected Path({:?}), got Path({:?})",
                                        sql, row_idx, col_idx, e, a
                                    )));
                                }
                            }
                        }
                        (types::PgValue::PgLsn(e), types::PgValue::PgLsn(a)) if e == a => {}
                        (types::PgValue::Name(e), types::PgValue::Name(a)) if e == a => {}
                        (types::PgValue::Xml(e), types::PgValue::Xml(a)) if e == a => {}
                        (types::PgValue::Money(e), types::PgValue::Money(a)) if e == a => {}
                        (types::PgValue::Char(e), types::PgValue::Char(a)) if e == a => {}
                        (types::PgValue::Line(e), types::PgValue::Line(a)) => {
                            // Compare line by comparing start and end points
                            if e.0 .0 .0 != a.0 .0 .0 || e.0 .0 .1 != a.0 .0 .1 || e.0 .0 .2 != a.0 .0 .2
                                || e.0 .1 .0 != a.0 .1 .0 || e.0 .1 .1 != a.0 .1 .1 || e.0 .1 .2 != a.0 .1 .2
                                || e.1 .0 .0 != a.1 .0 .0 || e.1 .0 .1 != a.1 .0 .1 || e.1 .0 .2 != a.1 .0 .2
                                || e.1 .1 .0 != a.1 .1 .0 || e.1 .1 .1 != a.1 .1 .1 || e.1 .1 .2 != a.1 .1 .2
                            {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Line({:?}), got Line({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::Lseg(e), types::PgValue::Lseg(a)) => {
                            // Compare lseg by comparing start and end points
                            if e.0 .0 .0 != a.0 .0 .0 || e.0 .0 .1 != a.0 .0 .1 || e.0 .0 .2 != a.0 .0 .2
                                || e.0 .1 .0 != a.0 .1 .0 || e.0 .1 .1 != a.0 .1 .1 || e.0 .1 .2 != a.0 .1 .2
                                || e.1 .0 .0 != a.1 .0 .0 || e.1 .0 .1 != a.1 .0 .1 || e.1 .0 .2 != a.1 .0 .2
                                || e.1 .1 .0 != a.1 .1 .0 || e.1 .1 .1 != a.1 .1 .1 || e.1 .1 .2 != a.1 .1 .2
                            {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Lseg({:?}), got Lseg({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::Polygon(e), types::PgValue::Polygon(a)) => {
                            if e.len() != a.len() {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Polygon({:?}), got Polygon({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                            for (ep, ap) in e.iter().zip(a.iter()) {
                                if ep.0 .0 != ap.0 .0 || ep.0 .1 != ap.0 .1 || ep.0 .2 != ap.0 .2
                                    || ep.1 .0 != ap.1 .0 || ep.1 .1 != ap.1 .1 || ep.1 .2 != ap.1 .2
                                {
                                    return Err(JobErrorX::Terminal(ferr!(
                                        "query '{}', row {}, col {}: expected Polygon({:?}), got Polygon({:?})",
                                        sql, row_idx, col_idx, e, a
                                    )));
                                }
                            }
                        }
                        (types::PgValue::Interval(e), types::PgValue::Interval(a)) => {
                            // Compare interval by comparing start and end dates
                            match (&e.start, &a.start) {
                                (types::Date::Ymd(e_ymd), types::Date::Ymd(a_ymd)) if e_ymd == a_ymd => {}
                                _ => {
                                    return Err(JobErrorX::Terminal(ferr!(
                                        "query '{}', row {}, col {}: expected Interval({:?}), got Interval({:?})",
                                        sql, row_idx, col_idx, e, a
                                    )));
                                }
                            }
                            match (&e.end, &a.end) {
                                (types::Date::Ymd(e_ymd), types::Date::Ymd(a_ymd)) if e_ymd == a_ymd => {}
                                _ => {
                                    return Err(JobErrorX::Terminal(ferr!(
                                        "query '{}', row {}, col {}: expected Interval({:?}), got Interval({:?})",
                                        sql, row_idx, col_idx, e, a
                                    )));
                                }
                            }
                            if e.start_inclusive != a.start_inclusive || e.end_inclusive != a.end_inclusive {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected Interval({:?}), got Interval({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::TimeTz(e), types::PgValue::TimeTz(a)) => {
                            if e.timesonze != a.timesonze {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected TimeTz({:?}), got TimeTz({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                            if e.time.hour != a.time.hour || e.time.min != a.time.min || e.time.sec != a.time.sec || e.time.micro != a.time.micro {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected TimeTz({:?}), got TimeTz({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                        }
                        (types::PgValue::TsQuery(e), types::PgValue::TsQuery(a)) if e == a => {}
                        (types::PgValue::TsVector(e), types::PgValue::TsVector(a)) => {
                            if e.len() != a.len() {
                                return Err(JobErrorX::Terminal(ferr!(
                                    "query '{}', row {}, col {}: expected TsVector({:?}), got TsVector({:?})",
                                    sql, row_idx, col_idx, e, a
                                )));
                            }
                            for (el, al) in e.iter().zip(a.iter()) {
                                if el.position != al.position || el.weight != al.weight || el.data != al.data {
                                    return Err(JobErrorX::Terminal(ferr!(
                                        "query '{}', row {}, col {}: expected TsVector({:?}), got TsVector({:?})",
                                        sql, row_idx, col_idx, e, a
                                    )));
                                }
                            }
                        }
                        (expected, actual) => {
                            return Err(JobErrorX::Terminal(ferr!(
                                "query '{}', row {}, col {}: expected {:?}, got {:?}",
                                sql, row_idx, col_idx, expected, actual
                            )));
                        }
                    }
                }
            }
        }

        Ok(Json(()))
    })?;

    Ok(())
}

