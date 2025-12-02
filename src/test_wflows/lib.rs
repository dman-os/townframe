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
            "wasi:keyvalue/atomics@0.2.0-draft": api_utils_rs::wit::wasi::keyvalue::atomics,
            "wasi:logging/logging@0.1.0-draft": api_utils_rs::wit::wasi::logging::logging,
            "wasmcloud:postgres/types@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::types,
            "wasmcloud:postgres/query@0.1.1-draft": api_utils_rs::wit::wasmcloud::postgres::query,
            "wasi:io/poll@0.2.6": api_utils_rs::wit::wasi::io::poll,
            "wasi:clocks/monotonic-clock@0.2.6": api_utils_rs::wit::wasi::clocks::monotonic_clock,
            "wasi:clocks/wall-clock@0.2.6": api_utils_rs::wit::wasi::clocks::wall_clock,
            "wasi:config/runtime@0.2.0-draft": api_utils_rs::wit::wasi::config::runtime,

            "townframe:api-utils/utils": api_utils_rs::wit::utils,

            "townframe:wflow/types": wflow_sdk::wit::townframe::wflow::types,
            "townframe:wflow/host": wflow_sdk::wit::townframe::wflow::host,
            "townframe:wflow/bundle": generate,
            "townframe:am-repo/repo": generate,
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
            "fails_once" => |cx, args: FailsOnceArgs| fails_once(cx, args),
            "fails_until_told" => |cx, args: FailsUntilToldArgs| fails_until_told(cx, args),
            "pglite_select_one" => |cx, args: PgliteSelectOneArgs| pglite_select_one(cx, args),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FailsOnceArgs {
    key: String,
}

fn fails_once(cx: WflowCtx, args: FailsOnceArgs) -> Result<(), JobErrorX> {
    use api_utils_rs::wit::wasi::keyvalue::store;

    // Get the current value from keyvalue store
    cx.effect(|| {
        let bucket = store::open("default")
            .map_err(|err| JobErrorX::Terminal(ferr!("error opening bucket: {:?}", err)))?;
        let key = &args.key;
        const VALUE: u64 = 42;
        match bucket.get(key) {
            Err(err) => Err(JobErrorX::Terminal(ferr!(
                "error getting keyvalue: {:?}",
                err
            ))),
            Ok(None) => match bucket.set(key, &VALUE.to_le_bytes()) {
                Ok(()) => Err(ferr!("first run, woohoo!").into()),
                Err(err) => Err(JobErrorX::Terminal(ferr!(
                    "error setting keyvalue: {:?}",
                    err
                ))),
            },
            Ok(Some(bytes)) => {
                // Parse as u64 (little-endian, 8 bytes)
                if bytes.len() != 8 {
                    return Err(ferr!("not u64").into());
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                let number = u64::from_le_bytes(buf);
                if number != VALUE {
                    Err(JobErrorX::Terminal(ferr!("unexpected value: {number}")))
                } else {
                    Ok(Json(()))
                }
            }
        }
    })?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct FailsUntilToldArgs {
    key: String,
}

fn fails_until_told(cx: WflowCtx, args: FailsUntilToldArgs) -> Result<(), JobErrorX> {
    use api_utils_rs::wit::wasi::keyvalue::store;

    // Check if we should succeed by reading from keyvalue store
    cx.effect(|| {
        let bucket = store::open("default")
            .map_err(|err| JobErrorX::Terminal(ferr!("error opening bucket: {:?}", err)))?;
        let key = &args.key;
        match bucket.get(key) {
            Err(err) => Err(JobErrorX::Terminal(ferr!(
                "error getting keyvalue: {:?}",
                err
            ))),
            Ok(None) => {
                // Key doesn't exist, fail transiently
                Err(ferr!("waiting for flag to be set").into())
            }
            Ok(Some(bytes)) => {
                // Check if the value is true (1 byte with value 1)
                if bytes.len() == 1 && bytes[0] == 1 {
                    // Flag is set, succeed
                    Ok(Json(()))
                } else {
                    // Flag is not set or wrong value, fail transiently
                    Err(ferr!("flag not set yet").into())
                }
            }
        }
    })?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
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
        // Single row, Int4
        (
            "SELECT 1 AS int4_val",
            vec![row(vec![entry("int4_val", types::PgValue::Int4(1))])],
        ),
        // Single row, Int8
        (
            "SELECT 9223372036854775807 AS int8_val",
            vec![row(vec![entry("int8_val", types::PgValue::Int8(9223372036854775807))])],
        ),
        // Single row, Text
        (
            "SELECT 'hello world' AS text_val",
            vec![row(vec![entry("text_val", types::PgValue::Text("hello world".to_string()))])],
        ),
        // Single row, Bool
        (
            "SELECT true AS bool_val",
            vec![row(vec![entry("bool_val", types::PgValue::Bool(true))])],
        ),
        // Single row, NULL
        (
            "SELECT NULL AS null_val",
            vec![row(vec![entry("null_val", types::PgValue::Null)])],
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
                        (types::PgValue::Int4(e), types::PgValue::Int4(a)) if e == a => {}
                        (types::PgValue::Int8(e), types::PgValue::Int8(a)) if e == a => {}
                        (types::PgValue::Text(e), types::PgValue::Text(a)) if e == a => {}
                        (types::PgValue::Bool(e), types::PgValue::Bool(a)) if e == a => {}
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
