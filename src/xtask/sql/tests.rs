use super::*;

/// A workspace root holding one unit at `src/<unit>` with the given
/// migration files. Returns `(root, unit_dir)`; the root must outlive
/// the test.
fn unit(unit: &str, files: &[(&str, &str)]) -> Res<(tempfile::TempDir, PathBuf)> {
    let root = tempfile::tempdir()?;
    let dir = root.path().join("src").join(unit);
    let migrations = dir.join("sqlite/migrations");
    std::fs::create_dir_all(&migrations)?;
    for (file, sql) in files {
        std::fs::write(migrations.join(file), sql)?;
    }
    Ok((root, dir))
}

#[test]
fn migration_versions_must_start_at_one_and_be_contiguous() -> Res<()> {
    let cases: &[(&[&str], &str)] = &[
        (&["000_init.sql"], "expected 1, found 0"),
        (&["002_init.sql"], "expected 1, found 2"),
        (&["001_init.sql", "003_gap.sql"], "expected 2, found 3"),
        (&["001_init.sql", "01_duplicate.sql"], "expected 2, found 1"),
        (&["init.sql"], "NNN_name.sql"),
        (&["_init.sql"], "NNN_name.sql"),
        (&["-1_init.sql"], "NNN_name.sql"),
        (&["one_init.sql"], "NNN_name.sql"),
        (&["+1_init.sql"], "NNN_name.sql"),
        (&["001_.sql"], "NNN_name.sql"),
        (
            &["9223372036854775808_init.sql"],
            "invalid migration version",
        ),
    ];
    for (files, expected) in cases {
        let files: Vec<(&str, &str)> = files.iter().map(|file| (*file, "SELECT 1;")).collect();
        let (root, dir) = unit("alpha", &files)?;
        for mode in [Mode::Check, Mode::Gen, Mode::GenCheck] {
            let error =
                process_unit(mode, root.path(), &dir).expect_err("invalid migration sequence");
            assert!(error.to_string().contains(expected), "{files:?}: {error}");
        }
        assert!(!dir.join("sqlite/gen.rs").exists());
    }
    Ok(())
}

#[test]
fn generated_migrations_are_ordered_and_checked_for_staleness() -> Res<()> {
    let files: Vec<(String, &str)> = (1..=12)
        .rev()
        .map(|version| (format!("{version}_step.sql"), "SELECT 1;"))
        .collect();
    let borrowed: Vec<(&str, &str)> = files
        .iter()
        .map(|(file, sql)| (file.as_str(), *sql))
        .collect();
    let (root, dir) = unit("alpha", &borrowed)?;
    let migrations_dir = dir.join("sqlite/migrations");
    let migrations = read_migrations(&migrations_dir)?;
    assert_eq!(
        migrations
            .iter()
            .map(|migration| migration.version)
            .collect::<Vec<_>>(),
        (1..=12).collect::<Vec<_>>()
    );
    process_unit(Mode::Gen, root.path(), &dir)?;
    let generated = std::fs::read_to_string(dir.join("sqlite/gen.rs"))?;
    let entries = (1..=12).map(|version| format!(
        "    Migration {{ version: {version}, name: \"{version}_step.sql\", sql: include_str!(\"migrations/{version}_step.sql\") }},\n"
    )).collect::<String>();
    assert!(generated.contains(&format!(
        "pub const MIGRATIONS: &[Migration<'static>] = &[\n{entries}];"
    )));
    assert!(generated.contains("use crate::sqlite_rs_alias::Migration;"));
    assert!(generated.contains("pub const MIGRATION_TABLE: &str = \"_alpha_migrations\";"));
    process_unit(Mode::GenCheck, root.path(), &dir)?;
    std::fs::write(migrations_dir.join("13_step.sql"), "SELECT 1;")?;
    let error =
        process_unit(Mode::GenCheck, root.path(), &dir).expect_err("migration list is stale");
    assert!(error.to_string().contains("out of date"), "{error}");
    Ok(())
}

#[test]
fn empty_migrations_generate_an_empty_constant() -> Res<()> {
    let (root, dir) = unit("alpha", &[])?;
    process_unit(Mode::Gen, root.path(), &dir)?;
    process_unit(Mode::GenCheck, root.path(), &dir)?;
    let generated = std::fs::read_to_string(dir.join("sqlite/gen.rs"))?;
    assert!(generated.contains("pub const MIGRATIONS: &[Migration<'static>] = &[\n];"));
    assert!(generated.ends_with('\n'));
    assert!(!generated.ends_with("\n\n"));
    Ok(())
}

#[tokio::test]
async fn discovery_ignores_empty_directories_left_by_removed_units() -> Res<()> {
    let root = tempfile::tempdir()?;
    let mut live_units = Vec::new();
    let mut removed_units = Vec::new();
    for suffix in ["ref", "set"] {
        let live = root
            .path()
            .join(format!("src/big_cabinet/index/datum_{suffix}"));
        let removed = root
            .path()
            .join(format!("src/big_cabinet/index/facet_{suffix}"));
        for dir in [&live, &removed] {
            std::fs::create_dir_all(dir.join("sqlite/migrations"))?;
            std::fs::create_dir_all(dir.join("sqlite/queries"))?;
        }
        std::fs::write(live.join("sqlite/migrations/001_init.sql"), "SELECT 1;")?;
        process_unit(Mode::Gen, root.path(), &live)?;
        live_units.push(live);
        removed_units.push(removed);
    }
    std::fs::write(removed_units[0].join("sqlite/migrations/.gitkeep"), "")?;
    for check in [true, false] {
        dispatch(
            root.path(),
            SqlCmd::Gen {
                check,
                units: Units {
                    dirs: Vec::new(),
                    all: true,
                },
            },
        )
        .await?;
    }
    assert_eq!(discover_dirs(root.path())?, live_units);
    for removed in removed_units {
        assert!(!removed.join("sqlite/gen.rs").exists());
    }
    Ok(())
}

#[test]
fn discovery_includes_query_only_units_and_validates_their_sql() -> Res<()> {
    let (root, dir) = unit("query_only", &[])?;
    std::fs::create_dir_all(dir.join("sqlite/queries"))?;
    let query = dir.join("sqlite/queries/value.sql");
    std::fs::write(
        &query,
        "-- name: value :one\n-- returns: value i64\nSELECT 1 AS value",
    )?;
    assert_eq!(discover_dirs(root.path())?, vec![dir.clone()]);
    process_unit(Mode::Gen, root.path(), &dir)?;
    process_unit(Mode::GenCheck, root.path(), &dir)?;
    std::fs::write(
        &query,
        "-- name: value :one\n-- returns: value i64\nSELECT missing AS value",
    )?;
    assert_eq!(discover_dirs(root.path())?, vec![dir.clone()]);
    let error = process_unit(Mode::GenCheck, root.path(), &dir).expect_err("invalid SQL");
    assert!(error.to_string().contains("prepare failed"), "{error}");
    Ok(())
}

#[test]
fn nested_units_derive_distinct_history_tables() -> Res<()> {
    let (root, crate_dir) = unit("daybook_core", &[("001_init.sql", "SELECT 1;")])?;
    let nested = root.path().join("src/daybook_core/blobs");
    std::fs::create_dir_all(nested.join("sqlite/migrations"))?;
    std::fs::write(nested.join("sqlite/migrations/001_init.sql"), "SELECT 1;")?;
    // A directory below `sqlite/` is never a unit, nor are hidden dirs.
    for ignored in [
        crate_dir.join("sqlite/queries/sqlite/migrations"),
        root.path().join("src/.hidden/sqlite/migrations"),
        root.path().join("src/target/sqlite/migrations"),
        root.path().join("src/node_modules/sqlite/migrations"),
    ] {
        std::fs::create_dir_all(&ignored)?;
        std::fs::write(ignored.join("001_init.sql"), "SELECT 1;")?;
    }
    assert_eq!(
        discover_dirs(root.path())?,
        vec![crate_dir.clone(), nested.clone()]
    );
    assert_eq!(
        migration_table(root.path(), &crate_dir)?,
        "_daybook_core_migrations"
    );
    assert_eq!(
        migration_table(root.path(), &nested)?,
        "_daybook_core_blobs_migrations"
    );
    let outside = tempfile::tempdir()?;
    std::fs::create_dir_all(outside.path().join("sqlite/migrations"))?;
    assert!(migration_table(root.path(), outside.path()).is_err());
    Ok(())
}

#[test]
fn generated_rows_existing_types_and_cardinality() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join("different_filename.sql");
    std::fs::write(
        &file,
        "-- name: get_item :optional\n-- param: id i64\n-- returns: id i64, name String\nSELECT :id AS id, 'item' AS name",
    )?;
    let mut query = parse_query(&file)?;
    let connection = RusqliteConnection::open_in_memory()?;
    check_query(&connection, &mut query)?;
    let output = generate("_t_migrations", &[], &[query]);
    assert!(output.contains("pub struct GetItemRow"));
    assert!(output.contains("impl crate::sqlite_rs_alias::FromRow for GetItemRow"));
    assert!(output.contains("QueryExecutor"));
    assert!(output.contains("set.optional()"));
    assert!(output.contains("queries/different_filename.sql"));
    assert!(!output.contains("get_item_tx"));
    assert!(output.ends_with('\n'));
    assert!(!output.ends_with("\n\n"));
    let mut query = parse_query(&file)?;
    query.into = Some("crate::domain::Item".into());
    for (kind, terminal) in [
        (Kind::One, "one_with"),
        (Kind::Optional, "optional_with"),
        (Kind::Many, "all_with"),
    ] {
        query.kind = kind;
        for write in [false, true] {
            query.write = write;
            let (_, body) = query_body(&query, "db", "GET_ITEM", &[]);
            assert!(body.contains(&format!("set.{terminal}(|row| Ok(crate::domain::Item {{ id: row.get::<i64>(\"id\")?, name: row.get::<String>(\"name\")? }}))")));
            assert_eq!(body.contains(".result_set"), write);
        }
    }
    let output = generate("_t_migrations", &[], &[query]);
    assert!(!output.contains("impl crate::sqlite_rs_alias::FromRow"));
    assert!(!output.contains("pub struct GetItemRow"));
    Ok(())
}

#[test]
fn checker_rejects_parameter_order_and_invalid_headers() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join("query.sql");
    let connection = RusqliteConnection::open_in_memory()?;
    for text in [
        "-- name: query :one\n-- param: first i64\n-- param: second i64\n-- returns: value i64\nSELECT :second + :first AS value",
        "-- name: query :one\n-- param: first i64\n-- returns: value i64\nSELECT @first AS value",
        "-- name: query :one\nSELECT 1",
        "-- name: query :one\n-- returns: type i64\nSELECT 1 AS type",
        "-- name: query :one\n-- returns: value i64, value i64\nSELECT 1 AS value, 2 AS value",
        "-- name: query :one\n-- into: x;bad\n-- returns: value i64\nSELECT 1 AS value",
        "-- name: query :exec\n-- into: Existing\nSELECT 1",
    ] {
        std::fs::write(&file, text)?;
        assert!(
            parse_query(&file)
                .and_then(|mut query| check_query(&connection, &mut query))
                .is_err(),
            "{text}"
        );
    }
    Ok(())
}

#[test]
fn checker_compares_declared_column_types_by_affinity() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join("query.sql");
    let connection = RusqliteConnection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, payload BLOB, loose ANY, flag INT) STRICT;",
    )?;
    let accepted = [
        "-- name: q :many\n-- returns: id i64, name String, score Option<f64>, payload Option<Vec<u8>>\nSELECT id, name, score, payload FROM items",
        // ANY accepts every mapping; bool maps onto INTEGER affinity.
        "-- name: q :many\n-- returns: loose String, flag bool\nSELECT loose, flag FROM items",
        // Expression columns carry no decltype and are trusted.
        "-- name: q :one\n-- returns: n i64, label String\nSELECT COUNT(*) AS n, 'x' AS label FROM items",
        // Aliases keep the underlying column's decltype.
        "-- name: q :many\n-- returns: item_name String\nSELECT name AS item_name FROM items",
    ];
    for text in accepted {
        std::fs::write(&file, text)?;
        let mut query = parse_query(&file)?;
        check_query(&connection, &mut query).unwrap_or_else(|err| panic!("{text}: {err}"));
    }
    let rejected = [
        (
            "-- name: q :many\n-- returns: id String\nSELECT id FROM items",
            "INTEGER",
        ),
        (
            "-- name: q :many\n-- returns: name i64\nSELECT name FROM items",
            "TEXT",
        ),
        (
            "-- name: q :many\n-- returns: score i64\nSELECT score FROM items",
            "REAL",
        ),
        (
            "-- name: q :many\n-- returns: payload String\nSELECT payload FROM items",
            "BLOB",
        ),
        (
            "-- name: q :many\n-- returns: name Json<Vec<i64>>\nSELECT name FROM items",
            "",
        ),
        (
            "-- name: q :many\n-- returns: id Json<Vec<i64>>\nSELECT id FROM items",
            "INTEGER",
        ),
    ];
    for (text, expected) in rejected {
        std::fs::write(&file, text)?;
        let mut query = parse_query(&file)?;
        let result = check_query(&connection, &mut query);
        if expected.is_empty() {
            // Json is text; a TEXT column accepts it.
            result.unwrap_or_else(|err| panic!("{text}: {err}"));
        } else {
            let error = result.expect_err(text).to_string();
            assert!(error.contains(expected), "{text}: {error}");
        }
    }
    assert_eq!(Affinity::of_decl_type("VARCHAR(255)"), Affinity::Text);
    assert_eq!(
        Affinity::of_decl_type("UNSIGNED BIG INT"),
        Affinity::Integer
    );
    assert_eq!(Affinity::of_decl_type("BOOLEAN"), Affinity::Any);
    assert_eq!(Affinity::of_decl_type("DOUBLE PRECISION"), Affinity::Real);
    assert_eq!(Affinity::of_decl_type(""), Affinity::Blob);
    assert_eq!(Affinity::of_decl_type("NUMERIC"), Affinity::Any);
    assert_eq!(Affinity::of_decl_type("DATETIME"), Affinity::Any);
    Ok(())
}

#[test]
fn migration_cli_inspection_does_not_write_and_discovery_is_sorted() -> Res<()> {
    let (root, alpha) = unit(
        "zeta",
        &[("001_init.sql", "CREATE TABLE items (id INTEGER);")],
    )?;
    let zeta = alpha;
    let alpha = root.path().join("src/alpha");
    std::fs::create_dir_all(alpha.join("sqlite/migrations"))?;
    std::fs::write(
        alpha.join("sqlite/migrations/001_init.sql"),
        "CREATE TABLE items (id INTEGER);",
    )?;
    std::fs::create_dir_all(root.path().join("src/not_sqlite"))?;
    assert_eq!(discover_dirs(root.path())?, vec![alpha.clone(), zeta]);
    let database = root.path().join("state.sqlite");
    migrate(root.path(), &alpha, &database, None, true)?;
    assert!(!database.exists());
    migrate(root.path(), &alpha, &database, None, false)?;
    let before = std::fs::read(&database)?;
    migrate(root.path(), &alpha, &database, None, true)?;
    assert_eq!(std::fs::read(&database)?, before);
    migrate(root.path(), &alpha, &database, None, false)?;
    let connection = RusqliteConnection::open(&database)?;
    let applied: i64 =
        connection.query_row("SELECT COUNT(*) FROM _alpha_migrations", [], |row| {
            row.get(0)
        })?;
    assert_eq!(applied, 1);
    Ok(())
}
