//! `cargo x sql-check|sql-gen [--check] <unit-dir>...`, or `--all`.
//!
//! The replacement for `cargo sqlx prepare`: instead of discovering
//! queries by compiling the workspace, it discovers `.sql` files.
//!
//! A *unit* is any directory under `src/` that owns a `sqlite/migrations/`
//! directory. Units are not tied to crates: `src/big_repo/` is one unit and
//! `src/daybook_core/blobs/` can be another, so a crate whose modules own
//! unrelated tables (possibly on different physical databases) declares one
//! unit per module. Several units may share one physical database; each
//! keeps its own history table (`MIGRATION_TABLE` in the generated code,
//! derived from the unit path, e.g. `_daybook_core_blobs_migrations`), and the
//! owner applies them with `Database::apply_migrations`.
//!
//! For each unit directory it expects:
//!
//! ```text
//! <unit>/sqlite/migrations/*.sql   (NNN_name.sql, applied in order)
//! <unit>/sqlite/queries/*.sql      (-- name: foo :one|:many|:optional|:exec)
//! <unit>/sqlite/gen.rs             (generated; checked in)
//! ```
//!
//! `check` applies the migrations to a temporary file database, then
//! *prepares* every query against the migrated schema. SQLite's prepare
//! step is the whole safety argument: unknown tables/columns, bad syntax
//! and named-parameter metadata come from SQLite itself; no SQL parser is
//! written here. For result columns that map straight onto a table column,
//! the declared column type (`sqlite3_column_decltype`) is converted to its
//! SQLite affinity and compared with the `-- returns:` type, so declaring
//! `String` for an `INTEGER` column fails the check rather than the first
//! decode at runtime. Expression columns have no decltype and are trusted.
//! `gen` additionally emits typed async functions (into `sqlite/gen.rs` via
//! `write`, or verifies them with `--check`).
//!
//! Query file format (headers are SQL comments before the statement):
//!
//! ```sql
//! -- name: get_item :optional
//! -- param: id i64
//! -- returns: id i64, name String, payload Option<Vec<u8>>
//! SELECT id, name, payload FROM items WHERE id = :id;
//! ```
//!
//! The generated `gen.rs` refers to the runtime through a crate-local
//! alias so the same output works inside `sqlite_rs` itself and inside
//! consumer crates: consumers put this next to `mod sqlite`:
//!
//! ```rust,ignore
//! use sqlite_rs as sqlite_rs_alias;
//! mod sqlite { pub mod queries { include!("sqlite/gen.rs"); } }
//! ```
//!
//! - `:exec` queries take params and return `ExecuteResult`.
//! - For `:one`/`:optional`/`:many`, `-- returns:` declares the result columns.
//!   Multiple columns generate a query-specific struct, or construct the existing
//!   Rust type specified by `-- into: crate::module::Type`.
//! - Parameters are declared in order of first appearance in the SQL;
//!   every `:name` token in the statement must be declared exactly once
//!   and vice versa. Types: i64, f64, String, Vec<u8>, bool, and
//!   Option<...> of those.

use crate::interlude::*;

use rusqlite::Connection as RusqliteConnection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Check,
    Gen,
    GenCheck,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    One,
    Optional,
    Many,
    Exec,
}

#[derive(Debug)]
struct MigrationSpec {
    version: i64,
    file_name: String,
    sql: String,
}

#[derive(Debug)]
struct QuerySpec {
    name: String,
    kind: Kind,
    params: Vec<(String, String)>,
    returns: Vec<(String, String)>,
    into: Option<String>,
    write: bool,
    sql: String,
    file: PathBuf,
}

/// Every unit directory below `root/src`, sorted. A unit is the parent of a
/// `sqlite/migrations` directory; the walk descends into subdirectories so
/// `src/<crate>/<module>/sqlite/` units are found too, but never descends
/// into a `sqlite/` directory itself, since nothing below it can be a unit.
pub fn discover_dirs(root: &Path) -> Res<Vec<PathBuf>> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Res<()> {
        if dir.join("sqlite/migrations").is_dir() {
            out.push(dir.to_path_buf());
        }
        for entry in
            std::fs::read_dir(dir).wrap_err_with(|| format!("reading {}", dir.display()))?
        {
            let path = entry?.path();
            let name = path.file_name().and_then(|name| name.to_str());
            if path.is_dir()
                && !matches!(name, Some("sqlite" | "target" | "node_modules"))
                && !name.is_some_and(|name| name.starts_with('.'))
            {
                walk(&path, out)?;
            }
        }
        Ok(())
    }
    let mut dirs = Vec::new();
    walk(&root.join("src"), &mut dirs)?;
    dirs.sort();
    Ok(dirs)
}

pub async fn workspace_root() -> Res<PathBuf> {
    let output = tokio::process::Command::new("cargo")
        .args(["locate-project", "--workspace", "--message-format", "plain"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .await?;
    eyre::ensure!(
        output.status.success(),
        "cargo locate-project failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let manifest = PathBuf::from(String::from_utf8(output.stdout)?.trim());
    Ok(manifest
        .parent()
        .ok_or_eyre("workspace manifest has no parent")?
        .to_path_buf())
}

/// The history table a unit records its applied migrations in. Derived from
/// the unit path below `src/` so two units sharing a physical database can
/// never collide: `src/daybook_core/blobs` -> `_daybook_core_blobs_migrations`.
fn migration_table(root: &Path, dir: &Path) -> Res<String> {
    let dir = dir.canonicalize()?;
    let relative = dir
        .strip_prefix(root.canonicalize()?.join("src"))
        .wrap_err_with(|| format!("{} is not below {}/src", dir.display(), root.display()))?;
    let mut name = String::from("_");
    for component in relative.components() {
        let component = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| eyre::eyre!("{}: non UTF-8 path component", dir.display()))?;
        eyre::ensure!(
            valid_rust_name(component),
            "{}: unit path component `{component}` is not an identifier",
            dir.display()
        );
        name.push_str(component);
        name.push('_');
    }
    name.push_str("migrations");
    Ok(name)
}

pub fn migrate(
    root: &Path,
    dir: &Path,
    database: &Path,
    table: Option<&str>,
    inspect: bool,
) -> Res<()> {
    let table = match table {
        Some(table) => table.to_owned(),
        None => migration_table(root, dir)?,
    };
    let migrations = read_migrations(&dir.join("sqlite/migrations"))?;
    let borrowed: Vec<sqlite_rs::Migration<'_>> = migrations
        .iter()
        .map(|migration| sqlite_rs::Migration {
            version: migration.version,
            name: &migration.file_name,
            sql: &migration.sql,
        })
        .collect();
    sqlite_rs::init_sqlite_vec();
    if inspect && !database.try_exists()? {
        for migration in &borrowed {
            println!("{} {} Pending", migration.version, migration.name);
        }
        return Ok(());
    }
    let flags = if inspect {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
    } else {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
    };
    let mut connection = RusqliteConnection::open_with_flags(database, flags)?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    if !inspect {
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        sqlite_rs::run_migrations(&mut connection, &table, &borrowed)?;
    }
    for (migration, state) in sqlite_rs::migration_status(&connection, &table, &borrowed)? {
        println!("{} {} {state:?}", migration.version, migration.name);
    }
    Ok(())
}

pub async fn cli(mode: Mode, root: &Path, dirs: Vec<PathBuf>) -> Res<()> {
    if dirs.is_empty() {
        eyre::bail!("usage: cargo xtask sql check|gen [--check] <unit-dir>...");
    }
    let mut failures: Vec<String> = Vec::new();
    for dir in &dirs {
        if let Err(err) = process_unit(mode, root, dir) {
            failures.push(format!(
                "{}: {err}",
                dir.strip_prefix(root).unwrap_or(dir).display()
            ));
        }
    }
    if !failures.is_empty() {
        for failure in &failures {
            eprintln!("sql: {failure}");
        }
        eyre::bail!(
            "{} unit(s) failed sql {}",
            failures.len(),
            match mode {
                Mode::Check => "check",
                Mode::Gen | Mode::GenCheck => "gen",
            }
        );
    }
    Ok(())
}

fn process_unit(mode: Mode, root: &Path, dir: &Path) -> Res<()> {
    let sqlite_dir = dir.join("sqlite");
    let mig_dir = sqlite_dir.join("migrations");
    let query_dir = sqlite_dir.join("queries");
    if !mig_dir.is_dir() {
        eyre::bail!("missing {}", mig_dir.display());
    }
    let table = migration_table(root, dir)?;
    let migrations = read_migrations(&mig_dir)?;
    let mut queries = Vec::new();
    if query_dir.is_dir() {
        for path in read_sql_paths(&query_dir)? {
            queries.push(parse_query(&path)?);
        }
    }

    // Apply migrations to a fresh temporary database, then prepare every
    // query against the resulting schema.
    let tmp = tempfile::tempdir().wrap_err("creating temp dir for sql check")?;
    let db_path = tmp.path().join("check.sqlite");
    sqlite_rs::init_sqlite_vec();
    let mut conn = RusqliteConnection::open(&db_path).wrap_err("opening temp database")?;
    // Reuse the runtime's migration runner so checker and runtime cannot
    // disagree about how migrations are applied.
    let owned: Vec<sqlite_rs::Migration> = migrations
        .iter()
        .map(|migration| sqlite_rs::Migration {
            version: migration.version,
            name: &migration.file_name,
            sql: &migration.sql,
        })
        .collect();
    sqlite_rs::run_migrations(&mut conn, &table, &owned)
        .map_err(|err| eyre::eyre!("applying migrations: {err}"))?;
    let mut names = std::collections::HashSet::new();
    for query in &mut queries {
        if !names.insert(query.name.clone()) {
            eyre::bail!("duplicate query name {}", query.name);
        }
        check_query(&conn, query)?;
    }

    if mode != Mode::Check {
        let generated = generate(&table, &migrations, &queries);
        let gen_path = sqlite_dir.join("gen.rs");
        match mode {
            Mode::Gen => {
                std::fs::write(&gen_path, &generated)
                    .wrap_err_with(|| format!("writing {}", gen_path.display()))?;
            }
            Mode::GenCheck => {
                let existing = std::fs::read_to_string(&gen_path).unwrap_or_default();
                if existing != generated {
                    eyre::bail!(
                        "{} is out of date; run `cargo x sql-gen {}`",
                        gen_path.display(),
                        dir.display()
                    );
                }
            }
            Mode::Check => unreachable!(),
        }
    }
    println!(
        "sql: {} ok ({} migrations, {} queries)",
        dir.strip_prefix(root).unwrap_or(dir).display(),
        migrations.len(),
        queries.len()
    );
    Ok(())
}

fn read_sql_paths(dir: &Path) -> Res<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir).wrap_err_with(|| format!("reading {}", dir.display()))? {
        let path = entry
            .wrap_err_with(|| format!("reading entry in {}", dir.display()))?
            .path();
        if path.extension().is_some_and(|extension| extension == "sql") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn read_migrations(dir: &Path) -> Res<Vec<MigrationSpec>> {
    let mut migrations = Vec::new();
    for path in read_sql_paths(dir)? {
        let file_name = path
            .file_name()
            .expect("migration file name")
            .to_str()
            .ok_or_else(|| eyre::eyre!("{}: migration file name is not UTF-8", path.display()))?;
        let stem = path
            .file_stem()
            .expect("migration file stem")
            .to_str()
            .expect("validated UTF-8");
        let (prefix, name) = stem
            .split_once('_')
            .ok_or_else(|| eyre::eyre!("{file_name}: migration file name must be NNN_name.sql"))?;
        if prefix.is_empty() || !prefix.bytes().all(|byte| byte.is_ascii_digit()) || name.is_empty()
        {
            eyre::bail!("{file_name}: migration file name must be NNN_name.sql");
        }
        let version = prefix
            .parse::<i64>()
            .wrap_err_with(|| format!("{file_name}: invalid migration version"))?;
        let sql = std::fs::read_to_string(&path)
            .wrap_err_with(|| format!("reading {}", path.display()))?;
        migrations.push(MigrationSpec {
            version,
            file_name: file_name.to_owned(),
            sql,
        });
    }
    migrations.sort_by_key(|migration| migration.version);
    for (index, migration) in migrations.iter().enumerate() {
        let expected = i64::try_from(index)? + 1;
        if migration.version != expected {
            eyre::bail!(
                "{}: migration versions must be contiguous starting at 1; expected {expected}, found {}",
                migration.file_name,
                migration.version
            );
        }
    }
    Ok(migrations)
}

/// Prepare-only validation against an already-migrated connection.
fn check_query(conn: &RusqliteConnection, query: &mut QuerySpec) -> Res<()> {
    let stmt = conn
        .prepare(&query.sql)
        .wrap_err_with(|| format!("{}: prepare failed", query.file.display()))?;
    let declared: Vec<&str> = query.params.iter().map(|(name, _)| name.as_str()).collect();
    let expected_count = stmt.parameter_count();
    if expected_count != declared.len() {
        eyre::bail!(
            "{}: statement takes {expected_count} parameters but header declares {} {declared:?}",
            query.file.display(),
            declared.len()
        );
    }
    for idx in 1..=expected_count {
        let Some(name) = stmt.parameter_name(idx) else {
            eyre::bail!(
                "{}: parameter #{idx} is positional; named `:name` parameters are required",
                query.file.display()
            );
        };
        if name != format!(":{}", declared[idx - 1]) {
            eyre::bail!(
                "{}: parameter #{idx} is {name}, expected :{} in declaration order",
                query.file.display(),
                declared[idx - 1]
            );
        }
    }
    query.write = !stmt.readonly();
    if query.kind != Kind::Exec {
        let columns = stmt.columns();
        let names: Vec<&str> = columns.iter().map(|column| column.name()).collect();
        let returned: Vec<&str> = query
            .returns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        if names != returned {
            eyre::bail!(
                "{}: statement returns columns {names:?} but header declares {returned:?}",
                query.file.display()
            );
        }
        for (column, (name, ty)) in columns.iter().zip(&query.returns) {
            // Expression columns (COUNT(*), CASE, literals, ...) have no
            // declared type; only table-backed columns can be checked here.
            let Some(decl) = column.decl_type() else {
                continue;
            };
            let column_affinity = Affinity::of_decl_type(decl);
            let declared_affinity = Affinity::of_rust_type(ty);
            if !column_affinity.accepts(declared_affinity) {
                eyre::bail!(
                    "{}: column {name} is declared `{decl}` ({column_affinity:?}) in the schema but the header maps it to `{ty}` ({declared_affinity:?})",
                    query.file.display()
                );
            }
        }
    }
    Ok(())
}

/// SQLite column affinity, as derived by `sqlite3` from a declared column
/// type (the "Determination Of Column Affinity" rules). Used to compare a
/// table column's `decltype` with the Rust type declared in `-- returns:`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Affinity {
    Integer,
    Real,
    Text,
    Blob,
    /// `NUMERIC` affinity, and `ANY` in `STRICT` tables: the column stores
    /// whatever it is handed, so no Rust type can be ruled out.
    Any,
}

impl Affinity {
    fn of_decl_type(decl: &str) -> Self {
        let decl = decl.to_ascii_uppercase();
        if decl.contains("INT") {
            Self::Integer
        } else if decl.contains("CHAR") || decl.contains("CLOB") || decl.contains("TEXT") {
            Self::Text
        } else if decl.contains("BLOB") || decl.is_empty() {
            Self::Blob
        } else if decl.contains("REAL") || decl.contains("FLOA") || decl.contains("DOUB") {
            Self::Real
        } else {
            Self::Any
        }
    }

    fn of_rust_type(ty: &str) -> Self {
        let inner = ty
            .strip_prefix("Option<")
            .and_then(|inner| inner.strip_suffix('>'))
            .unwrap_or(ty);
        match inner {
            "i64" | "bool" => Self::Integer,
            "f64" => Self::Real,
            "String" => Self::Text,
            "Vec<u8>" => Self::Blob,
            // `Json<...>` is stored as serialized text.
            _ => Self::Text,
        }
    }

    fn accepts(self, declared: Self) -> bool {
        self == Self::Any || self == declared
    }
}

fn parse_query(path: &Path) -> Res<QuerySpec> {
    let text =
        std::fs::read_to_string(path).wrap_err_with(|| format!("reading {}", path.display()))?;
    let mut name: Option<String> = None;
    let mut kind: Option<Kind> = None;
    let mut params: Vec<(String, String)> = Vec::new();
    let mut returns: Vec<(String, String)> = Vec::new();
    let mut into = None;
    let mut sql_lines: Vec<&str> = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("-- name:") {
            let mut parts = rest.split_whitespace();
            name = Some(
                parts
                    .next()
                    .ok_or_else(|| {
                        eyre::eyre!("{}: `-- name:` missing query name", path.display())
                    })?
                    .to_string(),
            );
            kind = Some(match parts.next() {
                Some(":one") => Kind::One,
                Some(":optional") => Kind::Optional,
                Some(":many") => Kind::Many,
                Some(":exec") => Kind::Exec,
                other => {
                    eyre::bail!(
                        "{}: `-- name:` needs one of :one/:optional/:many/:exec, got {other:?}",
                        path.display()
                    )
                }
            });
        } else if let Some(rest) = trimmed.strip_prefix("-- into:") {
            let path = rest.trim();
            eyre::ensure!(
                into.is_none()
                    && !path.is_empty()
                    && path
                        .split("::")
                        .all(|part| valid_rust_name(part)
                            || matches!(part, "crate" | "super" | "self")),
                "invalid or repeated -- into: {path}"
            );
            into = Some(path.to_owned());
        } else if let Some(rest) = trimmed.strip_prefix("-- param:") {
            params.push(parse_typed(rest, path, "param")?);
        } else if let Some(rest) = trimmed.strip_prefix("-- returns:") {
            for item in rest.split(',') {
                let item = item.trim();
                if item.is_empty() {
                    continue;
                }
                returns.push(parse_typed(item, path, "returns")?);
            }
        } else if trimmed.starts_with("--") {
            // Ordinary SQL comment: part of the statement's documentation,
            // not sent to SQLite.
            continue;
        } else {
            sql_lines.push(line);
        }
    }
    let sql = sql_lines.join("\n").trim().to_string();
    if sql.is_empty() {
        eyre::bail!("{}: no SQL statement found", path.display());
    }
    let name = name.ok_or_else(|| eyre::eyre!("{}: missing `-- name:` header", path.display()))?;
    eyre::ensure!(valid_rust_name(&name), "invalid query name {name}");
    let kind = kind.expect("kind set with name");
    eyre::ensure!(
        kind == Kind::Exec || !returns.is_empty(),
        "{}: -- returns: is required",
        path.display()
    );
    eyre::ensure!(
        kind != Kind::Exec || (into.is_none() && returns.is_empty()),
        ":exec cannot declare -- into: or -- returns:"
    );
    for fields in [&params, &returns] {
        let mut seen = std::collections::HashSet::new();
        for (field, _) in fields {
            eyre::ensure!(
                valid_rust_name(field) && seen.insert(field),
                "invalid or duplicate field {field}"
            );
        }
    }
    eyre::ensure!(
        !params.iter().any(|(name, _)| name == "db"),
        "parameter name db is reserved"
    );
    Ok(QuerySpec {
        name,
        kind,
        params,
        returns,
        into,
        write: false,
        sql,
        file: path.to_path_buf(),
    })
}

fn parse_typed(text: &str, path: &Path, what: &str) -> Res<(String, String)> {
    let mut parts = text.trim().splitn(2, char::is_whitespace);
    let name = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| eyre::eyre!("{}: malformed `-- {what}:` header", path.display()))?
        .to_string();
    let ty = parts
        .next()
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .ok_or_else(|| eyre::eyre!("{}: `-- {what}: {name}` missing type", path.display()))?
        .to_string();
    validate_type(&ty, path)?;
    Ok((name, ty))
}

fn valid_rust_name(name: &str) -> bool {
    syn::parse_str::<syn::Ident>(name).is_ok()
        && !matches!(name, "self" | "Self" | "super" | "crate")
}

fn validate_type(ty: &str, path: &Path) -> Res<()> {
    const BASE: &[&str] = &["i64", "f64", "String", "Vec<u8>", "bool"];
    let inner = ty
        .strip_prefix("Option<")
        .and_then(|inner| inner.strip_suffix('>'))
        .unwrap_or(ty);
    if BASE.contains(&inner) {
        return Ok(());
    }
    if let Some(element) = ty
        .strip_prefix("Json<Vec<")
        .and_then(|inner| inner.strip_suffix(">>"))
        && BASE.contains(&element)
    {
        return Ok(());
    }
    eyre::bail!("{}: unsupported type `{ty}`", path.display())
}

fn rust_type(ty: &str) -> String {
    ty.replace("Json<", "crate::sqlite_rs_alias::Json<")
}

fn value_expr(ty: &str, name: &str) -> String {
    if ty.starts_with("Json<") {
        format!("{name}.into_value()?")
    } else {
        format!("crate::sqlite_rs_alias::IntoValue::into_value({name})")
    }
}

fn row_return_ty(query: &QuerySpec) -> String {
    use heck::ToUpperCamelCase;
    if let Some(path) = &query.into {
        path.clone()
    } else if query.returns.len() == 1 {
        rust_type(&query.returns[0].1)
    } else {
        format!("{}Row", query.name.to_upper_camel_case())
    }
}

fn generate(table: &str, migrations: &[MigrationSpec], queries: &[QuerySpec]) -> String {
    let mut out = String::from(
        "// @generated by `cargo x sql-gen`. Do not edit by hand.\nuse crate::sqlite_rs_alias::Migration;\n",
    );
    if !queries.is_empty() {
        out.push_str("use crate::sqlite_rs_alias::SqlResult;\n");
    }
    out.push_str(&format!(
        "\npub const MIGRATION_TABLE: &str = {table:?};\n\npub const MIGRATIONS: &[Migration<'static>] = &[\n"
    ));
    for migration in migrations {
        let path = format!("migrations/{}", migration.file_name);
        out.push_str(&format!(
            "    Migration {{ version: {}, name: {:?}, sql: include_str!({path:?}) }},\n",
            migration.version, migration.file_name
        ));
    }
    out.push_str("];\n\n");
    for query in queries {
        if query.kind != Kind::Exec && query.into.is_none() && query.returns.len() > 1 {
            let ty = row_return_ty(query);
            out.push_str(&format!(
                "#[derive(Clone, Debug, PartialEq)]\npub struct {ty} {{\n"
            ));
            for (field, ty) in &query.returns {
                out.push_str(&format!("    pub {field}: {},\n", rust_type(ty)));
            }
            let decoded = decode_expr(query);
            out.push_str(&format!("}}\nimpl crate::sqlite_rs_alias::FromRow for {ty} {{\n    fn from_row(row: &crate::sqlite_rs_alias::Row) -> SqlResult<Self> {{\n        row.require_width({})?;\n        Ok({decoded})\n    }}\n}}\n\n", query.returns.len()));
        }
        let const_name = query.name.to_uppercase();
        let file = format!(
            "queries/{}",
            query
                .file
                .file_name()
                .expect("query filename")
                .to_str()
                .expect("UTF-8 query filename")
        );
        out.push_str(&format!(
            "pub const {const_name}_SQL: &str = include_str!({file:?});\n"
        ));
        let args: String = query
            .params
            .iter()
            .map(|(name, ty)| format!(", {name}: {}", rust_type(ty)))
            .collect();
        let params: Vec<String> = query
            .params
            .iter()
            .map(|(name, ty)| value_expr(ty, name))
            .collect();
        let receiver = if query.kind == Kind::Exec || query.write {
            "Executor"
        } else {
            "QueryExecutor"
        };
        let (ret, body) = query_body(query, "db", &const_name, &params);
        // Wide tables mean wide signatures; the generated function mirrors the
        // statement's parameter list and there is nothing to group. The
        // receiver counts towards clippy's limit of seven.
        if query.params.len() + 1 > 7 {
            out.push_str("#[allow(clippy::too_many_arguments)]\n");
        }
        out.push_str(&format!("pub async fn {}(db: &impl crate::sqlite_rs_alias::{receiver}{args}) -> {ret} {{\n{body}}}\n\n", query.name));
    }
    out
}

/// Render the body of one generated function. `recv` is `db` or `tx`; both
/// expose identical `query`/`execute` methods, so the body is shared.
fn query_body(
    query: &QuerySpec,
    recv: &str,
    const_name: &str,
    params_vec: &[String],
) -> (String, String) {
    let params = params_vec.join(", ");
    if query.kind == Kind::Exec {
        return (
            "SqlResult<crate::sqlite_rs_alias::ExecuteResult>".into(),
            format!("    {recv}.execute({const_name}_SQL, &[{params}]).await\n"),
        );
    }
    let ty = row_return_ty(query);
    let (ret, terminal) = match query.kind {
        Kind::One => (format!("SqlResult<{ty}>"), "one"),
        Kind::Optional => (format!("SqlResult<Option<{ty}>>"), "optional"),
        Kind::Many => (format!("SqlResult<Vec<{ty}>>"), "all"),
        Kind::Exec => unreachable!(),
    };
    let method = if query.write { "execute" } else { "query" };
    let result = if query.write { ".result_set" } else { "" };
    let mut body =
        format!("    let set = {recv}.{method}({const_name}_SQL, &[{params}]).await?{result};\n");
    if query.into.is_none() {
        body.push_str(&format!("    set.{terminal}()\n"));
    } else {
        let decode = decode_expr(query);
        body.push_str(&format!("    set.{terminal}_with(|row| Ok({decode}))\n"));
    }
    (ret, body)
}

/// Expression decoding `row` (in scope) for `:one`/`:optional` queries.
fn decode_expr(query: &QuerySpec) -> String {
    let fields: Vec<String> = query
        .returns
        .iter()
        .map(|(name, ty)| format!("{name}: row.get::<{}>({name:?})?", rust_type(ty)))
        .collect();
    format!("{} {{ {} }}", row_return_ty(query), fields.join(", "))
}

#[cfg(test)]
mod tests {
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
        Ok(())
    }

    #[test]
    fn nested_units_derive_distinct_history_tables() -> Res<()> {
        let (root, crate_dir) = unit("daybook_core", &[])?;
        let nested = root.path().join("src/daybook_core/blobs");
        std::fs::create_dir_all(nested.join("sqlite/migrations"))?;
        // A directory below `sqlite/` is never a unit, nor are hidden dirs.
        std::fs::create_dir_all(crate_dir.join("sqlite/queries/sqlite/migrations"))?;
        std::fs::create_dir_all(root.path().join("src/.hidden/sqlite/migrations"))?;
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
}
