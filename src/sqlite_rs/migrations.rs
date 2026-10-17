use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationState {
    Applied,
    Pending,
}

pub fn migration_status<'sql>(
    connection: &RusConnection,
    table: &str,
    migrations: &[Migration<'sql>],
) -> SqlResult<Vec<(Migration<'sql>, MigrationState)>> {
    assert!(
        query::valid_identifier(table),
        "invalid migration table name"
    );
    for (index, migration) in migrations.iter().enumerate() {
        let expected = i64::try_from(index).expect("migration count fits i64") + 1;
        if migration.version != expected {
            return Err(SqlError::InvalidQuery(format!(
                "migration versions must be contiguous starting at 1; expected {expected}, found {}",
                migration.version
            )));
        }
    }
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [table],
        |row| row.get(0),
    )?;
    let mut applied = Vec::new();
    if exists {
        let mut statement = connection.prepare(&format!(
            "SELECT version, checksum FROM \"{table}\" ORDER BY version"
        ))?;
        let records = statement.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for record in records {
            applied.push(record?);
        }
    }
    for (index, (version, stored)) in applied.iter().enumerate() {
        let Some(migration) = migrations.get(index) else {
            return Err(SqlError::Unexpected(format!(
                "database has unknown migration version {version}"
            )));
        };
        if *version != migration.version {
            return Err(SqlError::Unexpected(format!(
                "migration history is not a contiguous prefix at version {version}"
            )));
        }
        if *stored != checksum(migration.sql) {
            return Err(SqlError::Unexpected(format!(
                "migration {} ({}) checksum changed after being applied",
                migration.version, migration.name
            )));
        }
    }
    Ok(migrations
        .iter()
        .enumerate()
        .map(|(index, migration)| {
            (
                *migration,
                if index < applied.len() {
                    MigrationState::Applied
                } else {
                    MigrationState::Pending
                },
            )
        })
        .collect())
}

pub fn run_migrations(
    connection: &mut RusConnection,
    table: &str,
    migrations: &[Migration<'_>],
) -> SqlResult<()> {
    connection
        .authorizer(None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>);
    let result = apply_migrations(connection, table, migrations);
    super::authorize(connection, false);
    result
}

fn apply_migrations(
    connection: &mut RusConnection,
    table: &str,
    migrations: &[Migration<'_>],
) -> SqlResult<()> {
    migration_status(connection, table, migrations)?;
    for migration in migrations {
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let status = migration_status(&transaction, table, migrations)?;
        if status[usize::try_from(migration.version - 1).expect("validated version")].1
            == MigrationState::Applied
        {
            transaction.commit()?;
            continue;
        }
        transaction.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" (
                 version INTEGER PRIMARY KEY
               , name TEXT NOT NULL
               , checksum TEXT NOT NULL
             )"
        ))?;
        super::authorize(&transaction, false);
        let applied = transaction.execute_batch(migration.sql);
        transaction.authorizer(
            None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>,
        );
        applied?;
        transaction.execute(
            &format!("INSERT INTO \"{table}\" (version, name, checksum) VALUES (?1, ?2, ?3)"),
            rusqlite::params![migration.version, migration.name, checksum(migration.sql)],
        )?;
        transaction.commit()?;
    }
    Ok(())
}

fn checksum(sql: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in sql.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_rolls_back_schema_and_history_but_keeps_previous_versions() -> SqlResult<()> {
        let mut connection = RusConnection::open_in_memory()?;
        let mut migrations = [
            Migration {
                version: 1,
                name: "first",
                sql: "CREATE TABLE first (id INTEGER);",
            },
            Migration {
                version: 2,
                name: "second",
                sql: "CREATE TABLE second (id INTEGER); INSERT INTO missing VALUES (1);",
            },
        ];
        assert!(run_migrations(&mut connection, "history", &migrations).is_err());
        assert_eq!(
            migration_status(&connection, "history", &migrations)?,
            vec![
                (migrations[0], MigrationState::Applied),
                (migrations[1], MigrationState::Pending)
            ]
        );
        assert!(!connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'second')",
            [],
            |row| row.get::<_, bool>(0)
        )?);
        migrations[1].sql = "CREATE TABLE second (id INTEGER);";
        run_migrations(&mut connection, "history", &migrations)?;
        assert!(migration_status(&connection, "history", &migrations[..1]).is_err());
        connection.execute("DELETE FROM history WHERE version = 1", [])?;
        assert!(migration_status(&connection, "history", &migrations).is_err());
        Ok(())
    }

    #[test]
    fn migration_control_statements_and_version_gaps_are_rejected() -> SqlResult<()> {
        for sql in [
            "CREATE TABLE partial (id INTEGER); COMMIT;",
            "SAVEPOINT rogue;",
            "BEGIN;",
        ] {
            let mut connection = RusConnection::open_in_memory()?;
            let migrations = [Migration {
                version: 1,
                name: "invalid",
                sql,
            }];
            assert!(run_migrations(&mut connection, "history", &migrations).is_err());
            assert!(connection.is_autocommit());
            assert!(!connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'partial')",
                [],
                |row| row.get::<_, bool>(0)
            )?);
        }
        let connection = RusConnection::open_in_memory()?;
        for version in [0, 2, -1] {
            assert!(
                migration_status(
                    &connection,
                    "history",
                    &[Migration {
                        version,
                        name: "gap",
                        sql: "SELECT 1"
                    }]
                )
                .is_err()
            );
        }
        Ok(())
    }
}
