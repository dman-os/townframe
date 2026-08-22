// Kysely dialect over the townframe:sqlite WIT connection.
//
// btress_auth receives a pre-provisioned sqlite `Connection` handle via
// `townframe:api-utils/http-service get-args` (key "auth-db"). This module
// adapts Kysely's query compiler/driver onto that handle: Kysely's
// `SqliteQueryCompiler` produces SQL + parameters, and the driver executes
// them through the WIT `connection.query` / `transaction` methods.
//
// Date params are stored as ISO-8601 TEXT (matching better-sqlite3/better-auth
// conventions); better-auth parses string date columns back into `Date` on
// read, so no further conversion is needed. Integer params map to
// `sql-value.integer` and integer result columns come back as JS numbers.

import type {
  Connection,
  Transaction,
} from "townframe:sqlite/sqlite-connection";
import type { ResultRow, SqlValue } from "townframe:sqlite/types";
import {
  type CompiledQuery,
  type DatabaseConnection,
  type DatabaseIntrospector,
  type Dialect,
  type Driver,
  type Kysely,
  type QueryResult,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
} from "kysely";

/** Convert a Kysely parameter into a `townframe:sqlite` sql-value. */
function toSqlValue(value: unknown): SqlValue {
  if (value === null || value === undefined) return { tag: "null" };
  switch (typeof value) {
    case "string":
      return { tag: "text", val: value };
    case "boolean":
      return { tag: "integer", val: value ? 1n : 0n };
    case "bigint":
      return { tag: "integer", val: value };
    case "number":
      return Number.isInteger(value)
        ? { tag: "integer", val: BigInt(value) }
        : { tag: "real", val: value };
  }
  if (value instanceof Date) return { tag: "text", val: value.toISOString() };
  if (value instanceof Uint8Array) return { tag: "blob", val: value };
  return { tag: "text", val: String(value) };
}

/** Convert a `townframe:sqlite` sql-value back into a JS value. */
function fromSqlValue(value: SqlValue): unknown {
  switch (value.tag) {
    case "null":
      return null;
    case "integer":
      // Better-auth values fit in JS numbers; large ints are not used here.
      return Number(value.val);
    case "real":
      return value.val;
    case "text":
      return value.val;
    case "blob":
      return value.val;
  }
}

/** A `result-row` (list of column-name/value entries) -> a Kysely row object. */
function rowToRecord(row: ResultRow): Record<string, unknown> {
  const record: Record<string, unknown> = {};
  for (const entry of row) {
    record[entry.columnName] = fromSqlValue(entry.value);
  }
  return record;
}

class WitsqlConnection implements DatabaseConnection {
  private tx: Transaction | null = null;

  constructor(private readonly conn: Connection) {}

  async executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
    const { sql, parameters } = compiledQuery;
    const target: Connection | Transaction = this.tx ?? this.conn;
    const rows = target.query(sql, parameters.map(toSqlValue));
    return {
      rows: rows.map(rowToRecord) as R[],
    };
  }

  async *streamQuery<R>(
    compiledQuery: CompiledQuery,
    chunkSize: number,
  ): AsyncIterableIterator<QueryResult<R>> {
    const { rows } = await this.executeQuery<R>(compiledQuery);
    for (let i = 0; i < rows.length; i += chunkSize) {
      yield { rows: rows.slice(i, i + chunkSize) };
    }
  }

  async beginTransaction(): Promise<void> {
    this.tx = this.conn.beginTransaction();
  }

  async commitTransaction(): Promise<void> {
    if (!this.tx) throw new Error("commitTransaction: no active transaction");
    this.tx.commit();
    this.tx = null;
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.tx) throw new Error("rollbackTransaction: no active transaction");
    this.tx.rollback();
    this.tx = null;
  }

  async destroy(): Promise<void> {
    // The connection handle is provided by the host service-args and lives for
    // the component's lifetime; do not dispose it here.
  }
}

class WitsqlDriver implements Driver {
  constructor(private readonly conn: Connection) {}

  async init(): Promise<void> {}

  async acquireConnection(): Promise<DatabaseConnection> {
    return new WitsqlConnection(this.conn);
  }

  async beginTransaction(connection: DatabaseConnection): Promise<void> {
    await (connection as WitsqlConnection).beginTransaction();
  }

  async commitTransaction(connection: DatabaseConnection): Promise<void> {
    await (connection as WitsqlConnection).commitTransaction();
  }

  async rollbackTransaction(connection: DatabaseConnection): Promise<void> {
    await (connection as WitsqlConnection).rollbackTransaction();
  }

  async releaseConnection(): Promise<void> {}

  async destroy(): Promise<void> {}
}

export class WitsqlDialect implements Dialect {
  constructor(private readonly conn: Connection) {}

  createAdapter() {
    return new SqliteAdapter();
  }

  createDriver(): Driver {
    return new WitsqlDriver(this.conn);
  }

  createQueryCompiler() {
    return new SqliteQueryCompiler();
  }

  createIntrospector(db: Kysely<unknown>): DatabaseIntrospector {
    return new SqliteIntrospector(db);
  }
}

/** better-auth's core tables for SQLite (TEXT dates as ISO-8601, booleans as 0/1).
 *
 * STRICT tables (SQLite 3.37+) enforce column types at write time; all
 * columns use TEXT/INTEGER per better-auth's schema. Indices mirror the
 * `index: true` fields in better-auth's core schema (session.userId,
 * account.userId, verification.identifier).
 */
export const AUTH_SCHEMA_DDL = `
CREATE TABLE IF NOT EXISTS user (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  emailVerified INTEGER NOT NULL DEFAULT 0,
  image TEXT,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS session (
  id TEXT PRIMARY KEY NOT NULL,
  expiresAt TEXT NOT NULL,
  token TEXT NOT NULL UNIQUE,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL,
  ipAddress TEXT,
  userAgent TEXT,
  userId TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS idx_session_user_id ON session (userId);
CREATE TABLE IF NOT EXISTS account (
  id TEXT PRIMARY KEY NOT NULL,
  accountId TEXT NOT NULL,
  providerId TEXT NOT NULL,
  userId TEXT NOT NULL,
  accessToken TEXT,
  refreshToken TEXT,
  idToken TEXT,
  accessTokenExpiresAt TEXT,
  refreshTokenExpiresAt TEXT,
  scope TEXT,
  password TEXT,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS idx_account_user_id ON account (userId);
CREATE TABLE IF NOT EXISTS verification (
  id TEXT PRIMARY KEY NOT NULL,
  identifier TEXT NOT NULL,
  value TEXT NOT NULL,
  expiresAt TEXT NOT NULL,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS idx_verification_identifier ON verification (identifier);
`;
