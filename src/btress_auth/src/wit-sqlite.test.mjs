import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { Kysely, sql } from "kysely";
import { WitsqlDialect } from "./wit-sqlite.ts";

function database(context) {
  const sqlite = new DatabaseSync(":memory:");
  context.after(() => sqlite.close());
  let disposed = 0;
  const connection = {
    execute(query, params) {
      const statement = sqlite.prepare(query);
      statement.setReadBigInts(true);
      const columns = statement.columns().map((column) => column.name);
      const changesBefore = sqlite
        .prepare("SELECT total_changes() AS count")
        .get().count;
      const records = statement.all(
        ...params.map((value) => (value.tag === "null" ? null : value.val)),
      );
      const metadata = sqlite
        .prepare(
          "SELECT total_changes() AS total, changes() AS count, last_insert_rowid() AS id",
        )
        .get();
      return {
        rowsAffected: BigInt(
          metadata.total === changesBefore ? 0 : metadata.count,
        ),
        lastInsertRowid: BigInt(metadata.id),
        resultSet: {
          columns,
          rows: records.map((record) =>
            columns.map((column) => {
              const value = record[column];
              if (value === null) return { tag: "null" };
              if (typeof value === "bigint")
                return { tag: "integer", val: value };
              if (typeof value === "number") return { tag: "real", val: value };
              if (typeof value === "string") return { tag: "text", val: value };
              return { tag: "blob", val: value };
            }),
          ),
        },
      };
    },
    beginTransaction() {
      sqlite.exec("BEGIN IMMEDIATE");
      return {
        execute: connection.execute,
        commit() {
          sqlite.exec("COMMIT");
        },
        rollback() {
          sqlite.exec("ROLLBACK");
        },
        [Symbol.dispose]() {
          disposed += 1;
        },
      };
    },
  };
  sqlite.exec(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value REAL, payload BLOB)",
  );
  const db = new Kysely({ dialect: new WitsqlDialect(connection) });
  context.after(() => db.destroy());
  return { db, disposed: () => disposed };
}

test("RETURNING preserves rows and values", async (context) => {
  const { db } = database(context);
  const row = await db
    .insertInto("items")
    .values({
      name: "returned",
      value: 1.5,
      payload: new Uint8Array([1, 2, 3]),
    })
    .returningAll()
    .executeTakeFirstOrThrow();
  assert.deepEqual(row, {
    id: 1,
    name: "returned",
    value: 1.5,
    payload: new Uint8Array([1, 2, 3]),
  });
});

test("CTE mutations and commented reads use SQLite statement results", async (context) => {
  const { db } = database(context);
  const inserted =
    await sql`WITH source(name) AS (VALUES ('cte')) INSERT INTO items (name) SELECT name FROM source RETURNING id, name`.execute(
      db,
    );
  assert.deepEqual(inserted.rows, [{ id: 1, name: "cte" }]);
  assert.equal(inserted.numAffectedRows, 1n);
  assert.equal(inserted.insertId, 1n);
  const read = await sql`/* read */ SELECT name, payload FROM items`.execute(
    db,
  );
  assert.deepEqual(read.rows, [{ name: "cte", payload: null }]);
  assert.equal(read.numAffectedRows, 0n);
  const updated = await db
    .updateTable("items")
    .set({ name: "updated" })
    .executeTakeFirstOrThrow();
  assert.equal(updated.numUpdatedRows, 1n);
});

test("transactions return rows and dispose finalized resources", async (context) => {
  const { db, disposed } = database(context);
  const committed = await db
    .transaction()
    .execute(async (tx) =>
      tx
        .insertInto("items")
        .values({ name: "committed" })
        .returning("id")
        .executeTakeFirstOrThrow(),
    );
  assert.deepEqual(committed, { id: 1 });
  await assert.rejects(
    db.transaction().execute(async (tx) => {
      await tx.insertInto("items").values({ name: "rolled-back" }).execute();
      throw new Error("rollback sentinel");
    }),
    /rollback sentinel/,
  );
  assert.deepEqual(
    await db.selectFrom("items").select(["id", "name"]).execute(),
    [{ id: 1, name: "committed" }],
  );
  assert.equal(disposed(), 2);
});
