// import { log as wasiLog } from "wasi:logging/logging@0.1.0-draft";
import { configure, getLogger } from "@logtape/logtape";
import { betterAuth } from "better-auth/minimal";
import { getArgs } from "townframe:api-utils/http-service";

import { getConfig } from "./config.js";
import { switchMap } from "./utils.js";
import { AUTH_SCHEMA_DDL, WitsqlDialect } from "./wit-sqlite.js";

function wasiLog(...args: any[]) {
  console.log(...args);
}

let authPromise: Promise<ReturnType<typeof betterAuth>> | null = null;

async function buildAuth(
  config: ReturnType<typeof getConfig>,
  betterAuthLog: ReturnType<ReturnType<typeof getLogger>["getChild"]>,
) {
  if (authPromise) return authPromise;
  authPromise = (async () => {
    // Capabilities arrive via http-service get-args: a pre-provisioned sqlite
    // connection handle (host provisioned it through ServicePlugin).
    const args = getArgs();
    const entry =
      args.sqliteConnections.find(([key]) => key === "auth-db") ??
      args.sqliteConnections[0];
    if (!entry) {
      throw new Error("http-service get-args returned no sqlite connection");
    }
    const conn = entry[1];

    // The component owns its schema: ensure better-auth's tables exist.
    conn.queryBatch(AUTH_SCHEMA_DDL);

    const dialect = new WitsqlDialect(conn);
    return betterAuth({
      baseURL: config.$BETTER_AUTH_URL,
      secret: config.$BETTER_AUTH_SECRET,
      database: {
        dialect,
        type: "sqlite",
      },
      logger: {
        log(lvl, msg) {
          betterAuthLog[lvl](msg);
        },
      },
    });
  })();
  return authPromise;
}

export async function appCx() {
  await configure({
    loggers: [
      {
        category: "central",
        sinks: ["wasiLog"],
      },
      {
        category: ["logtape", "meta"],
        lowestLevel: "warning",
      },
    ],
    sinks: {
      wasiLog: (lr) => {
        wasiLog(
          switchMap(lr.level, {
            trace: "trace",
            debug: "debug",
            info: "info",
            warning: "warn",
            error: "error",
            fatal: "critical",
          }),
          lr.category.join(),
          // TODO: use an inspector instead of json stringify
          lr.message.join() + JSON.stringify(lr.properties),
        );
      },
    },
  });
  const log = getLogger(["central"]);
  const betterAuthLog = log.getChild(["better-auth"]);

  const config = getConfig();
  const auth = await buildAuth(config, betterAuthLog);
  return {
    log,
    config,
    auth,
  };
}
