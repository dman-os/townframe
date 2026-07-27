import { log as wasiLog } from "wasi:logging/logging@0.1.0-draft";
import { configure, getLogger } from "@logtape/logtape";
import { betterAuth } from "better-auth";

import { getConfig } from "./config.js";
import { switchMap } from "./utils.js";

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
  return {
    log,
    config,
    auth: betterAuth({
      baseURL: config.$BETTER_AUTH_URL,
      secret: config.$BETTER_AUTH_SECRET,
      logger: {
        log(lvl, msg) {
          betterAuthLog[lvl](msg);
        },
      },
    }),
  };
}
