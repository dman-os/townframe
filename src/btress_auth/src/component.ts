import {
  incomingHandler as adapter,
  fire,
} from "@bytecodealliance/jco-std/wasi/0.2.x/http/adapters/hono/server";
import { Hono } from "hono";
import { cors } from "hono/cors";
import { logger } from "hono/logger";

import { appCx } from "./context.js";

type Variables = {
  requestId: string;
  startTime: number;
};

async function setup() {
  const cx = await appCx();
  const app = new Hono<{ Variables: Variables }>();

  const honoLogger = cx.log.getChild(["hono"]);
  // Standard health endpoint for Kubernetes liveness/readiness probes.
  // NOTE: put the logger first to avoid healthz logs
  app.get("/healthz", (c) => c.text("ok", 200));

  app.use(
    logger((...rest: string[]) => {
      honoLogger.info(rest[0] as string, {
        items: rest.splice(1),
      });
    }),
  );

  // cors must come before routes
  const allowedAuthOrigins = new Set<string>([
    "http://localhost:8071",
    "http://localhost:3000", // btress_sysadmin origin (login page)
    "daybook-app://",
  ]);
  app.use(
    "/api/auth/*",
    cors({
      origin: (origin) => {
        if (!origin) {
          return cx.config.$BTRESS_URL;
        }
        if (allowedAuthOrigins.has(origin)) {
          return origin;
        }
        return "";
      },
      allowHeaders: ["Content-Type", "Authorization"],
      allowMethods: ["POST", "GET", "OPTIONS"],
      exposeHeaders: ["Content-Length"],
      maxAge: 600,
      credentials: true,
    }),
  );
  app.on(["POST", "GET"], "/api/auth/*", (c) => {
    // auth is always built by the time requests arrive (setup() awaits appCx())
    const auth = cx.auth;
    if (!auth) throw new Error("auth context was not initialized");
    return auth.handler(c.req.raw);
  });

  app.get("/healthz", (c) => c.text("ok", 200));

  fire(app);
}

export const incomingHandler = {
  async handle(req: any, resp: any) {
    await setup();
    await adapter.handle(req, resp);
  },
};
