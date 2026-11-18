#!/usr/bin/env -S deno run --allow-all

// Look at and drive the daytxt GUI without touching anyone's desktop: builds and serves the web
// shell through `x/daytxt-web.ts` on a free localhost port, then runs a headless Playwright
// browser (WebKit by default, the engine the desktop's WebKitGTK uses; or Chromium) against it
// through `src/daytxt_web/gui/driver.ts`. The web build is a dev and Playwright artifact only.
//
//   deno run --allow-all x/daytxt-gui.ts shot <route> <out.png> [--theme <css>] [--browser webkit|chromium|firefox]
//   deno run --allow-all x/daytxt-gui.ts inspect <selector> [--route <route>] [--theme <css>]
//   deno run --allow-all x/daytxt-gui.ts run <script.ts> [--route <route>]
//
// `<route>` is `home`, `settings`, `theme-lab`, or a raw `DAYTXT_SHELL_OPEN` spec
// (`settings,settings/counter`). Micros are admitted enabled (`?enable=all`, the web counterpart of
// `DAYTXT_ENABLE_MICROS=all`), so nothing needs clicking. Every process this starts is stopped on
// exit, on failure and on a signal.

import { parseArgs } from "jsr:@std/cli@1/parse-args";
import { resolve } from "jsr:@std/path@1";
import { $ } from "./utils.ts";

const flags = parseArgs(Deno.args, {
  string: ["browser", "route", "theme", "width", "height", "port"],
  boolean: ["full-page", "verbose", "help"],
  default: { browser: "webkit", route: "home", width: "1280", height: "800" },
});
const [action, target, out] = flags._.map(String);
const usage =
  "usage: x/daytxt-gui.ts shot <route> <out.png> | inspect <selector> | run <script.ts>  [--route r] [--theme css] [--browser webkit|chromium|firefox] [--width n] [--height n] [--full-page] [--verbose]";
const fail = (message: string): never => {
  console.error(`daytxt-gui: ${message}`);
  Deno.exit(1);
};
if (flags.help || !["shot", "inspect", "run"].includes(action) || !target) {
  fail(usage);
}
if (action === "shot" && !out) fail(usage);

const root = $.relativeDir("../");
const children: Deno.ChildProcess[] = [];

/// Every descendant of `pid`, deepest first: the web build runs cargo under deno under us.
async function descendants(pid: number): Promise<number[]> {
  const listed = await new Deno.Command("pgrep", {
    args: ["-P", String(pid)],
    stdout: "piped",
  }).output();
  const kids = new TextDecoder().decode(listed.stdout).split("\n").filter(
    Boolean,
  ).map(Number);
  const deeper = await Promise.all(kids.map(descendants));
  return [...deeper.flat(), ...kids];
}

let cleaned = false;
async function cleanup() {
  if (cleaned) return;
  cleaned = true;
  for (const child of children) {
    for (const pid of [...await descendants(child.pid), child.pid]) {
      try {
        Deno.kill(pid, "SIGTERM");
      } catch { /* already gone */ }
    }
    await child.status.catch(() => {});
  }
}
for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"] as const) {
  Deno.addSignalListener(signal, async () => {
    await cleanup();
    Deno.exit(130);
  });
}

async function freePort(): Promise<number> {
  const listener = Deno.listen({ hostname: "127.0.0.1", port: 0 });
  const { port } = listener.addr as Deno.NetAddr;
  listener.close();
  return port;
}

async function main(): Promise<number> {
  const port = flags.port ? Number(flags.port) : await freePort();
  const base = `http://127.0.0.1:${port}`;

  // The serve process is the one `x/daytxt-web.ts` already is: it builds, then serves. Its output
  // is kept so a build failure is reported, not guessed at.
  const server = new Deno.Command(Deno.execPath(), {
    args: ["run", "--allow-all", root.join("x/daytxt-web.ts").toString()],
    env: { HOST: "127.0.0.1", PORT: String(port) },
    stdout: "piped",
    stderr: "piped",
  }).spawn();
  children.push(server);
  const tail: string[] = [];
  const pump = async (stream: ReadableStream<Uint8Array>) => {
    for await (
      const line of stream.pipeThrough(new TextDecoderStream()).pipeThrough(
        new TransformStream({
          transform(chunk, controller) {
            for (const l of chunk.split("\n")) controller.enqueue(l);
          },
        }),
      )
    ) {
      if (flags.verbose) console.error(line);
      tail.push(line);
      if (tail.length > 60) tail.shift();
    }
  };
  pump(server.stdout);
  pump(server.stderr);
  let serverExit: Deno.CommandStatus | undefined;
  server.status.then((status) => serverExit = status);

  // Ready is the server answering, not a delay; a build that dies stops the wait at once.
  console.error(`daytxt-gui: building and serving the web shell on ${base}`);
  for (;;) {
    if (serverExit) {
      return fail(
        `the web shell server exited (${serverExit.code}) before it was ready:\n${
          tail.join("\n")
        }`,
      );
    }
    try {
      const response = await fetch(`${base}/`);
      await response.body?.cancel();
      if (response.ok) break;
    } catch { /* not listening yet */ }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  const driverOptions = {
    base,
    browser: flags.browser,
    action,
    route: action === "shot" ? target : flags.route,
    out: out ? resolve(out) : undefined,
    selector: action === "inspect" ? target : undefined,
    script: action === "run" ? target : undefined,
    theme: flags.theme,
    width: Number(flags.width),
    height: Number(flags.height),
    fullPage: flags["full-page"],
  };
  if (driverOptions.theme) driverOptions.theme = resolve(driverOptions.theme);
  if (driverOptions.script) driverOptions.script = resolve(driverOptions.script);
  if (driverOptions.out) {
    await Deno.mkdir(driverOptions.out.replace(/\/[^/]*$/, ""), {
      recursive: true,
    });
  }
  const driver = new Deno.Command("node", {
    args: [root.join("src/daytxt_web/gui/driver.ts").toString(), JSON.stringify(driverOptions)],
    cwd: root.join("src/daytxt_web").toString(),
    stdout: "inherit",
    stderr: "inherit",
  }).spawn();
  children.push(driver);
  return (await driver.status).code;
}

let code = 1;
try {
  code = await main();
} finally {
  await cleanup();
}
Deno.exit(code);
