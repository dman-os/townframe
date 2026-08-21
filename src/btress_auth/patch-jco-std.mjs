// The jco-std wasi-http response adapter throws on body-less responses
// (302/204/304 redirects from better-auth's magic-link verify have no body),
// and the host aborts if the response body is finished with an open, unwritten
// stream. Fix: for `resp.body === null`, never open the write stream and just
// finish the body directly — the canonical wasi-http way to express an empty
// response. Applied to the `0.2.x` template copy that rolldown bundles; run
// before `pnpm run build:ts`.
import { readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const pkg =
  "@bytecodealliance+jco-std@0.1.3/node_modules/@bytecodealliance/jco-std";
// Resolve the jco-std response.js from the workspace root's .pnpm store,
// anchored to this script's location (cwd-independent).
const scriptDir = dirname(fileURLToPath(import.meta.url));
const candidates = [
  join(scriptDir, "../../node_modules/.pnpm"),
  join(scriptDir, "node_modules/.pnpm"),
];

let target = null;
for (const base of candidates) {
  const p = join(base, pkg, "dist/0.2.x/http/types/response.js");
  try {
    await readFile(p, "utf8");
    target = p;
    break;
  } catch {
    // keep looking
  }
}
if (!target) {
  throw new Error(
    "patch-jco-std: jco-std response.js not found under node_modules/.pnpm",
  );
}

const src = await readFile(target, "utf8");
if (src.includes("finish the body directly")) {
  console.log("patch-jco-std: already patched, skipping");
  process.exit(0);
}

const old = `        {
            // Create a stream for the response body
            const outputStream = outgoingBody.write();
            if (resp.body === null) {
                throw new Error("unexpectedly missing resp.body");
            }
            const pollable = outputStream.subscribe();`;

const newStart = `        {
            if (resp.body === null) {
                // Redirect/204/304 responses legitimately have no body: never
                // open the write stream, just finish the body directly (the
                // canonical wasi-http way to express an empty response).
            } else {
            // Create a stream for the response body
            const outputStream = outgoingBody.write();
            const pollable = outputStream.subscribe();`;

if (!src.includes(old)) {
  console.error(
    "patch-jco-std: original throw block not found — jco-std changed upstream?",
  );
  process.exit(1);
}
let next = src.replace(old, newStart);

const oldEnd = `            // Clean up pollable & stream
            pollable[Symbol.dispose]();
            outputStream[Symbol.dispose]();
        }
        // Set the outgoing response body w/ no trailers`;
const newEnd = `            // Clean up pollable & stream
            pollable[Symbol.dispose]();
            outputStream[Symbol.dispose]();
            }
        }
        // Set the outgoing response body w/ no trailers`;
if (!next.includes(oldEnd)) {
  console.error(
    "patch-jco-std: end block not found — jco-std changed upstream?",
  );
  process.exit(1);
}
next = next.replace(oldEnd, newEnd);

await writeFile(target, next);
console.log(`patch-jco-std: patched ${target}`);
