#!/usr/bin/env -S deno run --allow-all
// P5: serve an already-built `dayframe_web` site (x/dayframe-web.ts output) with the Pierre
// spike component swapped for another build, so the same host and browser run different guest
// bytes. The package index pins a BLAKE3 digest of the component, so it is recomputed here.
//
//   deno run --allow-all serve.ts <site-dir> <component.wasm>     (HOST/PORT as dayframe-web.ts)
import { crypto } from "jsr:@std/crypto@1.0.5";

const [siteDir, component] = Deno.args;
const bytes = await Deno.readFile(component);
const digest = [...new Uint8Array(await crypto.subtle.digest("BLAKE3", bytes.slice().buffer))]
  .map((b) => b.toString(16).padStart(2, "0")).join("");
const index = JSON.parse(await Deno.readTextFile(`${siteDir}/packages/index.json`));
const entry = index.find((e: { location: string }) => e.location.includes("pierre-spike-scroll"));
entry.digest = digest;
const types: Record<string, string> = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".wasm": "application/wasm",
};
Deno.serve({
  hostname: Deno.env.get("HOST") ?? "127.0.0.1",
  port: Number(Deno.env.get("PORT") ?? "4518"),
}, async (request) => {
  const name = decodeURIComponent(new URL(request.url).pathname).replace(/^\//, "") || "index.html";
  if (name === "favicon.ico") return new Response(null, { status: 204 });
  if (name === "packages/index.json") return Response.json(index);
  if (name === entry.location) {
    return new Response(bytes, { headers: { "content-type": "application/wasm" } });
  }
  if (name.includes("..")) return new Response("not found", { status: 404 });
  try {
    const file = await Deno.open(`${siteDir}/${name}`);
    const ext = name.slice(name.lastIndexOf("."));
    return new Response(file.readable, {
      headers: { "content-type": types[ext] ?? "application/octet-stream" },
    });
  } catch {
    return new Response("not found", { status: 404 });
  }
});
