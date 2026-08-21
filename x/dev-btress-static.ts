#!/usr/bin/env -S deno run --allow-all
// Serve the cargo-leptos site output (target/site) on port 3001.
// The btress_sysadmin SSR HTML references its hydrate assets from
// http://localhost:3001 (see app.rs HydrationScripts root) — the wasm
// component's own /public volume serving does not work, so this static
// server is the fallback. Run alongside `daybook_cli server`.

const siteDir = new URL("../target/site/", import.meta.url);
const port = 3001;
const headers = {
  "Access-Control-Allow-Origin": "*",
  "Cache-Control": "no-cache",
};

function contentType(path: string): string {
  if (path.endsWith(".js")) return "application/javascript";
  if (path.endsWith(".wasm")) return "application/wasm";
  if (path.endsWith(".css")) return "text/css";
  return "application/octet-stream";
}

console.log(
  `dev-btress-static: serving ${siteDir} on http://127.0.0.1:${port}`,
);
Deno.serve({ hostname: "127.0.0.1", port }, async (request) => {
  const pathname = decodeURIComponent(new URL(request.url).pathname);
  if (pathname.includes("..")) {
    return new Response("bad path", { status: 400, headers });
  }

  try {
    const file = await Deno.readFile(
      new URL(pathname.replace(/^\/+/, ""), siteDir),
    );
    return new Response(file, {
      headers: { ...headers, "Content-Type": contentType(pathname) },
    });
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) {
      return new Response("not found", { status: 404, headers });
    }
    throw error;
  }
});
