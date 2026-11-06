#!/usr/bin/env -S deno run --allow-all
// Static server for the ladder pages. `deno run --allow-all serve.ts <port> <mount=dir>...`:
// each mount maps a URL prefix to a directory ("/" = a site root). Prints its PID line.
const [portArg, ...mounts] = Deno.args;
const table = mounts.map((m) => {
  const [prefix, dir] = m.split("=");
  return { prefix, dir };
}).sort((a, b) => b.prefix.length - a.prefix.length);
const types: Record<string, string> = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".json": "application/json",
  ".wasm": "application/wasm",
};
console.log(`serve pid ${Deno.pid} port ${portArg}`);
Deno.serve({ hostname: "127.0.0.1", port: Number(portArg) }, async (request) => {
  const path = decodeURIComponent(new URL(request.url).pathname);
  if (path.includes("..")) return new Response("no", { status: 400 });
  for (const { prefix, dir } of table) {
    if (!path.startsWith(prefix)) continue;
    let rel = path.slice(prefix.length);
    if (rel === "" || rel.endsWith("/")) rel += "index.html";
    try {
      const file = await Deno.open(`${dir}/${rel}`);
      const ext = rel.slice(rel.lastIndexOf("."));
      return new Response(file.readable, {
        headers: { "content-type": types[ext] ?? "application/octet-stream" },
      });
    } catch { /* try the next mount */ }
  }
  return new Response("not found", { status: 404 });
});
