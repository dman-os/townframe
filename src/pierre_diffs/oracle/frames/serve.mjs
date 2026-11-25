import http from "node:http"; import fs from "node:fs"; import path from "node:path";
const dir = process.argv[2], port = Number(process.argv[3]);
http.createServer((q, r) => { const f = path.join(dir, q.url.split("?")[0] === "/" ? "index.html" : q.url.split("?")[0]); fs.readFile(f, (e, b) => { if (e) { r.statusCode = 404; r.end(); } else { r.setHeader("content-type", f.endsWith(".js") ? "text/javascript" : f.endsWith(".html") ? "text/html" : "text/plain"); r.end(b); } }); }).listen(port, "127.0.0.1");
