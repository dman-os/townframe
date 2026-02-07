#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`cargo b -p daybook_api -p daybook_http --target wasm32-wasip2`;
await $`wac plug --plug daybook_api.wasm daybook_http.wasm -o daybook_http_plugged.wasm`
  .cwd($.relativeDir("../target/wasm32-wasip2/debug/"));
