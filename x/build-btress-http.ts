#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`cargo b -p btress_api -p btress_http --target wasm32-wasip2`;
await $`wac plug --plug btress_api.wasm btress_http.wasm -o btress_http_plugged.wasm`
  .cwd(
    $.relativeDir("../target/wasm32-wasip2/debug/"),
  );
