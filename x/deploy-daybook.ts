#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`wash build`.cwd($.relativeDir("../src/daybook_http/"));
await $`wash app deploy ./src/daybook_http/local.wadm.yaml --replace`;
