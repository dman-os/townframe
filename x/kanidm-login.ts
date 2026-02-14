#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`${$.path(import.meta.resolve("./kanidm-recover.ts")}`;
await $`kanidm login -D idm_admin`;
