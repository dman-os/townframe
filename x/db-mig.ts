#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`${$.path(import.meta.resolve("./db-mig-btress.ts")}`;
