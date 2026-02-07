#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`cargo nextest run -p tests_http`.env({ DOTENV_ENV: "test" });
