#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`./gradlew  detektMainDesktop --auto-correct ${$.argv}`.cwd(
	$.relativeDir("../src/daybook_compose/"),
);
