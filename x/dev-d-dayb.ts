#!/usr/bin/env -S deno run --allow-all

import { $ } from "./utils.ts";

await $`${$.path(import.meta.resolve("./gen-ffi-dayb.ts"))}`;

const currentLibraryPath = $.env.LD_LIBRARY_PATH ?? "";
const debugLibraryPath = $.relativeDir("../target/debug/").toString();

await $`./gradlew desktopRunHot -PmainClass=org.example.daybook.MainKt --auto`
  .cwd($.relativeDir("../src/daybook_compose/"))
  .env({
    LD_LIBRARY_PATH: `${currentLibraryPath}:${debugLibraryPath}`,
  });
