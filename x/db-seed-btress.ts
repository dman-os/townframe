#!/usr/bin/env -S deno run --allow-all

import { $, DOCKER_CMD, toolsDir } from "./utils.ts";

await $`./x/db-mig-btress.ts`;
await $`bash -lc "${DOCKER_CMD} exec -i townframe_postgres_1 psql -U postgres -v SEARCH_PATH=zitadel,spicedb,granary,btress -d btress < ./src/btress_api/fixtures/000_test_data.sql"`
  .cwd(
    toolsDir(),
  );
