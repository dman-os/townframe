import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { defineConfig } from "rolldown";

const require = createRequire(import.meta.url);
const forgesDist = join(
  dirname(require.resolve("forges/package.json")),
  "dist",
);

export default defineConfig({
  input: "src/component.ts",
  external: /wasi:.*|townframe:.*/,
  resolve: {
    alias: {
      // `forges` only exports `threadKey` from its root entry, and bundling the
      // root entry trips a rolldown chunking bug (an empty extra chunk for the
      // gitlab module that rejects `output.file`). Reach the shared model chunk
      // directly instead. The hashed filename is brittle; spike only.
      "forges-internal-model": join(forgesDist, "model-Biiu1CfI.mjs"),
    },
  },
  output: {
    codeSplitting: false,
    file: "dist/component.js",
    format: "esm",
  },
});
