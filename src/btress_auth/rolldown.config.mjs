import { defineConfig } from "rolldown";

export default defineConfig({
  input: "src/component.ts",
  external: /wasi:.*|townframe:.*/,
  output: {
    codeSplitting: false,
    file: "dist/component.js",
    format: "esm",
  },
});
