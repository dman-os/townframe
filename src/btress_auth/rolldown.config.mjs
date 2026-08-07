import { defineConfig } from "rolldown";

export default defineConfig({
  input: "src/component.ts",
  external: /wasi:.*/,
  output: {
    codeSplitting: false,
    file: "dist/component.js",
    format: "esm",
  },
});
