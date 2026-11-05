// P5 control: rewrite the guest's core module so the stack pointer is a wasm global instead of
// the imported `env.__wasm_get_stack_pointer`/`__wasm_set_stack_pointer` (which the component
// build backs with `context.get/set 0`).
//
//   node patch-sp.mjs <module.wat> <out.wat>
//
// Textual edit of `wasm-tools print` output: drop the two imports and define the same-named
// functions over a mutable global initialised to `__init_stack_pointer` (1048576). Every call
// site is untouched, so the only difference is what the two functions do.
import { readFileSync, writeFileSync } from "node:fs";
const [inp, out] = process.argv.slice(2);
let wat = readFileSync(inp, "utf8");
const lines = wat.split("\n");
const keep = [];
let inserted = false;
for (const line of lines) {
  if (/^\s*\(import "env" "__wasm_(get|set)_stack_pointer"/.test(line)) continue;
  if (!inserted && /^\s*\(func /.test(line)) {
    keep.push(
      "  (global $p5_sp (mut i32) (i32.const 1048576))",
      "  (func $__wasm_get_stack_pointer (result i32) global.get $p5_sp)",
      "  (func $__wasm_set_stack_pointer (param i32) local.get 0 global.set $p5_sp)",
    );
    inserted = true;
  }
  keep.push(line);
}
if (!inserted) throw new Error("no func definition found");
writeFileSync(out, keep.join("\n"));
