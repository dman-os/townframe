// P5 control: instantiate the guest's core module directly in V8, with no wcmp.
//
//   node direct.mjs <core-module.wasm> <sp-mode> [reps] [tokens]
//
// The core module is `wasm-tools component unbundle <component> --module-dir <dir>` output
// (`unbundled-module0.wasm`), built with `--features p5-direct` so it exports `p5_init`,
// `p5_bench`, `p5_len` (see `lib.rs`, `mod p5_direct`).
//
// sp-mode:
//   js      the module still imports `env.__wasm_{get,set}_stack_pointer` and we back them with a
//           JS closure over one variable: the cheapest possible host-function stack pointer
//           (wcmp backs the same two imports with a `context.get/set 0` host trampoline).
//   global  the module has been rewritten (see `patch-sp.mjs`) so the stack pointer is a wasm
//           global and the imports are gone; any `env` stack imports passed here are unused.
//   jscall  like `js` but each get/set also does `Math.sqrt(x)` to model a slightly heavier host
//           function, to show the cost is per host crossing and not per byte of work.
import { readFileSync } from "node:fs";

const [file, mode = "js", repsArg = "5", tokensArg = "7"] = process.argv.slice(2);
const reps = Number(repsArg);
const tokens = Number(tokensArg);
const module = new WebAssembly.Module(readFileSync(file));

let sp = 1048576; // `__init_stack_pointer`, the top of the linker's 1 MiB `--stack-first` stack
let tls = 0;
const env = {
  __wasm_get_stack_pointer: () => sp,
  __wasm_set_stack_pointer: (v) => { sp = v; },
  __wasm_get_tls_base: () => tls,
  __wasm_set_tls_base: (v) => { tls = v; },
};
if (mode === "jscall") {
  env.__wasm_get_stack_pointer = () => (Math.sqrt(sp) , sp);
  env.__wasm_set_stack_pointer = (v) => { Math.sqrt(v); sp = v; };
}
let calls = 0;
const imports = {};
for (const imp of WebAssembly.Module.imports(module)) {
  imports[imp.module] ??= {};
  imports[imp.module][imp.name] = imp.module === "env" && imp.name in env
    ? env[imp.name]
    : imp.module === "wasi:clocks/monotonic-clock@0.3.0" && imp.name === "now"
    ? () => BigInt(Math.round(performance.now() * 1e6))
    : imp.module === "wasi:random/insecure-seed@0.3.0" && imp.name === "get-insecure-seed"
    ? (ret) => { // returns (u64, u64) through a pointer; any fixed seed will do
      new DataView(exports.memory.buffer).setBigUint64(ret, 0x1234_5678n, true);
      new DataView(exports.memory.buffer).setBigUint64(ret + 8, 0x9abc_def0n, true);
    }
    : (...args) => { throw new Error(`unexpected import ${imp.module} ${imp.name}(${args})`); };
}
let exports;
exports = new WebAssembly.Instance(module, imports).exports;
exports._initialize?.();
exports.p5_init();
// Stack-pointer imports one diff makes (`p5_diff`: one 50-row shift into the null writer).
if (mode === "js") {
  let gets = 0, sets = 0;
  const g = env.__wasm_get_stack_pointer, st = env.__wasm_set_stack_pointer;
  env.__wasm_get_stack_pointer = () => { gets++; return g(); };
  env.__wasm_set_stack_pointer = (v) => { sets++; st(v); };
  // Counting wrappers only apply to a fresh instance; build one for the count.
  const counted = {};
  for (const m in imports) counted[m] = { ...imports[m] };
  counted.env.__wasm_get_stack_pointer = env.__wasm_get_stack_pointer;
  counted.env.__wasm_set_stack_pointer = env.__wasm_set_stack_pointer;
  const inst = new WebAssembly.Instance(module, counted).exports;
  exports = inst; sp = 1048576; inst._initialize?.(); inst.p5_init();
  inst.p5_diff(tokens); gets = sets = 0;
  inst.p5_diff(tokens);
  console.log(`stack-pointer imports per diff (tokens ${tokens}): get ${gets} set ${sets}`);
  exports = new WebAssembly.Instance(module, imports).exports; sp = 1048576; exports._initialize?.(); exports.p5_init();
}
const dec = new TextDecoder();
for (let i = 0; i < reps; i++) {
  const ptr = exports.p5_bench(tokens);
  const text = dec.decode(new Uint8Array(exports.memory.buffer, ptr, exports.p5_len()));
  console.log(`${mode} run ${i}: ${text}`);
}
