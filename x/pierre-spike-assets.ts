#!/usr/bin/env -S deno run --allow-read --allow-write

// Derives the style-spike assets of `micro_pierre_spike` (docs/scratch/pierre-spikes.md, "P1 style")
// from two inputs that live outside this repository:
//
//   --css   Pierre's `packages/diffs/src/style.css` (1.5.2).
//   --dump  `{ pre, styles }` serialized from a real `FileDiff` render in Chromium: `pre` is the
//           rendered `<pre>` of the shadow root as `{ t, a, c }` nodes (tag, attributes, children;
//           a string child is a text node) and `styles[0].text` is the theme stylesheet Pierre
//           appends to its shadow root (`data-theme-css`).
//
// and writes, into `--out`:
//
//   rows.verbatim.json      the dumped `<pre>`, untouched: Pierre's DOM contract.
//   rows.classes.json       the same rows with the contract rewritten as classes (see below).
//   pierre.verbatim.css     style.css + theme css; `:host` becomes `.pd-host` because the rows are
//                           light DOM under a wrapper, not a shadow root.
//   pierre.classes.css      the class form of the above, plus the token and column-width classes.
//   pierre.classes.scope.css  the same with `:host` as `:scope`, for a host that wraps the sheet in
//                           an `@scope` rule (the stylesheet capability).
//   stats.json              what the rewrite did, for the report.
//
// The class rewrite is mechanical and has exactly three rules, all derived from the stylesheet:
//   1. `[data-x]` selects `.pd-x`; `[data-x='v']` selects `.pd-x--v`. An element gets `pd-x` for
//      each `data-x` the stylesheet selects on, and `pd-x--v` when `v` is a value it selects on.
//      Attributes no selector names (`data-line-index`, `data-alt-line`) are dropped: they exist
//      for Pierre's JS, not for its CSS.
//   2. The two per-token custom properties `--diffs-token-dark/-light` become `hl-N`, one class per
//      distinct pair, with the rule emitted beside the stylesheet (Pierre's `useCSSClasses` mode
//      is meant to produce these classes; see the report for why it does not).
//   3. `--diffs-min-number-column-width-default: Nch` becomes `pd-ncw-N`; the remaining inline
//      properties (`grid-row`, `min-height`) stay inline, as `style:<property>` attributes the
//      guest emits in the `style` attribute namespace.
import { parseArgs } from "jsr:@std/cli@1.0.9/parse-args";

type Hast = string | { t: string; a: Record<string, string>; c: Hast[] };

const args = parseArgs(Deno.args, { string: ["css", "dump", "out"] });
if (!args.css || !args.dump || !args.out) {
  throw new Error(
    "usage: pierre-spike-assets.ts --css <style.css> --dump <dump.json> --out <dir>",
  );
}
const css = await Deno.readTextFile(args.css);
const dump = JSON.parse(await Deno.readTextFile(args.dump)) as {
  pre: Hast;
  styles: { text: string }[];
};
const themeCss = dump.styles[0].text;

const attributeSelector = /\[data-([a-z0-9-]+)(?:=(['"])(.*?)\2)?\]/g;
const selectedNames = new Set<string>();
const selectedValues = new Set<string>();
for (const match of (css + themeCss).matchAll(attributeSelector)) {
  selectedNames.add(match[1]);
  if (match[3] !== undefined) selectedValues.add(`${match[1]}=${match[3]}`);
}

function classSelectors(source: string): string {
  return source.replace(
    attributeSelector,
    (_all, name: string, _quote: string, value?: string) =>
      value === undefined ? `.pd-${name}` : `.pd-${name}--${value}`,
  );
}

const hlClasses = new Map<string, number>();
const stats = {
  nodes: 0,
  texts: 0,
  dataAttributesSeen: {} as Record<string, number>,
  dataAttributesDropped: {} as Record<string, number>,
  attributeSelectorsRewritten: [...css.matchAll(attributeSelector)].length,
  selectedNames: selectedNames.size,
  selectedValues: selectedValues.size,
  hostSelectors: [...css.matchAll(/:host/g)].length,
  tokenClasses: 0,
  columnWidths: [] as number[],
};
const columnWidths = new Set<number>();

function toClasses(node: Hast): Hast {
  if (typeof node === "string") {
    stats.texts++;
    return node;
  }
  stats.nodes++;
  const attributes: Record<string, string> = {};
  const classes: string[] = [];
  for (const [name, value] of Object.entries(node.a)) {
    if (name === "class") {
      classes.push(value);
    } else if (name.startsWith("data-")) {
      const bare = name.slice(5);
      stats.dataAttributesSeen[name] = (stats.dataAttributesSeen[name] ?? 0) +
        1;
      if (selectedNames.has(bare)) classes.push(`pd-${bare}`);
      else {
        stats.dataAttributesDropped[name] =
          (stats.dataAttributesDropped[name] ?? 0) + 1;
      }
      if (selectedValues.has(`${bare}=${value}`)) {
        classes.push(`pd-${bare}--${value}`);
      }
    } else if (name === "style") {
      const declarations = new Map<string, string>();
      for (const part of value.split(";")) {
        const at = part.indexOf(":");
        if (at > 0) {
          declarations.set(part.slice(0, at).trim(), part.slice(at + 1).trim());
        }
      }
      const dark = declarations.get("--diffs-token-dark");
      const light = declarations.get("--diffs-token-light");
      if (dark !== undefined && light !== undefined) {
        const pair = `${dark}|${light}`;
        if (!hlClasses.has(pair)) hlClasses.set(pair, hlClasses.size);
        classes.push(`hl-${hlClasses.get(pair)}`);
        declarations.delete("--diffs-token-dark");
        declarations.delete("--diffs-token-light");
      }
      const width = declarations.get("--diffs-min-number-column-width-default");
      if (width !== undefined) {
        const digits = Number(width.replace("ch", ""));
        columnWidths.add(digits);
        classes.push(`pd-ncw-${digits}`);
        declarations.delete("--diffs-min-number-column-width-default");
      }
      for (const [property, setting] of declarations) {
        if (property.startsWith("--")) {
          throw new Error(`unhandled custom property ${property}`);
        }
        attributes[`style:${property}`] = setting;
      }
    } else {
      attributes[name] = value;
    }
  }
  if (classes.length > 0) attributes.class = classes.join(" ");
  return { t: node.t, a: attributes, c: node.c.map(toClasses) };
}

const classRows = toClasses(dump.pre);
stats.tokenClasses = hlClasses.size;
stats.columnWidths = [...columnWidths].sort((a, b) => a - b);

const tokenCss = [...hlClasses].map(([pair, index]) => {
  const [dark, light] = pair.split("|");
  return `.hl-${index}{--diffs-token-dark:${dark};--diffs-token-light:${light}}`;
}).join("\n");
// Column width classes for every digit count a file can have up to the 1M-line ceiling; the
// rows only use the ones seen, but the class set is closed and tiny.
const widthCss = Array.from(
  { length: 8 },
  (_, index) =>
    `.pd-ncw-${index + 1}{--diffs-min-number-column-width-default:${
      index + 1
    }ch}`,
).join("\n");

const verbatim = `${css}\n${themeCss}\n`.replaceAll(":host", ".pd-host");
const classForm = classSelectors(`${css}\n${themeCss}\n`);
const write = (name: string, text: string) =>
  Deno.writeTextFile(`${args.out}/${name}`, text);
await write("rows.verbatim.json", JSON.stringify(dump.pre));
await write("rows.classes.json", JSON.stringify(classRows));
await write("pierre.verbatim.css", verbatim);
await write(
  "pierre.classes.css",
  `${classForm.replaceAll(":host", ".pd-host")}\n${tokenCss}\n${widthCss}\n`,
);
await write(
  "pierre.classes.scope.css",
  `${classForm.replaceAll(":host", ":scope")}\n${tokenCss}\n${widthCss}\n`,
);
await write("stats.json", JSON.stringify(stats, null, 2) + "\n");
console.log(JSON.stringify(stats));
