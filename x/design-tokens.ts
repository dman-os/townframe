#!/usr/bin/env -S deno run --allow-read --allow-write

/**
 * Validate the small DTCG foundation subset and generate the CSS preview.
 *
 * The generated file is intentionally separate from tokens.css until the
 * runtime migration is complete. The JSON files are the authored source.
 *
 * The colour system (ADR 055) is a second authored source, `palette.tokens.json`: twelve-step
 * ramps written once for light and dark. It generates `palette.generated.css` (the ramps as
 * `light-dark()` custom properties and the roles derived from them) and every derived pair is
 * checked at WCAG AA for each scheme and contrast level: a failing pair fails the build.
 */

type JsonObject = Record<string, unknown>;
type TokenType =
  | "color"
  | "cubicBezier"
  | "dimension"
  | "duration"
  | "fontFamily"
  | "fontWeight"
  | "number"
  | "shadow";

type Token = {
  path: string;
  type: TokenType;
  value: unknown;
  source: string;
  layer: "primitive" | "semantic";
};

const root = new URL("../", import.meta.url);
const tokenDirectory = new URL("src/clay_dioxus_kit/styles/tokens/", root);
const outputUrl = new URL(
  "src/clay_dioxus_kit/styles/tokens.generated.css",
  root,
);
const sources = ["primitives.tokens.json", "semantic.tokens.json"];
const supportedTypes = new Set<TokenType>([
  "color",
  "cubicBezier",
  "dimension",
  "duration",
  "fontFamily",
  "fontWeight",
  "number",
  "shadow",
]);
const tokens = new Map<string, Token>();

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fail(message: string): never {
  throw new Error(`design tokens: ${message}`);
}

function parseFile(name: string): unknown {
  const path = new URL(name, tokenDirectory);
  try {
    return JSON.parse(Deno.readTextFileSync(path));
  } catch (error) {
    fail(
      `cannot read ${name}: ${error instanceof Error ? error.message : error}`,
    );
  }
}

function validateName(name: string, path: string): void {
  if (!name || name.startsWith("$")) {
    fail(`invalid token/group name ${name || "<empty>"} at ${path}`);
  }
  if (name.includes(".") || name.includes("{") || name.includes("}")) {
    fail(`reserved character in token/group name ${path}`);
  }
}

const allowedGroupMetadata = new Set(["$description", "$type", "$deprecated"]);
const allowedTokenMetadata = new Set([
  "$value",
  "$type",
  "$description",
  "$deprecated",
]);

function validateMetadata(
  value: JsonObject,
  path: string,
  token: boolean,
): void {
  const allowed = token ? allowedTokenMetadata : allowedGroupMetadata;
  for (const [name, metadata] of Object.entries(value)) {
    if (!name.startsWith("$")) continue;
    if (!allowed.has(name)) fail(`unsupported metadata ${name} at ${path}`);
    if (name === "$description" && typeof metadata !== "string") {
      fail(`$description at ${path} must be a string`);
    }
    if (
      name === "$deprecated" && typeof metadata !== "boolean" &&
      typeof metadata !== "string"
    ) {
      fail(`$deprecated at ${path} must be a boolean or string`);
    }
  }
}

function collect(
  value: unknown,
  prefix: string[],
  inheritedType: TokenType | undefined,
  source: string,
): void {
  if (!isObject(value)) {
    fail(`${source} root and groups must be objects`);
  }

  const currentPath = prefix.join(".") || source;
  validateMetadata(value, currentPath, false);
  const localType = value.$type ?? inheritedType;
  if (
    localType !== undefined &&
    (typeof localType !== "string" ||
      !supportedTypes.has(localType as TokenType))
  ) {
    fail(
      `unsupported or invalid $type ${String(localType)} at ${
        prefix.join(".") || source
      }`,
    );
  }

  for (const [name, child] of Object.entries(value)) {
    if (name.startsWith("$")) continue;
    validateName(name, [...prefix, name].join("."));
    if (!isObject(child)) {
      fail(`group or token ${[...prefix, name].join(".")} must be an object`);
    }

    const path = [...prefix, name].join(".");
    const isToken = "$value" in child;
    validateMetadata(child, path, isToken);
    if (isToken) {
      if (tokens.has(path)) fail(`duplicate token ${path}`);
      const tokenType = child.$type ?? localType;
      if (
        typeof tokenType !== "string" ||
        !supportedTypes.has(tokenType as TokenType)
      ) {
        fail(
          `token ${path} needs one of the supported $type values: ${
            [...supportedTypes].join(", ")
          }`,
        );
      }
      tokens.set(path, {
        path,
        type: tokenType as TokenType,
        value: child.$value,
        source,
        layer: source === "primitives.tokens.json" ? "primitive" : "semantic",
      });
      continue;
    }

    collect(
      child,
      [...prefix, name],
      localType as TokenType | undefined,
      source,
    );
  }
}

function alias(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const match = value.match(/^\{([^{}]+)\}$/);
  return match?.[1];
}

function aliasTarget(value: unknown): Token | undefined {
  const target = alias(value);
  return target ? tokens.get(target) : undefined;
}

function validateAliases(
  value: unknown,
  owner: Token,
  expectedType: TokenType | undefined,
  path: string,
): void {
  const target = alias(value);
  if (target) {
    const targetToken = aliasTarget(value);
    if (!targetToken) fail(`alias references missing token ${target}`);
    if (targetToken.type !== expectedType) {
      fail(
        `alias ${owner.path} at ${path} targets ${targetToken.path} with type ${targetToken.type}, expected ${expectedType}`,
      );
    }
    if (owner.layer === "primitive" && targetToken.layer !== "primitive") {
      fail(
        `primitive token ${owner.path} may not depend on ${targetToken.layer} token ${targetToken.path}`,
      );
    }
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) =>
      validateAliases(item, owner, expectedType, `${path}[${index}]`)
    );
    return;
  }
  if (!isObject(value)) return;
  for (const [key, child] of Object.entries(value)) {
    const childType = owner.type === "shadow"
      ? key === "color"
        ? "color"
        : ["offsetX", "offsetY", "blur", "spread"].includes(key)
        ? "dimension"
        : undefined
      : expectedType;
    validateAliases(child, owner, childType, `${path}.${key}`);
  }
}

function resolveToken(path: string, stack: string[]): unknown {
  const token = tokens.get(path);
  if (!token) fail(`alias references missing token ${path}`);
  if (stack.includes(path)) {
    fail(`cyclic alias: ${[...stack, path].join(" -> ")}`);
  }
  return resolveValue(token.value, [...stack, path]);
}

function resolveValue(value: unknown, stack: string[]): unknown {
  const target = alias(value);
  if (target) return resolveToken(target, stack);
  if (
    typeof value === "string" || typeof value === "number" || value === null
  ) return value;
  if (Array.isArray(value)) {
    return value.map((item) => resolveValue(item, stack));
  }
  if (isObject(value)) {
    return Object.fromEntries(
      Object.entries(value).map((
        [key, child],
      ) => [key, resolveValue(child, stack)]),
    );
  }
  fail(`unsupported value at ${stack.at(-1) ?? "unknown"}`);
}

function dimension(value: unknown, path: string): string {
  if (
    !isObject(value) || typeof value.value !== "number" ||
    !Number.isFinite(value.value) || typeof value.unit !== "string"
  ) {
    fail(
      `dimension ${path} must be a DTCG object with numeric value and explicit unit`,
    );
  }

  const isShadow = path.startsWith("shadow.") || path.startsWith("elevation.");
  const isSpacing = /(^|\.)spacing(?:\.|$)/.test(path);
  const isRadius = /(^|\.)radius(?:[-.]|$)/.test(path);
  const isPill = path === "dimension.radius.pill" ||
    path === "dimension.radius-pill";
  const isBorder = /(^|\.)border(?:[-.]|$)/.test(path);
  const isFontSize = /(^|\.)font-size(?:[-.]|$)/.test(path);
  const isTarget = /(^|\.)target(?:[-.]|$)/.test(path);

  if (isPill) {
    if (value.unit !== "px" || value.value !== 999) {
      fail(`dimension ${path} must use the approved 999px pill sentinel`);
    }
  } else if (isShadow || isBorder) {
    if (value.unit !== "px") fail(`dimension ${path} must use px`);
  } else if (isSpacing || isRadius || isFontSize || isTarget) {
    if (value.unit !== "rem") fail(`dimension ${path} must use rem`);
  } else {
    fail(`dimension ${path} has no documented unit policy`);
  }

  return `${value.value}${value.unit}`;
}

function color(value: unknown, path: string): string {
  if (
    !isObject(value) || value.colorSpace !== "srgb" ||
    !Array.isArray(value.components) || value.components.length !== 3
  ) {
    fail(`color ${path} must use an srgb colorSpace with three components`);
  }
  if (
    !value.components.every((component) =>
      typeof component === "number" && component >= 0 && component <= 1
    )
  ) {
    fail(`color ${path} has components outside the srgb range`);
  }
  const alpha = value.alpha ?? 1;
  if (typeof alpha !== "number" || alpha < 0 || alpha > 1) {
    fail(`color ${path} has an invalid alpha`);
  }
  const channels = value.components.map((component) =>
    Math.round((component as number) * 255)
  );
  if (alpha === 1) {
    return `#${
      channels.map((channel) => channel.toString(16).padStart(2, "0")).join("")
    }`;
  }
  return `rgba(${channels.join(", ")}, ${alpha})`;
}

function duration(value: unknown, path: string): string {
  if (
    !isObject(value) || typeof value.value !== "number" ||
    !Number.isFinite(value.value) || value.value < 0 || value.unit !== "ms"
  ) {
    fail(
      `duration ${path} must be a DTCG object with a non-negative value in ms`,
    );
  }
  return `${value.value}ms`;
}

function cubicBezier(value: unknown, path: string): string {
  if (
    !Array.isArray(value) || value.length !== 4 ||
    !value.every((point) => typeof point === "number" && Number.isFinite(point))
  ) {
    fail(`cubicBezier ${path} must be four finite numbers`);
  }
  // CSS rejects a curve whose x coordinates leave [0, 1].
  if (value[0] < 0 || value[0] > 1 || value[2] < 0 || value[2] > 1) {
    fail(`cubicBezier ${path} has an x coordinate outside 0 through 1`);
  }
  return `cubic-bezier(${value.join(", ")})`;
}

function fontFamily(value: unknown, path: string): string {
  const families = typeof value === "string" ? [value] : value;
  if (
    !Array.isArray(families) || families.length === 0 ||
    !families.every((item) => typeof item === "string" && item.length > 0)
  ) {
    fail(`fontFamily ${path} must be a non-empty string array`);
  }
  return (families as string[]).map((family) =>
    /\s/.test(family) ? `"${family}"` : family
  ).join(", ");
}

function shadow(value: unknown, path: string): string {
  if (!Array.isArray(value)) fail(`shadow ${path} must be an array`);
  return value.map((entry, index) => {
    if (!isObject(entry)) fail(`shadow ${path}[${index}] must be an object`);
    const entryPath = `${path}[${index}]`;
    const colorValue = color(entry.color, `${entryPath}.color`);
    const x = dimension(entry.offsetX, `${entryPath}.offsetX`);
    const y = dimension(entry.offsetY, `${entryPath}.offsetY`);
    const blur = dimension(entry.blur, `${entryPath}.blur`);
    const spread = dimension(
      entry.spread ?? { value: 0, unit: "px" },
      `${entryPath}.spread`,
    );
    return `${x} ${y} ${blur} ${spread} ${colorValue}`;
  }).join(", ") || "none";
}

function cssValue(token: Token, value: unknown): string {
  switch (token.type) {
    case "color":
      return color(value, token.path);
    case "cubicBezier":
      return cubicBezier(value, token.path);
    case "dimension":
      return dimension(value, token.path);
    case "duration":
      return duration(value, token.path);
    case "fontFamily":
      return fontFamily(value, token.path);
    case "fontWeight":
      if (
        typeof value !== "number" || !Number.isInteger(value) || value < 1 ||
        value > 1000
      ) {
        fail(`fontWeight ${token.path} must be an integer from 1 through 1000`);
      }
      return String(value);
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        fail(`number ${token.path} must be finite`);
      }
      return String(value);
    case "shadow":
      return shadow(value, token.path);
  }
}

const runtimeAliases: Record<string, string> = {
  "dimension.spacing": "spacing",
  "dimension.radius-sm": "radius-sm",
  "dimension.radius-md": "radius-md",
  "dimension.radius-lg": "radius-lg",
  "dimension.radius-xl": "radius-xl",
  "dimension.radius-pill": "radius-pill",
  "dimension.border-width-hairline": "border-width-hairline",
  "dimension.border-width-thin": "border-width-thin",
  "dimension.border-width-thick": "border-width-thick",
  "color.canvas": "color-canvas",
  "color.surface": "color-surface",
  "color.surface-raised": "color-surface-raised",
  "color.surface-sunken": "color-surface-sunken",
  "color.text": "color-text",
  "color.text-muted": "color-text-muted",
  "color.text-subtle": "color-text-subtle",
  "color.border": "color-border",
  "color.focus": "color-focus",
  "color.primary": "color-primary",
  "color.on-primary": "color-on-primary",
  "color.accent": "color-accent",
  "color.on-accent": "color-on-accent",
  "color.accent-container": "color-accent-container",
  "color.on-accent-container": "color-on-accent-container",
  "color.selected": "color-selected",
  "color.on-selected": "color-on-selected",
  "color.scrim": "color-scrim",
  "color.danger": "color-danger",
  "color.on-danger": "color-on-danger",
  "font.family.sans": "font-family-sans",
  "font.family.mono": "font-family-mono",
  "elevation.none": "shadow-none",
  "elevation.sm": "shadow-sm",
  "elevation.md": "shadow-md",
  "elevation.raised": "shadow-raised",
};

function cssName(path: string): string {
  return `--ct-${path.replaceAll(".", "-")}`;
}

function runtimeName(path: string): string | undefined {
  const alias = runtimeAliases[path];
  return alias ? `--ct-${alias}` : undefined;
}

function tailwindName(path: string, type: TokenType): string | undefined {
  const alias = runtimeAliases[path];
  if (!alias) return undefined;
  if (type === "color" && alias.startsWith("color-")) {
    return `--color-${alias.slice("color-".length)}`;
  }
  if (type === "dimension" && alias.startsWith("radius-")) {
    return `--radius-${alias.slice("radius-".length)}`;
  }
  if (type === "dimension" && alias.startsWith("border-width-")) {
    return `--border-width-${alias.slice("border-width-".length)}`;
  }
  if (type === "dimension" && alias === "spacing") return "--spacing";
  if (type === "shadow" && alias.startsWith("shadow-")) {
    return `--shadow-${alias.slice("shadow-".length)}`;
  }
  return undefined;
}

for (const source of sources) collect(parseFile(source), [], undefined, source);

const emittedCssNames = new Set<string>();
const declarations = [...tokens.values()].flatMap((token) => {
  validateAliases(token.value, token, token.type, token.path);
  const value = resolveToken(token.path, []);
  const css = cssValue(token, value);
  const names = [
    ...new Set(
      [cssName(token.path), runtimeName(token.path)].filter((
        name,
      ): name is string => Boolean(name)),
    ),
  ].filter((name) => {
    if (emittedCssNames.has(name)) return false;
    emittedCssNames.add(name);
    return true;
  });
  return names.map((name) => `    ${name}: ${css};`);
});

const tailwindAliases = [...tokens.values()]
  .map((token) => {
    const name = tailwindName(token.path, token.type);
    const runtime = runtimeName(token.path);
    return name && runtime ? `  ${name}: var(${runtime});` : undefined;
  })
  .filter((line): line is string => Boolean(line));

const compatibilityAliases = [
  ["--color-background", "color.canvas"],
  ["--color-foreground", "color.text"],
  ["--color-foreground-muted", "color.text-muted"],
  ["--color-foreground-subtle", "color.text-subtle"],
  ["--color-card", "color.surface"],
  ["--color-card-foreground", "color.text"],
  ["--color-primary-foreground", "color.on-primary"],
  ["--color-secondary", "color.accent"],
  ["--color-secondary-foreground", "color.on-accent"],
  ["--color-destructive", "color.danger"],
  ["--color-destructive-foreground", "color.on-danger"],
  ["--color-ring", "color.focus"],
].map(([name, path]) => {
  const runtime = runtimeName(path);
  if (!runtime) {
    fail(`compatibility alias ${name} has no authored runtime token ${path}`);
  }
  return `  ${name}: var(${runtime});`;
});

const generated = [
  "/* @generated by x/design-tokens.ts; edit the .tokens.json sources instead. */",
  "",
  "@layer tokens {",
  "  :root {",
  ...declarations,
  "    --ct-font-family: var(--ct-font-family-sans);",
  "  }",
  "}",
  "",
  "/* Tailwind names are derived from semantic token paths. Legacy aliases remain for migration. */",
  "@theme inline {",
  ...tailwindAliases,
  ...compatibilityAliases,
  "  --font-sans: var(--ct-font-family-sans);",
  "  --font-mono: var(--ct-font-family-mono);",
  "}",
  "",
].join("\n");

// ---------------------------------------------------------------------------------------------
// The palette (ADR 055): ramps, derived roles, and the contrast check.
// ---------------------------------------------------------------------------------------------

const paletteSource = "palette.tokens.json";
const paletteOutputUrl = new URL(
  "src/clay_dioxus_kit/styles/palette.generated.css",
  root,
);
const rampNames = [
  "neutral",
  "primary",
  "accent",
  "danger",
  "success",
  "warning",
  "info",
] as const;
type RampName = typeof rampNames[number];
const schemes = ["light", "dark"] as const;
type Scheme = typeof schemes[number];
type ContrastLevel = "standard" | "more";
type Step = number | "contrast";
type Pair = Record<Scheme, string>;
type Palette = Record<RampName, Record<string, Pair>>;
/** `scrim` is the neutral's step 12 at this alpha: the former overlay token's 32%. */
const scrimPercent = 32;

/** A role takes one step of one ramp; `more` is what `data-contrast="more"` takes instead. */
type RoleRef = { ramp: RampName; step: Step };
type Role = { name: string; standard: RoleRef; more?: RoleRef };

/**
 * The role derivation (ADR 055 §3.2, §4.2). Every colour family is a Material quartet (X, on-X,
 * X-container, on-X-container) over its ramp; the surfaces are a ladder over the neutral. The
 * `more` variant moves muted text to step 12, the border to the strong border and X to step 10.
 */
const statusFamilies = [
  "primary",
  "accent",
  "danger",
  "success",
  "warning",
  "info",
] as const;
const roles: Role[] = [
  { name: "canvas", standard: { ramp: "neutral", step: 1 } },
  { name: "surface-sunken", standard: { ramp: "neutral", step: 2 } },
  { name: "surface", standard: { ramp: "neutral", step: 3 } },
  { name: "surface-raised", standard: { ramp: "neutral", step: 4 } },
  { name: "text", standard: { ramp: "neutral", step: 12 } },
  {
    name: "text-muted",
    standard: { ramp: "neutral", step: 11 },
    more: { ramp: "neutral", step: 12 },
  },
  {
    name: "text-subtle",
    standard: { ramp: "neutral", step: 11 },
    more: { ramp: "neutral", step: 12 },
  },
  {
    name: "border",
    standard: { ramp: "neutral", step: 6 },
    more: { ramp: "neutral", step: 7 },
  },
  { name: "border-strong", standard: { ramp: "neutral", step: 7 } },
  { name: "focus", standard: { ramp: "primary", step: 8 } },
  ...statusFamilies.flatMap((family): Role[] => [
    {
      name: family,
      standard: { ramp: family, step: 9 },
      more: { ramp: family, step: 10 },
    },
    { name: `on-${family}`, standard: { ramp: family, step: "contrast" } },
    { name: `${family}-container`, standard: { ramp: family, step: 3 } },
    { name: `on-${family}-container`, standard: { ramp: family, step: 11 } },
  ]),
];

/** Foreground role, background role and the least ratio: 4.5 for text, 3 for non-text UI. */
const surfaceRoles = [
  "canvas",
  "surface-sunken",
  "surface",
  "surface-raised",
];
const requiredPairs: [string, string, number][] = [
  ...["text", "text-muted", "text-subtle"].flatMap((text) =>
    surfaceRoles.map((surface): [string, string, number] => [
      text,
      surface,
      4.5,
    ])
  ),
  ...statusFamilies.flatMap((family): [string, string, number][] => [
    [`on-${family}`, family, 4.5],
    [`on-${family}-container`, `${family}-container`, 4.5],
  ]),
  ...["focus", "border-strong"].flatMap((line) =>
    surfaceRoles.map((surface): [string, string, number] => [
      line,
      surface,
      3,
    ])
  ),
];
/** Under `more` the plain border is the strong one, so it carries the same 3:1. */
const requiredPairsMore: [string, string, number][] = [
  ...requiredPairs,
  ...surfaceRoles.map((surface): [string, string, number] => [
    "border",
    surface,
    3,
  ]),
];

function hexChannels(hex: string, path: string): [number, number, number] {
  const match = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex);
  if (!match) fail(`${path} must be a #rrggbb colour, got ${hex}`);
  return [1, 2, 3].map((index) => parseInt(match[index], 16)) as [
    number,
    number,
    number,
  ];
}

/** WCAG 2.x relative luminance of an sRGB colour. */
function luminance(hex: string): number {
  const [r, g, b] = hexChannels(hex, "luminance").map((channel) => {
    const value = channel / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrastRatio(a: string, b: string): number {
  const [light, dark] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (light + 0.05) / (dark + 0.05);
}

function parsePalette(source: unknown): Palette {
  if (!isObject(source) || !isObject(source.ramps)) {
    fail(`${paletteSource} needs a ramps object`);
  }
  const ramps = source.ramps;
  const unknown = Object.keys(ramps).filter((name) =>
    !(rampNames as readonly string[]).includes(name)
  );
  if (unknown.length) fail(`${paletteSource} has unknown ramps ${unknown}`);
  const keys = [
    ...Array.from({ length: 12 }, (_, index) => String(index + 1)),
    "contrast",
  ];
  const palette = {} as Palette;
  for (const name of rampNames) {
    const ramp = ramps[name];
    if (!isObject(ramp)) fail(`${paletteSource} lacks the ${name} ramp`);
    const extra = Object.keys(ramp).filter((key) => !keys.includes(key));
    if (extra.length) fail(`ramp ${name} has unknown steps ${extra}`);
    palette[name] = {};
    for (const key of keys) {
      const pair = ramp[key];
      if (!isObject(pair)) fail(`ramp ${name} lacks step ${key}`);
      const entry = {} as Pair;
      for (const scheme of schemes) {
        const value = pair[scheme];
        if (typeof value !== "string") {
          fail(`ramp ${name} step ${key} lacks a ${scheme} value`);
        }
        hexChannels(value, `${name}.${key}.${scheme}`);
        entry[scheme] = value.toLowerCase();
      }
      palette[name][key] = entry;
    }
  }
  return palette;
}

function stepValue(
  palette: Palette,
  ref: RoleRef,
  scheme: Scheme,
): string {
  return palette[ref.ramp][String(ref.step)][scheme];
}

/**
 * Every required pair, for each scheme and contrast level, against its least ratio. A failing
 * pair is reported with the colours, so the palette can be fixed from the message alone.
 */
function contrastFailures(
  palette: Palette,
  tightest?: { margin: number; label: string; checked: number },
): string[] {
  const failures: string[] = [];
  for (const level of ["standard", "more"] as const) {
    const resolved = (name: string, scheme: Scheme) => {
      const role = roles.find((candidate) => candidate.name === name);
      if (!role) fail(`required pair names the unknown role ${name}`);
      const ref = level === "more" && role.more ? role.more : role.standard;
      return stepValue(palette, ref, scheme);
    };
    for (const scheme of schemes) {
      for (
        const [foreground, background, least] of level === "more"
          ? requiredPairsMore
          : requiredPairs
      ) {
        const fg = resolved(foreground, scheme);
        const bg = resolved(background, scheme);
        const ratio = contrastRatio(fg, bg);
        if (tightest) {
          tightest.checked++;
          if (ratio - least < tightest.margin) {
            tightest.margin = ratio - least;
            tightest.label =
              `${scheme}/${level}: ${foreground} on ${background} ${
                ratio.toFixed(2)
              }:1 (needs ${least}:1)`;
          }
        }
        if (ratio < least) {
          failures.push(
            `${scheme}/${level}: ${foreground} ${fg} on ${background} ${bg} is ${
              ratio.toFixed(2)
            }:1, needs ${least}:1`,
          );
        }
      }
    }
  }
  return failures;
}

function checkContrast(palette: Palette): void {
  const tightest = { margin: Infinity, label: "", checked: 0 };
  const failures = contrastFailures(palette, tightest);
  if (failures.length) {
    fail(
      `the palette fails WCAG AA for ${failures.length} pair(s):\n  ${
        failures.join("\n  ")
      }`,
    );
  }
  console.log(
    `palette contrast: ${tightest.checked} pairs pass WCAG AA; tightest ${tightest.label}`,
  );
}

function paletteName(ramp: RampName, step: Step): string {
  return `--ct-palette-${ramp}-${step}`;
}

function generatePaletteCss(palette: Palette): string {
  const ramps = rampNames.flatMap((ramp) =>
    [...Array.from({ length: 12 }, (_, index) => index + 1), "contrast"]
      .map((step) => {
        const pair = palette[ramp][String(step)];
        return `    ${
          paletteName(ramp, step as Step)
        }: light-dark(${pair.light}, ${pair.dark});`;
      })
  );
  const reference = (ref: RoleRef) => `var(${paletteName(ref.ramp, ref.step)})`;
  const derived = roles.map((role) =>
    `    --ct-color-${role.name}: ${reference(role.standard)};`
  );
  const more = roles.filter((role) => role.more).map((role) =>
    `    --ct-color-${role.name}: ${reference(role.more!)};`
  );
  return [
    "/* @generated by x/design-tokens.ts from tokens/palette.tokens.json; edit the JSON instead. */",
    "",
    "/*",
    " * The palette's ramps are `light-dark()` values: a custom property holding one resolves where it",
    " * is used, against the `color-scheme` of the element using it, so any subtree that sets its own",
    " * `color-scheme` shows the other scheme. The roles are derived from the ramps (ADR 055 §3.2).",
    " */",
    "@layer tokens {",
    "  :root {",
    ...ramps,
    "",
    ...derived,
    `    --ct-color-scrim: color-mix(in srgb, var(${
      paletteName("neutral", 12)
    }) ${scrimPercent}%, transparent);`,
    "  }",
    "",
    '  /* The more-contrast variant: the host sets data-contrast="more" on the root (ADR 055 §4.2). */',
    '  :root[data-contrast="more"] {',
    ...more,
    "  }",
    "}",
    "",
  ].join("\n");
}

const palette = parsePalette(parseFile(paletteSource));
checkContrast(palette);
const paletteCss = generatePaletteCss(palette);

const mode = Deno.args[0];

function expectFailure(name: string, callback: () => void): void {
  try {
    callback();
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("design tokens:")) {
      return;
    }
    throw error;
  }
  fail(`self-test ${name} unexpectedly passed`);
}

function selfTest(): void {
  if (
    dimension({ value: 0.25, unit: "rem" }, "dimension.spacing.1") !== "0.25rem"
  ) {
    fail("self-test rem dimension serialization failed");
  }
  if (
    dimension({ value: 1, unit: "px" }, "dimension.border.hairline") !== "1px"
  ) {
    fail("self-test px border serialization failed");
  }
  expectFailure(
    "legacy dimension string",
    () => dimension("4px", "dimension.spacing.1"),
  );
  expectFailure(
    "spacing px",
    () => dimension({ value: 4, unit: "px" }, "dimension.spacing.1"),
  );
  expectFailure(
    "radius px",
    () => dimension({ value: 4, unit: "px" }, "dimension.radius.sm"),
  );
  expectFailure(
    "radius rem",
    () => dimension({ value: 999, unit: "rem" }, "dimension.radius.pill"),
  );
  expectFailure(
    "invalid pill sentinel",
    () => dimension({ value: 998, unit: "px" }, "dimension.radius.pill"),
  );
  expectFailure(
    "misplaced pill sentinel",
    () =>
      dimension({ value: 999, unit: "px" }, "dimension.spacing.radius-pill"),
  );
  expectFailure(
    "border rem",
    () => dimension({ value: 1, unit: "rem" }, "dimension.border.hairline"),
  );
  expectFailure(
    "shadow em",
    () => dimension({ value: 1, unit: "em" }, "shadow.sm[0].offsetX"),
  );
  if (
    dimension({ value: 3, unit: "rem" }, "dimension.target-coarse") !== "3rem"
  ) {
    fail("self-test rem target serialization failed");
  }
  expectFailure(
    "target px",
    () => dimension({ value: 48, unit: "px" }, "dimension.target-coarse"),
  );
  expectFailure(
    "unclassified dimension",
    () => dimension({ value: 1, unit: "px" }, "dimension.unknown"),
  );
  if (
    duration({ value: 250, unit: "ms" }, "motion.duration.medium") !== "250ms"
  ) {
    fail("self-test duration serialization failed");
  }
  expectFailure(
    "duration in seconds",
    () => duration({ value: 0.25, unit: "s" }, "motion.duration.medium"),
  );
  expectFailure(
    "negative duration",
    () => duration({ value: -1, unit: "ms" }, "motion.duration.medium"),
  );
  if (
    cubicBezier([0.05, 0.7, 0.1, 1], "motion.easing.decelerate") !==
      "cubic-bezier(0.05, 0.7, 0.1, 1)"
  ) {
    fail("self-test cubicBezier serialization failed");
  }
  expectFailure(
    "cubicBezier x outside range",
    () => cubicBezier([1.5, 0, 0, 1], "motion.easing.decelerate"),
  );
  expectFailure(
    "cubicBezier arity",
    () => cubicBezier([0, 0, 1], "motion.easing.decelerate"),
  );
  expectFailure(
    "unknown metadata",
    () => validateMetadata({ $unknown: true }, "self-test", false),
  );

  const primitive: Token = {
    path: "color.primitive",
    type: "color",
    value: {},
    source: "primitives.tokens.json",
    layer: "primitive",
  };
  const semantic: Token = {
    path: "color.semantic",
    type: "color",
    value: {},
    source: "semantic.tokens.json",
    layer: "semantic",
  };
  const dimensionToken: Token = {
    path: "dimension.primitive",
    type: "dimension",
    value: {},
    source: "primitives.tokens.json",
    layer: "primitive",
  };
  tokens.clear();
  tokens.set(primitive.path, primitive);
  tokens.set(semantic.path, semantic);
  tokens.set(dimensionToken.path, dimensionToken);
  expectFailure(
    "primitive layer dependency",
    () =>
      validateAliases("{color.semantic}", primitive, "color", primitive.path),
  );
  expectFailure(
    "alias type mismatch",
    () =>
      validateAliases(
        "{dimension.primitive}",
        semantic,
        "color",
        semantic.path,
      ),
  );
  if (!generated.includes("--ct-font-family: var(--ct-font-family-sans);")) {
    fail("self-test generated legacy font alias is missing");
  }
  if (!generated.includes("--ct-font-family-mono:")) {
    fail("self-test generated monospace font family is missing");
  }
  if (contrastRatio("#000000", "#ffffff").toFixed(2) !== "21.00") {
    fail("self-test black on white must be 21:1");
  }
  if (contrastRatio("#767676", "#ffffff").toFixed(2) !== "4.54") {
    fail("self-test #767676 on white must be 4.54:1");
  }
  expectFailure(
    "non-hex palette colour",
    () => hexChannels("red", "self-test"),
  );
  const failing = structuredClone(palette);
  failing.neutral["12"].light = failing.neutral["1"].light;
  expectFailure("unreadable text", () => checkContrast(failing));
  const missing = structuredClone(palette) as unknown as Record<
    string,
    Record<string, unknown>
  >;
  delete missing.primary["9"];
  expectFailure(
    "missing step",
    () => parsePalette({ ramps: missing }),
  );
  if (!paletteCss.includes("--ct-palette-neutral-1: light-dark(")) {
    fail("self-test generated palette ramps are missing");
  }
  console.log("design token self-tests passed");
}

if (mode === "--self-test") {
  selfTest();
} else if (mode === "--write") {
  await Deno.writeTextFile(outputUrl, generated);
  await Deno.writeTextFile(paletteOutputUrl, paletteCss);
  console.log(`wrote ${outputUrl.pathname} and ${paletteOutputUrl.pathname}`);
} else if (mode === "--check") {
  for (
    const [url, expected] of [[outputUrl, generated], [
      paletteOutputUrl,
      paletteCss,
    ]] as const
  ) {
    let existing: string;
    try {
      existing = await Deno.readTextFile(url);
    } catch {
      fail(`generated output ${url.pathname} is missing; run with --write`);
    }
    if (existing !== expected) {
      fail(`generated output ${url.pathname} is stale; run with --write`);
    }
    console.log(`checked ${url.pathname}`);
  }
} else if (mode !== "--self-test") {
  fail(
    "usage: deno run --allow-read --allow-write x/design-tokens.ts --write|--check|--self-test",
  );
}
