#!/usr/bin/env -S deno run --allow-read --allow-write

/**
 * Validate the small DTCG foundation subset and generate the CSS preview.
 *
 * The generated file is intentionally separate from tokens.css until the
 * runtime migration is complete. The JSON files are the authored source.
 */

type JsonObject = Record<string, unknown>;
type TokenType =
  | "color"
  | "dimension"
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
const tokenDirectory = new URL("src/ui/styles/tokens/", root);
const outputUrl = new URL("src/ui/styles/tokens.generated.css", root);
const sources = ["primitives.tokens.json", "semantic.tokens.json"];
const supportedTypes = new Set<TokenType>([
  "color",
  "dimension",
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

  if (isPill) {
    if (value.unit !== "px" || value.value !== 999) {
      fail(`dimension ${path} must use the approved 999px pill sentinel`);
    }
  } else if (isShadow || isBorder) {
    if (value.unit !== "px") fail(`dimension ${path} must use px`);
  } else if (isSpacing || isRadius) {
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
    case "dimension":
      return dimension(value, token.path);
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
  "color.danger": "color-danger",
  "color.on-danger": "color-on-danger",
  "font.family.sans": "font-family-sans",
  "elevation.none": "shadow-none",
  "elevation.sm": "shadow-sm",
  "elevation.md": "shadow-md",
  "elevation.raised": "shadow-raised",
};

function cssName(path: string): string {
  return `--df-${path.replaceAll(".", "-")}`;
}

function runtimeName(path: string): string | undefined {
  const alias = runtimeAliases[path];
  return alias ? `--df-${alias}` : undefined;
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
  "@layer theme {",
  "  :root {",
  ...declarations,
  "    --df-font-family: var(--df-font-family-sans);",
  "  }",
  "}",
  "",
  "/* Tailwind names are derived from semantic token paths. Legacy aliases remain for migration. */",
  "@theme inline {",
  ...tailwindAliases,
  ...compatibilityAliases,
  "  --font-sans: var(--df-font-family-sans);",
  "}",
  "",
].join("\n");

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
  expectFailure(
    "unclassified dimension",
    () => dimension({ value: 1, unit: "px" }, "dimension.unknown"),
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
  if (!generated.includes("--df-font-family: var(--df-font-family-sans);")) {
    fail("self-test generated legacy font alias is missing");
  }
  console.log("design token self-tests passed");
}

if (mode === "--self-test") {
  selfTest();
} else if (mode === "--write") {
  await Deno.writeTextFile(outputUrl, generated);
  console.log(`wrote ${new URL(outputUrl).pathname}`);
} else if (mode === "--check") {
  let existing: string;
  try {
    existing = await Deno.readTextFile(outputUrl);
  } catch {
    fail(`generated output is missing; run with --write`);
  }
  if (existing !== generated) {
    fail(`generated output is stale; run with --write`);
  }
  console.log(`checked ${new URL(outputUrl).pathname}`);
} else if (mode !== "--self-test") {
  fail(
    "usage: deno run --allow-read --allow-write x/design-tokens.ts --write|--check|--self-test",
  );
}
