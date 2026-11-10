// Differential scenarios for `FileDiff.render` (S12a-2b): hydration, file
// loading and the render orchestration, over a real DOM.
//
// The published `FileDiff` runs over jsdom 26.1.0 (the version @pierre/diffs'
// own tests pin): real elements, shadow roots, `innerHTML` parsing and style
// serialization. Fakes: `ResizeObserver` and `requestAnimationFrame`
// (recorders; frames never run), a no-op `CSSStyleSheet` (jsdom has no
// constructable stylesheets), and the instance's renderer, whose cache layer is
// replaced by the synchronous semantics of the Rust `DiffHunksRenderer`
// (`renderDiff` over a plain-text stub highlighter plus `processDiffResult`, as
// the S6 oracle does; `getDiffForNextRender` is the identity; `hydrate` and
// `clearRenderCache` do nothing). Containers are never connected to the document,
// so the scrollbar gutter is never measured (`host.isConnected === false`).
//
// A scenario is a list of steps over one or two instances (a second instance
// hydrates a container the first rendered). After every step the oracle records
// the step's return value, the trace since the previous step (the host calls
// the managers make, in the vocabulary `observer.create`/`observe`/`unobserve`/
// `observer.disconnect`/`listener.add`/`listener.remove` for scroll, and the
// callbacks the options carry), the owner render requests, a few instance
// fields, and the tree of every named container: elements as
// `[TAG, [[name, value]...] sorted, [children...], shadowChildren | null]`, text
// as strings, `style` (except that of the gutter utility node) canonicalised to `name:value;` and an empty one dropped,
// the sprite as a hash of its serialisation. Nodes are named by their path from
// the top of their tree (`label/index/~/index`, `~` being the shadow root),
// where the labels are the named containers. The Rust port replays the same
// scenarios (`components/file_diff_render_oracle_tests.rs`).
//
// `cleanUp` (S12a-2c), `isLineRenderable`, `getNearestRenderableLine`,
// `revealLine` and `expandHunk` are steps too. One deliberate difference: the
// published `cleanUp` leaves the sprite and the theme and unsafe CSS `style`
// elements in the shadow root (a later instance on the container adopts them);
// the Rust instance owns the nodes it creates and frees them, so this oracle
// removes those three elements after the published `cleanUp` (see `cleanUp`
// in `runScenario`) and the trees compare equal. A cleaned-up instance takes no
// further renders (they throw), so scenarios end an instance with `cleanUp`
// and may let another instance render over the same container.
//
// What this establishes: the instance's decisions, call order into its managers,
// and the document it builds, for the same inputs. What it cannot: a browser's
// layout and event delivery, or the worker pool and async highlight cache.


import { clone } from "./s11a_cases.mjs";
import { stubHighlighter } from "./stub_highlighter.mjs";

// ---- the pools scenarios name ----------------------------------------------------

function syntheticPair(random, lineCount) {
  const old = Array.from({ length: lineCount }, (_, index) => `line ${index + 1}`);
  const changed = old.slice();
  const edits = 1 + random.int(3);
  for (let edit = 0; edit < edits; edit++) {
    const at = random.int(changed.length);
    switch (random.int(3)) {
      case 0:
        changed[at] = `edited ${at}`;
        break;
      case 1:
        changed.splice(at, 0, `inserted ${at}`);
        break;
      default:
        changed.splice(at, 1);
    }
  }
  return [old.join("\n") + "\n", changed.join("\n") + "\n"];
}

/** Templates (JSON data) of the inputs scenarios name; `freshPool` makes live copies. */
async function buildTemplates(oracle, random) {
  const { parseDiffFromFile } = await import(oracle.pierre("utils/parseDiffFromFile.js"));
  const { parsePatchFiles } = await import(oracle.pierre("utils/parsePatchFiles.js"));
  const jsdiff = await import(oracle.jsdiff);
  const pairs = [10, 14, 8, 12].map((count) => syntheticPair(random, count));
  const file = (name, contents, cacheKey) =>
    cacheKey === undefined ? { name, contents } : { name, contents, cacheKey };
  const files = {
    F0: { old: file("a.txt", pairs[0][0]), new: file("a.txt", pairs[0][1]) },
    F0b: { old: file("a.txt", pairs[0][0]), new: file("a.txt", pairs[0][1]) },
    F1: { old: file("a.txt", pairs[1][0]), new: file("a.txt", pairs[1][1]) },
    F2: { old: file("b.txt", pairs[2][0]), new: file("b.txt", pairs[2][1]) },
    F3: { old: null, new: file("c.txt", pairs[3][1]) },
    F4: { old: file("d.txt", pairs[3][0]), new: null },
    F5: { old: file("a.txt", pairs[0][0], "o1"), new: file("a.txt", pairs[0][1], "n1") },
    F5b: { old: file("a.txt", pairs[0][0], "o1"), new: file("a.txt", pairs[1][1], "n1") },
  };
  // Larger files with collapsed context between hunks (no random draws, so the
  // scenarios generated before these existed are unchanged).
  const numbered = (count, edits) =>
    Array.from({ length: count }, (_, index) => (edits.includes(index + 1) ? `edited ${index + 1}` : `line ${index + 1}`)).join("\n") + "\n";
  const plain = (count) => numbered(count, []);
  files.F6 = { old: file("big.txt", plain(60)), new: file("big.txt", numbered(60, [10, 30, 50])) };
  // Leading and trailing gaps of exactly two lines (the collapsed context threshold).
  files.F8 = { old: file("gap.txt", plain(30)), new: file("gap.txt", numbered(30, [7, 24])) };
  files.F7 = { old: file("mid.txt", plain(40)), new: file("mid.txt", numbered(40, [2, 20])) };
  const diffs = {};
  for (const [name, source] of [
    ["D0", files.F0],
    ["D1", files.F1],
    ["D2", files.F2],
    ["D6", files.F6],
    ["D7", files.F7],
    ["D8", files.F8],
  ]) {
    diffs[name] = JSON.parse(JSON.stringify(parseDiffFromFile(source.old, source.new)));
  }
  diffs.D2b = clone(diffs.D2);
  diffs.K1 = { ...clone(diffs.D1), cacheKey: "ck1" };
  diffs.K2 = { ...clone(diffs.D1), cacheKey: "ck1" };
  diffs.K3 = { ...clone(diffs.D0), cacheKey: "ck3" };
  // Partial diffs: parsed from a patch with one line of context.
  const partials = {};
  for (const [name, [oldText, newText]] of [
    ["P0", pairs[0]],
    ["P1", pairs[1]],
  ]) {
    const patch = jsdiff.createTwoFilesPatch("p.txt", "p.txt", oldText, newText, undefined, undefined, { context: 1 });
    diffs[name] = JSON.parse(JSON.stringify(parsePatchFiles(patch)[0].files[0]));
    partials[name] = {
      old: file("p.txt", oldText),
      new: file("p.txt", newText),
    };
  }
  const annotations = {
    A0: [{ side: "additions", lineNumber: 2, metadata: 1 }],
    A1: [
      { side: "additions", lineNumber: 2, metadata: 1 },
      { side: "deletions", lineNumber: 3, metadata: 2 },
      { side: "additions", lineNumber: 5, metadata: 3 },
    ],
    A2: [{ side: "additions", lineNumber: 2, metadata: 1 }],
    A3: [{ side: "deletions", lineNumber: 1, metadata: 9 }],
    E: [],
    E2: [],
  };
  return { files, diffs, annotations, partials };
}

function freshPool(templates) {
  return {
    files: clone(templates.files),
    diffs: clone(templates.diffs),
    annotations: clone(templates.annotations),
    partials: clone(templates.partials),
  };
}

// ---- the DOM ----------------------------------------------------------------------

const GLOBALS = [
  "document",
  "window",
  "HTMLElement",
  "SVGElement",
  "HTMLPreElement",
  "HTMLStyleElement",
  "Element",
  "Node",
  "customElements",
  "ResizeObserver",
  "requestAnimationFrame",
  "cancelAnimationFrame",
  "CSSStyleSheet",
];

function fnv32(text) {
  let hash = 0x811c9dc5;
  for (const byte of Buffer.from(text, "utf8")) {
    hash ^= byte;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

/** `name:value;` declarations, trimmed; `null` when there are none. */
function canonicalStyle(style) {
  const declarations = style
    .split(";")
    .map((declaration) => declaration.trim())
    .filter((declaration) => declaration !== "")
    .map((declaration) => {
      const at = declaration.indexOf(":");
      return `${declaration.slice(0, at).trim()}:${declaration.slice(at + 1).trim()};`;
    });
  return declarations.length === 0 ? null : declarations.join("");
}

function signature(node) {
  if (node.nodeType === 3) return node.data;
  const attributes = Array.from(node.attributes)
    .map((attribute) => `${attribute.name}="${attribute.value}"`)
    .join(" ");
  const children = Array.from(node.childNodes).map(signature).join("");
  return `<${node.localName} ${attributes}>${children}</${node.localName}>`;
}

/**
 * The non-scroll listener removals (the interaction manager's `cleanUp`) in a
 * step, as a sorted set after the rest: the two sides remove the same listeners
 * in a different order, which nothing observes.
 */
function orderedTrace(trace) {
  const isOther = (entry) => entry[0] === "listener.remove" && entry[2] !== "scroll";
  // The listener's node is unnamed (the pre of a render that replaced it is
  // detached by then): the set is of (event type) removals.
  const others = trace.filter(isOther).map((entry) => ["listener.remove", "*", entry[2]]).sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : 1));
  return [...trace.filter((entry) => !isOther(entry)), ...others];
}

async function withWorld(JSDOM, oracleModules, body) {
  const dom = new JSDOM("<!doctype html><html><body></body></html>");
  const { window } = dom;
  const saved = new Map();
  const world = {
    trace: [],
    labels: new Map(),
    frames: [],
  };
  const install = (name, value) => {
    saved.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, { value, configurable: true, writable: true });
  };
  install("document", window.document);
  install("window", window);
  for (const name of ["HTMLElement", "SVGElement", "HTMLPreElement", "HTMLStyleElement", "Element", "Node", "customElements"]) {
    install(name, window[name]);
  }
  install(
    "ResizeObserver",
    class {
      constructor() {
        world.trace.push(["observer.create"]);
      }
      observe(element) {
        world.trace.push(["observe", world.nameOf(element)]);
      }
      unobserve(element) {
        world.trace.push(["unobserve", world.nameOf(element)]);
      }
      disconnect() {
        world.trace.push(["observer.disconnect"]);
      }
    },
  );
  install("requestAnimationFrame", (callback) => world.frames.push(callback));
  install("cancelAnimationFrame", () => {});
  install(
    "CSSStyleSheet",
    class {
      replaceSync() {}
    },
  );
  const proto = window.EventTarget.prototype;
  const add = proto.addEventListener;
  const remove = proto.removeEventListener;
  proto.addEventListener = function (type, ...rest) {
    if (type === "scroll") world.trace.push(["listener.add", world.nameOf(this), "scroll"]);
    return add.call(this, type, ...rest);
  };
  proto.removeEventListener = function (type, ...rest) {
    world.trace.push(["listener.remove", world.nameOf(this), type]);
    return remove.call(this, type, ...rest);
  };
  // The path from the top of the node's tree; `~` steps into a shadow root.
  world.nameOf = (node) => {
    const segments = [];
    let current = node;
    for (;;) {
      if (current.parentNode) {
        segments.push(String(Array.prototype.indexOf.call(current.parentNode.childNodes, current)));
        current = current.parentNode;
      } else if (current.host) {
        segments.push("~");
        current = current.host;
      } else break;
    }
    return [world.labels.get(current) ?? "?", ...segments.reverse()].join("/");
  };
  world.document = window.document;
  const { ResizeManager, clearRenderQueue } = oracleModules;
  // The dist keeps module-level state (the shared resize observer, the frame
  // queue): reset it around every scenario so no other oracle sees ours.
  const resetStatics = () => {
    ResizeManager.resizeObserver = undefined;
    ResizeManager.managersByElement.clear();
    clearRenderQueue();
  };
  resetStatics();
  try {
    return await body(world);
  } finally {
    resetStatics();
    proto.addEventListener = add;
    proto.removeEventListener = remove;
    for (const [name, descriptor] of saved) {
      if (descriptor === undefined) delete globalThis[name];
      else Object.defineProperty(globalThis, name, descriptor);
    }
    window.close();
  }
}

function snapshotTree(node) {
  if (node.nodeType === 3) return node.data;
  if (node.localName === "svg" && node.hasAttribute("data-icon-sprite")) {
    return ["SVG", `sprite:${fnv32(signature(node))}`];
  }
  const attributes = [];
  for (const attribute of node.attributes) {
    if (attribute.name === "style") {
      // jsdom's CSSStyleDeclaration rewrites `top = '0'` as `0px` and drops
      // `touch-action`, so the gutter utility's style is left to the oracle of
      // `createGutterUtilityContentNode` (utils/dom).
      if (node.getAttribute("slot") === "gutter-utility-slot") continue;
      const style = canonicalStyle(attribute.value);
      if (style !== null) attributes.push(["style", style]);
    } else {
      attributes.push([attribute.name, attribute.value]);
    }
  }
  attributes.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  return [
    node.localName.toUpperCase(),
    attributes,
    Array.from(node.childNodes).map(snapshotTree),
    node.shadowRoot ? Array.from(node.shadowRoot.childNodes).map(snapshotTree) : null,
  ];
}

// ---- options ----------------------------------------------------------------------

const PLAIN_FIELDS = [
  "themeType",
  "overflow",
  "diffStyle",
  "collapsed",
  "disableFileHeader",
  "disableVirtualizationBuffers",
  "disableLineNumbers",
  "disableBackground",
  "diffIndicators",
  "expandUnchanged",
  "unsafeCSS",
  "lineHoverHighlight",
  "enableGutterUtility",
  "collapsedContextThreshold",
  "expansionLineCount",
];

export function buildInstanceOptions(spec, run) {
  const options = {};
  for (const field of PLAIN_FIELDS) if (spec[field] !== undefined) options[field] = spec[field];
  if (spec.theme !== undefined) options.theme = clone(spec.theme);
  if (spec.hunkSeparators !== undefined) {
    options.hunkSeparators =
      spec.hunkSeparators === "fn"
        ? run.callback("hunkSeparators", (hunk) => {
            const element = document.createElement("span");
            element.setAttribute("data-separator-content", hunk.slotName);
            run.trace.push(["cb", "hunkSeparators", hunk.slotName, hunk.hunkIndex]);
            return element;
          })
        : spec.hunkSeparators;
  }
  const element = (tag, attributes, text) => {
    const node = document.createElement(tag);
    for (const [name, value] of attributes) node.setAttribute(name, value);
    if (text !== undefined) node.textContent = text;
    return node;
  };
  if (spec.renderAnnotation) {
    options.renderAnnotation = run.callback("renderAnnotation", (annotation, fileDiff) => {
      run.trace.push(["cb", "renderAnnotation", annotation.side, annotation.lineNumber, run.diffName(fileDiff)]);
      if (annotation.lineNumber % 5 === 0) return undefined;
      return element("span", [["data-annotation", `${annotation.side}-${annotation.lineNumber}-${annotation.metadata}`]], "note");
    });
  }
  for (const [flag, field, tag] of [
    ["renderHeaderPrefix", "renderHeaderPrefix", "prefix"],
    ["renderHeaderFilenameSuffix", "renderHeaderFilenameSuffix", "suffix"],
    ["renderHeaderMetadata", "renderHeaderMetadata", "metadata"],
    ["renderCustomHeader", "renderCustomHeader", "custom"],
  ]) {
    if (!spec[flag]) continue;
    options[field] = run.callback(field, (fileDiff) => {
      run.trace.push(["cb", field, fileDiff.name]);
      if (tag === "suffix" && fileDiff.type === "new") return undefined;
      return element("span", [[`data-${tag}`, ""]], fileDiff.name);
    });
  }
  if (spec.renderGutterUtility) {
    options.renderGutterUtility = run.callback("renderGutterUtility", () => {
      run.trace.push(["cb", "renderGutterUtility"]);
      return element("div", [["data-utility", ""]], "u");
    });
  }
  if (spec.onPostRender) {
    options.onPostRender = run.callback("onPostRender", (node, _instance, phase) => {
      run.trace.push(["cb", "onPostRender", phase, run.nameOf(node)]);
    });
  }
  if (spec.loadDiffFiles) {
    options.loadDiffFiles = run.callback("loadDiffFiles", (fileDiff) => {
      return new Promise((resolve) => {
        run.pendingLoad = { fileDiff, resolve };
      });
    });
  }
  return options;
}

// ---- running a scenario -----------------------------------------------------------

/** The synchronous renderer semantics the Rust port has (see the file header). */
function installSynchronousRenderer(instance, oracleModules) {
  const { DEFAULT_RENDER_RANGE, isDiffPlainText, renderDiffWithHighlighter } = oracleModules;
  const renderer = instance.hunksRenderer;
  renderer.getDiffForNextRender = (diff) => diff;
  renderer.hydrate = () => {};
  renderer.clearRenderCache = () => {};
  renderer.renderDiff = function (diff, renderRange = DEFAULT_RENDER_RANGE) {
    const defaults = this.getOptionsWithDefaults();
    const options = {
      theme: defaults.theme,
      useTokenTransformer: this.options.useTokenTransformer === true,
      tokenizeMaxLineLength: defaults.tokenizeMaxLineLength,
      lineDiffType: defaults.lineDiffType,
      maxLineDiffLength: defaults.maxLineDiffLength,
    };
    const hasContent = diff.additionLines.length > 0 || diff.deletionLines.length > 0;
    const forcePlainText = !hasContent || isDiffPlainText(diff);
    const isDefaultRange =
      renderRange.startingLine === 0 &&
      renderRange.totalLines === Number.POSITIVE_INFINITY &&
      renderRange.bufferBefore === 0 &&
      renderRange.bufferAfter === 0;
    const themed = renderDiffWithHighlighter(
      diff,
      stubHighlighter,
      options,
      forcePlainText
        ? {
            forcePlainText: true,
            startingLine: renderRange.startingLine,
            totalLines: renderRange.totalLines,
            expandedHunks: isDefaultRange || defaults.expandUnchanged ? true : this.expandedHunks,
            collapsedContextThreshold: defaults.collapsedContextThreshold,
          }
        : { forcePlainText: false, collapsedContextThreshold: defaults.collapsedContextThreshold },
    );
    const result = this.processDiffResult(diff, renderRange, themed);
    return result;
  };
}

function range(spec) {
  if (spec == null) return undefined;
  return { ...spec, totalLines: spec.totalLines ?? Number.POSITIVE_INFINITY };
}

function runScenario(JSDOM, oracleModules, templates, scenario) {
  const { FileDiff, ResizeManager } = oracleModules;
  return withWorld(JSDOM, oracleModules, (world) => {
    const pool = freshPool(templates);
    const names = new Map();
    for (const [name, diff] of Object.entries(pool.diffs)) names.set(diff, name);
    const annotationNames = new Map();
    for (const [name, list] of Object.entries(pool.annotations)) annotationNames.set(list, name);
    annotationNames.set(oracleModules.EMPTY_LINE_ANNOTATIONS, "EMPTY");
    const run = {
      trace: world.trace,
      nameOf: world.nameOf,
      pendingLoad: undefined,
      callbacks: new Map(),
      ownerRequests: 0,
      callback(field, make) {
        if (!this.callbacks.has(field)) this.callbacks.set(field, make);
        return this.callbacks.get(field);
      },
      diffName: (diff) => names.get(diff) ?? "?",
    };
    const containers = {};
    const container = (name) => {
      if (name === undefined) return undefined;
      if (!containers[name]) {
        containers[name] = document.createElement(name === "W" ? "div" : "diffs-container");
        world.labels.set(containers[name], name);
      }
      return containers[name];
    };
    const instances = [];
    const makeInstance = (spec) => {
      const instance = new FileDiff(
        buildInstanceOptions(spec ?? {}, run),
        undefined,
        scenario.managed
          ? () => {
              run.ownerRequests += 1;
              run.trace.push(["ownerRender"]);
            }
          : undefined,
      );
      installSynchronousRenderer(instance, oracleModules);
      instances.push(instance);
      return instance;
    };
    makeInstance(scenario.options);

    const input = (step) => {
      const props = {};
      if (step.diff !== undefined) props.fileDiff = pool.diffs[step.diff];
      if (step.files !== undefined) {
        const source = pool.files[step.files] ?? pool.partials[step.files];
        props.oldFile = source.old;
        props.newFile = source.new;
      }
      if (step.annotations !== undefined) props.lineAnnotations = pool.annotations[step.annotations];
      if (step.range !== undefined) props.renderRange = range(step.range);
      if (step.force) props.forceRender = true;
      if (step.defer) props.deferManagers = true;
      if (step.noEmit) props.preventEmit = true;
      if (step.container !== undefined) props.fileContainer = container(step.container);
      if (step.wrapper !== undefined) props.containerWrapper = container(step.wrapper);
      return props;
    };

    const stateOf = (instance) => ({
      inputDiff: instance.fileDiff === undefined ? null : (names.get(instance.fileDiff) ?? "parsed"),
      renderedDiff: instance.renderedData ? (names.get(instance.renderedData.fileDiff) ?? "parsed") : null,
      renderedAnnotations: instance.renderedData
        ? (annotationNames.get(instance.renderedData.lineAnnotations) ?? "?")
        : null,
      pendingDiff: instance.pendingData ? (names.get(instance.pendingData.fileDiff) ?? "parsed") : null,
      pendingAnnotations: instance.pendingData
        ? (annotationNames.get(instance.pendingData.lineAnnotations) ?? "?")
        : null,
      lastRowCount: instance.lastRowCount ?? null,
      pendingFiles: instance.pendingFiles ? (names.get(instance.pendingFiles.fileDiff) ?? "?") : null,
      hasFiles: [instance.deletionFile != null, instance.additionFile != null],
      renderRange: instance.renderRange
        ? [instance.renderRange.startingLine, Number.isFinite(instance.renderRange.totalLines) ? instance.renderRange.totalLines : null, instance.renderRange.bufferBefore, instance.renderRange.bufferAfter]
        : null,
      enabled: instance.enabled,
      mounted: instance.mounted,
      managersDirty: instance.managersDirty,
      expanded: Array.from(instance.hunksRenderer.expandedHunks.entries())
        .map(([index, region]) => [index, region.fromStart, region.fromEnd])
        .sort((a, b) => a[0] - b[0]),
    });

    const snapshots = [];
    let previousTrees = "{}";
    const record = (step, ret, instance) => {
      const trees = {};
      for (const [name, element] of Object.entries(containers)) trees[name] = snapshotTree(element);
      const serialized = JSON.stringify(trees);
      // A step that leaves the DOM as it was says "same" in the golden (the
      // trees of a scenario that queries a lot would otherwise dominate its
      // size); the replay checks the port's DOM did not change either.
      const same = serialized === previousTrees;
      if ((step.op === "isLineRenderable" || step.op === "nearestLine") && !same) {
        throw new Error(`${scenario.name}: ${step.op} changed the DOM`);
      }
      previousTrees = serialized;
      snapshots.push({
        ret,
        trace: orderedTrace(world.trace.splice(0)),
        state: stateOf(instance),
        trees: same ? "same" : trees,
      });
    };

    return (async () => {
      for (const step of scenario.steps) {
        const instance = instances[step.instance ?? 0] ?? makeInstance(step.options);
        let ret = null;
        switch (step.op) {
          case "render":
            ret = instance.render(input(step));
            break;
          case "setOptions":
            instance.setOptions(buildInstanceOptions(step.spec, run));
            break;
          case "setThemeType":
            instance.setThemeType(step.themeType);
            break;
          case "setLineAnnotations":
            instance.setLineAnnotations(pool.annotations[step.annotations]);
            break;
          case "rerender":
            instance.rerender();
            break;
          case "flushManagers":
            instance.flushManagers();
            break;
          case "hydrate": {
            const props = input(step);
            instance.hydrate(props);
            break;
          }
          case "filesLoaded": {
            const { resolve } = run.pendingLoad;
            run.pendingLoad = undefined;
            const source = pool.partials[step.files];
            resolve({ oldFile: source.old, newFile: source.new });
            await instance.pendingFiles?.promise;
            break;
          }
          case "expandHunk":
            instance.expandHunk(step.hunkIndex, step.direction, step.count);
            break;
          case "isLineRenderable":
            ret = instance.isLineRenderable(step.line);
            break;
          case "nearestLine":
            ret = instance.getNearestRenderableLine(step.line, step.direction) ?? null;
            break;
          case "revealLine":
            ret = instance.revealLine(step.line);
            break;
          case "cleanUp": {
            // The nodes the Rust instance frees and the published one leaves
            // behind (see the file header).
            const left = [instance.spriteSVG, instance.themeCSSStyle, instance.unsafeCSSStyle];
            instance.cleanUp(step.recycle === true);
            for (const node of left) node?.remove();
            break;
          }
          default:
            throw new Error(`unknown step ${step.op}`);
        }
        record(step, ret, instance);
      }
      return snapshots;
    })();
  });
}

// ---- scenarios -----------------------------------------------------------------------

const WINDOWS = [
  { startingLine: 0, totalLines: 8, bufferBefore: 0, bufferAfter: 0 },
  { startingLine: 2, totalLines: 8, bufferBefore: 10, bufferAfter: 20 },
  { startingLine: 4, totalLines: 6, bufferBefore: 0, bufferAfter: 30 },
  { startingLine: 1, totalLines: 9, bufferBefore: 5, bufferAfter: 0 },
  { startingLine: 30, totalLines: 5, bufferBefore: 0, bufferAfter: 0 },
  { startingLine: 3, totalLines: null, bufferBefore: 0, bufferAfter: 0 },
];

const SPECS = {
  base: {},
  unified: { diffStyle: "unified" },
  wrap: { overflow: "wrap" },
  headers: { renderHeaderPrefix: true, renderHeaderFilenameSuffix: true, renderHeaderMetadata: true },
  custom: { renderCustomHeader: true },
  annotated: { renderAnnotation: true },
  everything: {
    renderAnnotation: true,
    renderHeaderPrefix: true,
    renderHeaderMetadata: true,
    renderGutterUtility: true,
    hunkSeparators: "fn",
    onPostRender: true,
  },
};

function handwritten() {
  const r = (extra) => ({ op: "render", ...extra });
  return [
    { name: "a diff renders and an identical render does nothing", options: SPECS.base, steps: [r({ diff: "D0", container: "A" }), r({ diff: "D0" }), r({ diff: "D0", force: true })] },
    { name: "unified", options: SPECS.unified, steps: [r({ diff: "D0", container: "A" }), r({ diff: "D1" })] },
    { name: "switching the diff style replaces the columns", options: SPECS.base, steps: [r({ diff: "D0", container: "A" }), { op: "setOptions", spec: SPECS.unified }, r({ diff: "D0" }), { op: "setOptions", spec: SPECS.base }, r({ diff: "D0" })] },
    { name: "files render and compare by content", options: SPECS.base, steps: [r({ files: "F0", container: "A" }), r({ files: "F0b" }), r({ files: "F1" }), r({ files: "F5" }), r({ files: "F5" }), r({ files: "F5b" })] },
    { name: "an added and a deleted file", options: SPECS.base, steps: [r({ files: "F3", container: "A" }), r({ files: "F4" }), r({ files: "F3" })] },
    { name: "annotations", options: SPECS.annotated, steps: [r({ diff: "D0", container: "A", annotations: "A0" }), r({ diff: "D0", annotations: "A0" }), r({ diff: "D0", annotations: "A2" }), r({ diff: "D0", annotations: "A1" }), r({ diff: "D0", annotations: "E" }), r({ diff: "D0", annotations: "E2" }), { op: "setLineAnnotations", annotations: "A3" }, r({ diff: "D0" })] },
    { name: "annotations on a new diff", options: SPECS.annotated, steps: [r({ diff: "D0", container: "A", annotations: "A1" }), r({ diff: "D1" }), r({ diff: "D1", annotations: "A3" })] },
    { name: "partial renders over overlapping windows", options: SPECS.base, steps: [r({ diff: "D1", container: "A", range: WINDOWS[0] }), r({ diff: "D1", range: WINDOWS[1] }), r({ diff: "D1", range: WINDOWS[2] }), r({ diff: "D1", range: WINDOWS[3] }), r({ diff: "D1", range: WINDOWS[4] }), r({ diff: "D1", range: WINDOWS[5] }), r({ diff: "D1" })] },
    { name: "partial renders, unified", options: SPECS.unified, steps: [r({ diff: "D1", container: "A", range: WINDOWS[0] }), r({ diff: "D1", range: WINDOWS[1] }), r({ diff: "D1", range: WINDOWS[2] }), r({ diff: "D1", range: WINDOWS[3] }), r({ diff: "D1", range: WINDOWS[0] })] },
    { name: "partial renders, wrap", options: SPECS.wrap, steps: [r({ diff: "D1", container: "A", range: WINDOWS[0] }), r({ diff: "D1", range: WINDOWS[1] }), r({ diff: "D1", range: WINDOWS[3] })] },
    { name: "partial renders of an added file", options: SPECS.base, steps: [r({ files: "F3", container: "A", range: WINDOWS[0] }), r({ files: "F3", range: WINDOWS[1] }), r({ files: "F3", range: WINDOWS[3] })] },
    { name: "collapsing and expanding", options: SPECS.headers, steps: [r({ diff: "D0", container: "A" }), { op: "setOptions", spec: { ...SPECS.headers, collapsed: true } }, r({ diff: "D0" }), r({ diff: "D1" }), { op: "setOptions", spec: SPECS.headers }, r({ diff: "D1" })] },
    { name: "the file header comes and goes", options: SPECS.headers, steps: [r({ diff: "D0", container: "A" }), { op: "setOptions", spec: { ...SPECS.headers, disableFileHeader: true } }, r({ diff: "D0", force: true }), { op: "setOptions", spec: SPECS.headers }, r({ diff: "D0", force: true }), r({ diff: "D1" })] },
    { name: "header slots", options: SPECS.headers, steps: [r({ diff: "D0", container: "A" }), r({ diff: "D1" }), r({ diff: "D2" }), { op: "setOptions", spec: SPECS.custom }, r({ diff: "D2", force: true }), { op: "setOptions", spec: SPECS.headers }, r({ diff: "D2", force: true })] },
    { name: "header slots of a new file", options: SPECS.headers, steps: [r({ files: "F3", container: "A" }), r({ files: "F4" })] },
    { name: "an owner-managed instance asks its owner to render", options: SPECS.everything, managed: true, steps: [r({ diff: "D0", container: "A", annotations: "A0" }), { op: "rerender" }, r({ diff: "D0", annotations: "A0" }), r({ diff: "D1" })] },
    { name: "custom separators and the gutter utility", options: SPECS.everything, steps: [r({ diff: "D0", container: "A", annotations: "A1" }), r({ diff: "D1", annotations: "A1" }), r({ diff: "D1", annotations: "A0" }), r({ diff: "D1", annotations: "E" })] },
    { name: "theme type updates", options: { ...SPECS.base, theme: "pierre-dark" }, steps: [r({ diff: "D0", container: "A" }), { op: "setThemeType", themeType: "light" }, { op: "setThemeType", themeType: "dark" }, { op: "setOptions", spec: { theme: { dark: "pierre-dark", light: "pierre-light" } } }, r({ diff: "D0" }), { op: "setThemeType", themeType: "light" }, r({ diff: "D0" })] },
    { name: "theme objects", options: { theme: { dark: "pierre-dark", light: "pierre-light" } }, steps: [r({ diff: "D0", container: "A" }), { op: "setOptions", spec: { theme: { dark: "pierre-dark", light: "pierre-light" }, themeType: "dark" } }, r({ diff: "D0" }), { op: "setOptions", spec: { theme: { dark: "pierre-light", light: "pierre-dark" }, themeType: "dark" } }, r({ diff: "D0" }), { op: "setThemeType", themeType: "light" }] },
    { name: "unsafe CSS", options: { unsafeCSS: "a{b:c}" }, steps: [r({ diff: "D0", container: "A" }), { op: "setOptions", spec: {} }, r({ diff: "D0", force: true }), { op: "setOptions", spec: { unsafeCSS: "d{e:f}" } }, r({ diff: "D0", force: true })] },
    { name: "a new container", options: SPECS.everything, steps: [r({ diff: "D0", container: "A", annotations: "A0" }), r({ diff: "D0", container: "B", annotations: "A0" }), r({ diff: "D1", container: "B", wrapper: "W" })] },
    { name: "a wrapper", options: SPECS.base, steps: [r({ diff: "D0", wrapper: "W" }), r({ diff: "D1", wrapper: "W" })] },
    { name: "post render phases", options: { onPostRender: true }, steps: [r({ diff: "D0", container: "A" }), r({ diff: "D1" }), r({ diff: "D1", noEmit: true, force: true }), r({ diff: "D1", container: "B" })] },
    { name: "deferred managers", options: SPECS.base, steps: [r({ diff: "D0", container: "A", defer: true }), r({ diff: "D1", defer: true }), { op: "flushManagers" }, { op: "flushManagers" }] },
    { name: "rerender", options: SPECS.base, steps: [{ op: "rerender" }, r({ diff: "D0", container: "A", range: WINDOWS[0] }), { op: "rerender" }, r({ diff: "D1" }), { op: "rerender" }] },
    { name: "cache keys make diffs equal", options: SPECS.base, steps: [r({ diff: "K1", container: "A" }), r({ diff: "K2" }), r({ diff: "K3" }), r({ diff: "D2b" }), r({ diff: "D2" })] },
    { name: "hydrating a rendered container", options: SPECS.everything, steps: [r({ diff: "D0", container: "A", annotations: "A0", defer: true }), { op: "hydrate", instance: 1, options: SPECS.everything, container: "A", diff: "D0", annotations: "A0" }, { op: "rerender", instance: 1 }, r({ instance: 1, diff: "D1", annotations: "A1" })] },
    { name: "hydrating with files", options: SPECS.base, steps: [r({ files: "F0", container: "A", defer: true }), { op: "hydrate", instance: 1, options: SPECS.base, container: "A", files: "F0" }, r({ instance: 1, files: "F0b" }), r({ instance: 1, files: "F1" })] },
    { name: "hydrating a container with nothing in it", options: SPECS.base, steps: [{ op: "render", diff: "D0", container: "B", range: WINDOWS[0] }, { op: "hydrate", instance: 1, options: SPECS.headers, container: "A", diff: "D1" }] },
    { name: "collapsing drops custom separators, annotations and the gutter utility", options: SPECS.everything, steps: [r({ diff: "D1", container: "A", annotations: "A1" }), { op: "setOptions", spec: { ...SPECS.everything, collapsed: true } }, r({ diff: "D1" }), { op: "setOptions", spec: SPECS.everything }, r({ diff: "D1" })] },
    { name: "a window over a changed diff renders in full", options: SPECS.base, steps: [r({ diff: "D1", container: "A", range: WINDOWS[0] }), r({ diff: "D2", range: WINDOWS[1] }), r({ diff: "D1", range: WINDOWS[2] }), r({ diff: "D1", range: WINDOWS[3] })] },
    { name: "windows sliding past annotation rows", options: { ...SPECS.annotated, expandUnchanged: true }, steps: [r({ diff: "D1", container: "A", annotations: "A1", range: { startingLine: 0, totalLines: 8, bufferBefore: 0, bufferAfter: 0 } }), ...[1, 2, 3, 4, 5, 6, 3, 1].map((start) => r({ diff: "D1", annotations: "A1", range: { startingLine: start, totalLines: 8, bufferBefore: 0, bufferAfter: 0 } }))] },
    { name: "windows sliding past annotation rows, unified", options: { ...SPECS.annotated, diffStyle: "unified", expandUnchanged: true }, steps: [r({ diff: "D1", container: "A", annotations: "A1", range: { startingLine: 0, totalLines: 8, bufferBefore: 0, bufferAfter: 0 } }), ...[1, 2, 3, 4, 5, 6, 3, 1].map((start) => r({ diff: "D1", annotations: "A1", range: { startingLine: start, totalLines: 8, bufferBefore: 0, bufferAfter: 0 } }))] },
    { name: "adjacent windows", options: SPECS.base, steps: [r({ diff: "D1", container: "A", range: { startingLine: 0, totalLines: 8, bufferBefore: 0, bufferAfter: 0 } }), r({ diff: "D1", range: { startingLine: 8, totalLines: 5, bufferBefore: 0, bufferAfter: 0 } }), r({ diff: "D1", range: { startingLine: 13, totalLines: 4, bufferBefore: 0, bufferAfter: 0 } })] },
    { name: "hydrating a container that has no header", options: { disableFileHeader: true }, steps: [r({ diff: "D0", container: "A", defer: true }), { op: "hydrate", instance: 1, options: SPECS.headers, container: "A", diff: "D0" }] },
    { name: "a theme change turns a window shift into a full render", options: { theme: "pierre-dark" }, steps: [r({ diff: "D1", container: "A", range: WINDOWS[0] }), { op: "setOptions", spec: { theme: { dark: "pierre-dark", light: "pierre-light" } } }, r({ diff: "D1", range: WINDOWS[3] })] },
    { name: "loading the files of a partial diff", options: { loadDiffFiles: true, expandUnchanged: true }, steps: [r({ diff: "P0", container: "A" }), { op: "filesLoaded", files: "P0" }, r({ diff: "P0" })] },
    { name: "a partial diff without expanding asks for nothing", options: { loadDiffFiles: true }, steps: [r({ diff: "P1", container: "A" }), r({ diff: "P1", force: true })] },
  ];
}

/** Scenarios for `cleanUp` and the line queries (S12a-2c). */
function handwrittenLines() {
  const r = (extra) => ({ op: "render", ...extra });
  const clean = (extra) => ({ op: "cleanUp", ...extra });
  const q = (line) => ({ op: "isLineRenderable", line });
  const near = (line, direction) => ({ op: "nearestLine", line, direction });
  const reveal = (line) => ({ op: "revealLine", line });
  const sweep = [0, 1, 5, 7, 8, 9, 10, 11, 12, 13, 14, 20, 21, 25, 26, 27, 28, 29, 30, 33, 36, 37, 44, 46, 47, 50, 53, 54, 55, 56, 57, 60, 61, 70];
  const queries = (lines) => lines.flatMap((line) => [q(line), near(line, "up"), near(line, "down")]);
  return [
    { name: "cleanUp frees everything a full render made", options: SPECS.everything, steps: [r({ diff: "D1", container: "A", annotations: "A1", range: WINDOWS[1] }), clean(), { op: "render", instance: 1, options: SPECS.everything, diff: "D1", container: "A", annotations: "A1" }, clean({ instance: 1 })] },
    { name: "cleanUp of an owner-managed instance leaves its container attached", options: SPECS.everything, managed: true, steps: [r({ diff: "D0", container: "A", wrapper: "W", annotations: "A0" }), clean(), clean()] },
    { name: "cleanUp detaches an unmanaged container from its wrapper", options: SPECS.everything, steps: [r({ diff: "D0", container: "A", wrapper: "W", annotations: "A0" }), clean(), r({ instance: 1, diff: "D1", container: "A", wrapper: "W" })] },
    { name: "cleanUp before anything rendered", options: SPECS.everything, steps: [clean(), clean()] },
    { name: "cleanUp with deferred managers", options: SPECS.base, steps: [r({ diff: "D0", container: "A", defer: true }), clean()] },
    { name: "cleanUp of a collapsed render and of unified columns", options: SPECS.unified, steps: [r({ diff: "D0", container: "A" }), { op: "setOptions", spec: { ...SPECS.unified, collapsed: true } }, r({ diff: "D0" }), clean()] },
    { name: "cleanUp removes buffers", options: SPECS.headers, steps: [r({ diff: "D1", container: "A", range: WINDOWS[1] }), clean()] },
    { name: "cleanUp of a hydrated instance", options: SPECS.everything, steps: [r({ diff: "D0", container: "A", annotations: "A0", defer: true }), { op: "hydrate", instance: 1, options: SPECS.everything, container: "A", diff: "D0", annotations: "A0" }, clean({ instance: 1 })] },
    { name: "cleanUp with a theme object and unsafe CSS", options: { theme: { dark: "pierre-dark", light: "pierre-light" }, unsafeCSS: "a{b:c}", onPostRender: true }, steps: [r({ diff: "D0", container: "A" }), { op: "setThemeType", themeType: "dark" }, clean(), r({ instance: 1, diff: "D0", container: "A" })] },
    { name: "recycling keeps the rendered diff and the expanded hunks", options: SPECS.base, steps: [r({ diff: "D6", container: "A" }), { op: "expandHunk", hunkIndex: 1, direction: "up", count: 2 }, clean({ recycle: true }), q(20), near(20, "down"), reveal(33)] },
    { name: "a full clean up forgets the rendered diff and the expanded hunks", options: SPECS.base, steps: [r({ diff: "D6", container: "A" }), { op: "expandHunk", hunkIndex: 1, direction: "up", count: 2 }, q(20), clean(), q(20), near(20, "down"), reveal(20)] },
    { name: "recycling then a later instance", options: SPECS.everything, steps: [r({ diff: "D1", container: "A", annotations: "A1" }), clean({ recycle: true }), r({ instance: 1, diff: "D1", container: "A", annotations: "A1" })] },
    { name: "line queries without a rendered diff", options: SPECS.base, steps: [q(3), near(3, "up"), near(3, "down"), reveal(3)] },
    { name: "line queries over collapsed context", options: SPECS.base, steps: [r({ diff: "D6", container: "A" }), ...queries(sweep)] },
    { name: "line queries with a bigger threshold", options: { collapsedContextThreshold: 12 }, steps: [r({ diff: "D6", container: "A" }), ...queries(sweep)] },
    { name: "line queries with a zero threshold", options: { collapsedContextThreshold: 0 }, steps: [r({ diff: "D6", container: "A" }), ...queries(sweep)] },
    { name: "line queries of everything expanded", options: { expandUnchanged: true }, steps: [r({ diff: "D6", container: "A" }), ...queries(sweep), reveal(20)] },
    { name: "line queries of a partial diff", options: SPECS.base, steps: [r({ diff: "P0", container: "A" }), ...queries([1, 4, 8, 20]), reveal(4)] },
    { name: "line queries of an added and a deleted file", options: SPECS.base, steps: [r({ files: "F3", container: "A" }), ...queries([1, 5, 30]), reveal(2), r({ files: "F4" }), ...queries([1, 5, 30]), reveal(2)] },
    { name: "revealing lines in each gap", options: SPECS.base, steps: [r({ diff: "D6", container: "A" }), reveal(8), ...queries([8, 9, 10]), reveal(25), reveal(26), reveal(27), reveal(28), ...queries(sweep), reveal(12), reveal(46), reveal(47), reveal(54), reveal(58), reveal(60), reveal(61), ...queries(sweep)] },
    { name: "revealing with a small step expands one slice at a time", options: { expansionLineCount: 2 }, steps: [r({ diff: "D6", container: "A" }), reveal(20), reveal(20), reveal(20), reveal(20), reveal(20), reveal(20), reveal(20), reveal(58), reveal(58), reveal(58), reveal(58), ...queries(sweep)] },
    { name: "revealing with a zero step", options: { expansionLineCount: 0 }, steps: [r({ diff: "D6", container: "A" }), reveal(20), reveal(20), reveal(14), reveal(58), ...queries(sweep)] },
    { name: "revealing in a file with a leading gap and a trailing gap", options: SPECS.unified, steps: [r({ diff: "D7", container: "A" }), ...[1, 2, 5, 9, 12, 15, 24, 30, 38, 40].flatMap((line) => [reveal(line), q(line)])] },
    { name: "queries after the render window moved", options: SPECS.base, steps: [r({ diff: "D6", container: "A", range: WINDOWS[0] }), reveal(20), r({ diff: "D6", range: WINDOWS[1] }), ...queries([10, 20, 21, 30]), clean()] },
    { name: "gaps of exactly the threshold", options: SPECS.base, steps: [r({ diff: "D8", container: "A" }), ...queries([1, 2, 3, 4, 5, 6, 12, 18, 28, 29, 30, 31]), reveal(1), reveal(2), reveal(29), reveal(30), ...queries([1, 2, 3, 29, 30])] },
    { name: "queries after a new diff", options: SPECS.base, steps: [r({ diff: "D6", container: "A" }), reveal(20), r({ diff: "D7" }), ...queries([5, 6, 7, 20]), reveal(30)] },
  ];
}

function randomScenario(random, index, diffNames) {
  const fields = {
    themeType: ["system", "light", "dark"],
    overflow: ["scroll", "wrap"],
    diffStyle: ["split", "unified"],
    collapsed: [true, false],
    disableFileHeader: [true, false],
    disableVirtualizationBuffers: [true, false],
    disableLineNumbers: [true, false],
    disableBackground: [true, false],
    diffIndicators: ["bars", "classic", "none"],
    unsafeCSS: ["a{b:c}", ""],
  };
  const randomSpec = () => {
    const spec = {};
    for (const [field, values] of Object.entries(fields)) {
      if (random.next() < 0.25) spec[field] = random.pick(values);
    }
    for (const flag of ["renderAnnotation", "renderHeaderPrefix", "renderHeaderFilenameSuffix", "renderHeaderMetadata", "renderCustomHeader", "renderGutterUtility", "onPostRender"]) {
      if (random.next() < 0.2) spec[flag] = true;
    }
    spec.hunkSeparators = random.pick([undefined, "simple", "metadata", "line-info", "line-info-basic", "fn"]);
    if (spec.hunkSeparators === undefined) delete spec.hunkSeparators;
    const theme = random.pick([undefined, "pierre-dark", "pierre-light", { dark: "pierre-dark", light: "pierre-light" }]);
    if (theme !== undefined) spec.theme = theme;
    return spec;
  };
  const inputs = [...diffNames.map((name) => ({ diff: name })), ...["F0", "F0b", "F1", "F2", "F3", "F4", "F5"].map((name) => ({ files: name }))];
  const steps = [];
  const count = 4 + random.int(6);
  for (let step = 0; step < count; step++) {
    const roll = random.next();
    if (roll < 0.62) {
      const move = { op: "render", ...random.pick(inputs) };
      if (random.next() < 0.35) move.annotations = random.pick(["A0", "A1", "A2", "A3", "E", "E2"]);
      if (random.next() < 0.4) move.range = random.pick(WINDOWS);
      if (random.next() < 0.12) move.force = true;
      if (random.next() < 0.1) move.defer = true;
      if (random.next() < 0.08) move.noEmit = true;
      if (random.next() < 0.12) move.container = random.pick(["A", "B"]);
      if (random.next() < 0.06) move.wrapper = "W";
      steps.push(move);
    } else if (roll < 0.75) steps.push({ op: "setOptions", spec: randomSpec() });
    else if (roll < 0.82) steps.push({ op: "setThemeType", themeType: random.pick(["system", "light", "dark"]) });
    else if (roll < 0.88) steps.push({ op: "setLineAnnotations", annotations: random.pick(["A0", "A1", "A2", "E"]) });
    else if (roll < 0.94) steps.push({ op: "rerender" });
    else steps.push({ op: "flushManagers" });
  }
  return { name: `random ${index}`, options: randomSpec(), managed: random.next() < 0.12, steps };
}

/** A random sequence of renders, expansions and line queries; may end in `cleanUp`. */
function randomLineScenario(random, index) {
  const spec = { ...random.pick([SPECS.base, SPECS.base, SPECS.unified, SPECS.annotated, SPECS.headers, SPECS.everything]) };
  const threshold = random.pick([undefined, undefined, 0, 1, 2, 5, 12]);
  if (threshold !== undefined) spec.collapsedContextThreshold = threshold;
  const step = random.pick([undefined, undefined, 0, 1, 2, 3, 50]);
  if (step !== undefined) spec.expansionLineCount = step;
  const diffs = ["D6", "D6", "D6", "D7", "D8", "D1", "D0"];
  const steps = [{ op: "render", diff: random.pick(diffs), container: "A", ...(random.next() < 0.3 ? { annotations: random.pick(["A0", "A1", "E"]) } : {}) }];
  const maxLine = 66;
  const count = 6 + random.int(14);
  for (let at = 0; at < count; at++) {
    const roll = random.next();
    const line = random.int(maxLine);
    if (roll < 0.22) steps.push({ op: "isLineRenderable", line });
    else if (roll < 0.4) steps.push({ op: "nearestLine", line, direction: random.pick(["up", "down"]) });
    else if (roll < 0.66) steps.push({ op: "revealLine", line });
    else if (roll < 0.76) steps.push({ op: "expandHunk", hunkIndex: random.int(5), direction: random.pick(["up", "down", "both"]), ...(random.next() < 0.5 ? { count: 1 + random.int(6) } : {}) });
    else if (roll < 0.86) steps.push({ op: "render", diff: random.pick(diffs), ...(random.next() < 0.4 ? { range: random.pick(WINDOWS) } : {}), ...(random.next() < 0.3 ? { force: true } : {}) });
    else if (roll < 0.94) {
      const next = { ...random.pick([SPECS.base, SPECS.unified, SPECS.annotated]) };
      if (random.next() < 0.5) next.expandUnchanged = true;
      if (random.next() < 0.5) next.collapsedContextThreshold = random.pick([0, 1, 3, 12]);
      if (random.next() < 0.5) next.expansionLineCount = random.pick([1, 4, 100]);
      steps.push({ op: "setOptions", spec: next });
    } else steps.push({ op: "setOptions", spec: { ...spec, collapsed: random.next() < 0.5 } });
  }
  if (random.next() < 0.5) {
    steps.push({ op: "cleanUp", recycle: random.next() < 0.3 });
    for (let at = 0; at < 3; at++) {
      const line = random.int(maxLine);
      steps.push(random.pick([{ op: "isLineRenderable", line }, { op: "nearestLine", line, direction: random.pick(["up", "down"]) }, { op: "revealLine", line }]));
    }
    if (random.next() < 0.4) steps.push({ op: "render", instance: 1, options: spec, diff: random.pick(diffs), container: "A" });
    if (random.next() < 0.5) steps.push({ op: "cleanUp", instance: steps.some((entry) => entry.instance === 1) ? 1 : 0 });
  }
  return { name: `random lines ${index}`, options: spec, managed: random.next() < 0.1, steps };
}

export async function buildS12bRender(oracle, random) {
  const { JSDOM } = await import(oracle.jsdom);
  const modules = {
    ...(await import(oracle.pierre("components/FileDiff.js"))),
    ...(await import(oracle.pierre("managers/ResizeManager.js"))),
    ...(await import(oracle.pierre("managers/UniversalRenderingManager.js"))),
    ...(await import(oracle.pierre("constants.js"))),
    ...(await import(oracle.pierre("utils/isDiffPlainText.js"))),
    ...(await import(oracle.pierre("utils/renderDiffWithHighlighter.js"))),
  };
  const templates = await buildTemplates(oracle, random);
  const scenarios = handwritten();
  const diffNames = ["D0", "D1", "D2", "D2b", "K1", "K2", "K3"];
  for (let index = 0; index < 220; index++) scenarios.push(randomScenario(random, index, diffNames));
  scenarios.push(...handwrittenLines());
  for (let index = 0; index < 90; index++) scenarios.push(randomLineScenario(random, index));
  const cases = [];
  for (const scenario of scenarios) {
    const snapshots = await runScenario(JSDOM, modules, templates, scenario);
    cases.push({ scenario, snapshots });
  }
  return { templates, cases };
}
