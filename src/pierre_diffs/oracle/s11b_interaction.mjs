// Differential scenarios for InteractionManager (S11b).
//
// The published class runs against a scripted fake DOM: a tree of named
// elements (attributes, parents, children, siblings, slots), `closest` and
// `querySelectorAll` over the attribute selectors the manager uses, pointer
// events that carry a target and answer `composedPath()` as the target and its
// ancestors, an `elementFromPoint` answering from a table, and a
// `requestAnimationFrame` that runs when a step says so. The trace is
// everything the manager does to the world (attribute writes, the gutter
// utility nodes, listener registration, frame requests), to its callbacks
// (every option callback records its arguments) and to the event (prevent
// default and stop propagation), plus the value of every query step. The Rust
// port replays the same scenarios (`managers/interaction_oracle_tests.rs`).
//
// What this establishes: the manager's decisions for the same path, target and
// coordinates. What it cannot: real hit-testing, real pointer capture, a
// browser's event ordering. Token interactions, merge-conflict actions and the
// editor active line are not exercised (they are not ported).

import { clone } from "./s11a_cases.mjs";

// ---- the fake DOM -------------------------------------------------------------

export class FakeHTMLElement {}

/** The scenario being run: trace emitter, naming, listener tables. */
let ctx;

/** `[a]`, `[a="v"]`, `[a^="v"]` compound and comma-separated selector lists. */
function parseSelector(selector) {
  // Commas inside `[...]` (a split-view line index) do not separate selectors.
  return selector.split(/,(?![^[]*\])/).map((part) => {
    const conditions = [];
    for (const match of part.trim().matchAll(/\[([a-z-]+)(?:([\^*]?=)"([^"]*)")?\]/g)) {
      conditions.push({ name: match[1], operator: match[2], value: match[3] });
    }
    return conditions;
  });
}

function matches(element, parsed) {
  return parsed.some((conditions) =>
    conditions.every(({ name, operator, value }) => {
      const actual = element.getAttribute(name);
      if (actual === null) return false;
      if (operator === undefined) return true;
      if (operator === "=") return actual === value;
      return operator === "*=" ? actual.includes(value) : actual.startsWith(value);
    }),
  );
}

export class FakeNode extends FakeHTMLElement {
  constructor(tag) {
    super();
    this.tagName = tag.toUpperCase();
    this.attrs = [];
    this.parentNode = null;
    this.childNodes = [];
    this.assignedSlot = null;
    this.internal = false;
    this.label = undefined;
  }
  get children() {
    return this.childNodes;
  }
  get previousElementSibling() {
    if (!this.parentNode) return null;
    const siblings = this.parentNode.childNodes;
    return siblings[siblings.indexOf(this) - 1] ?? null;
  }
  get nextSibling() {
    if (!this.parentNode) return null;
    const siblings = this.parentNode.childNodes;
    return siblings[siblings.indexOf(this) + 1] ?? null;
  }
  get firstElementChild() {
    return this.childNodes[0] ?? null;
  }
  // `slot.name = ...` assigns the attribute.
  get name() {
    return this.getAttribute("name");
  }
  set name(value) {
    this.setAttribute("name", value);
  }
  // What ResizeManager and ScrollSyncManager read (S12a-2a): a declared
  // height, always connected, `data-*` attributes as `dataset`, a `style` that
  // records, `scrollTo` and scroll listeners that record. None of it is used
  // by InteractionManager, so the S11b traces are unchanged.
  get isConnected() {
    return true;
  }
  get dataset() {
    return Object.fromEntries(
      this.attrs.filter(([key]) => key.startsWith("data-")).map(([key]) => [key.slice(5), ""]),
    );
  }
  get scrollLeft() {
    return 0;
  }
  getBoundingClientRect() {
    return { height: this.height ?? 0 };
  }
  get style() {
    return {
      setProperty: (property, value) => ctx.emit("style.set", ctx.nm(this), property, value),
      removeProperty: (property) => ctx.emit("style.remove", ctx.nm(this), property),
    };
  }
  scrollTo({ left }) {
    ctx.emit("scrollTo", ctx.nm(this), left ?? null);
  }
  hasAttribute(name) {
    return this.getAttribute(name) !== null;
  }
  getAttribute(name) {
    return this.attrs.find(([key]) => key === name)?.[1] ?? null;
  }
  setAttribute(name, value) {
    const found = this.attrs.find(([key]) => key === name);
    if (found) found[1] = String(value);
    else this.attrs.push([name, String(value)]);
    if (!this.internal) ctx.emit("attr.set", ctx.nm(this), name, String(value));
  }
  removeAttribute(name) {
    this.attrs = this.attrs.filter(([key]) => key !== name);
    if (!this.internal) ctx.emit("attr.remove", ctx.nm(this), name);
  }
  appendChild(child) {
    child.detachQuietly();
    this.childNodes.push(child);
    child.parentNode = this;
    ctx.emit("append", ctx.nm(this), ctx.nm(child));
  }
  detachQuietly() {
    if (this.parentNode) {
      this.parentNode.childNodes = this.parentNode.childNodes.filter((n) => n !== this);
      this.parentNode = null;
    }
  }
  remove() {
    const parent = this.parentNode;
    this.detachQuietly();
    if (parent && !parent.internal) ctx.emit("remove", ctx.nm(this));
  }
  replaceChildren(...nodes) {
    for (const old of this.childNodes) old.parentNode = null;
    this.childNodes = [];
    for (const node of nodes) {
      node.detachQuietly();
      this.childNodes.push(node);
      node.parentNode = this;
    }
    ctx.emit("replaceChildren", ctx.nm(this), nodes.map(ctx.nm));
  }
  closest(selector) {
    const parsed = parseSelector(selector);
    for (let node = this; node; node = node.parentNode) if (matches(node, parsed)) return node;
    return null;
  }
  querySelectorAll(selector) {
    const parsed = parseSelector(selector);
    const found = [];
    const visit = (node) => {
      for (const child of node.childNodes) {
        if (matches(child, parsed)) found.push(child);
        visit(child);
      }
    };
    visit(this);
    return found;
  }
  querySelector(selector) {
    return this.querySelectorAll(selector)[0] ?? null;
  }
  getRootNode() {
    return ctx.rootNode;
  }
  set innerHTML(html) {
    // The scratch element `ensureGutterUtilityNode` parses the button into.
    this.internal = true;
    this.childNodes = [];
    for (const node of parseHtml(html)) {
      node.parentNode = this;
      this.childNodes.push(node);
    }
  }
  addEventListener(type) {
    if (type === "scroll") {
      ctx.emit("listener.add", ctx.nm(this), type);
      return;
    }
    ctx.emit("pre.addEventListener", ctx.nm(this), type);
    ctx.preListeners.set(type, this);
  }
  removeEventListener(type) {
    if (type === "scroll") {
      ctx.emit("listener.remove", ctx.nm(this), type);
      return;
    }
    ctx.emit("pre.removeEventListener", ctx.nm(this), type);
    ctx.preListeners.delete(type);
  }
}

export class FakeButton extends FakeNode {
  constructor() {
    super("button");
  }
}

/** Parses the markup `hastToHtml` writes for the utility button. */
function parseHtml(html) {
  const nodes = [];
  const stack = [];
  for (const token of html.matchAll(/<\/?[a-zA-Z][^>]*>/g)) {
    const text = token[0];
    if (text.startsWith("</")) {
      stack.pop();
      continue;
    }
    const tag = /^<([a-zA-Z][a-zA-Z0-9]*)/.exec(text)[1];
    const node = tag.toLowerCase() === "button" ? new FakeButton() : new FakeNode(tag);
    node.internal = true;
    ctx.created.push(node);
    for (const attr of text.matchAll(/\s([a-zA-Z-]+)(?:="([^"]*)")?/g)) {
      node.attrs.push([attr[1], attr[2] ?? ""]);
    }
    const parent = stack.at(-1);
    if (parent) {
      node.parentNode = parent;
      parent.childNodes.push(node);
    } else nodes.push(node);
    if (!text.endsWith("/>")) stack.push(node);
  }
  return nodes;
}

export function makeWorld(spec, rec) {
  const state = { recording: false };
  const emit = (...entry) => {
    if (state.recording) rec(...entry);
  };
  const byName = new Map();
  const nm = (node) => {
    if (node.label !== undefined) return node.label;
    if (node.getAttribute("data-gutter-utility-slot") !== null) return "$container";
    if (node.tagName === "SLOT") return "$slot";
    if (node instanceof FakeButton) return "$button";
    return "$temp";
  };

  function build(description, parent) {
    const element = new FakeNode(description.tag);
    element.label = description.name;
    byName.set(description.name, element);
    for (const [key, value] of description.attrs ?? []) element.attrs.push([key, value]);
    if (parent) {
      element.parentNode = parent;
      parent.childNodes.push(element);
    }
    for (const child of description.children ?? []) build(child, element);
    return element;
  }
  const roots = spec.tree.map((description) => build(description, null));
  for (const [slotted, slot] of Object.entries(spec.slots ?? {})) {
    byName.get(slotted).assignedSlot = byName.get(slot);
  }

  const preListeners = new Map();
  const documentListeners = new Map();
  const elementFromPoint = (x, y) => {
    const hit = spec.hits?.[`${x},${y}`];
    return hit == null ? null : byName.get(hit);
  };
  const rootNode = spec.hitTestAvailable === false ? {} : { elementFromPoint };
  const fakeDocument = {
    createElement: (tag) => {
      const node = new FakeNode(tag);
      ctx.created.push(node);
      return node;
    },
    addEventListener(type) {
      emit("document.addEventListener", type);
      documentListeners.set(type, true);
    },
    removeEventListener(type) {
      emit("document.removeEventListener", type);
      documentListeners.delete(type);
    },
  };
  if (spec.hitTestAvailable !== false) fakeDocument.elementFromPoint = elementFromPoint;

  ctx = { emit, nm, rootNode, preListeners, created: [] };
  return { byName, nm, emit, state, fakeDocument, preListeners, documentListeners, roots };
}

// ---- running one scenario -------------------------------------------------------

function runScenario(api, scenario) {
  const trace = [];
  const rec = (...entry) => trace.push(entry);
  api.clearRenderQueue();
  const world = makeWorld(scenario.world, rec);
  const { byName, nm, state, fakeDocument } = world;

  const frames = new Map();
  let nextFrameId = 0;
  const runRound = (time) => {
    const pending = [...frames.values()];
    frames.clear();
    for (const callback of pending) callback(time);
    return pending.length > 0;
  };
  const globals = {
    HTMLElement: FakeHTMLElement,
    HTMLButtonElement: FakeButton,
    Element: FakeHTMLElement,
    document: fakeDocument,
    requestAnimationFrame(callback) {
      const id = ++nextFrameId;
      frames.set(id, callback);
      rec("raf.request", id);
      return id;
    },
    cancelAnimationFrame(id) {
      rec("raf.cancel", id);
      frames.delete(id);
    },
  };
  const saved = new Map();
  for (const [key, value] of Object.entries(globals)) {
    saved.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
    Object.defineProperty(globalThis, key, { configurable: true, writable: true, value });
  }
  const savedError = console.error;
  console.error = () => {};

  // `$button`, `$container`, `$slot`: nodes the manager created, by role.
  const el = (name) =>
    name == null
      ? null
      : name.startsWith("$")
        ? ctx.created.find((node) => nm(node) === name)
        : byName.get(name);
  const rangeOut = (range) => (range == null ? null : clone(range));

  function eventProps(props) {
    const out = {
      type: props.type,
      lineNumber: props.lineNumber,
      numberColumn: props.numberColumn,
      lineElement: nm(props.lineElement),
      numberElement: nm(props.numberElement),
    };
    if (props.type === "diff-line") {
      out.annotationSide = props.annotationSide;
      out.lineType = props.lineType;
    }
    return out;
  }
  function eventOut(event) {
    return {
      pointerId: event.pointerId,
      pointerType: event.pointerType,
      shiftKey: event.shiftKey,
      target: event.target ? nm(event.target) : null,
    };
  }

  function buildOptions(spec) {
    const options = {};
    for (const key of [
      "lineHoverHighlight",
      "enableGutterUtility",
      "usesCustomGutterUtility",
      "enableLineSelection",
      "controlledSelection",
    ]) {
      if (spec[key] !== undefined) options[key] = spec[key];
    }
    const line = (name) => (props) =>
      rec(name, eventProps(props), eventOut(props.event));
    for (const [flag, name] of [
      ["onLineClick", "onLineClick"],
      ["onLineNumberClick", "onLineNumberClick"],
      ["onLineEnter", "onLineEnter"],
      ["onLineLeave", "onLineLeave"],
    ]) {
      if (spec[flag]) options[name] = line(name);
    }
    if (spec.onGutterUtilityClick) {
      options.onGutterUtilityClick = (range) => rec("onGutterUtilityClick", rangeOut(range));
    }
    for (const name of [
      "onLineSelected",
      "onLineSelectionStart",
      "onLineSelectionChange",
      "onLineSelectionEnd",
    ]) {
      if (spec[name]) options[name] = (range) => rec(name, rangeOut(range));
    }
    if (spec.onHunkExpand) {
      options.onHunkExpand = (hunkIndex, direction, override) =>
        rec("onHunkExpand", hunkIndex, direction, override === Number.POSITIVE_INFINITY ? "inf" : (override ?? null));
    }
    if (spec.lineIndex != null) {
      options.getLineIndex = (lineNumber, side) => spec.lineIndex[`${side}:${lineNumber}`];
    }
    return options;
  }

  try {
    const { pluckInteractionOptions, InteractionManager } = api;
    const initial = scenario.options ?? {};
    const hasRender = initial.hasRenderGutterUtility === true;
    let manager;
    const pluck = (spec) => {
      const base = buildOptions(spec);
      return pluckInteractionOptions(
        { ...base, ...(hasRender ? { renderGutterUtility: () => null } : {}) },
        spec.onHunkExpand ? buildOptions(spec).onHunkExpand : undefined,
        base.getLineIndex,
      );
    };
    try {
      manager = new InteractionManager(scenario.mode, pluck(initial));
    } catch (error) {
      rec("throws", error.message);
      return trace;
    }
    state.recording = true;

    for (const step of scenario.steps) {
      try {
        switch (step.op) {
          case "setOptions": {
            const spec = step.options;
            const render = spec.hasRenderGutterUtility === true;
            const base = buildOptions(spec);
            manager.setOptions(
              pluckInteractionOptions(
                { ...base, ...(render ? { renderGutterUtility: () => null } : {}) },
                base.onHunkExpand,
                base.getLineIndex,
              ),
            );
            break;
          }
          case "setup":
            manager.setup(el(step.pre));
            break;
          case "cleanUp":
            manager.cleanUp();
            break;
          case "setSelection":
            manager.setSelection(clone(step.range), step.options ?? undefined);
            break;
          case "setSelectionDirty":
            manager.setSelectionDirty();
            break;
          case "getSelection":
            rec("result", "getSelection", rangeOut(manager.getSelection()));
            break;
          case "getHoveredLine":
            rec("result", "getHoveredLine", manager.getHoveredLine() ?? null);
            break;
          case "isSelectionDirty":
            rec("result", "isSelectionDirty", manager.isSelectionDirty());
            break;
          case "attr": {
            const node = el(step.target);
            if (step.value == null) node.attrs = node.attrs.filter(([k]) => k !== step.name);
            else {
              const found = node.attrs.find(([k]) => k === step.name);
              if (found) found[1] = step.value;
              else node.attrs.push([step.name, step.value]);
            }
            break;
          }
          case "frames": {
            let rounds = 0;
            while (rounds < step.limit && runRound(step.time + 16 * (rounds + 1))) rounds++;
            break;
          }
          case "event": {
            const target = el(step.target);
            const path = [];
            for (let node = target; node; node = node.parentNode) path.push(node);
            path.push({ shadowRoot: true }, { document: true });
            let prevented = false;
            let stopped = false;
            const event = {
              pointerId: step.pointerId ?? 1,
              pointerType: step.pointerType ?? "mouse",
              button: step.button ?? 0,
              shiftKey: step.shiftKey ?? false,
              clientX: step.clientX ?? Number.NaN,
              clientY: step.clientY ?? Number.NaN,
              target,
              composedPath: () => (target ? path : []),
              preventDefault() {
                prevented = true;
              },
              stopPropagation() {
                stopped = true;
              },
            };
            const handlers = {
              click: () => manager.handlePointerClick(event),
              pointerdown: () => manager.handlePointerDown(event),
              pointermove: () => manager.handlePointerMove(event),
              pointerleave: () => manager.handlePointerLeave(event),
              "document:pointermove": () => manager.handleDocumentPointerMove(event),
              "document:pointerup": () => manager.handleDocumentPointerUp(event),
              "document:pointercancel": () => manager.handleDocumentPointerCancel(event),
            };
            const isDocument = step.type.startsWith("document:");
            const registered = isDocument
              ? world.documentListeners.has(step.type.slice("document:".length))
              : world.preListeners.has(step.type);
            if (registered) {
              handlers[step.type]();
              rec("response", prevented, stopped);
            }
            break;
          }
          default:
            throw new Error(`unknown op ${step.op}`);
        }
      } catch (error) {
        rec("throws", error.message);
      }
    }
  } finally {
    console.error = savedError;
    for (const [key, descriptor] of saved) {
      if (descriptor === undefined) delete globalThis[key];
      else Object.defineProperty(globalThis, key, descriptor);
    }
  }
  return trace;
}

// ---- fixtures ------------------------------------------------------------------

let counter = 0;
function node(tag, attrs = [], children = [], name) {
  return { tag, attrs, children, name: name ?? `${tag}${counter++}` };
}

/** Rows of a file: gutter and content columns, with annotation rows. */
export function fileFixture(lineCount, annotated = []) {
  counter = 0;
  const gutterRows = [];
  const contentRows = [];
  const slots = {};
  const light = [];
  for (let index = 0; index < lineCount; index++) {
    const lineNumber = index + 1;
    gutterRows.push(
      node("div", [
        ["data-column-number", `${lineNumber}`],
        ["data-line-index", `${index}`],
        ["data-line-type", "context"],
      ], [node("span", [], [], `gnum${lineNumber}`)], `g${lineNumber}`),
    );
    contentRows.push(
      node("div", [
        ["data-line", `${lineNumber}`],
        ["data-line-index", `${index}`],
        ["data-line-type", "context"],
      ], [node("span", [["data-char", "0"]], [], `tok${lineNumber}`)], `c${lineNumber}`),
    );
    if (annotated.includes(lineNumber)) {
      gutterRows.push(node("div", [["data-gutter-buffer", "annotation"]], [], `gbuf${lineNumber}`));
      const slotName = `annotation-${lineNumber}`;
      contentRows.push(
        node("div", [["data-line-annotation", `${lineNumber},${lineNumber}`]], [
          node("slot", [["name", slotName]], [], `slot${lineNumber}`),
        ], `a${lineNumber}`),
      );
      light.push(node("div", [["slot", slotName]], [node("span", [], [], `lc${lineNumber}`)], `light${lineNumber}`));
      slots[`light${lineNumber}`] = `slot${lineNumber}`;
    }
  }
  const code = node("div", [["data-code", ""]], [
    node("div", [["data-gutter", ""]], gutterRows, "gutter"),
    node("div", [["data-content", ""]], contentRows, "content"),
  ], "code");
  return { tree: [node("pre", [], [code], "pre"), ...light], slots };
}

/** A diff: unified (one code column) or split (deletions and additions). */
export function diffFixture(split, rows) {
  counter = 0;
  // rows: [{type, del?, add?}] with 1-based line numbers; unified index is the
  // row position, split index is the position within the column's own list.
  const columns = split
    ? [
        ["deletions", rows.filter((row) => row.type !== "add")],
        ["additions", rows.filter((row) => row.type !== "del")],
      ]
    : [["unified", rows]];
  const slots = {};
  const light = [];
  const codes = columns.map(([kind, list]) => {
    const gutterRows = [];
    const contentRows = [];
    list.forEach((row, position) => {
      const lineNumber = kind === "deletions" ? row.del : kind === "additions" ? row.add : (row.add ?? row.del);
      const lineType = row.type === "add" ? "change-addition" : row.type === "del" ? "change-deletion" : "context";
      const index = `${row.unified ?? rows.indexOf(row)},${position}`;
      const tag = `${kind[0]}${lineNumber}`;
      gutterRows.push(node("div", [["data-column-number", `${lineNumber}`], ["data-line-index", index], ["data-line-type", lineType]], [node("span", [], [], `gnum-${tag}`)], `g-${tag}`));
      contentRows.push(node("div", [["data-line", `${lineNumber}`], ["data-line-index", index], ["data-line-type", lineType]], [node("span", [["data-char", "0"]], [], `tok-${tag}`)], `c-${tag}`));
      if (row.annotation) {
        const slotName = `annotation-${kind === "unified" ? (row.add != null ? "additions" : "deletions") : kind}-${lineNumber}`;
        gutterRows.push(node("div", [["data-gutter-buffer", "annotation"]], [], `gbuf-${tag}`));
        contentRows.push(node("div", [["data-line-annotation", `${lineNumber},${lineNumber}`]], [node("slot", [["name", slotName]], [], `slot-${tag}`)], `a-${tag}`));
        light.push(node("div", [["slot", slotName]], [node("span", [], [], `lc-${tag}`)], `light-${tag}`));
        slots[`light-${tag}`] = `slot-${tag}`;
      }
    });
    const attrs = [["data-code", ""], [`data-${kind}`, ""]];
    return node("code", attrs, [
      node("div", [["data-gutter", ""]], gutterRows, `gutter-${kind}`),
      node("div", [["data-content", ""]], contentRows, `content-${kind}`),
    ], `code-${kind}`);
  });
  const separator = node("div", [["data-separator", ""], ["data-expand-index", "3"]], [
    node("div", [["data-unmodified-lines", ""]], [
      node("button", [["data-expand-button", ""], ["data-expand-up", ""]], [node("svg", [], [], "sep-svg")], "sep-up"),
      node("button", [["data-expand-button", ""], ["data-expand-all-button", ""]], [], "sep-all"),
    ], "sep-lines"),
  ], "sep");
  // The separator row exists in both columns of the first code element.
  codes[0].children.at(-1).children.push(separator);
  codes[0].children[0].children.push(node("div", [["data-separator", ""]], [], "gsep"));
  const pre = node("pre", [["data-diff-type", split ? "split" : "single"]], codes, "pre");
  return { tree: [pre, ...light], slots };
}

function lineIndexTable(rows, split) {
  const table = {};
  const place = (row) => {
    const unified = rows.indexOf(row);
    const delPos = rows.filter((r) => r.type !== "add").indexOf(row);
    const addPos = rows.filter((r) => r.type !== "del").indexOf(row);
    return { unified, delPos, addPos };
  };
  // Every line number 1..8 resolves on every side (the nearest row when the
  // side has no such line), except line 8 on `undefined`, which has no row:
  // that keeps the "No valid rowRange" path reachable.
  for (let lineNumber = 1; lineNumber <= 8; lineNumber++) {
    for (const side of ["additions", "deletions", "undefined"]) {
      const key = side === "deletions" ? "del" : "add";
      let row = rows.find((candidate) => candidate[key] === lineNumber);
      if (row === undefined) row = rows.find((candidate) => candidate.add === lineNumber || candidate.del === lineNumber);
      if (row === undefined) {
        if (lineNumber === 8) continue;
        row = rows.at(-1);
      }
      const { unified, delPos, addPos } = place(row);
      const splitIndex = side === "deletions" ? delPos : addPos;
      table[`${side}:${lineNumber}`] = [unified, split ? Math.max(splitIndex, 0) : unified];
    }
  }
  return table;
}

export const DIFF_ROWS = [
  { type: "context", del: 1, add: 1 },
  { type: "context", del: 2, add: 2, annotation: true },
  { type: "del", del: 3 },
  { type: "add", add: 3 },
  { type: "add", add: 4 },
  { type: "context", del: 4, add: 5 },
  { type: "context", del: 5, add: 6 },
];

// ---- scenarios -------------------------------------------------------------------

function ev(type, target, extra = {}) {
  return { op: "event", type, target, ...extra };
}

function handwritten() {
  const scenarios = [];
  const add = (name, mode, world, options, steps) =>
    scenarios.push({ name, mode, world, options, steps });
  const file = fileFixture(8, [3]);
  const unified = diffFixture(false, DIFF_ROWS);
  const split = diffFixture(true, DIFF_ROWS);
  unified.hits = {};
  const all = {
    lineHoverHighlight: "both",
    enableGutterUtility: true,
    enableLineSelection: true,
    onLineClick: true,
    onLineNumberClick: true,
    onLineEnter: true,
    onLineLeave: true,
    onLineSelected: true,
    onLineSelectionStart: true,
    onLineSelectionChange: true,
    onLineSelectionEnd: true,
    onHunkExpand: true,
  };

  add("file: hover, enter, leave, utility follows hover", "file", file, all, [
    { op: "setup", pre: "pre" },
    ev("pointermove", "c2"),
    ev("pointermove", "tok2"),
    ev("pointermove", "c3"),
    ev("pointermove", "g4"),
    ev("pointermove", "c5", { pointerType: "touch" }),
    { op: "getHoveredLine" },
    ev("pointerleave", "pre"),
    { op: "getHoveredLine" },
    { op: "cleanUp" },
  ]);
  add("file: clicks on lines and numbers", "file", file, all, [
    { op: "setup", pre: "pre" },
    ev("click", "c2"),
    ev("click", "g2"),
    ev("click", "tok4"),
    ev("click", "gnum4"),
    ev("click", "gutter"),
    { op: "cleanUp" },
  ]);
  add("file: line selection by number, shift extend, drag, release", "file", file, all, [
    { op: "setup", pre: "pre" },
    ev("pointerdown", "g2", { pointerId: 7 }),
    ev("document:pointermove", "c4", { pointerId: 7 }),
    ev("document:pointermove", "c5", { pointerId: 8 }),
    ev("document:pointerup", "c5", { pointerId: 7 }),
    { op: "frames", limit: 5, time: 0 },
    { op: "getSelection" },
    ev("pointerdown", "g7", { pointerId: 2, shiftKey: true }),
    ev("document:pointerup", "g7", { pointerId: 2 }),
    { op: "frames", limit: 5, time: 100 },
    { op: "getSelection" },
    ev("pointerdown", "g1", { pointerId: 3, shiftKey: true }),
    ev("document:pointercancel", "g1", { pointerId: 3 }),
    { op: "getSelection" },
    { op: "cleanUp" },
  ]);
  add("file: single line then click again unselects, drag turns into a select", "file", file, all, [
    { op: "setup", pre: "pre" },
    ev("pointerdown", "g2", { pointerId: 1 }),
    ev("document:pointerup", "g2", { pointerId: 1 }),
    { op: "frames", limit: 3, time: 0 },
    ev("pointerdown", "g2", { pointerId: 1 }),
    ev("document:pointerup", "g2", { pointerId: 1 }),
    { op: "frames", limit: 3, time: 50 },
    ev("pointerdown", "g2", { pointerId: 1 }),
    ev("pointerdown", "g2", { pointerId: 1 }),
    ev("document:pointerup", "g2", { pointerId: 1 }),
    ev("pointerdown", "g2", { pointerId: 4 }),
    ev("document:pointermove", "g2", { pointerId: 4 }),
    ev("document:pointermove", "g3", { pointerId: 4 }),
    ev("document:pointerup", "g3", { pointerId: 4 }),
    { op: "frames", limit: 3, time: 100 },
    { op: "cleanUp" },
  ]);
  add("file: touch tap on a gutter row reveals the utility, content drag does not", "file", file, all, [
    { op: "setup", pre: "pre" },
    ev("pointerdown", "c3", { pointerType: "touch" }),
    ev("document:pointerup", "c3", { pointerType: "touch" }),
    ev("pointerdown", "g5", { pointerType: "touch", pointerId: 9 }),
    ev("document:pointerup", "g5", { pointerType: "touch", pointerId: 9 }),
    ev("pointerdown", "c6", { pointerType: "mouse", button: 2 }),
    ev("pointerdown", "g6", { pointerType: "pen", pointerId: 3 }),
    ev("document:pointerup", "g6", { pointerType: "pen", pointerId: 3 }),
    { op: "frames", limit: 3, time: 0 },
    { op: "cleanUp" },
  ]);
  add("file: set selection with notify, style and dirty paths", "file", file, all, [
    { op: "setup", pre: "pre" },
    { op: "setSelection", range: { start: 2, end: 5 } },
    { op: "setSelection", range: { start: 2, end: 5 } },
    { op: "setSelection", range: { start: 2, end: 5 }, options: { lineNumberOnly: true } },
    { op: "setSelection", range: { start: 5, end: 2 }, options: { notify: false } },
    { op: "setSelection", range: { start: 3, end: 3 } },
    { op: "setSelection", range: null },
    { op: "setSelectionDirty" },
    { op: "isSelectionDirty" },
    { op: "setSelection", range: null },
    { op: "isSelectionDirty" },
    { op: "setSelection", range: { start: 1, end: 8 } },
    { op: "cleanUp" },
    { op: "setSelection", range: { start: 1, end: 2 } },
  ]);
  add("file: annotation rows inside a selection", "file", file, all, [
    { op: "setup", pre: "pre" },
    { op: "setSelection", range: { start: 2, end: 4 } },
    { op: "setSelection", range: { start: 3, end: 3 } },
    { op: "setSelection", range: { start: 3, end: 5 } },
    { op: "setSelection", range: { start: 1, end: 3 } },
    { op: "setSelection", range: { start: 3, end: 3 }, options: { lineNumberOnly: true } },
    { op: "cleanUp" },
  ]);
  add("file: touch drag follows coordinates, annotations and slotted content", "file", { ...file, hits: { "10,10": "c5", "11,11": "tok6", "12,12": "a3", "13,13": "lc3", "14,14": "gbuf3", "15,15": "pre", "16,16": null } }, all, [
    { op: "setup", pre: "pre" },
    ev("pointerdown", "g2", { pointerType: "touch", pointerId: 5 }),
    ev("document:pointermove", "g2", { pointerType: "touch", pointerId: 5, clientX: 10, clientY: 10 }),
    ev("document:pointermove", "g2", { pointerType: "touch", pointerId: 5, clientX: 11, clientY: 11 }),
    ev("document:pointermove", "g2", { pointerType: "touch", pointerId: 5, clientX: 12, clientY: 12 }),
    ev("document:pointermove", "g2", { pointerType: "touch", pointerId: 5, clientX: 13, clientY: 13 }),
    ev("document:pointermove", "g2", { pointerType: "touch", pointerId: 5, clientX: 14, clientY: 14 }),
    ev("document:pointermove", "g2", { pointerType: "touch", pointerId: 5, clientX: 15, clientY: 15 }),
    ev("document:pointermove", "g8", { pointerType: "touch", pointerId: 5, clientX: 16, clientY: 16 }),
    ev("document:pointermove", "g8", { pointerType: "touch", pointerId: 5 }),
    ev("document:pointerup", "g8", { pointerType: "touch", pointerId: 5, clientX: 10, clientY: 10 }),
    { op: "frames", limit: 3, time: 0 },
    { op: "cleanUp" },
  ]);
  add("file: drag falls back to the event path without hit testing", "file", { ...file, hitTestAvailable: false }, all, [
    { op: "setup", pre: "pre" },
    ev("pointerdown", "g2", { pointerId: 5 }),
    ev("document:pointermove", "c6", { pointerId: 5, clientX: 1, clientY: 1 }),
    ev("document:pointerup", "c6", { pointerId: 5, clientX: 1, clientY: 1 }),
    { op: "frames", limit: 3, time: 0 },
    { op: "cleanUp" },
  ]);
  add("file: gutter utility drag selects and notifies with line selection off", "file", file, {
    enableGutterUtility: true,
    onGutterUtilityClick: true,
    onLineSelected: true,
    onLineSelectionStart: true,
    onLineSelectionChange: true,
    onLineSelectionEnd: true,
  }, [
    { op: "setup", pre: "pre" },
    ev("pointermove", "c4"),
    ev("pointerdown", "$button"),
    ev("pointerdown", "g4", { pointerId: 3 }),
    ev("document:pointerup", "g4", { pointerId: 3 }),
    { op: "frames", limit: 3, time: 0 },
    { op: "cleanUp" },
  ]);
  add("file: custom gutter utility slot", "file", file, {
    enableGutterUtility: true,
    usesCustomGutterUtility: true,
    hasRenderGutterUtility: true,
    onLineClick: true,
  }, [
    { op: "setup", pre: "pre" },
    ev("pointermove", "c2"),
    ev("pointermove", "c3"),
    { op: "setOptions", options: { enableGutterUtility: true, usesCustomGutterUtility: false, onLineClick: true } },
    { op: "setup", pre: "pre" },
    ev("pointermove", "c4"),
    { op: "setOptions", options: { onLineClick: true } },
    { op: "setup", pre: "pre" },
    { op: "setOptions", options: {} },
    { op: "setup", pre: "pre" },
    { op: "cleanUp" },
  ]);
  add("both gutter utility APIs throw", "file", file, {
    enableGutterUtility: true,
    onGutterUtilityClick: true,
    hasRenderGutterUtility: true,
  }, [{ op: "setup", pre: "pre" }]);

  const table = lineIndexTable(DIFF_ROWS, false);
  const splitTable = lineIndexTable(DIFF_ROWS, true);
  add("diff unified: hover, side and line type, hunk expand clicks", "diff", { ...unified, hits: {} }, { ...all, lineIndex: table }, [
    { op: "setup", pre: "pre" },
    ev("pointermove", "c-u3"),
    ev("pointermove", "tok-u4"),
    ev("pointermove", "g-u4"),
    { op: "getHoveredLine" },
    ev("click", "c-u3"),
    ev("click", "g-u4"),
    ev("click", "sep-up"),
    ev("click", "sep-up", { shiftKey: true }),
    ev("click", "sep-all"),
    ev("click", "sep-lines"),
    ev("click", "sep"),
    ev("pointerleave", "pre"),
    { op: "cleanUp" },
  ]);
  add("diff unified: selection across change rows", "diff", { ...unified, hits: {} }, { ...all, lineIndex: table }, [
    { op: "setup", pre: "pre" },
    ev("pointerdown", "g-u2", { pointerId: 1 }),
    ev("document:pointermove", "g-u3", { pointerId: 1 }),
    ev("document:pointermove", "g-u4", { pointerId: 1 }),
    ev("document:pointerup", "g-u4", { pointerId: 1 }),
    { op: "frames", limit: 3, time: 0 },
    { op: "getSelection" },
    { op: "setSelection", range: { start: 2, end: 4, side: "deletions", endSide: "additions" } },
    { op: "setSelection", range: { start: 1, end: 6, side: "additions" }, options: { activeLineSide: "additions" } },
    { op: "cleanUp" },
  ]);
  add("diff split: columns, sides, row indexes", "diff", { ...split, hits: {} }, { ...all, lineIndex: splitTable }, [
    { op: "setup", pre: "pre" },
    ev("pointermove", "c-d3"),
    ev("pointermove", "c-a3"),
    { op: "getHoveredLine" },
    ev("pointerdown", "g-d3", { pointerId: 1 }),
    ev("document:pointermove", "g-a4", { pointerId: 1 }),
    ev("document:pointerup", "g-a4", { pointerId: 1 }),
    { op: "frames", limit: 3, time: 0 },
    ev("pointerdown", "g-a5", { pointerId: 2, shiftKey: true }),
    ev("document:pointerup", "g-a5", { pointerId: 2 }),
    { op: "frames", limit: 3, time: 50 },
    { op: "setSelection", range: { start: 3, end: 4, side: "additions" }, options: { activeLineSide: "additions" } },
    { op: "setSelection", range: { start: 3, end: 3, side: "deletions" }, options: { activeLineSide: "deletions" } },
    { op: "setSelection", range: { start: 4, end: 5, side: "deletions" } },
    // The same range with only the highlighted side changed is a style change.
    { op: "setSelection", range: { start: 4, end: 5, side: "deletions" }, options: { activeLineSide: "deletions" } },
    { op: "setSelection", range: { start: 4, end: 5, side: "deletions" }, options: { activeLineSide: "additions" } },
    { op: "setSelection", range: { start: 4, end: 5, side: "deletions" }, options: { activeLineSide: "additions" } },
    { op: "cleanUp" },
  ]);
  add("diff split: controlled selection holds a proposal until committed", "diff", { ...split, hits: {} }, { ...all, controlledSelection: true, lineIndex: splitTable }, [
    { op: "setup", pre: "pre" },
    ev("pointerdown", "g-a3", { pointerId: 1 }),
    ev("document:pointermove", "g-a5", { pointerId: 1 }),
    ev("document:pointerup", "g-a5", { pointerId: 1 }),
    { op: "frames", limit: 3, time: 0 },
    { op: "getSelection" },
    { op: "setSelection", range: { start: 3, end: 5, side: "additions" } },
    { op: "cleanUp" },
  ]);
  add("render throws on an unknown row, retried after the DOM is fixed", "file", file, all, [
    { op: "setup", pre: "pre" },
    { op: "setSelection", range: { start: 50, end: 60 } },
    { op: "setSelection", range: { start: 2, end: 3 } },
    { op: "attr", target: "gutter", name: "data-removed", value: "1" },
    { op: "setSelectionDirty" },
    { op: "setSelection", range: { start: 2, end: 3 } },
  ]);
  return scenarios;
}

function fuzz(random, count) {
  const scenarios = [];
  for (let index = 0; index < count; index++) {
    const kind = random.pick(["file", "unified", "split"]);
    const annotated = [2, 4, 6].filter(() => random.next() < 0.5);
    const world =
      kind === "file"
        ? fileFixture(8, annotated)
        : diffFixture(kind === "split", DIFF_ROWS);
    const table =
      kind === "file" ? null : lineIndexTable(DIFF_ROWS, kind === "split");
    const names = [];
    const collect = (spec) => {
      names.push(spec.name);
      spec.children.forEach(collect);
    };
    world.tree.forEach(collect);
    const gutterNames = names.filter((name) => /^g/.test(name));
    const hits = {};
    for (let n = 0; n < 6; n++) hits[`${n},${n}`] = random.pick([...names, null]);
    world.hits = hits;
    if (random.next() < 0.15) world.hitTestAvailable = false;
    const optionSpec = () => {
      const spec = {};
      if (random.next() < 0.6) spec.lineHoverHighlight = random.pick(["disabled", "both", "number", "line"]);
      for (const flag of [
        "enableGutterUtility",
        "enableLineSelection",
        "controlledSelection",
        "onLineClick",
        "onLineNumberClick",
        "onLineEnter",
        "onLineLeave",
        "onLineSelected",
        "onLineSelectionStart",
        "onLineSelectionChange",
        "onLineSelectionEnd",
        "onHunkExpand",
      ]) {
        if (random.next() < 0.55) spec[flag] = true;
      }
      if (random.next() < 0.3) spec.onGutterUtilityClick = true;
      if (spec.enableGutterUtility && !spec.onGutterUtilityClick && random.next() < 0.3) {
        spec.hasRenderGutterUtility = true;
        spec.usesCustomGutterUtility = true;
      }
      if (table) spec.lineIndex = table;
      return spec;
    };
    const steps = [{ op: "setup", pre: "pre" }];
    const pointerTypes = ["mouse", "mouse", "touch", "pen"];
    const stepCount = 10 + random.int(30);
    for (let step = 0; step < stepCount; step++) {
      const roll = random.next();
      const target = random.pick(names);
      const common = {
        pointerType: random.pick(pointerTypes),
        pointerId: random.pick([1, 2]),
        button: random.pick([0, 0, 0, 2]),
        shiftKey: random.next() < 0.25,
        ...(random.next() < 0.4
          ? { clientX: random.int(6), clientY: random.int(6) }
          : {}),
      };
      if (roll < 0.25) {
        steps.push(ev("pointermove", target, common));
      } else if (roll < 0.35) {
        steps.push(ev("click", target, common));
      } else if (roll < 0.5) {
        steps.push(ev("pointerdown", random.next() < 0.7 ? random.pick(gutterNames) : target, common));
      } else if (roll < 0.64) {
        steps.push(ev(random.pick(["document:pointermove", "document:pointerup", "document:pointercancel", "document:pointermove"]), target, common));
      } else if (roll < 0.68) {
        steps.push(ev("pointerleave", "pre", common));
      } else if (roll < 0.78) {
        const start = 1 + random.int(8);
        steps.push({
          op: "setSelection",
          range: random.next() < 0.15 ? null : { start, end: 1 + random.int(8), ...(table && random.next() < 0.5 ? { side: random.pick(["additions", "deletions"]) } : {}) },
          options: random.next() < 0.4 ? { notify: random.pick([true, false]), lineNumberOnly: random.next() < 0.3, ...(random.next() < 0.3 ? { activeLineSide: random.pick(["additions", "deletions"]) } : {}) } : undefined,
        });
      } else if (roll < 0.84) {
        steps.push({ op: "frames", limit: 1 + random.int(4), time: step * 16 });
      } else if (roll < 0.88) {
        steps.push({ op: "setOptions", options: optionSpec() });
        steps.push({ op: "setup", pre: "pre" });
      } else if (roll < 0.92) {
        steps.push(random.pick([{ op: "getSelection" }, { op: "getHoveredLine" }, { op: "isSelectionDirty" }, { op: "setSelectionDirty" }]));
      } else if (roll < 0.96) {
        steps.push({ op: "cleanUp" });
      } else {
        steps.push({ op: "setup", pre: "pre" });
      }
    }
    steps.push({ op: "frames", limit: 10, time: 9000 }, { op: "cleanUp" });
    scenarios.push({
      name: `fuzz ${kind} ${index}`,
      mode: kind === "file" ? "file" : "diff",
      world,
      options: optionSpec(),
      steps,
    });
  }
  return scenarios;
}

export async function buildS11bInteraction(oracle, random) {
  const { InteractionManager, pluckInteractionOptions } = await import(
    oracle.pierre("managers/InteractionManager.js")
  );
  const { clearRenderQueue } = await import(
    oracle.pierre("managers/UniversalRenderingManager.js")
  );
  const { areSelectionsEqual } = await import(
    oracle.pierre("utils/areSelectionsEqual.js")
  );
  const { areSelectionPointsEqual } = await import(
    oracle.pierre("utils/areSelectionPointsEqual.js")
  );
  const { isGutterUtilityPath } = await import(
    oracle.pierre("utils/isGutterUtilityPath.js")
  );
  const { createGutterUtilityElement } = await import(
    oracle.pierre("utils/createGutterUtilityElement.js")
  );
  const api = { InteractionManager, pluckInteractionOptions, clearRenderQueue };
  const scenarios = [...handwritten(), ...fuzz(random, 140)];
  const cases = scenarios.map((scenario) => {
    const plain = JSON.parse(JSON.stringify(scenario));
    return { scenario: plain, trace: runScenario(api, plain) };
  });
  clearRenderQueue();

  const pick = random.pick;
  const side = () => pick([undefined, "additions", "deletions"]);
  const selection = () => {
    const value = { start: pick([1, 2, 3]), end: pick([1, 2, 3]) };
    const s1 = side();
    const s2 = side();
    if (s1 !== undefined) value.side = s1;
    if (s2 !== undefined) value.endSide = s2;
    return value;
  };
  const selections = Array.from({ length: 200 }, () => {
    const left = random.next() < 0.1 ? undefined : selection();
    const right = random.next() < 0.1 ? undefined : random.next() < 0.5 ? clone(left) : selection();
    return {
      left: left ?? null,
      right: right ?? null,
      expected: areSelectionsEqual(left, right),
    };
  });
  const points = Array.from({ length: 80 }, () => {
    const left = { lineNumber: pick([1, 2]), side: side() };
    const right = random.next() < 0.5 ? { ...left } : { lineNumber: pick([1, 2]), side: side() };
    return {
      left: { lineNumber: left.lineNumber, side: left.side ?? null },
      right: { lineNumber: right.lineNumber, side: right.side ?? null },
      expected: areSelectionPointsEqual(left, right),
    };
  });
  // Paths are attribute lists; non-elements in the path are skipped upstream
  // and are not part of the Rust model.
  const attrPool = [
    ["data-utility-button", ""],
    ["data-gutter-utility-slot", ""],
    ["slot", "gutter-utility-slot"],
    ["slot", "annotation-3"],
    ["name", "gutter-utility-slot"],
    ["name", "annotation-3"],
    ["data-line", "1"],
  ];
  const paths = Array.from({ length: 120 }, () => {
    const path = Array.from({ length: random.int(4) }, () => {
      const attrs = [];
      for (const entry of attrPool) {
        if (random.next() < 0.18 && !attrs.some(([key]) => key === entry[0])) attrs.push(entry);
      }
      return attrs;
    });
    const elements = path.map((attrs) => {
      const node = new FakeNode("div");
      node.internal = true;
      node.attrs = attrs.map((entry) => [...entry]);
      return node;
    });
    const saved = Object.getOwnPropertyDescriptor(globalThis, "HTMLElement");
    Object.defineProperty(globalThis, "HTMLElement", { configurable: true, writable: true, value: FakeHTMLElement });
    try {
      return { path, expected: isGutterUtilityPath([...elements, { document: true }]) };
    } finally {
      if (saved === undefined) delete globalThis.HTMLElement;
      else Object.defineProperty(globalThis, "HTMLElement", saved);
    }
  });
  return {
    cases,
    selections,
    points,
    paths,
    utilityButton: JSON.parse(JSON.stringify(createGutterUtilityElement())),
  };
}
