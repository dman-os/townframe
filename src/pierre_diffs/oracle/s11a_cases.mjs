// Differential cases for S11a: the small utils on the FileDiff closure
// (equality predicates, option helpers, DOM-building utils, CSS wrappers,
// platform detection, the scrollbar probe), run through the pinned
// @pierre/diffs dist. DOM-building utils run against a minimal fake DOM whose
// serialization (attribute list in insertion order, `style` as `name:value;`
// declarations) is the shape the Rust hast `Properties` has.
//
// The managers (ResizeManager, ScrollSyncManager) are in
// `oracle/s11a_managers.mjs`.

export function clone(value) {
  // structuredClone keeps `Infinity` (the JSON round trip would turn it into null).
  return value === undefined ? undefined : structuredClone(value);
}

// ---- fake DOM -------------------------------------------------------------

export class FakeNode {}

export class FakeElement extends FakeNode {
  constructor(tagName) {
    super();
    this.tagName = tagName.toUpperCase();
    this.attrs = [];
    this.childNodes = [];
    this.parentNode = null;
    const element = this;
    this.style = {
      setProperty(name, value) {
        const declarations = element.#declarations();
        const found = declarations.find(([key]) => key === name);
        if (found) found[1] = value;
        else declarations.push([name, value]);
        element.#writeDeclarations(declarations);
      },
      removeProperty(name) {
        const declarations = element
          .#declarations()
          .filter(([key]) => key !== name);
        element.#writeDeclarations(declarations);
      },
    };
    // camelCase `style.x = v` assignments used by the utils.
    this.style = new Proxy(this.style, {
      set(target, property, value) {
        const kebab = String(property).replace(
          /[A-Z]/g,
          (letter) => `-${letter.toLowerCase()}`,
        );
        target.setProperty(kebab, value);
        return true;
      },
    });
    this.dataset = new Proxy(
      {},
      {
        set: (_, property, value) => {
          const name = `data-${String(property).replace(/[A-Z]/g, (letter) => `-${letter.toLowerCase()}`)}`;
          this.setAttribute(name, value);
          return true;
        },
      },
    );
  }
  #declarations() {
    const style = this.getAttribute("style");
    return (style ?? "")
      .split(";")
      .filter((declaration) => declaration !== "")
      .map((declaration) => {
        const index = declaration.indexOf(":");
        return [declaration.slice(0, index), declaration.slice(index + 1)];
      });
  }
  #writeDeclarations(declarations) {
    if (declarations.length === 0) this.removeAttribute("style");
    else
      this.setAttribute(
        "style",
        declarations.map(([key, value]) => `${key}:${value};`).join(""),
      );
  }
  set slot(value) {
    this.setAttribute("slot", value);
  }
  get slot() {
    return this.getAttribute("slot") ?? "";
  }
  set textContent(value) {
    this.childNodes = value === "" ? [] : [{ text: value }];
  }
  get textContent() {
    return this.childNodes.map((node) => node.text ?? "").join("");
  }
  getAttribute(name) {
    return this.attrs.find(([key]) => key === name)?.[1] ?? null;
  }
  setAttribute(name, value) {
    const found = this.attrs.find(([key]) => key === name);
    if (found) found[1] = String(value);
    else this.attrs.push([name, String(value)]);
  }
  removeAttribute(name) {
    this.attrs = this.attrs.filter(([key]) => key !== name);
  }
  appendChild(child) {
    this.childNodes.push(child);
    child.parentNode = this;
  }
  remove() {
    if (this.parentNode) {
      this.parentNode.childNodes = this.parentNode.childNodes.filter(
        (node) => node !== this,
      );
      this.parentNode = null;
    }
  }
  /** The shape the Rust side compares. */
  snapshot() {
    return {
      tag: this.tagName.toLowerCase(),
      attrs: this.attrs.map(([key, value]) => [key, value]),
      children: this.childNodes.map((node) =>
        node instanceof FakeElement ? node.snapshot() : { text: node.text },
      ),
    };
  }
}

function withDocument(callback) {
  const saved = Object.getOwnPropertyDescriptor(globalThis, "document");
  Object.defineProperty(globalThis, "document", {
    configurable: true,
    writable: true,
    value: { createElement: (tag) => new FakeElement(tag) },
  });
  try {
    return callback();
  } finally {
    if (saved === undefined) delete globalThis.document;
    else Object.defineProperty(globalThis, "document", saved);
  }
}

function fromAttrs(tag, attrs, children = []) {
  const element = new FakeElement(tag);
  for (const [key, value] of attrs) element.setAttribute(key, value);
  for (const child of children) element.appendChild(child);
  return element;
}

// ---- predicates -------------------------------------------------------------

function predicateCases(random, api) {
  const pick = random.pick;
  const mutateIf = (value, mutate) => (random.next() < 0.4 ? mutate(value) : value);
  // Replaces exactly one key of a copy with the same key of a fresh value, so
  // every field gets cases where it alone differs.
  const mutateOne = (value, fresh) => {
    if (value === undefined || random.next() >= 0.6) return value;
    const alternative = fresh();
    const key = pick(Object.keys({ ...value, ...alternative }));
    const copy = clone(value);
    if (alternative[key] === undefined) delete copy[key];
    else copy[key] = alternative[key];
    return copy;
  };

  const annotation = () => {
    const value = {
      side: pick(["additions", "deletions"]),
      lineNumber: pick([0, 1, 2, 10]),
    };
    const metadata = pick([undefined, "a", "b", 1, 2, true]);
    if (metadata !== undefined) value.metadata = metadata;
    return value;
  };
  const annotations = Array.from({ length: 160 }, () => {
    const left = annotation();
    const right = mutateOne(clone(left), annotation);
    return {
      left,
      right,
      expected: api.areDiffLineAnnotationsEqual(left, right),
    };
  });

  const file = () => {
    const value = {
      name: pick(["a.ts", "b.ts"]),
      contents: pick(["", "x", "y"]),
    };
    for (const key of ["lang", "cacheKey", "header"]) {
      const v = pick([undefined, "p", "q"]);
      if (v !== undefined) value[key] = v;
    }
    return value;
  };
  const files = Array.from({ length: 200 }, () => {
    const left = random.next() < 0.15 ? undefined : file();
    const right =
      random.next() < 0.15 ? undefined : mutateOne(clone(left), file);
    return {
      left: left ?? null,
      right: right ?? null,
      expected: api.areFilesEqual(left, right),
    };
  });

  const hunk = () => {
    const value = {
      slotName: pick(["s0", "s1"]),
      hunkIndex: pick([0, 1]),
      lines: pick([1, 2, 5]),
      lineCountKnown: random.next() < 0.5,
      type: pick(["unified", "additions", "deletions"]),
    };
    if (random.next() < 0.6) {
      value.expandable = {
        chunked: random.next() < 0.5,
        up: random.next() < 0.5,
        down: random.next() < 0.5,
      };
    }
    return value;
  };
  const hunks = Array.from({ length: 200 }, () => {
    const left = hunk();
    const right = mutateOne(clone(left), hunk);
    return { left, right, expected: api.areHunkDataEqual(left, right) };
  });

  const object = () => {
    const value = {};
    for (const key of ["a", "b", "c", "d"]) {
      if (random.next() < 0.6) value[key] = pick(["x", "y", 1, 2, true, false]);
    }
    return value;
  };
  const objects = Array.from({ length: 120 }, () => {
    const kind = random.int(10);
    const left = kind === 0 ? undefined : object();
    let right = kind === 1 ? undefined : mutateIf(clone(left), () => object());
    let same = false;
    if (kind === 2) {
      right = left;
      same = true;
    }
    const omit =
      random.next() < 0.5
        ? undefined
        : ["a", "b", "c", "d"].filter(() => random.next() < 0.4);
    return {
      left: left ?? null,
      right: same ? null : (right ?? null),
      same,
      omit: omit ?? [],
      expected: api.areObjectsEqual(left, right, omit),
    };
  });

  const pre = () => {
    const value = {
      type: pick(["diff", "file"]),
      diffIndicators: pick(["bars", "classic", "none"]),
      disableBackground: random.next() < 0.5,
      disableLineNumbers: random.next() < 0.5,
      overflow: pick(["scroll", "wrap"]),
      split: random.next() < 0.5,
      totalLines: pick([1, 9, 10, 1000]),
    };
    if (random.next() < 0.6) {
      value.customProperties = {};
      for (const key of ["data-x", "data-y", "data-z"]) {
        if (random.next() < 0.5) value.customProperties[key] = pick(["p", 1, 2.5]);
      }
    }
    return value;
  };
  const pres = Array.from({ length: 300 }, () => {
    const left = random.next() < 0.1 ? undefined : pre();
    const right =
      random.next() < 0.1 ? undefined : mutateOne(clone(left), pre);
    return {
      left: left ?? null,
      right: right ?? null,
      expected: api.arePrePropertiesEqual(left, right),
    };
  });

  const range = () => ({
    startingLine: pick([0, 1, 5]),
    totalLines: pick([Number.POSITIVE_INFINITY, 10, 20]),
    bufferBefore: pick([0, 3]),
    bufferAfter: pick([0, 3]),
  });
  const ranges = Array.from({ length: 200 }, () => {
    const left = random.next() < 0.1 ? undefined : range();
    const right =
      random.next() < 0.1 ? undefined : mutateOne(clone(left), range);
    return {
      left: left ?? null,
      right: right ?? null,
      expected: api.areRenderRangesEqual(left, right),
    };
  });
  const defaultRanges = [
    ...ranges.flatMap(({ left, right }) => [left, right]).filter(Boolean),
    {
      startingLine: 0,
      totalLines: Number.POSITIVE_INFINITY,
      bufferBefore: 0,
      bufferAfter: 0,
    },
  ].map((value) => {
    const restored = {
      ...value,
      totalLines: value.totalLines ?? Number.POSITIVE_INFINITY,
    };
    return { range: value, expected: api.isDefaultRenderRange(restored) };
  });

  const theme = () =>
    pick([
      undefined,
      "pierre-dark",
      "pierre-light",
      { dark: "pierre-dark", light: "pierre-light" },
      { dark: "pierre-light", light: "pierre-dark" },
    ]);
  const themes = Array.from({ length: 60 }, () => {
    const left = theme();
    const right = random.next() < 0.4 ? clone(left) : theme();
    return {
      left: left ?? null,
      right: right ?? null,
      expected: api.areThemesEqual(left, right),
    };
  });

  const specs = [undefined, { top: 0, bottom: 1 }, { top: 0, bottom: 2 }];
  const windowSpecs = [];
  for (const left of specs)
    for (const right of specs)
      windowSpecs.push({
        left: left ?? null,
        right: right ?? null,
        expected: api.areVirtualWindowSpecsEqual(left, right),
      });

  return {
    annotations,
    files,
    hunks,
    objects,
    pres,
    ranges,
    defaultRanges,
    themes,
    windowSpecs,
  };
}

// ---- options ----------------------------------------------------------------

function optionCases(random, api) {
  const tokenTransformer = [];
  for (const useTokenTransformer of [undefined, true, false]) {
    for (const click of [false, true])
      for (const enter of [false, true])
        for (const leave of [false, true]) {
          const options = { useTokenTransformer };
          if (click) options.onTokenClick = () => {};
          if (enter) options.onTokenEnter = () => {};
          if (leave) options.onTokenLeave = () => {};
          tokenTransformer.push({
            useTokenTransformer: useTokenTransformer ?? null,
            click,
            enter,
            leave,
            expected: api.shouldUseTokenTransformer(options),
          });
        }
  }
  const absent = { absent: true, expected: api.shouldUseTokenTransformer(undefined) };

  const fileFor = () => ({ name: "n.ts", contents: "c" });
  const diffFileInput = [];
  for (const old of ["undefined", "null", "file"]) {
    for (const neu of ["undefined", "null", "file"]) {
      const input = {};
      if (old === "null") input.oldFile = null;
      else if (old === "file") input.oldFile = fileFor();
      if (neu === "null") input.newFile = null;
      else if (neu === "file") input.newFile = fileFor();
      let expected;
      try {
        expected = { result: api.getDiffFileInput(input, "ctx") ?? null };
      } catch (error) {
        expected = { throws: error.message };
      }
      diffFileInput.push({ old, new: neu, expected });
    }
  }
  return { tokenTransformer: [...tokenTransformer, absent], diffFileInput };
}

// ---- DOM-building utils -------------------------------------------------------

function domCases(random, api) {
  const pick = random.pick;
  const created = withDocument(() => ({
    annotationWrappers: ["slot-a", "annotation-1,2", ""].map((slot) => ({
      slot,
      expected: api.createAnnotationWrapperNode(slot).snapshot(),
    })),
    gutterUtility: api.createGutterUtilityContentNode().snapshot(),
    unsafeStyle: api.createUnsafeCSSStyleNode().snapshot(),
    hostThemeStyle: api.createHostThemeStyleNode().snapshot(),
  }));

  const styleNodes = ["style", "STYLE", "Style", "div", "styles", "code", "pre"].map(
    (tag) => ({
      tag,
      expected: api.isStyleNode({ tagName: tag.toUpperCase(), nodeName: tag }),
    }),
  );

  // setPreNodeProperties over fresh and pre-populated `pre` elements.
  const preAttrsPool = [
    ["data-file", ""],
    ["data-diff", ""],
    ["data-indicators", "bars"],
    ["data-background", ""],
    ["data-overflow", "wrap"],
    ["data-diff-type", "single"],
    ["data-disable-line-numbers", ""],
    ["foo", "bar"],
    ["style", "color:red;--diffs-min-number-column-width-default:2ch;"],
    ["style", "color:red;"],
  ];
  const preNodes = Array.from({ length: 80 }, () => {
    const initial = [];
    for (const entry of preAttrsPool)
      if (random.next() < 0.3 && !initial.some(([key]) => key === entry[0]))
        initial.push([...entry]);
    const config = {
      type: pick(["diff", "file"]),
      diffIndicators: pick(["bars", "classic", "none"]),
      disableBackground: random.next() < 0.5,
      disableLineNumbers: random.next() < 0.5,
      overflow: pick(["scroll", "wrap"]),
      split: random.next() < 0.5,
      totalLines: pick([1, 9, 10, 99, 100, 12345]),
    };
    if (random.next() < 0.6) {
      config.customProperties = {};
      for (const key of ["data-a", "data-b", "foo", "style-ish"]) {
        const v = pick([undefined, "p", 3, 1.5]);
        if (random.next() < 0.5) config.customProperties[key] = v;
      }
    }
    // Rust applies custom properties in sorted key order.
    if (config.customProperties) {
      const sorted = {};
      for (const key of Object.keys(config.customProperties).sort())
        sorted[key] = config.customProperties[key];
      config.customProperties = sorted;
    }
    const pre = fromAttrs("pre", initial);
    api.setPreNodeProperties(pre, config);
    return { initial, config: clone(config), expected: pre.snapshot() };
  });

  const codeNodes = withDocument(() =>
    Array.from({ length: 60 }, () => {
      const existing = random.next() < 0.4;
      const initial = existing
        ? [
            ["data-code", ""],
            ...(random.next() < 0.5 ? [["data-unified", ""]] : []),
            ...(random.next() < 0.5 ? [["style", "grid-row:span 3;"]] : []),
            ...(random.next() < 0.5 ? [["data-container-size", ""]] : []),
          ]
        : null;
      const props = {
        columnType: pick([undefined, "additions", "deletions", "unified"]),
        rowSpan: pick([undefined, 1, 4]),
        containerSize: pick([undefined, true, false]),
      };
      const code = initial ? fromAttrs("code", initial) : undefined;
      const result = api.getOrCreateCodeNode({
        ...(code ? { code } : {}),
        ...props,
      });
      return {
        initial,
        columnType: props.columnType ?? null,
        rowSpan: props.rowSpan ?? null,
        containerSize: props.containerSize ?? false,
        expected: result.snapshot(),
      };
    }),
  );

  const hostTheme = withDocument(() => {
    const cases = [];
    for (const css of ["", "   ", "\n\t", "a{b:c}", " x ", "\u00a0", "\ufeff", "q"]) {
      for (const hasCurrent of [false, true]) {
        const shadowRoot = {
          childNodes: [],
          appendChild(node) {
            this.childNodes.push(node);
            node.parentNode = this;
          },
        };
        let current;
        if (hasCurrent) {
          current = api.createHostThemeStyleNode();
          current.textContent = "old";
          shadowRoot.appendChild(current);
        }
        const result = api.upsertHostThemeStyle({
          shadowRoot,
          currentNode: current,
          themeCSS: css,
        });
        cases.push({
          css,
          hasCurrent,
          expected: result ? result.snapshot() : null,
          attached: result ? result.parentNode === shadowRoot : false,
        });
      }
    }
    return cases;
  });

  return { created, styleNodes, preNodes, codeNodes, hostTheme };
}

// ---- CSS wrappers -------------------------------------------------------------

function cssCases(random, api) {
  const pick = random.pick;
  const css = () =>
    pick([
      "",
      "a{b:c}",
      "--diffs-scrollbar-gutter-measured: 3px;color:red;",
      "x --diffs-scrollbar-gutter-measured:;--diffs-scrollbar-gutter-measured : 5px; y",
      "--diffs-scrollbar-gutter-measured:   ;z",
      "--diffs-scrollbar-gutter-measured\n:\n12px;--diffs-scrollbar-gutter-measured:1px;",
      "--diffs-scrollbar-gutter-measured:noend",
      "é--diffs-scrollbar-gutter-measured:7px;é",
      ":host{--a:1}",
    ]);
  const gutter = () => pick([undefined, 0, 7, 12.5]);
  const wrapCore = ["", "a{b:c}", "x\ny"].map((value) => ({
    css: value,
    expected: api.wrapCoreCSS(value),
  }));
  const wrapUnsafe = ["", "a{b:c}", "x\ny"].map((value) => ({
    css: value,
    expected: api.wrapUnsafeCSS(value),
  }));
  const wrapTheme = [];
  for (const themeType of [undefined, "system", "light", "dark"])
    for (const g of [undefined, 0, 7, 12.5])
      wrapTheme.push({
        css: ":host{--x:1}",
        themeType: themeType ?? null,
        gutter: g ?? null,
        expected: api.wrapThemeCSS(":host{--x:1}", themeType, g),
      });
  const patch = Array.from({ length: 40 }, () => {
    const value = css();
    const g = gutter();
    return {
      css: value,
      gutter: g ?? null,
      expected: api.patchScrollbarGutterSize(value, g),
    };
  });
  const declarations = [undefined, 0, 4, 12.5].map((g) => ({
    gutter: g ?? null,
    expected: api.createMeasuredScrollbarGutterDeclaration(g),
  }));
  return { wrapCore, wrapUnsafe, wrapTheme, patch, declarations };
}

// ---- platform -----------------------------------------------------------------

async function platformCases(random, api) {
  const uas = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/121.0 Mobile/15E148 Safari/605.1.15",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Android 13; Mobile; rv:121.0) Gecko/121.0 Firefox/121.0",
    "xFirefox/1 Firefox/2",
    "_Firefox/1",
    "Firefox/9 SAFARI",
    "safari",
    "SAFARI at start",
    "line\nbreak Safari",
    "Chrome Safari",
    "Android Safari",
    "chrom safari",
    "",
    "é Safari ünïcode",
    "Version/17 Safari\r\nChrome",
  ];
  const platforms = [
    undefined,
    "MacIntel",
    "macOS",
    "macos",
    "Linux x86_64",
    "linux armv8l",
    "Win32",
    "iPhone",
    "iPad",
    "iPod touch",
    "Android",
  ];
  const savedNavigator = Object.getOwnPropertyDescriptor(globalThis, "navigator");
  const savedWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  const cases = [];
  try {
    for (let index = 0; index < 160; index++) {
      const info = {
        platform: random.pick(platforms),
        userAgentDataPlatform: random.pick([undefined, "macOS", "Linux", "Windows"]),
        userAgent: random.pick(uas),
        hasSafariPushNotification: random.next() < 0.1,
      };
      Object.defineProperty(globalThis, "navigator", {
        configurable: true,
        writable: true,
        value: {
          ...(info.platform !== undefined ? { platform: info.platform } : {}),
          ...(info.userAgentDataPlatform !== undefined
            ? { userAgentData: { platform: info.userAgentDataPlatform } }
            : {}),
          userAgent: info.userAgent,
        },
      });
      Object.defineProperty(globalThis, "window", {
        configurable: true,
        writable: true,
        value: info.hasSafariPushNotification
          ? { safari: { pushNotification: {} } }
          : {},
      });
      api.resetPlatformDetection();
      cases.push({
        info: {
          platform: info.platform ?? null,
          userAgentDataPlatform: info.userAgentDataPlatform ?? null,
          userAgent: info.userAgent,
          hasSafariPushNotification: info.hasSafariPushNotification,
        },
        isMacLike: api.isMacLike(),
        isLinux: api.isLinux(),
        isSafari: api.isSafari(),
        isFirefox: api.isFirefox(),
      });
    }
  } finally {
    for (const [key, saved] of [
      ["navigator", savedNavigator],
      ["window", savedWindow],
    ]) {
      if (saved === undefined) delete globalThis[key];
      else Object.defineProperty(globalThis, key, saved);
    }
  }
  return cases;
}

// ---- scrollbar probe ----------------------------------------------------------

function scrollbarCases(api) {
  // The module keeps one cached measurement per process, so this is one
  // ordered sequence: not connected, measured, then cached.
  const steps = [
    { connected: false, offsetHeight: 0, clientHeight: 0 },
    { connected: true, offsetHeight: 29, clientHeight: 12 },
    { connected: true, offsetHeight: 99, clientHeight: 1 },
    { connected: false, offsetHeight: 0, clientHeight: 0 },
  ];
  const savedHTMLElement = Object.getOwnPropertyDescriptor(globalThis, "HTMLElement");
  const savedDocument = Object.getOwnPropertyDescriptor(globalThis, "document");
  class FakeHTMLElement {}
  Object.defineProperty(globalThis, "HTMLElement", {
    configurable: true,
    writable: true,
    value: FakeHTMLElement,
  });
  Object.defineProperty(globalThis, "document", {
    configurable: true,
    writable: true,
    value: { createElement: (tag) => new FakeElement(tag) },
  });
  const results = [];
  try {
    for (const step of steps) {
      const host = new FakeHTMLElement();
      host.isConnected = step.connected;
      const probes = [];
      const shadowRoot = {
        host,
        appendChild(node) {
          probes.push(node.snapshot());
          Object.defineProperty(node, "offsetHeight", { value: step.offsetHeight });
          Object.defineProperty(node, "clientHeight", { value: step.clientHeight });
        },
      };
      const value = api.getMeasuredScrollbarGutter(shadowRoot);
      results.push({ ...step, value: value ?? null, probes });
    }
  } finally {
    for (const [key, saved] of [
      ["HTMLElement", savedHTMLElement],
      ["document", savedDocument],
    ]) {
      if (saved === undefined) delete globalThis[key];
      else Object.defineProperty(globalThis, key, saved);
    }
  }
  return results;
}

// ---- entry ----------------------------------------------------------------------

export async function buildS11aUtils(oracle, random) {
  const imp = (name) => import(oracle.pierre(`utils/${name}.js`));
  const [
    annotations,
    files,
    hunks,
    objects,
    pre,
    ranges,
    themes,
    windowSpecs,
    defaultRange,
    token,
    diffInput,
    wrapperNode,
    gutterNode,
    unsafeNode,
    styleNode,
    hostTheme,
    codeNode,
    wrapperProps,
    css,
    platform,
    scrollbar,
  ] = await Promise.all([
    imp("areDiffLineAnnotationsEqual"),
    imp("areFilesEqual"),
    imp("areHunkDataEqual"),
    imp("areObjectsEqual"),
    imp("arePrePropertiesEqual"),
    imp("areRenderRangesEqual"),
    imp("areThemesEqual"),
    imp("areVirtualWindowSpecsEqual"),
    imp("isDefaultRenderRange"),
    imp("shouldUseTokenTransformer"),
    imp("getDiffFileInput"),
    imp("createAnnotationWrapperNode"),
    imp("createGutterUtilityContentNode"),
    imp("createUnsafeCSSStyleNode"),
    imp("isStyleNode"),
    imp("hostTheme"),
    imp("getOrCreateCodeNode"),
    imp("setWrapperNodeProps"),
    imp("cssWrappers"),
    imp("platform"),
    imp("scrollbarGutter"),
  ]);
  const api = {
    areDiffLineAnnotationsEqual: annotations.areDiffLineAnnotationsEqual,
    areFilesEqual: files.areFilesEqual,
    areHunkDataEqual: hunks.areHunkDataEqual,
    areObjectsEqual: objects.areObjectsEqual,
    arePrePropertiesEqual: pre.arePrePropertiesEqual,
    areRenderRangesEqual: ranges.areRenderRangesEqual,
    areThemesEqual: themes.areThemesEqual,
    areVirtualWindowSpecsEqual: windowSpecs.areVirtualWindowSpecsEqual,
    isDefaultRenderRange: defaultRange.isDefaultRenderRange,
    shouldUseTokenTransformer: token.shouldUseTokenTransformer,
    getDiffFileInput: diffInput.getDiffFileInput,
    createAnnotationWrapperNode: wrapperNode.createAnnotationWrapperNode,
    createGutterUtilityContentNode: gutterNode.createGutterUtilityContentNode,
    createUnsafeCSSStyleNode: unsafeNode.createUnsafeCSSStyleNode,
    isStyleNode: styleNode.isStyleNode,
    createHostThemeStyleNode: hostTheme.createHostThemeStyleNode,
    upsertHostThemeStyle: hostTheme.upsertHostThemeStyle,
    getOrCreateCodeNode: codeNode.getOrCreateCodeNode,
    setPreNodeProperties: wrapperProps.setPreNodeProperties,
    wrapCoreCSS: css.wrapCoreCSS,
    wrapUnsafeCSS: css.wrapUnsafeCSS,
    wrapThemeCSS: css.wrapThemeCSS,
    patchScrollbarGutterSize: css.patchScrollbarGutterSize,
    createMeasuredScrollbarGutterDeclaration:
      scrollbar.createMeasuredScrollbarGutterDeclaration,
    getMeasuredScrollbarGutter: scrollbar.getMeasuredScrollbarGutter,
    resetPlatformDetection: platform.resetPlatformDetection,
    isMacLike: platform.isMacLike,
    isLinux: platform.isLinux,
    isSafari: platform.isSafari,
    isFirefox: platform.isFirefox,
  };
  return {
    predicates: predicateCases(random, api),
    options: optionCases(random, api),
    dom: domCases(random, api),
    css: cssCases(random, api),
    platform: await platformCases(random, api),
    scrollbar: scrollbarCases(api),
  };
}
