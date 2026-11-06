// Shared by every rung page (classic script, exposes `window.Ladder`): the one workload, and the
// in-page driver that the Playwright spec calls. The workload is the Pierre spike micro's
// (`src/micro_pierre_spike_scroll/lib.rs`): 20 px rows, a 200-row window quantized to 50-line
// chunks and centred on the viewport (`window()` there), rows keyed by line number, positioned by
// an absolutely placed window inside a content box of the full file height.
(function () {
  const ROW = 20;
  const CHUNK = 50;
  const OVERSCROLL = 1000;
  const TOTAL_LINES = 8000;
  const SCROLLER_HEIGHT = 600;
  const ROW_STYLE = "height:20px;line-height:20px;white-space:pre;font-family:monospace;font-size:12px";
  const GUTTER_STYLE = "display:inline-block;width:8ch;text-align:right;opacity:0.5";

  /** `window()` of the micro, profile `pierre`: `[first line, line count]`. */
  function windowFor(scrollTop, viewport, totalLines) {
    const wanted = Math.ceil((viewport + 2 * OVERSCROLL) / ROW);
    const total = Math.ceil(wanted / CHUNK) * CHUNK + CHUNK;
    if (totalLines <= CHUNK) return [0, totalLines];
    const center = Math.floor((scrollTop + viewport / 2) / ROW);
    const centerHunk = Math.floor(center / CHUNK);
    const ideal = centerHunk - Math.floor(total / CHUNK / 2);
    const hunksInFile = Math.ceil(totalLines / CHUNK);
    const start = Math.min(Math.max(ideal, 0), hunksInFile) * CHUNK;
    const clamped = ideal < 0 ? total + ideal * CHUNK : total;
    return [start, Math.min(clamped, Math.max(totalLines - start, 0))];
  }

  // `(line * 0x9E3779B97F4A7C15) >> 32` as 8 hex digits, per the micro's `line_text`.
  const MIXED = new Array(TOTAL_LINES);
  for (let i = 0; i < TOTAL_LINES; i++) {
    const v = (BigInt(i) * 0x9E3779B97F4A7C15n) & 0xFFFFFFFFFFFFFFFFn;
    MIXED[i] = (v >> 32n).toString(16).padStart(8, "0");
  }
  const lineText = (line) => `let v${line} = step(${line % 97}) ^ 0x${MIXED[line]};`;

  let TOKENS = null; // {rust: [[[text, color]]], ts: ...}
  async function loadTokens(url) {
    TOKENS = await (await fetch(url)).json();
  }

  /**
   * The spans of one row for a workload, as `[text, style|null]` pairs:
   * `syn7` / `syn18` are the micro's synthetic rows (source 0, no span styles),
   * `corpus` its raw corpus tokens (source 1: rust and ts lines alternating),
   * `corpus-rust` the rust tokens alone (what real Pierre renders for `rust-1k.rs`).
   */
  function rowSpans(workload, line) {
    if (workload === "syn7" || workload === "syn18") {
      const tokens = workload === "syn7" ? 7 : 18;
      const text = lineText(line);
      const out = new Array(tokens);
      for (let t = 0; t < tokens; t++) out[t] = [text.slice(t % 8) + " ", null];
      return out;
    }
    let runs;
    if (workload === "corpus") {
      const lang = TOKENS[line % 2 === 0 ? "rust" : "ts"];
      runs = lang[Math.floor(line / 2) % lang.length];
    } else {
      runs = TOKENS.rust[line % 1000];
    }
    const out = new Array(runs.length);
    for (let i = 0; i < runs.length; i++) {
      out[i] = [runs[i][0], `color:hsl(${(runs[i][1] * 37) % 360},60%,70%)`];
    }
    return out;
  }

  const nextFrame = () => new Promise((resolve) => requestAnimationFrame(resolve));
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const Ladder = {
    ROW, CHUNK, TOTAL_LINES, SCROLLER_HEIGHT, ROW_STYLE, GUTTER_STYLE,
    windowFor, lineText, loadTokens, rowSpans, nextFrame, sleep,
    longtasks: [],
    /** Set by the rung: `{scroller, windowStart(): number|null}`. */
    rung: null,

    /**
     * `n` window shifts of one chunk (scrollTop += 1000), one at a time: set scrollTop, wait for the
     * rendered window to move, then two frames so the following style / layout / paint of the
     * frame that carried the commit lands before the end mark. Each commit is bracketed by
     * `performance.mark` so the Chromium trace can be sliced per commit.
     */
    async commits(n, warm) {
      const { scroller, windowStart } = this.rung;
      const out = [];
      for (let i = 0; i < n + warm; i++) {
        const before = windowStart();
        performance.mark(`c${i}-start`);
        const t0 = performance.now();
        scroller.scrollTop += ROW * CHUNK;
        let t1 = null;
        const deadline = t0 + 3000;
        while (performance.now() < deadline) {
          await nextFrame();
          if (windowStart() !== before) { t1 = performance.now(); break; }
        }
        await nextFrame();
        await nextFrame();
        const t2 = performance.now();
        performance.mark(`c${i}-end`);
        if (i >= warm) out.push({ i, t0, t1, t2, start: windowStart() });
        if (t1 === null) break;
      }
      return out;
    },

    /**
     * Continuous scroll: `frames` animation frames, `px` per frame (a scrollTop write in each rAF
     * callback). Marks `f<i>` at every callback so the trace gives main-thread busy time per frame.
     */
    async scrollRun(frames, px, from) {
      const { scroller } = this.rung;
      scroller.scrollTop = from;
      await nextFrame();
      await nextFrame();
      const stamps = [];
      this.longtasks.length = 0;
      await new Promise((resolve) => {
        let i = 0;
        const tick = (now) => {
          performance.mark(`f${i}`);
          stamps.push(now);
          scroller.scrollTop += px;
          if (++i < frames) requestAnimationFrame(tick); else resolve();
        };
        requestAnimationFrame(tick);
      });
      await nextFrame();
      await nextFrame();
      performance.mark("f-end");
      const intervals = stamps.slice(1).map((v, i) => v - stamps[i]);
      return { intervals, longtasks: [...this.longtasks], endScrollTop: scroller.scrollTop };
    },

    /** Node and element counts below the scroller (`root` may be a shadow root). */
    census(root) {
      let nodes = 0, elements = 0;
      const walk = (n) => {
        nodes++;
        if (n.nodeType === 1) elements++;
        for (let c = n.firstChild; c; c = c.nextSibling) walk(c);
      };
      walk(root);
      return { nodes, elements };
    },

    /** Counts DOM mutation records during `fn` (run once, outside the timed passes). */
    async countMutations(root, fn) {
      let records = 0, added = 0, removed = 0, attrs = 0, chars = 0;
      const mo = new MutationObserver((list) => {
        for (const r of list) {
          records++;
          added += r.addedNodes.length;
          removed += r.removedNodes.length;
          if (r.type === "attributes") attrs++;
          if (r.type === "characterData") chars++;
        }
      });
      mo.observe(root, { subtree: true, childList: true, attributes: true, characterData: true });
      await fn();
      await nextFrame();
      await nextFrame();
      mo.disconnect();
      return { records, added, removed, attrs, chars };
    },
  };
  new PerformanceObserver((list) => {
    for (const e of list.getEntries()) Ladder.longtasks.push(e.duration);
  }).observe({ entryTypes: ["longtask"] });
  window.Ladder = Ladder;
})();
