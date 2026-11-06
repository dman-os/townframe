// R1: hand-written vanilla JS. `?variant=dom` (createElement / createTextNode / appendChild) or
// `?variant=html` (an HTML string per added chunk, `insertAdjacentHTML`), or `?variant=static`
// (the control: all rows pre-rendered with the dom builder, no scroll handler). `?workload=` as in
// common.js. Each applyWindow records its build / attach split in `Ladder.r1Phases` (ms).
(async () => {
  const q = new URLSearchParams(location.search);
  const variant = q.get("variant") ?? "dom";
  const workload = q.get("workload") ?? "syn7";
  const { ROW, TOTAL_LINES, ROW_STYLE, GUTTER_STYLE } = Ladder;
  await Ladder.loadTokens("../corpus/tokens.json");
  const scroller = document.getElementById("scroller");
  const content = document.getElementById("content");
  const win = document.getElementById("window");
  content.style.height = `${TOTAL_LINES * ROW}px`;
  let start = -1, count = 0;
  const phases = [];
  let created = 0;

  const escape = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  function buildDom(line) {
    const div = document.createElement("div");
    div.setAttribute("style", ROW_STYLE);
    const gutter = document.createElement("span");
    gutter.setAttribute("style", GUTTER_STYLE);
    gutter.appendChild(document.createTextNode(String(line + 1)));
    div.appendChild(gutter);
    for (const [text, style] of Ladder.rowSpans(workload, line)) {
      const span = document.createElement("span");
      if (style !== null) span.setAttribute("style", style);
      span.appendChild(document.createTextNode(text));
      div.appendChild(span);
      created += 2;
    }
    created += 4;
    return div;
  }
  function buildHtml(line) {
    let html = `<div style="${ROW_STYLE}"><span style="${GUTTER_STYLE}">${line + 1}</span>`;
    for (const [text, style] of Ladder.rowSpans(workload, line)) {
      html += style === null ? `<span>${escape(text)}</span>` : `<span style="${style}">${escape(text)}</span>`;
    }
    return html + "</div>";
  }
  // Rows [from, to) as one detached unit: a fragment (dom) or a string (html).
  function build(from, to) {
    if (variant === "dom" || variant === "static") {
      const frag = document.createDocumentFragment();
      for (let l = from; l < to; l++) frag.appendChild(buildDom(l));
      return frag;
    }
    let html = "";
    for (let l = from; l < to; l++) html += buildHtml(l);
    return html;
  }
  function attach(unit, where) {
    if (variant === "dom") {
      if (where === "end") win.appendChild(unit); else win.insertBefore(unit, win.firstChild);
    } else {
      win.insertAdjacentHTML(where === "end" ? "beforeend" : "afterbegin", unit);
    }
  }
  function applyWindow(ns, nc) {
    const t0 = performance.now();
    let tBuild = 0, tAttach = 0;
    const timed = (f, kind) => {
      const t = performance.now();
      const r = f();
      const d = performance.now() - t;
      if (kind === "build") tBuild += d; else tAttach += d;
      return r;
    };
    const oldEnd = start + count, newEnd = ns + nc;
    if (start < 0 || ns >= oldEnd || newEnd <= start) {
      timed(() => { win.textContent = ""; }, "attach");
      const unit = timed(() => build(ns, newEnd), "build");
      timed(() => attach(unit, "end"), "attach");
    } else {
      // Rows leaving at the top, then at the bottom.
      timed(() => {
        for (let l = start; l < ns; l++) win.removeChild(win.firstChild);
        for (let l = newEnd; l < oldEnd; l++) win.removeChild(win.lastChild);
      }, "attach");
      if (ns < start) {
        const unit = timed(() => build(ns, start), "build");
        timed(() => attach(unit, "start"), "attach");
      }
      if (newEnd > oldEnd) {
        const unit = timed(() => build(oldEnd, newEnd), "build");
        timed(() => attach(unit, "end"), "attach");
      }
    }
    win.style.top = `${ns * ROW}px`;
    win.setAttribute("data-value", String(ns));
    win.setAttribute("data-state", String(nc));
    start = ns;
    count = nc;
    phases.push({ total: performance.now() - t0, build: tBuild, attach: tAttach });
  }
  if (variant === "static") {
    // Control: every row the run will scroll through is in the DOM from the start, and nothing
    // reacts to scroll. What a commit window costs here is the browser's own scroll + paint.
    const rows = Number(q.get("rows") ?? 4400);
    win.appendChild(build(0, rows));
    win.setAttribute("data-value", "0");
    const startOf = () => Ladder.windowFor(scroller.scrollTop, scroller.clientHeight, TOTAL_LINES)[0];
    Ladder.rung = {
      name: "r1-static", scroller, mutationRoot: scroller, censusRoot: win, windowStart: startOf,
    };
    window.__ready = true;
    return;
  }
  scroller.addEventListener("scroll", () => {
    const [ns, nc] = Ladder.windowFor(scroller.scrollTop, scroller.clientHeight, TOTAL_LINES);
    if (ns !== start || nc !== count) applyWindow(ns, nc);
  }, { passive: true });
  const [ns, nc] = Ladder.windowFor(0, scroller.clientHeight, TOTAL_LINES);
  applyWindow(ns, nc);
  Ladder.r1Phases = phases;
  Ladder.created = () => created;
  Ladder.rung = {
    name: `r1-${variant}`,
    scroller,
    mutationRoot: scroller,
    censusRoot: win,
    windowStart: () => Number(win.getAttribute("data-value")),
    phases: () => phases.splice(0),
  };
  document.title = "R1 ready";
  window.__ready = true;
})();
