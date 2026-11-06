// X4 spike (docs/scratch/pierre-spikes.md, "X4 receivers"): a receiver that applies a guest's
// presentation batch straight to the DOM, with no host-side retained tree and no host Dioxus diff.
// It is the Dioxus mutation stack machine of `clay_surface::SurfaceTree` (`tree/write.rs`) run on
// real nodes. `Receiver.apply` takes a whole batch as a flat `u32` op array plus one string blob
// (sledgehammer style); the helpers below the class are also used by the Rust receiver
// (`direct.rs`), which drives the DOM from wasm per op.
//
// What this prototype does NOT do (see the doc section): validation (tag / attribute / listener
// vocabulary, budgets, depth, stack balance), component slots (`mount-component`), volatile
// attribute semantics beyond value/checked/selected, event payloads other than scroll and
// click(mouse default), the stale-event check, a policy hook per element.

const NONE = 0xffffffff;

export function scrollJson(el) {
  return `{"scroll_top":${el.scrollTop},"scroll_left":${el.scrollLeft},"scroll_width":${el.scrollWidth},"scroll_height":${el.scrollHeight},"client_width":${el.clientWidth},"client_height":${el.clientHeight}}`;
}

// kind: 0 scroll, 1 scroll-by, 2 set-focus, 3 read client-rect, 4 read scroll-offset, 5 read scroll-size.
// Returns the JSON `value` of a read, "" for the rest.
export function runCommand(el, kind, a, b, smooth) {
  const behavior = smooth ? "smooth" : "instant";
  switch (kind) {
    case 0: el.scrollTo({ left: a, top: b, behavior }); return "";
    case 1: el.scrollBy({ left: a, top: b, behavior }); return "";
    case 2: if (a) el.focus(); else el.blur(); return "";
    case 3: { const r = el.getBoundingClientRect(); return `{"x":${r.x},"y":${r.y},"width":${r.width},"height":${r.height}}`; }
    case 4: return `{"x":${el.scrollLeft},"y":${el.scrollTop}}`;
    case 5: return `{"width":${el.scrollWidth},"height":${el.scrollHeight}}`;
  }
  throw new Error("unknown element command " + kind);
}

export function newRoot() {
  const root = document.createElement("div");
  root.style.display = "contents";
  return root;
}

export function attachRoot(containerId, root) {
  const host = document.getElementById(containerId);
  if (host === null) throw new Error("direct receiver container " + containerId + " is not in the document");
  host.appendChild(root);
}

export function queueMicrotaskFn(f) { queueMicrotask(f); }

export function setAttr(el, name, ns, kind, val) {
  // kind 0 text, 1 true (empty attribute), 2 remove.
  if (ns === "style") {
    if (kind === 2) el.style.removeProperty(name); else el.style.setProperty(name, kind === 1 ? "" : val);
    return;
  }
  if (ns !== null) {
    if (kind === 2) el.removeAttributeNS(ns, name); else el.setAttributeNS(ns, name, kind === 1 ? "" : val);
    return;
  }
  if (name === "value" || name === "checked" || name === "selected") {
    if (name === "value") el.value = kind === 2 ? "" : val; else el[name] = kind !== 2;
    return;
  }
  if (kind === 2) el.removeAttribute(name); else el.setAttribute(name, kind === 1 ? "" : val);
}

export function listenerOptions(name) {
  return name === "scroll" || name === "wheel" || name.startsWith("touch") ? { passive: true } : false;
}

export function eventJson(e, name, mouseJson) {
  switch (name) {
    case "scroll": return scrollJson(e.currentTarget);
    case "click": case "mousedown": case "mouseup": case "dblclick": return mouseJson;
  }
  throw new Error("the prototype receiver does not serialize `" + name + "` events");
}

export class Receiver {
  constructor(dispatch, mouseJson) {
    this.dispatch = dispatch; // (id, name, bubbles, json, revision) -> bit0 prevent-default, bit1 stop-propagation
    this.mouseJson = mouseJson;
    this.root = newRoot();
    this.nodes = [this.root];
    this.root.$id = 0;
    this.stack = [this.root];
    this.revision = 0;
    this.attached = false;
    this.pendingMounted = [];
    this.flushQueued = false;
  }

  fire(id, name, e) {
    const mask = this.dispatch(id, name, e.bubbles, eventJson(e, name, this.mouseJson), this.revision);
    if (mask & 1) e.preventDefault();
    if (mask & 2) e.stopPropagation();
  }

  flushMounted() {
    this.flushQueued = false;
    if (!this.attached) return;
    const ids = this.pendingMounted;
    this.pendingMounted = [];
    for (const id of ids) this.dispatch(id, "mounted", false, "null", this.revision);
  }

  attach(containerId) {
    attachRoot(containerId, this.root);
    this.attached = true;
    this.flushMounted();
  }

  clear() {
    this.root.replaceChildren();
    this.nodes.length = 1;
    this.stack.length = 1;
  }

  command(id, kind, a, b, smooth) {
    return runCommand(this.nodes[id], kind, a, b, smooth);
  }

  apply(ops, s) {
    // X4 native replay: `window.__clayCapture = true` keeps every batch (copied: `ops` is a view of
    // wasm memory) for the Blitz harness in bench/blitz-x4.
    if (globalThis.__clayCapture) (globalThis.__clayBatches ??= []).push([Array.from(ops), s]);
    const st = this.stack, nodes = this.nodes;
    const n = ops.length;
    let i = 0;
    const sub = () => { const o = ops[i++], l = ops[i++]; return s.substring(o, o + l); };
    const optSub = () => { const o = ops[i++], l = ops[i++]; return l === NONE ? null : s.substring(o, o + l); };
    const popN = (m) => st.splice(st.length - m, m);
    const fragment = (kids) => { const f = document.createDocumentFragment(); for (const k of kids) f.appendChild(k); return f; };
    while (i < n) {
      switch (ops[i++]) {
        case 0: st.push(nodes[ops[i++]]); break;
        case 1: { const id = ops[i++]; const t = st[st.length - 1]; nodes[id] = t; t.$id = id; break; }
        case 2: st[st.length - 1] = st[st.length - 1].childNodes[ops[i++]]; break;
        case 3: st.pop(); break;
        case 4: { const tag = sub(); const ns = optSub(); st.push(ns === null ? document.createElement(tag) : document.createElementNS(ns, tag)); break; }
        case 5: st.push(document.createTextNode(sub())); break;
        case 6: st[st.length - 1] = st[st.length - 1].cloneNode(true); break;
        case 7: { const m = ops[i++]; const kids = popN(m); const p = st[st.length - 1]; for (let k = 0; k < m; k++) p.appendChild(kids[k]); break; }
        case 8: { const m = ops[i++]; const kids = popN(m); const t = st.pop(); if (m === 1) t.replaceWith(kids[0]); else t.replaceWith(fragment(kids)); break; }
        case 9: { const m = ops[i++]; const kids = popN(m); const a = st[st.length - 1]; if (m === 1) a.after(kids[0]); else a.after(fragment(kids)); break; }
        case 10: { const m = ops[i++]; const kids = popN(m); const a = st[st.length - 1]; if (m === 1) a.before(kids[0]); else a.before(fragment(kids)); break; }
        case 11: { const name = sub(); const ns = optSub(); const kind = ops[i++]; const val = kind === 0 ? sub() : null; setAttr(st[st.length - 1], name, ns, kind, val); break; }
        case 12: { const t = st[st.length - 1]; const txt = sub(); if (t.nodeType === 3) t.data = txt; else t.textContent = txt; break; }
        case 13: {
          const name = sub(); const t = st[st.length - 1]; const id = t.$id;
          if (id === undefined) throw new Error("event listener target has no element id");
          if (name === "mounted") { this.pendingMounted.push(id); if (!this.flushQueued) { this.flushQueued = true; queueMicrotask(() => this.flushMounted()); } break; }
          const fn = (e) => this.fire(id, name, e);
          (t.$l ??= {})[name] = fn;
          t.addEventListener(name, fn, listenerOptions(name));
          break;
        }
        case 14: { const name = sub(); const t = st[st.length - 1]; const fn = t.$l?.[name]; if (fn) { t.removeEventListener(name, fn, listenerOptions(name)); delete t.$l[name]; } break; }
        case 15: st.pop().remove(); break;
        default: throw new Error("unknown op");
      }
    }
    this.revision++;
    return this.revision;
  }
}
