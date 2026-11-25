import { CodeView, parsePatchFiles } from "@pierre/diffs";
const patch = await (await fetch("/pr84.diff")).text();
const files = parsePatchFiles(patch).flatMap((p) => p.files);
const root = document.getElementById("root");
const cv = new CodeView({ diffStyle: "unified", themes: { dark: "pierre-dark", light: "pierre-light" } });
cv.setup(root);
cv.setItems(files.map((f, i) => ({ id: String(i), type: "diff", fileDiff: f })));
window.__cv = cv; window.__files = files.length; window.__ready = true;
