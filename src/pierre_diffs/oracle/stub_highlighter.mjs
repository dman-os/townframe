// The deterministic stand-in highlighter the S5-S12 oracles share.

/**
 * The stand-in highlighter both sides use: one uncolored token per non-empty
 * line, fixed themes. Mirrors `PlainTextHighlighter` in highlighter_types.rs.
 * (The dist's own plain-text path still asks a highlighter for tokens, so the
 * real Shiki is never involved and results are deterministic.)
 */
export const stubHighlighter = {
  name: "plain-text-stub",
  getTheme(name) {
    const light = name.includes("light");
    return {
      name,
      type: light ? "light" : "dark",
      fg: light ? "#070707" : "#fbfbfb",
      bg: light ? "#ffffff" : "#0b0b0b",
      colors: {
        "terminal.ansiGreen": light ? "#0dbe4e" : "#5ecc71",
        "gitDecoration.deletedResourceForeground": light
          ? "#ff2e3f"
          : "#ff6762",
      },
    };
  },
  codeToTokens(code) {
    const tokens = [];
    const terminator = /\r\n|\r|\n/g;
    let position = 0;
    let offset = 0;
    for (;;) {
      const match = terminator.exec(code);
      const line = code.slice(position, match ? match.index : code.length);
      tokens.push(line === "" ? [] : [{ content: line, offset }]);
      offset += line.length + (match ? match[0].length : 0);
      if (!match) break;
      position = match.index + match[0].length;
    }
    return { tokens };
  },
};
