import { describe, expect, it } from "vitest";
import { i18n } from "../i18n";
import { parseChapter, searchGuide } from "./parse";

const label = (key: string) => i18n.t(key);

const TEXT = `# Подключения

Вступление: откройте [[ui:sidebar.settings]].

## Простое подключение {#plain}

Укажите \`localhost:9092\`.

\`\`\`
## не заголовок {#nope}
\`\`\`

## Взаимный TLS {#mtls}

Нужен сертификат клиента, см. [[ui:sidebar.about]].
`;

describe("parseChapter", () => {
  it("splits sections with anchors and keeps the intro as id null", () => {
    const ch = parseChapter("connections", TEXT, label);
    expect(ch.id).toBe("connections");
    expect(ch.title).toBe("Подключения");
    expect(ch.sections.map((s) => s.id)).toEqual([null, "plain", "mtls"]);
    expect(ch.sections[0].title).toBe("Подключения");
    expect(ch.sections.map((s) => s.heading)).toEqual([false, true, true]);
    expect(ch.sections[1].title).toBe("Простое подключение");
    expect(ch.sections[1].markdown).toContain("## не заголовок {#nope}");
    expect(ch.sections[2].title).toBe("Взаимный TLS");
  });

  it("replaces ui labels with bold dictionary values", () => {
    const ch = parseChapter("connections", TEXT, label);
    expect(ch.sections[0].markdown).toContain("**Настройки**");
    expect(ch.sections[2].markdown).toContain("**О программе**");
    expect(ch.sections[0].markdown).not.toContain("[[ui:");
  });

  it("escapes markdown in label values", () => {
    const tricky = () => "<code>x</code> [a](b) *z* \\ # | ~ ! _u_";
    const ch = parseChapter("topics", "# T [[ui:k]]\n\nСм. [[ui:k]].\n", tricky);
    expect(ch.title).toBe("T <code>x</code> [a](b) *z* \\ # | ~ ! _u_");
    expect(ch.sections[0].markdown).toBe(
      "См. **\\<code\\>x\\</code\\> \\[a\\](b) \\*z\\* \\\\ \\# \\| \\~ \\! \\_u\\_**.",
    );
    const [hit] = searchGuide([ch], "[a](b) *z*");
    expect(hit.snippet).toBe("См. <code>x</code> [a](b) *z* \\ # | ~ ! _u_.");
  });

  it("keeps a heading without an anchor as a titled section", () => {
    const ch = parseChapter("topics", "# T\n\n## Без якоря\n\nТекст\n", label);
    expect(ch.sections).toEqual([{ id: null, title: "Без якоря", markdown: "Текст", heading: true }]);
  });

  it("omits an empty intro", () => {
    const ch = parseChapter("topics", "# Топики\n\n## Список {#list}\n\nТекст\n", label);
    expect(ch.sections.map((s) => s.id)).toEqual(["list"]);
  });
});

describe("searchGuide", () => {
  const chapters = [parseChapter("connections", TEXT, label)];

  it("finds a section case-insensitively with a snippet", () => {
    const res = searchGuide(chapters, "СЕРТИФИКАТ");
    expect(res).toHaveLength(1);
    expect(res[0]).toMatchObject({ chapter: "connections", section: "mtls", title: "Взаимный TLS" });
    expect(res[0].snippet.toLowerCase()).toContain("сертификат");
  });

  it("searches the text after label substitution", () => {
    const res = searchGuide(chapters, "настройки");
    expect(res.map((r) => r.section)).toEqual([null]);
    expect(res[0].snippet).toContain("Настройки");
    expect(res[0].snippet).not.toContain("**");
  });

  it("returns nothing for an empty or unknown query", () => {
    expect(searchGuide(chapters, "  ")).toEqual([]);
    expect(searchGuide(chapters, "zzzz")).toEqual([]);
  });
});
