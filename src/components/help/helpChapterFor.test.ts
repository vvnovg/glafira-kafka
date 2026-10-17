import { describe, expect, it } from "vitest";
import type { TabSpec } from "../../state/workspace";
import { helpChapterFor } from "./helpChapterFor";

const cases: [TabSpec | null, string][] = [
  [{ kind: "connection-form", connectionId: null }, "connections"],
  [{ kind: "cluster", connectionId: "c" }, "topics"],
  [{ kind: "topics", connectionId: "c" }, "topics"],
  [{ kind: "topic", connectionId: "c", topic: "t" }, "topics"],
  [{ kind: "producer", connectionId: "c" }, "producer"],
  [{ kind: "groups", connectionId: "c" }, "groups"],
  [{ kind: "group", connectionId: "c", groupId: "g" }, "groups"],
  [{ kind: "consumer", connectionId: "c", key: "k" }, "consumer-sessions"],
  [{ kind: "license" }, "license-updates"],
  [{ kind: "about" }, "license-updates"],
  [{ kind: "settings" }, "getting-started"],
  [{ kind: "help", chapter: "groups" }, "getting-started"],
  [null, "getting-started"],
];

describe("helpChapterFor", () => {
  it.each(cases)("%j → %s", (tab, chapter) => {
    expect(helpChapterFor(tab)).toBe(chapter);
  });
});
