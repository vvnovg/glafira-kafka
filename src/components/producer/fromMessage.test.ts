import { describe, expect, it } from "vitest";
import type { BrowsedMessage, DecodedData } from "../../api/types";
import { browsedToMessage } from "./fromMessage";

const d = (format: DecodedData["format"], text: string, truncated = false): DecodedData => ({ format, text, size: text.length, truncated });

describe("browsedToMessage", () => {
  it("сохраняет форматы и заголовки", () => {
    const m: BrowsedMessage = {
      topic: "orders",
      partition: 3,
      offset: 10,
      timestamp: 0,
      key: d("hex", "00ff"),
      value: d("json", '{"a":1}'),
      headers: [
        { name: "trace", value: "abc" },
        { name: "bin", value: "0xff00" },
        { name: "nil", value: null },
      ],
      size: 9,
    };
    expect(browsedToMessage(m)).toEqual({
      topic: "orders",
      partition: null,
      key: { format: "hex", text: "00ff" },
      value: { format: "json", text: '{"a":1}' },
      headers: [
        { name: "trace", value: "abc", format: "string" },
        { name: "bin", value: "ff00", format: "hex" },
        { name: "nil", value: "", format: "string" },
      ],
    });
  });

  it("null и усечённые значения", () => {
    const base: BrowsedMessage = { topic: "t", partition: 0, offset: 0, timestamp: null, key: null, value: d("string", "v"), headers: [], size: 1 };
    expect(browsedToMessage(base).key).toEqual({ format: "null", text: "" });
    expect(() => browsedToMessage({ ...base, value: d("string", "v", true) })).toThrow("больше 1 МБ");
  });
});
