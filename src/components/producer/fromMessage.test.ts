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

  const base: BrowsedMessage = { topic: "t", partition: 0, offset: 0, timestamp: null, key: null, value: d("string", "v"), headers: [], size: 1 };

  it("заголовки: граничные hex-значения", () => {
    const h = (value: string) => browsedToMessage({ ...base, headers: [{ name: "h", value }] }).headers[0];
    expect(h("0x")).toEqual({ name: "h", value: "0x", format: "string" });
    expect(h("0xabc")).toEqual({ name: "h", value: "0xabc", format: "string" });
    expect(h("0xABCD")).toEqual({ name: "h", value: "ABCD", format: "hex" });
    expect(h("0xff00")).toEqual({ name: "h", value: "ff00", format: "hex" });
  });

  it("потеряные при декодировании данные и невалидный JSON", () => {
    expect(() => browsedToMessage({ ...base, value: d("string", "a\uFFFDb") })).toThrow("с потерями");
    expect(() => browsedToMessage({ ...base, key: d("json", "\uFFFD") })).toThrow("с потерями");
    expect(browsedToMessage({ ...base, value: d("hex", "\uFFFD".length ? "ff" : "") }).value.format).toBe("hex");
    const bad = { ...d("json", "{oops"), error: "Невалидный JSON" };
    expect(browsedToMessage({ ...base, value: bad }).value).toEqual({ format: "string", text: "{oops" });
  });

  it("пустой ключ и base64", () => {
    const m = browsedToMessage({ ...base, key: d("string", ""), value: d("base64", "AAH/") });
    expect(m.key).toEqual({ format: "string", text: "" });
    expect(m.value).toEqual({ format: "base64", text: "AAH/" });
  });
});
