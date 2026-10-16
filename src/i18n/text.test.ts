import { describe, expect, it } from "vitest";
import { i18n } from "./index";
import { tr } from "./text";

describe("tr", () => {
  it("ключ из словаря, иначе запасной текст", async () => {
    i18n.addResource("ru", "translation", "errors.test.x", "Подключение «{{name}}»");
    expect(tr({ key: "errors.test.x", params: { name: "a" }, message: "Connection a" })).toBe("Подключение «a»");
    expect(tr({ key: "errors.unknown", message: "Fallback text" })).toBe("Fallback text");
    expect(tr({ key: "errors.generic", params: { message: "как есть" }, message: "как есть" })).toBe("как есть");
  });

  it("без ключа — text, затем message", () => {
    expect(tr({ text: "Plain", message: "other" })).toBe("Plain");
    expect(tr({ message: "only message" })).toBe("only message");
    expect(tr({})).toBe("");
  });
});
