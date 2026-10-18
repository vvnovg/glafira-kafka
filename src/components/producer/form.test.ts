import { describe, expect, it } from "vitest";
import { DEFAULT_SETTINGS, emptyProducerForm, formToMessage, messageToForm } from "./form";

describe("producer form", () => {
  it("пустая форма", () => {
    const f = emptyProducerForm("orders");
    expect(f.topic).toBe("orders");
    expect(f.key).toEqual({ format: "null", text: "" });
    expect(f.value).toEqual({ format: "json", text: "" });
    expect(f.settings).toEqual(DEFAULT_SETTINGS);
  });

  it("форма → сообщение", () => {
    const f = { ...emptyProducerForm(" orders "), partition: " 2 ", value: { format: "json" as const, text: '{"a":1}' } };
    f.headers = [{ name: "trace", value: "x", format: "string" }, { name: " ", value: "", format: "string" }];
    expect(formToMessage(f)).toEqual({
      topic: "orders",
      partition: 2,
      key: { format: "null", text: "" },
      value: { format: "json", text: '{"a":1}' },
      headers: [{ name: "trace", value: "x", format: "string" }],
    });
    expect(formToMessage({ ...f, partition: "" }).partition).toBeNull();
  });

  it("ошибки", () => {
    expect(() => formToMessage(emptyProducerForm(""))).toThrow("Укажите топик");
    expect(() => formToMessage({ ...emptyProducerForm("t"), partition: "-1" })).toThrow("Партиция — целое число ≥ 0");
    expect(() => formToMessage({ ...emptyProducerForm("t"), partition: "x" })).toThrow("Партиция — целое число ≥ 0");
  });

  it("сообщение → форма", () => {
    const m = formToMessage({ ...emptyProducerForm("t"), partition: "1" });
    const f = messageToForm(m, { acks: "1", compression: "gzip", idempotence: false });
    expect(f.partition).toBe("1");
    expect(f.settings.compression).toBe("gzip");
    expect(messageToForm({ ...m, partition: null }).partition).toBe("");
  });
});
