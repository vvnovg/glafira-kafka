import { describe, expect, it } from "vitest";
import { formatProps, newProfile, parseProps, withProtocol } from "./profile";

describe("profile helpers", () => {
  it("новый профиль — PLAINTEXT localhost", () => {
    const p = newProfile();
    expect(p.id).toBe("");
    expect(p.security).toEqual({ protocol: "PLAINTEXT" });
    expect(p.bootstrapServers).toBe("localhost:9092");
  });

  it("withProtocol сохраняет уже введённые tls/sasl", () => {
    const sasl = withProtocol({ protocol: "PLAINTEXT" }, "SASL_SSL");
    expect(sasl).toEqual({
      protocol: "SASL_SSL",
      tls: { caLocation: null, verifyHostname: true },
      sasl: { mechanism: "PLAIN", username: "" },
    });
    const edited = { ...sasl, sasl: { mechanism: "SCRAM-SHA-512" as const, username: "bob" } };
    const plain = withProtocol(edited, "SASL_PLAINTEXT");
    expect(plain).toEqual({ protocol: "SASL_PLAINTEXT", sasl: { mechanism: "SCRAM-SHA-512", username: "bob" } });
    expect(withProtocol(plain, "PLAINTEXT")).toEqual({ protocol: "PLAINTEXT" });
  });

  it("parseProps / formatProps", () => {
    expect(parseProps("a=1\n\n  b.c = x=y \n# коммент")).toEqual({ a: "1", "b.c": "x=y" });
    expect(formatProps({ a: "1", b: "2" })).toBe("a=1\nb=2");
    expect(() => parseProps("oops")).toThrow("Строка 1");
  });
});
