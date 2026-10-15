import { beforeEach, describe, expect, it, vi } from "vitest";

const invoke = vi.fn();
vi.mock("@tauri-apps/api/core", () => ({
  invoke: (...args: unknown[]) => invoke(...args),
  Channel: class {
    onmessage: (m: unknown) => void = () => {};
  },
}));

import { api, toAppError } from "./client";
import type { BrowseParams } from "./types";

beforeEach(() => {
  invoke.mockReset();
});

describe("api client", () => {
  it("передаёт аргументы в camelCase", async () => {
    invoke.mockResolvedValue([]);
    await api.getTopicStats("c1", ["a", "b"]);
    expect(invoke).toHaveBeenCalledWith("get_topic_stats", { connectionId: "c1", topics: ["a", "b"] });
  });

  it("оборачивает профиль и пароль в input", async () => {
    invoke.mockResolvedValue({ ok: true, steps: [] });
    const profile = { id: "", name: "x" } as never;
    await api.testConnection(profile, "pw");
    expect(invoke).toHaveBeenCalledWith("test_connection", { input: { profile, saslPassword: "pw" } });
  });

  it("пробрасывает AppError как есть", async () => {
    const err = { code: "NOT_FOUND", message: "нет", retryable: false };
    invoke.mockRejectedValue(err);
    await expect(api.listTopics("c")).rejects.toEqual(err);
  });

  it("строковые ошибки превращает в INTERNAL", async () => {
    invoke.mockRejectedValue("boom");
    await expect(api.listTopics("c")).rejects.toEqual({ code: "INTERNAL", message: "boom", retryable: false });
  });

  it("startBrowse передаёт Channel и вызывает обработчик", async () => {
    invoke.mockResolvedValue("sid");
    const onEvent = vi.fn();
    const id = await api.startBrowse({ topic: "t" } as BrowseParams, onEvent);
    expect(id).toBe("sid");
    const [, args] = invoke.mock.calls[0];
    args.onEvent.onmessage({ type: "reachedEnd" });
    expect(onEvent).toHaveBeenCalledWith({ type: "reachedEnd" });
  });

  it("toAppError понимает Error", () => {
    expect(toAppError(new Error("x"))).toEqual({ code: "INTERNAL", message: "x", retryable: false });
  });

  it("операции с топиками передают подтверждение отдельными полями", async () => {
    invoke.mockResolvedValue([]);
    await api.deleteTopics("c1", ["a", "b"], { confirm: "prod-1", typedName: "2" });
    expect(invoke).toHaveBeenCalledWith("delete_topics", {
      connectionId: "c1",
      topics: ["a", "b"],
      confirm: "prod-1",
      typedConfirmation: "2",
    });
    await api.purgeTopic("c1", "t", null, { confirm: null, typedName: "t" });
    expect(invoke).toHaveBeenLastCalledWith("purge_topic", {
      connectionId: "c1",
      topic: "t",
      partitions: null,
      confirm: null,
      typedName: "t",
    });
  });

  it("сброс оффсетов передаёт план и подтверждение", async () => {
    invoke.mockResolvedValue([]);
    const offsets = [{ topic: "t", partition: 0, offset: 5 }];
    await api.resetOffsets("c1", "g", offsets, null);
    expect(invoke).toHaveBeenCalledWith("reset_offsets", { connectionId: "c1", groupId: "g", offsets, confirm: null });
  });
});
