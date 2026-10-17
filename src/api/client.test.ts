import { beforeEach, describe, expect, it, vi } from "vitest";

const invoke = vi.fn();
vi.mock("@tauri-apps/api/core", () => ({
  invoke: (...args: unknown[]) => invoke(...args),
  Channel: class {
    onmessage: (m: unknown) => void = () => {};
  },
}));

import { api, toAppError } from "./client";
import type { BrowseParams, ConnectionProfile, ConsumerParams, ProduceMessage } from "./types";

beforeEach(() => {
  invoke.mockReset();
});

describe("api client", () => {
  it("saveConnection передаёт пароли TLS", async () => {
    invoke.mockResolvedValue({});
    const tls = { keyPassword: null, keystorePassword: "s", truststorePassword: "" };
    await api.saveConnection({ id: "a" } as ConnectionProfile, undefined, tls);
    expect(invoke).toHaveBeenCalledWith("save_connection", {
      input: { profile: { id: "a" }, saslPassword: null, tlsPasswords: tls },
    });
  });

  it("передаёт аргументы в camelCase", async () => {
    invoke.mockResolvedValue([]);
    await api.getTopicStats("c1", ["a", "b"]);
    expect(invoke).toHaveBeenCalledWith("get_topic_stats", { connectionId: "c1", topics: ["a", "b"] });
  });

  it("оборачивает профиль и пароль в input", async () => {
    invoke.mockResolvedValue({ ok: true, steps: [] });
    const profile = { id: "", name: "x" } as never;
    await api.testConnection(profile, "pw");
    expect(invoke).toHaveBeenCalledWith("test_connection", { input: { profile, saslPassword: "pw", tlsPasswords: null } });
  });

  it("previewImport и applyImport передают цель и флаг обновления", async () => {
    invoke.mockResolvedValue({ added: [], updated: [], unchanged: [], skipped: [], warnings: [], needsPasswords: 0 });
    await api.previewImport("/f.json", null, true);
    expect(invoke).toHaveBeenCalledWith("preview_import", { path: "/f.json", targetFolderId: null, updateExisting: true });
    await api.applyImport("/f.json", "t", false);
    expect(invoke).toHaveBeenCalledWith("apply_import", { path: "/f.json", targetFolderId: "t", updateExisting: false });
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

  it("toAppError сохраняет key и params ошибки из Rust", () => {
    const e = { code: "READ_ONLY", key: "errors.guard.readOnly", params: { name: "p" }, message: "Connection “p” is read-only", retryable: false };
    expect(toAppError(e)).toEqual(e);
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
  it("startConsumer передаёт параметры, Channel и подтверждение", async () => {
    invoke.mockResolvedValue("sid");
    const onEvent = vi.fn();
    const id = await api.startConsumer({ groupId: "g" } as ConsumerParams, onEvent, "prod-1");
    expect(id).toBe("sid");
    const [cmd, args] = invoke.mock.calls[0];
    expect(cmd).toBe("start_consumer");
    expect(args.confirm).toBe("prod-1");
    args.onEvent.onmessage({ type: "recovered" });
    expect(onEvent).toHaveBeenCalledWith({ type: "recovered" });
  });

  it("produce и commitOffsets", async () => {
    invoke.mockResolvedValue({ partition: 0, offset: 1, timestamp: 2 });
    const message = { topic: "t", partition: null, key: { format: "null", text: "" }, value: { format: "string", text: "v" }, headers: [] } as ProduceMessage;
    const settings = { acks: "all", compression: "none", idempotence: false } as const;
    await api.produce("c", message, settings, null);
    expect(invoke).toHaveBeenCalledWith("produce", { connectionId: "c", message, settings, confirm: null });
    await api.commitOffsets("s", { type: "allConsumed" }, null);
    expect(invoke).toHaveBeenLastCalledWith("commit_offsets", { sessionId: "s", target: { type: "allConsumed" }, confirm: null });
  });

  it("pause/resume, шаблоны и suggestGroupId", async () => {
    invoke.mockResolvedValue(undefined);
    const parts = [{ topic: "t", partition: 0 }];
    await api.pausePartitions("s", parts);
    expect(invoke).toHaveBeenLastCalledWith("pause_partitions", { sessionId: "s", partitions: parts });
    await api.resumePartitions("s", parts);
    expect(invoke).toHaveBeenLastCalledWith("resume_partitions", { sessionId: "s", partitions: parts });
    await api.deleteTemplate("x");
    expect(invoke).toHaveBeenLastCalledWith("delete_template", { id: "x" });
    await api.listHistory("c");
    expect(invoke).toHaveBeenLastCalledWith("list_history", { connectionId: "c" });
    await api.suggestGroupId();
    expect(invoke.mock.calls[invoke.mock.calls.length - 1][0]).toBe("suggest_group_id");
  });

  it("startBatch отдаёт события Channel", async () => {
    invoke.mockResolvedValue("bid");
    const onEvent = vi.fn();
    await api.startBatch({} as never, onEvent, null);
    const [cmd, args] = invoke.mock.calls[0];
    expect(cmd).toBe("produce_batch");
    args.onEvent.onmessage({ type: "progress", sent: 1, failed: 0, total: null });
    expect(onEvent).toHaveBeenCalled();
  });

  it("setLicenseKey передаёт ключ", async () => {
    invoke.mockResolvedValue({ tier: "pro" });
    await api.setLicenseKey("a.b");
    expect(invoke).toHaveBeenCalledWith("set_license_key", { key: "a.b" });
  });
});
