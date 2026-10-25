import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { McpAuditEntry } from "../../api/types";
import { auditTarget, browsePrefillFromAudit, isBrowsable, outwardLocal } from "./auditPrefill";

const call = (args: unknown, patch: Partial<McpAuditEntry> = {}): McpAuditEntry => ({
  ts: 1,
  event: "call",
  tool: "read_messages",
  outcome: "ok",
  connection: { id: "c1", name: "dev" },
  args,
  scanned: 7,
  returned: 7,
  masked: 2,
  ...patch,
});

/** Как форма читает значение datetime-local (`parseLocalTime` в browse/params.ts). */
const back = (local: string | undefined) => new Date(local ?? "").getTime();

/** Окно формы содержит окно вызова: начало не позже `from`, конец не раньше `to`. */
function expectOutward(form: { fromTime?: string; toTime?: string; timestamp?: string }, from: number, to: number) {
  expect(back(form.fromTime)).toBeLessThanOrEqual(from);
  expect(back(form.toTime)).toBeGreaterThanOrEqual(to);
  expect(form.timestamp).toBe(form.fromTime);
}

describe("browsePrefillFromAudit", () => {
  it("окно по времени: границы наружу до секунды, партиции и фильтры", () => {
    const from = Date.parse("2026-10-24T14:31:00.250+03:00");
    const to = 1_792_841_525_123;
    const r = browsePrefillFromAudit(
      call({
        connectionId: "c1",
        topic: "orders",
        from: "2026-10-24T14:31:00.250+03:00",
        to,
        partitions: [2, 0],
        value: { pattern: "FAILED", regex: true },
        header: { name: "trace", value: { pattern: "t-1", regex: true } },
        jsonPath: "$.a",
        valueFormat: "json",
      }),
    );
    expect(r).not.toBeNull();
    expect(r?.connectionId).toBe("c1");
    expect(r?.topic).toBe("orders");
    expect(r?.prefill.inexact).toEqual([]);
    const form = r?.prefill.form ?? {};
    expectOutward(form, from, to);
    // Вне перевода часов — не дальше секунды от границ вызова.
    expect(from - back(form.fromTime)).toBeLessThan(1000);
    expect(back(form.toTime) - to).toBeLessThan(1000);
    expect(form).toMatchObject({
      startType: "timestamp",
      partitions: "2, 0",
      valuePattern: "FAILED",
      regex: true,
      caseSensitive: false,
      headerName: "trace",
      headerValue: "t-1",
      jsonPath: "$.a",
      keyFormat: "auto",
      valueFormat: "json",
    });
  });

  it("режим «офсеты»: начало, партиция и лимит", () => {
    const r = browsePrefillFromAudit(call({ topic: "orders", partition: 1, startOffset: 10, endOffset: 25 }));
    expect(r?.prefill.form).toMatchObject({ startType: "offset", offset: 10, partitions: "1", limit: 15 });
    expect(r?.prefill.inexact).toEqual([]);
    const open = browsePrefillFromAudit(call({ topic: "orders", partition: 1, startOffset: 10 }));
    expect(open?.prefill.form.limit).toBeUndefined();
  });

  it("режим «офсеты» с фильтром: лимит формы считает найденные сообщения — перенос неточный", () => {
    const range = { topic: "orders", partition: 1, startOffset: 10, endOffset: 25 };
    for (const filter of [
      { value: { pattern: "FAILED" } },
      { key: { pattern: "k" } },
      { header: { name: "trace" } },
      { jsonPath: "$.a" },
    ]) {
      expect(browsePrefillFromAudit(call({ ...range, ...filter }))?.prefill.inexact).toEqual(["limit"]);
    }
    // Без endOffset лимит не переносится — и пометки о нём нет.
    const open = { topic: "orders", partition: 1, startOffset: 10, value: { pattern: "FAILED" } };
    expect(browsePrefillFromAudit(call(open))?.prefill.inexact).toEqual([]);
    // Пустые фильтры сервер отбрасывает — они не в счёт.
    const empty = { value: { pattern: "" }, header: { name: " ", value: { pattern: "x" } }, jsonPath: " " };
    expect(browsePrefillFromAudit(call({ ...range, ...empty }))?.prefill.inexact).toEqual([]);
  });

  it("summarize_messages — всегда окно: лишние partition и startOffset сервер не читает", () => {
    const r = browsePrefillFromAudit(
      call(
        { topic: "orders", from: 0, to: 60_000, partitions: [3], partition: 1, startOffset: 10, endOffset: 25, bucketSeconds: 60, topKeys: 5 },
        { tool: "summarize_messages" },
      ),
    );
    expect(r?.prefill.form).toMatchObject({ startType: "timestamp", partitions: "3" });
    expect(r?.prefill.form.offset).toBeUndefined();
    expect(r?.prefill.form.limit).toBeUndefined();
    expectOutward(r?.prefill.form ?? {}, 0, 60_000);
  });

  it("разные флаги ключа и значения — берутся флаги значения, перенос неточный", () => {
    const r = browsePrefillFromAudit(
      call({ topic: "t", from: 0, to: 1, key: { pattern: "k", regex: true }, value: { pattern: "v", caseSensitive: true } }),
    );
    expect(r?.prefill.inexact).toEqual(["flags"]);
    expect(r?.prefill.form).toMatchObject({ keyPattern: "k", valuePattern: "v", regex: false, caseSensitive: true });
  });

  it("фильтр только по значению заголовка переносит свои флаги", () => {
    const r = browsePrefillFromAudit(
      call({ topic: "t", from: 0, to: 1, header: { name: "trace", value: { pattern: "t-\\d+", regex: true, caseSensitive: true } } }),
    );
    expect(r?.prefill.inexact).toEqual([]);
    expect(r?.prefill.form).toMatchObject({ headerName: "trace", headerValue: "t-\\d+", regex: true, caseSensitive: true });
  });

  it("фильтр только по ключу переносит свои флаги", () => {
    const r = browsePrefillFromAudit(call({ topic: "t", from: 0, to: 1, key: { pattern: "k.*", regex: true, caseSensitive: true } }));
    expect(r?.prefill.inexact).toEqual([]);
    expect(r?.prefill.form).toMatchObject({ keyPattern: "k.*", regex: true, caseSensitive: true });
  });

  it("флаги значения заголовка расходятся с флагами значения — перенос неточный", () => {
    const r = browsePrefillFromAudit(
      call({ topic: "t", from: 0, to: 1, value: { pattern: "v" }, header: { name: "h", value: { pattern: "x", regex: true } } }),
    );
    expect(r?.prefill.inexact).toEqual(["flags"]);
    expect(r?.prefill.form).toMatchObject({ regex: false, caseSensitive: false });
  });

  it("пустые шаблоны и заголовок без имени не влияют на флаги", () => {
    // Пустое значение не перебивает флаги ключа и не даёт расхождения.
    const r = browsePrefillFromAudit(
      call({ topic: "t", from: 0, to: 1, value: { pattern: "", caseSensitive: true }, key: { pattern: "k", regex: true } }),
    );
    expect(r?.prefill.inexact).toEqual([]);
    expect(r?.prefill.form).toMatchObject({ keyPattern: "k", valuePattern: "", regex: true, caseSensitive: false });
    // Заголовок без имени сервер отбрасывает вместе с фильтром его значения.
    const h = browsePrefillFromAudit(
      call({ topic: "t", from: 0, to: 1, value: { pattern: "v" }, header: { name: "  ", value: { pattern: "x", regex: true } } }),
    );
    expect(h?.prefill.inexact).toEqual([]);
    expect(h?.prefill.form).toMatchObject({ regex: false, headerValue: "" });
  });

  it("повторить нельзя: усечённые аргументы, ошибка, другой инструмент, нет времени", () => {
    expect(browsePrefillFromAudit(call('{"topic":"orders"…'))).toBeNull();
    expect(browsePrefillFromAudit(call({ topic: "t", from: 0, to: 1 }, { outcome: "error" }))).toBeNull();
    expect(browsePrefillFromAudit(call({ groupId: "g", from: 0, to: 1 }, { tool: "get_group_commit_history" }))).toBeNull();
    expect(browsePrefillFromAudit(call({ topic: "t", from: "yesterday", to: 1 }))).toBeNull();
    expect(browsePrefillFromAudit(call({ topic: "t", from: 0, to: 1 }, { connection: null }))).toBeNull();
    expect(browsePrefillFromAudit(call({ topic: "t", from: 0, to: 1 }, { tool: "summarize_messages" }))).not.toBeNull();
  });
});

/**
 * Модель Берлина 25.10.2026 без часового пояса машины: до 01:00Z — UTC+2 (CEST), с 01:00Z — UTC+1 (CET), поэтому
 * местное 02:00–02:59:59 называет два момента. `pass` — какой из них возвращает разбор строки.
 */
const SHIFT = Date.UTC(2026, 9, 25, 1, 0);
const HOUR = 3_600_000;
const berlinFormat = (ms: number) => new Date(ms + (ms < SHIFT ? 2 : 1) * HOUR).toISOString().slice(0, 19);
const berlinParse = (pass: "first" | "later") => (s: string) => {
  const wall = Date.parse(`${s}Z`);
  const instants = [wall - 2 * HOUR, wall - HOUR].filter((ms) => berlinFormat(ms) === s);
  return (pass === "first" ? instants[0] : instants[instants.length - 1]) ?? Number.NaN;
};
const at = (h: number, m: number, s = 0) => Date.UTC(2026, 9, 25, h, m, s);

describe("outwardLocal: повторяющийся час на модели Берлина", () => {
  it("модель: местное 02:10 — два момента, 03:20 — один", () => {
    expect(berlinFormat(at(0, 10))).toBe("2026-10-25T02:10:00");
    expect(berlinFormat(at(1, 10))).toBe("2026-10-25T02:10:00");
    expect(berlinParse("first")("2026-10-25T02:10:00")).toBe(at(0, 10));
    expect(berlinParse("later")("2026-10-25T02:10:00")).toBe(at(1, 10));
    expect(berlinParse("later")("2026-10-25T03:20:00")).toBe(at(2, 20));
  });

  it("разбор берёт первый проход (V8): конец окна во втором проходе отодвигается на час позже", () => {
    const parse = berlinParse("first");
    expect(outwardLocal(at(1, 20, 1), 1, berlinFormat, parse)).toBe("2026-10-25T03:20:01");
    // Начало во втором проходе и так читается раньше — без поправки.
    expect(outwardLocal(at(1, 10), -1, berlinFormat, parse)).toBe("2026-10-25T02:10:00");
  });

  it("разбор берёт второй проход (возможно, JavaScriptCore в WKWebView): начало в первом проходе — на час раньше", () => {
    const parse = berlinParse("later");
    expect(outwardLocal(at(0, 10), -1, berlinFormat, parse)).toBe("2026-10-25T01:10:00");
    // Конец в первом проходе и так читается позже — без поправки.
    expect(outwardLocal(at(0, 20, 1), 1, berlinFormat, parse)).toBe("2026-10-25T02:20:01");
  });

  it("при любом разборе граница снаружи и не дальше сдвига часов, вне складки — точно", () => {
    const points = [Date.UTC(2026, 9, 24, 23, 30), at(0, 10), at(0, 59, 59), at(1, 0), at(1, 20, 1), at(1, 59, 59), at(2, 0), at(2, 20, 1)];
    for (const pass of ["first", "later"] as const) {
      const parse = berlinParse(pass);
      for (const ms of points) {
        for (const dir of [1, -1] as const) {
          const back = parse(outwardLocal(ms, dir, berlinFormat, parse));
          expect(dir * (back - ms), `${pass} ${new Date(ms).toISOString()} dir ${dir}`).toBeGreaterThanOrEqual(0);
          expect(Math.abs(back - ms)).toBeLessThanOrEqual(HOUR);
        }
      }
      expect(outwardLocal(at(2, 20, 1), 1, berlinFormat, parse)).toBe("2026-10-25T03:20:01");
      expect(outwardLocal(at(2, 20, 1), -1, berlinFormat, parse)).toBe("2026-10-25T03:20:01");
    }
  });
});

/** Платформа процесса Vitest; в проекте нет @types/node. */
const PLATFORM = (globalThis as { process?: { platform?: string } }).process?.platform;

describe("browsePrefillFromAudit: повторяющийся час в часовом поясе машины", () => {
  // Сквозная проверка с настоящими toLocalInput и new Date. vi.stubEnv присваивает process.env.TZ; на macOS и Linux
  // Node при этом перечитывает пояс (Vitest, пул forks, запускает файл в отдельном процессе), на Windows — нет:
  // там пояс берётся из системы, и тесты блока пропускаются (логику проверяет блок с моделью выше).
  // 25.10.2026 в Берлине 03:00 CEST → 02:00 CET: местное 02:00–02:59:59 повторяется.
  let zoneSwitched = false;
  beforeAll(() => {
    vi.stubEnv("TZ", "Europe/Berlin");
    zoneSwitched =
      new Date(Date.UTC(2026, 9, 25, 0, 30)).getTimezoneOffset() === -120 &&
      new Date(Date.UTC(2026, 9, 25, 1, 30)).getTimezoneOffset() === -60;
  });
  afterAll(() => {
    vi.unstubAllEnvs();
  });

  it("на macOS и Linux пояс сменился", ({ skip }) => {
    skip(PLATFORM === "win32", "Windows: Node не перечитывает process.env.TZ");
    expect(zoneSwitched).toBe(true);
  });

  it("окно во втором проходе 02:xx остаётся снаружи", ({ skip }) => {
    skip(!zoneSwitched, "часовой пояс не сменился");
    const from = Date.UTC(2026, 9, 25, 1, 10, 0, 400);
    const to = Date.UTC(2026, 9, 25, 1, 20, 0, 900);
    const r = browsePrefillFromAudit(call({ topic: "t", from: new Date(from).toISOString(), to }));
    expectOutward(r?.prefill.form ?? {}, from, to);
  });

  it("начало в первом проходе, конец во втором: конец не попадает внутрь окна", ({ skip }) => {
    skip(!zoneSwitched, "часовой пояс не сменился");
    const from = Date.UTC(2026, 9, 25, 0, 50);
    const to = Date.UTC(2026, 9, 25, 1, 30);
    const r = browsePrefillFromAudit(call({ topic: "t", from, to }));
    const form = r?.prefill.form ?? {};
    expectOutward(form, from, to);
    expect(from - back(form.fromTime)).toBeLessThan(1000);
    // Расширение — не больше сдвига часов.
    expect(back(form.toTime) - to).toBeLessThanOrEqual(3_600_000);
  });

  it("обычное время рядом с переводом — до секунды", ({ skip }) => {
    skip(!zoneSwitched, "часовой пояс не сменился");
    const from = Date.UTC(2026, 9, 25, 2, 10, 0, 1);
    const to = Date.UTC(2026, 9, 25, 2, 20, 0, 1);
    const form = browsePrefillFromAudit(call({ topic: "t", from, to }))?.prefill.form ?? {};
    expectOutward(form, from, to);
    expect(back(form.toTime) - to).toBeLessThan(1000);
  });
});

describe("isBrowsable", () => {
  it("кнопка есть у успешных read_messages и summarize_messages, даже с усечёнными аргументами", () => {
    expect(isBrowsable(call({ topic: "t", from: 0, to: 1 }))).toBe(true);
    expect(isBrowsable(call('{"topic":"orders"…'))).toBe(true);
    expect(isBrowsable(call({ topic: "t", from: 0, to: 1 }, { tool: "summarize_messages" }))).toBe(true);
    expect(isBrowsable(call({ topic: "t", from: 0, to: 1 }, { outcome: "error" }))).toBe(false);
    expect(isBrowsable(call({ groupId: "g" }, { tool: "get_group_commit_history" }))).toBe(false);
    expect(isBrowsable({ ts: 0, event: "approved" })).toBe(false);
  });
});

describe("auditTarget", () => {
  it("топик или группа из аргументов", () => {
    expect(auditTarget(call({ topic: "orders" }))).toBe("orders");
    expect(auditTarget(call({ groupId: "svc" }))).toBe("svc");
    expect(auditTarget(call("усечено…"))).toBe("");
  });
});
