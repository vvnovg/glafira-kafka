import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { BatchEvent, ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";
import { emptyProducerForm } from "./form";

let onEvent: (e: BatchEvent) => void = () => {};

vi.mock("@tauri-apps/plugin-dialog", () => ({ open: vi.fn().mockResolvedValue("/data/msgs.jsonl") }));
vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      startBatch: vi.fn(async (_p: unknown, cb: (e: BatchEvent) => void) => {
        onEvent = cb;
        return "sid";
      }),
      stopSession: vi.fn().mockResolvedValue(true),
    },
  };
});

import { api } from "../../api/client";
import { BatchPanel } from "./BatchPanel";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const form = { ...emptyProducerForm("orders"), value: { format: "json" as const, text: '{"n":{{seq}}}' } };

beforeEach(() => vi.clearAllMocks());

const prod: ConnectionProfile = { ...dev, id: "p", name: "prod-1", environment: "prod" };

describe("BatchPanel", () => {
  it("N копий с ограничением скорости, прогресс и отмена", async () => {
    const user = userEvent.setup();
    renderWithClient(<BatchPanel connection={dev} form={form} />);
    await user.clear(screen.getByLabelText("Копий"));
    await user.type(screen.getByLabelText("Копий"), "100");
    await user.type(screen.getByLabelText("Сообщений в секунду"), "10");
    await user.click(screen.getByRole("button", { name: "Запустить" }));
    const [params, , confirm] = vi.mocked(api.startBatch).mock.calls[0];
    expect(params).toMatchObject({ connectionId: "c", ratePerSec: 10, source: { type: "copies", count: 100, message: { topic: "orders" } } });
    expect(confirm).toBeNull();
    act(() => onEvent({ type: "progress", sent: 40, failed: 0, total: 100 }));
    expect(screen.getByText("отправлено 40 из 100 · ошибок 0")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(api.stopSession).toHaveBeenCalledWith("sid");
  });

  it("файл: формат по расширению", async () => {
    const user = userEvent.setup();
    renderWithClient(<BatchPanel connection={dev} form={form} />);
    await user.click(screen.getByLabelText("Из файла"));
    await user.click(screen.getByRole("button", { name: "Выбрать файл…" }));
    expect(await screen.findByText("/data/msgs.jsonl")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Запустить" }));
    expect(vi.mocked(api.startBatch).mock.calls[0][0].source).toEqual({ type: "file", path: "/data/msgs.jsonl", format: "jsonl", topic: "orders" });
  });

  it("итог: отменено до отправки, усечённый список ошибок и «без строки»", async () => {
    const user = userEvent.setup();
    renderWithClient(<BatchPanel connection={dev} form={form} />);
    await user.click(screen.getByRole("button", { name: "Запустить" }));
    act(() => onEvent({ type: "progress", sent: 3, failed: 25, total: null }));
    expect(screen.getByRole("status")).toHaveTextContent("отправлено 3 из — · ошибок 25");
    expect(screen.getByTitle(/неизвестно/)).toHaveTextContent("—");
    act(() => onEvent({ type: "errors", errors: [{ line: null, message: { key: "errors.generic", params: { message: "сводка" }, text: "сводка" } }, { line: 2, message: { key: "errors.source.lineTooLong", params: { line: "2" }, text: "Line 2 is longer than 16 MiB" } }] }));
    act(() => onEvent({ type: "finished", sent: 3, failed: 25, notSent: 7, cancelled: true }));
    expect(screen.getByText(/отменено до отправки: 7/)).toBeInTheDocument();
    expect(screen.getByText(/без строки: сводка/)).toBeInTheDocument();
    expect(screen.getByText(/строка 2: Строка 2 длиннее 16 МиБ/)).toBeInTheDocument();
    expect(screen.getByText(/показаны 2 из 25/)).toBeInTheDocument();
  });

  it("отклонённый startBatch даёт ошибку, а не вечный running", async () => {
    vi.mocked(api.startBatch).mockRejectedValueOnce({ code: "INVALID_INPUT", message: "нет доступа", retryable: false });
    const user = userEvent.setup();
    renderWithClient(<BatchPanel connection={dev} form={form} />);
    await user.click(screen.getByRole("button", { name: "Запустить" }));
    expect(await screen.findByText(/нет доступа/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Запустить" })).toBeEnabled();
  });

  it("«Отменить» сбрасывает состояние без терминального события", async () => {
    const user = userEvent.setup();
    renderWithClient(<BatchPanel connection={dev} form={form} />);
    await user.click(screen.getByRole("button", { name: "Запустить" }));
    await user.click(screen.getByRole("button", { name: "Отменить" }));
    expect(screen.getByRole("button", { name: "Запустить" })).toBeEnabled();
    expect(screen.getByRole("status")).toHaveTextContent("отменено");
  });

  it("prod требует подтверждения на странице", async () => {
    const user = userEvent.setup();
    renderWithClient(<BatchPanel connection={prod} form={form} />);
    await user.click(screen.getByRole("button", { name: "Запустить" }));
    expect(api.startBatch).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Запустить в prod" })).toBeInTheDocument();
  });

  it("подключение только для чтения не запускает пакет", () => {
    renderWithClient(<BatchPanel connection={{ ...dev, readOnly: true }} form={form} />);
    expect(screen.queryByRole("button", { name: "Запустить" })).not.toBeInTheDocument();
  });

  it("подсказка у скорости пакетной отправки", () => {
    renderWithClient(<BatchPanel connection={dev} form={form} />);
    expect(screen.getByRole("button", { name: "Подсказка: Скорость" })).toBeInTheDocument();
  });
});
