import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { previewOffsetReset: vi.fn(), resetOffsets: vi.fn() } };
});

import { api } from "../../api/client";
import { ResetOffsetsPanel } from "./ResetOffsetsPanel";

const prod: ConnectionProfile = { ...newProfile(), id: "c", name: "prod-1", environment: "prod" };

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.previewOffsetReset).mockResolvedValue([
    { topic: "t", partition: 0, current: 5, target: 0, low: 0, high: 9 },
    { topic: "t", partition: 1, current: null, target: null, low: 0, high: 3, error: "Нет закоммиченного оффсета — сдвиг невозможен" },
  ]);
  vi.mocked(api.resetOffsets).mockResolvedValue([{ topic: "t", partition: 0, offset: 0 }]);
});

describe("ResetOffsetsPanel", () => {
  it("применяет ровно показанный план после подтверждения prod", async () => {
    const user = userEvent.setup();
    const onApplied = vi.fn();
    renderWithClient(<ResetOffsetsPanel connection={prod} groupId="g" onApplied={onApplied} onClose={() => {}} />);
    expect(screen.queryByRole("button", { name: /Применить/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    expect(api.previewOffsetReset).toHaveBeenCalledWith("c", { groupId: "g", strategy: { type: "earliest" }, scope: { type: "all" } });
    expect(await screen.findByText("Нет закоммиченного оффсета — сдвиг невозможен")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Применить (1)" }));
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(screen.getByRole("button", { name: "Сбросить оффсеты" }));
    expect(api.resetOffsets).toHaveBeenCalledWith("c", "g", [{ topic: "t", partition: 0, offset: 0 }], "prod-1");
    await vi.waitFor(() => expect(onApplied).toHaveBeenCalled());
  });

  it("изменение формы сбрасывает предпросмотр", async () => {
    const user = userEvent.setup();
    renderWithClient(<ResetOffsetsPanel connection={prod} groupId="g" onApplied={() => {}} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    await screen.findByRole("button", { name: "Применить (1)" });
    await user.selectOptions(screen.getByLabelText("Стратегия"), "latest");
    expect(screen.queryByRole("button", { name: /Применить/ })).not.toBeInTheDocument();
  });

  it("показывает результат по партициям и не применяет при пустом плане", async () => {
    const user = userEvent.setup();
    const dev = { ...prod, environment: "dev" as const };
    vi.mocked(api.resetOffsets).mockResolvedValue([{ topic: "t", partition: 0, offset: 0, error: "boom" }]);
    renderWithClient(<ResetOffsetsPanel connection={dev} groupId="g" onApplied={() => {}} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    await user.click(await screen.findByRole("button", { name: "Применить (1)" }));
    expect(await screen.findByText(/t:0 — ошибка: boom/)).toBeInTheDocument();
    vi.mocked(api.previewOffsetReset).mockResolvedValue([{ topic: "t", partition: 1, current: null, target: null, low: 0, high: 3, error: "x" }]);
    await user.selectOptions(screen.getByLabelText("Стратегия"), "latest");
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    expect(await screen.findByRole("button", { name: "Применить (0)" })).toBeDisabled();
  });

  it("повторный предпросмотр после применения очищает старый результат", async () => {
    const user = userEvent.setup();
    const dev = { ...prod, environment: "dev" as const };
    renderWithClient(<ResetOffsetsPanel connection={dev} groupId="g" onApplied={() => {}} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    await user.click(await screen.findByRole("button", { name: "Применить (1)" }));
    expect(await screen.findByText(/оффсет 0 установлен/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    expect(await screen.findByRole("button", { name: "Применить (1)" })).toBeEnabled();
    expect(screen.queryByText(/оффсет 0 установлен/)).toBeNull();
  });

  it("пустой предпросмотр", async () => {
    const user = userEvent.setup();
    vi.mocked(api.previewOffsetReset).mockResolvedValue([]);
    renderWithClient(<ResetOffsetsPanel connection={prod} groupId="g" onApplied={() => {}} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    expect(await screen.findByText("Нет партиций для сброса")).toBeInTheDocument();
  });
});
