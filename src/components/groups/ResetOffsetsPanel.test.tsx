import { fireEvent, screen } from "@testing-library/react";
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
    {
      topic: "t",
      partition: 1,
      current: null,
      target: null,
      low: 0,
      high: 3,
      error: { key: "errors.groups.noCommittedOffset", params: {}, text: "No committed offset — cannot shift" },
    },
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
    expect(api.resetOffsets).toHaveBeenCalledWith("c", "g", [{ topic: "t", partition: 0, offset: 0 }], "prod-1", null);
    await vi.waitFor(() => expect(onApplied).toHaveBeenCalled());
  });

  it("из предложения LLM: форма заполнена, в команду уходят proposalId и запрос предпросмотра", async () => {
    const user = userEvent.setup();
    const dev = { ...prod, environment: "dev" as const };
    renderWithClient(
      <ResetOffsetsPanel
        connection={dev}
        groupId="g"
        prefill={{ strategy: { type: "offset", offset: 0 }, scope: { type: "topics", topics: ["t"] }, proposalId: "p1", nonce: 1 }}
        onApplied={() => {}}
        onClose={() => {}}
      />,
    );
    expect(screen.getByLabelText("Стратегия")).toHaveValue("offset");
    expect(screen.getByLabelText("Оффсет")).toHaveValue(0);
    expect(screen.getByLabelText("Область")).toHaveValue("topics");
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    const request = { groupId: "g", strategy: { type: "offset", offset: 0 }, scope: { type: "topics", topics: ["t"] } };
    expect(api.previewOffsetReset).toHaveBeenCalledWith("c", request);
    await user.click(await screen.findByRole("button", { name: "Применить (1)" }));
    expect(api.resetOffsets).toHaveBeenCalledWith("c", "g", [{ topic: "t", partition: 0, offset: 0 }], null, { id: "p1", request });
  });

  it("из предложения LLM со временем: без правки поля уходит исходный timestampMs, после правки — новое значение", async () => {
    const user = userEvent.setup();
    const dev = { ...prod, environment: "dev" as const };
    // Миллисекунды поле datetime-local не показывает: перечитанное из поля время было бы другим.
    const timestampMs = 1730000000123;
    renderWithClient(
      <ResetOffsetsPanel
        connection={dev}
        groupId="g"
        prefill={{ strategy: { type: "timestamp", timestampMs }, scope: { type: "all" }, proposalId: "p1", nonce: 1 }}
        onApplied={() => {}}
        onClose={() => {}}
      />,
    );
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    const request = { groupId: "g", strategy: { type: "timestamp", timestampMs }, scope: { type: "all" } };
    expect(api.previewOffsetReset).toHaveBeenCalledWith("c", request);
    await user.click(await screen.findByRole("button", { name: "Применить (1)" }));
    expect(api.resetOffsets).toHaveBeenCalledWith("c", "g", [{ topic: "t", partition: 0, offset: 0 }], null, { id: "p1", request });
    // Человек поправил время — в запросе то, что в поле.
    fireEvent.change(screen.getByLabelText("Дата и время (локальное)"), { target: { value: "2024-10-27T05:00:00" } });
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    const edited = { ...request, strategy: { type: "timestamp", timestampMs: new Date("2024-10-27T05:00:00").getTime() } };
    expect(api.previewOffsetReset).toHaveBeenLastCalledWith("c", edited);
    await user.click(await screen.findByRole("button", { name: "Применить (1)" }));
    expect(api.resetOffsets).toHaveBeenLastCalledWith("c", "g", [{ topic: "t", partition: 0, offset: 0 }], null, { id: "p1", request: edited });
  });

  it("blockedReason блокирует «Применить» и показывает подсказку", async () => {
    const user = userEvent.setup();
    const dev = { ...prod, environment: "dev" as const };
    renderWithClient(<ResetOffsetsPanel connection={dev} groupId="g" blockedReason="нельзя" onApplied={() => {}} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    const apply = await screen.findByRole("button", { name: "Применить (1)" });
    expect(apply).toBeDisabled();
    expect(apply).toHaveAttribute("title", "нельзя");
    expect(screen.getByText("нельзя")).toBeInTheDocument();
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
    vi.mocked(api.resetOffsets).mockResolvedValue([{ topic: "t", partition: 0, offset: 0, error: { key: "errors.generic", params: { message: "boom" }, text: "boom" } }]);
    renderWithClient(<ResetOffsetsPanel connection={dev} groupId="g" onApplied={() => {}} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    await user.click(await screen.findByRole("button", { name: "Применить (1)" }));
    expect(await screen.findByText(/t:0 — ошибка: boom/)).toBeInTheDocument();
    vi.mocked(api.previewOffsetReset).mockResolvedValue([{ topic: "t", partition: 1, current: null, target: null, low: 0, high: 3, error: { key: "errors.generic", params: { message: "x" }, text: "x" } }]);
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

  it("подсказки «?»: текст стратегии зависит от выбранной стратегии", async () => {
    const user = userEvent.setup();
    renderWithClient(<ResetOffsetsPanel connection={prod} groupId="g" onApplied={() => {}} onClose={() => {}} />);
    for (const label of ["Стратегия", "Область", "Активные участники группы", "Предпросмотр"]) {
      expect(screen.getByRole("button", { name: `Подсказка: ${label}` })).toBeInTheDocument();
    }
    // Значок рядом с подписью, а не внутри: доступное имя поля не меняется.
    expect(screen.getByLabelText("Стратегия")).toHaveAttribute("id", "rs-strategy");
    expect(screen.getByLabelText("Область")).toHaveAttribute("id", "rs-scope");
    const icon = screen.getByRole("button", { name: "Подсказка: Стратегия" });
    await user.click(icon);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Группа перечитает всё, что ещё хранится в топике.");
    await user.selectOptions(screen.getByLabelText("Стратегия"), "latest");
    // Клик по списку закрывает подсказку; снова открытая — уже с текстом новой стратегии.
    expect(screen.queryByRole("tooltip")).toBeNull();
    await user.click(icon);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Группа пропустит всё накопленное и начнёт с новых сообщений.");
  });
});
