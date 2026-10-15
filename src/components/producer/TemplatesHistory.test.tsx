import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { HistoryEntry, Template } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { DEFAULT_SETTINGS, emptyProducerForm } from "./form";

const message = { topic: "orders", partition: null, key: { format: "string" as const, text: "k" }, value: { format: "json" as const, text: "{}" }, headers: [] };
const tpl: Template = { id: "t1", name: "заказ", connectionId: null, message, settings: DEFAULT_SETTINGS };
const history: HistoryEntry[] = [
  { id: "h1", sentAt: 0, message: { ...message, key: { format: "string", text: "old" } }, settings: DEFAULT_SETTINGS, result: { partition: 0, offset: 7, timestamp: 0 }, truncated: false },
  { id: "h2", sentAt: 0, message, settings: DEFAULT_SETTINGS, result: { partition: 0, offset: 8, timestamp: 0 }, truncated: true },
];

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listTemplates: vi.fn(),
      saveTemplate: vi.fn(async (t: Template) => ({ ...t, id: "new" })),
      deleteTemplate: vi.fn().mockResolvedValue(undefined),
      listHistory: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { HistoryList } from "./HistoryList";
import { TemplatesBar } from "./TemplatesBar";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listTemplates).mockResolvedValue([tpl]);
  vi.mocked(api.listHistory).mockResolvedValue(history);
});

describe("TemplatesBar", () => {
  it("загружает и сохраняет шаблон", async () => {
    const user = userEvent.setup();
    const onLoad = vi.fn();
    renderWithClient(<TemplatesBar connectionId="c" form={emptyProducerForm("orders")} onLoad={onLoad} />);
    await screen.findByRole("option", { name: /заказ/ });
    await user.selectOptions(screen.getByLabelText("Шаблон"), "t1");
    expect(onLoad).toHaveBeenCalledWith(expect.objectContaining({ topic: "orders", key: { format: "string", text: "k" } }));
    await user.click(screen.getByRole("button", { name: "Сохранить как шаблон" }));
    await user.type(screen.getByLabelText("Имя шаблона"), "новый");
    await user.click(screen.getByLabelText("Для всех подключений"));
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveTemplate).toHaveBeenCalledWith(expect.objectContaining({ id: "", name: "новый", connectionId: null }));
  });

  it("удаляет шаблон только после подтверждения на странице", async () => {
    const user = userEvent.setup();
    renderWithClient(<TemplatesBar connectionId="c" form={emptyProducerForm()} onLoad={vi.fn()} />);
    await screen.findByRole("option", { name: /заказ/ });
    await user.selectOptions(screen.getByLabelText("Шаблон"), "t1");
    await user.click(screen.getByRole("button", { name: "удалить шаблон" }));
    expect(api.deleteTemplate).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    expect(api.deleteTemplate).toHaveBeenCalledWith("t1");
  });
});

describe("HistoryList", () => {
  it("повторяет запись, усечённую — нельзя", async () => {
    const user = userEvent.setup();
    const onRepeat = vi.fn();
    renderWithClient(<HistoryList connectionId="c" onRepeat={onRepeat} />);
    const buttons = await screen.findAllByRole("button", { name: "Повторить" });
    expect(buttons[1]).toBeDisabled();
    expect(buttons[1]).toHaveAttribute("title", expect.stringContaining("64 КБ"));
    await user.click(buttons[0]);
    expect(onRepeat).toHaveBeenCalledWith(expect.objectContaining({ key: { format: "string", text: "old" } }));
  });

  it("повтор — точная копия: {{…}} из истории не отрисовывается заново", async () => {
    const user = userEvent.setup();
    const onRepeat = vi.fn();
    vi.mocked(api.listHistory).mockResolvedValue([
      {
        ...history[0],
        message: {
          ...message,
          key: { format: "string", text: "{{uuid}}" },
          value: { format: "json", text: '{"a":"{{env HOME}}"}' },
          headers: [{ name: "h", value: "{{seq}}", format: "string" }],
        },
      },
    ]);
    renderWithClient(<HistoryList connectionId="c" onRepeat={onRepeat} />);
    await user.click(await screen.findByRole("button", { name: "Повторить" }));
    expect(onRepeat).toHaveBeenCalledWith(
      expect.objectContaining({
        key: { format: "string", text: "\\{{uuid}}" },
        value: { format: "json", text: '{"a":"\\{{env HOME}}"}' },
        headers: [{ name: "h", value: "\\{{seq}}", format: "string" }],
      }),
    );
  });

  it("неизвестный offset показывает «—» с причиной", async () => {
    vi.mocked(api.listHistory).mockResolvedValue([
      { ...history[0], result: { partition: 0, offset: null, timestamp: 0 } },
    ]);
    renderWithClient(<HistoryList connectionId="c" onRepeat={vi.fn()} />);
    const dash = await screen.findByText("—");
    expect(dash).toHaveAttribute("title", expect.any(String));
  });
});
