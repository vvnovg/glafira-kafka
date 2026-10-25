import { QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    api: {
      ...actual.api,
      startBrowse: vi.fn(() => new Promise<string>(() => {})),
      stopSession: vi.fn(async () => true),
    },
  };
});

import { api } from "../../api/client";
import { BrowseForm } from "./BrowseForm";
import { BrowsePanel } from "./BrowsePanel";
import { defaultBrowseForm } from "./params";

describe("BrowseForm", () => {
  it("у полей фильтров есть видимые подписи, не исчезающие при вводе", () => {
    render(<BrowseForm form={{ ...defaultBrowseForm, jsonPath: '$.status == "FAILED"' }} onChange={() => {}} />);
    for (const label of ["Ключ содержит", "Значение содержит", "Заголовок", "Значение заголовка", "JSONPath"]) {
      expect(screen.getByText(label)).toBeVisible();
      expect(screen.getByLabelText(label)).toBeInTheDocument();
    }
    expect(screen.getByLabelText("JSONPath")).toHaveValue('$.status == "FAILED"');
  });

  it("подсказки «?» у начальной позиции, лимита и формата", async () => {
    const user = userEvent.setup();
    render(<BrowseForm form={defaultBrowseForm} onChange={() => {}} />);
    expect(screen.getByRole("button", { name: "Подсказка: Начальная позиция" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Подсказка: Формат" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Подсказка: Лимит" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Сколько сообщений показать. Большие значения замедляют чтение.");
  });

  it("форма заполняется из журнала MCP, чтение не начинается само", () => {
    const prefill = {
      form: { startType: "offset" as const, offset: 7, partitions: "2", valuePattern: "FAILED", regex: true },
      inexact: ["flags" as const],
      nonce: 1,
    };
    renderWithClient(<BrowsePanel connectionId="c" topic="t" prefill={prefill} />);
    expect(screen.getByLabelText("Значение содержит")).toHaveValue("FAILED");
    expect(screen.getByLabelText("regex")).toBeChecked();
    expect(screen.getByLabelText("Партиции")).toHaveValue("2");
    expect(screen.getByText(/Фильтры перенесены неточно/)).toBeInTheDocument();
    expect(api.startBrowse).not.toHaveBeenCalled();
  });

  it("повторное «Открыть» (новый nonce) заполняет форму заново: ошибка формы и пометки — от нового заполнения", async () => {
    const user = userEvent.setup();
    const first = {
      // Начало «С времени» без времени — ошибка формы по «Читать».
      form: { startType: "timestamp" as const, timestamp: "", valuePattern: "A" },
      inexact: ["flags" as const, "limit" as const],
      nonce: 1,
    };
    const { rerender, client } = renderWithClient(<BrowsePanel connectionId="c" topic="t" prefill={first} />);
    expect(screen.getByText(/Фильтры перенесены неточно/)).toBeInTheDocument();
    expect(screen.getByText(/Лимит перенесён неточно/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Читать" }));
    expect(screen.getByText("Укажите корректное время начала")).toBeInTheDocument();
    await user.clear(screen.getByLabelText("Значение содержит"));
    await user.type(screen.getByLabelText("Значение содержит"), "edited");
    const second = { form: { startType: "offset" as const, offset: 3, valuePattern: "B" }, inexact: [], nonce: 2 };
    rerender(
      <QueryClientProvider client={client}>
        <BrowsePanel connectionId="c" topic="t" prefill={second} />
      </QueryClientProvider>,
    );
    expect(screen.getByLabelText("Значение содержит")).toHaveValue("B");
    expect(screen.queryByText("Укажите корректное время начала")).not.toBeInTheDocument();
    expect(screen.queryByText(/Фильтры перенесены неточно/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Лимит перенесён неточно/)).not.toBeInTheDocument();
    expect(api.startBrowse).not.toHaveBeenCalled();
  });

  it("подсказка «?» у кнопки «Читать» в панели просмотра", () => {
    renderWithClient(<BrowsePanel connectionId="c" topic="t" />);
    expect(screen.getByRole("button", { name: "Подсказка: Читать" })).toBeInTheDocument();
  });
});
