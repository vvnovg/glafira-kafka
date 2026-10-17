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

  it("подсказка «?» у кнопки «Читать» в панели просмотра", () => {
    renderWithClient(<BrowsePanel connectionId="c" topic="t" />);
    expect(screen.getByRole("button", { name: "Подсказка: Читать" })).toBeInTheDocument();
  });
});
