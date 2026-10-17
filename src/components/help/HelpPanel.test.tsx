import { act, fireEvent, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { i18n } from "../../i18n";
import { renderWithClient } from "../../test/render";
import { HelpPanel } from "./HelpPanel";

const openUrl = vi.fn();
vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: (url: string) => openUrl(url) }));

vi.mock("../../guide/chapters", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../guide/chapters")>();
  return {
    ...actual,
    loadGuide: vi.fn().mockResolvedValue({
      "getting-started":
        "# Начало работы\n\nОткройте [[ui:sidebar.settings]].\n\n## Первое подключение {#first-connection}\n\nСм. [mTLS](connections.md#mtls), [сайт](https://example.com) и [файл](../admin.md).\n",
      connections: "# Подключения\n\n## Простое {#plain}\n\nАдрес брокера.\n\n## mTLS {#mtls}\n\nНужен сертификат клиента.\n",
    }),
  };
});

const EMPTY = {};

describe("HelpPanel", () => {
  beforeEach(() => {
    openUrl.mockReset();
    Element.prototype.scrollIntoView = vi.fn();
  });

  it("shows the contents and the first chapter", async () => {
    renderWithClient(<HelpPanel spec={EMPTY} />);
    expect(await screen.findByRole("heading", { level: 1, name: "Начало работы" })).toBeInTheDocument();
    const toc = screen.getByRole("navigation", { name: "Содержание" });
    expect(toc).toHaveTextContent("Начало работы");
    expect(toc).toHaveTextContent("Подключения");
    expect(toc).toHaveTextContent("Первое подключение");
    expect(screen.getByRole("heading", { level: 2, name: "Первое подключение" })).toHaveAttribute("id", "first-connection");
    expect(screen.getByText("Настройки").tagName).toBe("STRONG");
  });

  it("opens the chapter from the spec", async () => {
    renderWithClient(<HelpPanel spec={{ chapter: "connections", anchor: "mtls" }} />);
    expect(await screen.findByRole("heading", { level: 1, name: "Подключения" })).toBeInTheDocument();
    await waitFor(() => expect(Element.prototype.scrollIntoView).toHaveBeenCalled());
  });

  it("follows an internal link to the chapter and anchor", async () => {
    const { container } = renderWithClient(<HelpPanel spec={EMPTY} />);
    fireEvent.click(await screen.findByRole("link", { name: "mTLS" }));
    expect(await screen.findByRole("heading", { level: 1, name: "Подключения" })).toBeInTheDocument();
    const target = container.querySelector('[id="mtls"]');
    expect(target).not.toBeNull();
    await waitFor(() => expect(Element.prototype.scrollIntoView).toHaveBeenCalled());
    expect(openUrl).not.toHaveBeenCalled();
  });

  it("opens https links in the system browser and leaves other links inactive", async () => {
    renderWithClient(<HelpPanel spec={EMPTY} />);
    fireEvent.click(await screen.findByRole("link", { name: "сайт" }));
    expect(openUrl).toHaveBeenCalledWith("https://example.com");
    expect(screen.queryByRole("link", { name: "файл" })).toBeNull();
    expect(screen.getByText("файл")).toBeInTheDocument();
  });

  it("searches and jumps to a result", async () => {
    renderWithClient(<HelpPanel spec={EMPTY} />);
    await screen.findByRole("heading", { level: 1, name: "Начало работы" });
    const input = screen.getByRole("searchbox", { name: "Поиск по руководству" });
    fireEvent.change(input, { target: { value: "СЕРТИФИКАТ" } });
    const hit = screen.getByRole("button", { name: /Подключения › mTLS/ });
    fireEvent.click(hit);
    expect(await screen.findByRole("heading", { level: 1, name: "Подключения" })).toBeInTheDocument();
    fireEvent.change(input, { target: { value: "zzzz" } });
    expect(screen.getByText("Ничего не найдено")).toBeInTheDocument();
  });

  it("re-renders labels when the language changes", async () => {
    renderWithClient(<HelpPanel spec={EMPTY} />);
    expect(await screen.findByText("Настройки")).toBeInTheDocument();
    await act(async () => {
      await i18n.changeLanguage("en");
    });
    expect(await screen.findByText("Settings")).toBeInTheDocument();
    await act(async () => {
      await i18n.changeLanguage("ru");
    });
    expect(await screen.findByText("Настройки")).toBeInTheDocument();
  });
});
