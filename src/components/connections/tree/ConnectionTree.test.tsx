import { fireEvent, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionFolder, ConnectionProfile } from "../../../api/types";
import { renderWithClient } from "../../../test/render";
import { newProfile } from "../profile";

vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return {
    ...orig,
    api: {
      getLicenseStatus: vi.fn(() => new Promise(() => {})),
      listFolders: vi.fn(),
      listConnections: vi.fn(),
      createFolder: vi.fn(),
      renameFolder: vi.fn(),
      deleteFolder: vi.fn(),
      moveItems: vi.fn(),
    },
  };
});

import { api } from "../../../api/client";
import { useWorkspace } from "../../../state/workspace";
import { ConnectionTree } from "./ConnectionTree";

const FOLDERS: ConnectionFolder[] = [
  { id: "a", name: "Проект А", parentId: null, order: 0 },
  { id: "b", name: "ИФТ", parentId: "a", order: 0 },
];
const CONNS: ConnectionProfile[] = [
  { ...newProfile(), id: "x", name: "kafka-ift", folderId: "b", order: 0 },
  { ...newProfile(), id: "y", name: "local", folderId: null, order: 0 },
];

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  vi.mocked(api.listFolders).mockResolvedValue(FOLDERS);
  vi.mocked(api.listConnections).mockResolvedValue(CONNS);
  useWorkspace.setState({ tabs: [], activeId: null });
});

/** Строка папки (с кнопкой «⋯» и целью перетаскивания) по имени. */
const folderRow = (name: string) => screen.getByRole("button", { name: `Развернуть или свернуть ${name}` }).closest(".tree-folder") as HTMLElement;
const connRow = (name: string) => screen.getByText(name).closest(".tree-conn") as HTMLElement;

function stubDataTransfer() {
  const data = new Map<string, string>();
  return {
    data,
    dropEffect: "none",
    effectAllowed: "all",
    setData: (type: string, value: string) => data.set(type, value),
    getData: (type: string) => data.get(type) ?? "",
    types: [] as string[],
  };
}

async function openFolderMenu(name: string) {
  const user = userEvent.setup();
  await user.click(within(folderRow(name)).getByRole("button", { name: "Ещё действия" }));
  return { user, menu: screen.getByRole("menu") };
}

describe("ConnectionTree", () => {
  it("показывает папки с подключениями и пункты подключения", async () => {
    renderWithClient(<ConnectionTree />);
    expect(await screen.findByText("kafka-ift")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Развернуть или свернуть Проект А" })).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByRole("button", { name: "Развернуть или свернуть ИФТ" })).toBeInTheDocument();
    expect(screen.getByText("local")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Топики" })).toHaveLength(2);
  });

  it("сворачивание папки сохраняется между перерисовками", async () => {
    const user = userEvent.setup();
    const first = renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    await user.click(screen.getByRole("button", { name: "Развернуть или свернуть Проект А" }));
    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Развернуть или свернуть Проект А" })).toHaveAttribute("aria-expanded", "false");
    expect(JSON.parse(localStorage.getItem("glafira.tree.collapsed") ?? "[]")).toEqual(["a"]);
    first.unmount();

    renderWithClient(<ConnectionTree />);
    expect(await screen.findByText("local")).toBeInTheDocument();
    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();
  });

  it("испорченное значение в localStorage не ломает дерево", async () => {
    localStorage.setItem("glafira.tree.collapsed", "{not json");
    renderWithClient(<ConnectionTree />);
    expect(await screen.findByText("kafka-ift")).toBeInTheDocument();
  });

  it("фильтр скрывает несовпавшие, разворачивает папки и сообщает, если ничего нет", async () => {
    localStorage.setItem("glafira.tree.collapsed", JSON.stringify(["a"]));
    const user = userEvent.setup();
    renderWithClient(<ConnectionTree />);
    await screen.findByText("local");
    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();

    const filter = screen.getByRole("searchbox", { name: "Фильтр подключений" });
    await user.type(filter, "ift");
    expect(screen.getByText("kafka-ift")).toBeInTheDocument();
    expect(screen.queryByText("local")).not.toBeInTheDocument();
    expect(screen.queryByText("Ничего не найдено")).not.toBeInTheDocument();

    await user.clear(filter);
    await user.type(filter, "zzz");
    expect(screen.getByText("Ничего не найдено")).toBeInTheDocument();
    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();

    await user.clear(filter);
    expect(screen.getByText("local")).toBeInTheDocument();
    expect(screen.queryByText("kafka-ift")).not.toBeInTheDocument();
  });

  it("подключения видны, даже если папки не загрузились", async () => {
    vi.mocked(api.listFolders).mockRejectedValue(new Error("boom"));
    renderWithClient(<ConnectionTree />);
    expect(await screen.findByText("kafka-ift")).toBeInTheDocument();
    expect(screen.getByText("local")).toBeInTheDocument();
  });

  it("«+ папка» создаёт папку в корне по Enter", async () => {
    vi.mocked(api.createFolder).mockResolvedValue({ id: "n", name: "ИФТ", parentId: null, order: 1 });
    const user = userEvent.setup();
    renderWithClient(<ConnectionTree />);
    await screen.findByText("local");
    await user.click(screen.getByRole("button", { name: "Новая папка" }));
    const input = screen.getByRole("textbox", { name: "Имя папки" });
    expect(input).toHaveFocus();
    await user.type(input, "ИФТ{Enter}");
    expect(api.createFolder).toHaveBeenCalledWith("ИФТ", null);
    expect(await screen.findByText("local")).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "Имя папки" })).not.toBeInTheDocument();
  });

  it("Esc отменяет ввод имени", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionTree />);
    await screen.findByText("local");
    await user.click(screen.getByRole("button", { name: "Новая папка" }));
    await user.type(screen.getByRole("textbox", { name: "Имя папки" }), "x{Escape}");
    expect(screen.queryByRole("textbox", { name: "Имя папки" })).not.toBeInTheDocument();
    expect(api.createFolder).not.toHaveBeenCalled();
  });

  it("ошибка из Rust показывается под полем имени", async () => {
    vi.mocked(api.createFolder).mockRejectedValue({
      code: "INVALID_INPUT",
      key: "errors.folders.nameTaken",
      params: { name: "ИФТ" },
      message: "Folder “ИФТ” already exists here",
      retryable: false,
    });
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    const { user } = await openFolderMenu("Проект А");
    await user.click(screen.getByRole("menuitem", { name: "Новая папка" }));
    const input = screen.getByRole("textbox", { name: "Имя папки" });
    await user.type(input, "ИФТ{Enter}");
    expect(api.createFolder).toHaveBeenCalledWith("ИФТ", "a");
    expect(await screen.findByText("Папка «ИФТ» здесь уже есть")).toBeInTheDocument();
    expect(input).toBeInTheDocument();
  });

  it("меню папки открывается правым кликом и закрывается по Esc", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    fireEvent.contextMenu(folderRow("ИФТ"));
    const menu = screen.getByRole("menu");
    expect(within(menu).getAllByRole("menuitem").map((m) => m.textContent)).toEqual([
      "Новая папка",
      "Новое подключение здесь",
      "Переименовать",
      "Удалить",
    ]);
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
  });

  it("меню закрывается кликом вне его", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    await openFolderMenu("ИФТ");
    await user.click(screen.getByText("local"));
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
  });

  it("переименование через меню", async () => {
    vi.mocked(api.renameFolder).mockResolvedValue({ id: "a", name: "Проект Б", parentId: null, order: 0 });
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    const { user } = await openFolderMenu("Проект А");
    await user.click(screen.getByRole("menuitem", { name: "Переименовать" }));
    const input = screen.getByRole("textbox", { name: "Имя папки" });
    expect(input).toHaveValue("Проект А");
    await user.clear(input);
    await user.type(input, "Проект Б{Enter}");
    expect(api.renameFolder).toHaveBeenCalledWith("a", "Проект Б");
  });

  it("удаление с подтверждением внутри страницы и ошибка «не пуста»", async () => {
    vi.mocked(api.deleteFolder).mockRejectedValueOnce({
      code: "INVALID_INPUT",
      key: "errors.folders.notEmpty",
      message: "Folder is not empty",
      retryable: false,
    });
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    const { user } = await openFolderMenu("Проект А");
    await user.click(screen.getByRole("menuitem", { name: "Удалить" }));
    expect(api.deleteFolder).not.toHaveBeenCalled();
    const group = screen.getByRole("group", { name: "Удалить папку «Проект А»?" });
    await user.click(within(group).getByRole("button", { name: "Удалить" }));
    expect(api.deleteFolder).toHaveBeenCalledWith("a");
    expect(await within(group).findByText(/Папка не пуста/)).toBeInTheDocument();

    await user.click(within(group).getByRole("button", { name: "Отмена" }));
    expect(screen.queryByRole("group", { name: "Удалить папку «Проект А»?" })).not.toBeInTheDocument();
  });

  it("удаление пустой папки", async () => {
    vi.mocked(api.deleteFolder).mockResolvedValue(undefined);
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    const { user } = await openFolderMenu("ИФТ");
    await user.click(screen.getByRole("menuitem", { name: "Удалить" }));
    await user.click(within(screen.getByRole("group", { name: "Удалить папку «ИФТ»?" })).getByRole("button", { name: "Удалить" }));
    expect(api.deleteFolder).toHaveBeenCalledWith("b");
    await vi.waitFor(() => expect(screen.queryByRole("group")).not.toBeInTheDocument());
  });

  it("«Новое подключение здесь» открывает форму с папкой", async () => {
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    const { user } = await openFolderMenu("ИФТ");
    await user.click(screen.getByRole("menuitem", { name: "Новое подключение здесь" }));
    expect(useWorkspace.getState().tabs).toEqual([
      expect.objectContaining({ kind: "connection-form", connectionId: null, folderId: "b" }),
    ]);
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
  });

  it("перетаскивание подключения на папку переносит его внутрь", async () => {
    vi.mocked(api.moveItems).mockResolvedValue(undefined);
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    const dataTransfer = stubDataTransfer();
    fireEvent.dragStart(connRow("local"), { dataTransfer });
    const notPrevented = fireEvent.dragOver(folderRow("ИФТ"), { dataTransfer });
    expect(notPrevented).toBe(false);
    expect(folderRow("ИФТ")).toHaveClass("drop-into");
    fireEvent.drop(folderRow("ИФТ"), { dataTransfer });
    expect(api.moveItems).toHaveBeenCalledWith([{ kind: "connection", id: "y" }], "b", null);
  });

  it("перетаскивание на подключение ставит перед ним в его папку", async () => {
    vi.mocked(api.moveItems).mockResolvedValue(undefined);
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    const dataTransfer = stubDataTransfer();
    fireEvent.dragStart(connRow("local"), { dataTransfer });
    fireEvent.dragOver(connRow("kafka-ift"), { dataTransfer });
    fireEvent.drop(connRow("kafka-ift"), { dataTransfer });
    expect(api.moveItems).toHaveBeenCalledWith([{ kind: "connection", id: "y" }], "b", { kind: "connection", id: "x" });
  });

  it("папку нельзя бросить в её потомка", async () => {
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    const dataTransfer = stubDataTransfer();
    fireEvent.dragStart(folderRow("Проект А"), { dataTransfer });
    const notPrevented = fireEvent.dragOver(folderRow("ИФТ"), { dataTransfer });
    expect(notPrevented).toBe(true);
    expect(folderRow("ИФТ")).not.toHaveClass("drop-into");
    fireEvent.drop(folderRow("ИФТ"), { dataTransfer });
    expect(api.moveItems).not.toHaveBeenCalled();
  });

  it("перетаскивание на заголовок панели переносит в корень", async () => {
    vi.mocked(api.moveItems).mockResolvedValue(undefined);
    renderWithClient(<ConnectionTree />);
    await screen.findByText("kafka-ift");
    const dataTransfer = stubDataTransfer();
    const header = screen.getByText("Подключения").closest(".sidebar-header") as HTMLElement;
    fireEvent.dragStart(folderRow("ИФТ"), { dataTransfer });
    expect(fireEvent.dragOver(header, { dataTransfer })).toBe(false);
    fireEvent.drop(header, { dataTransfer });
    expect(api.moveItems).toHaveBeenCalledWith([{ kind: "folder", id: "b" }], null, null);
  });
});
