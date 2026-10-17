import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionFolder, ConnectionProfile } from "../../../api/types";
import { renderWithClient } from "../../../test/render";
import { newProfile } from "../profile";

vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return {
    ...orig,
    api: { getLicenseStatus: vi.fn(() => new Promise(() => {})), listFolders: vi.fn(), listConnections: vi.fn() },
  };
});

import { api } from "../../../api/client";
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
});

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
});
