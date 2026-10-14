import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      testConnection: vi.fn(),
      saveConnection: vi.fn(),
      deleteConnection: vi.fn(),
      listConnections: vi.fn().mockResolvedValue([]),
    },
  };
});

import { api } from "../../api/client";
import { useWorkspace } from "../../state/workspace";
import { ConnectionForm } from "./ConnectionForm";
import { newProfile } from "./profile";

beforeEach(() => {
  vi.clearAllMocks();
});

describe("ConnectionForm", () => {
  it("собирает профиль SASL_SSL и показывает шаги проверки", async () => {
    vi.mocked(api.testConnection).mockResolvedValue({
      ok: false,
      steps: [
        { name: "config", ok: true, message: "Конфигурация корректна" },
        { name: "tcp", ok: true, message: "TCP-соединение установлено" },
        {
          name: "kafka",
          ok: false,
          message: "Ошибка аутентификации",
          error: { code: "AUTH_FAILED", message: "Ошибка аутентификации", details: "SASL authentication error", retryable: false },
        },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);

    await user.type(screen.getByLabelText("Имя"), "dev");
    await user.selectOptions(screen.getByLabelText("Протокол"), "SASL_SSL");
    await user.type(screen.getByLabelText("Пользователь"), "alice");
    await user.type(screen.getByLabelText("Пароль"), "secret");
    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));

    expect(api.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "dev",
        security: {
          protocol: "SASL_SSL",
          tls: { caLocation: null, verifyHostname: true },
          sasl: { mechanism: "PLAIN", username: "alice" },
        },
      }),
      "secret",
    );
    expect(await screen.findByText("Ошибка аутентификации")).toBeInTheDocument();
    expect(screen.getByText("SASL authentication error")).toBeInTheDocument();
  });

  it("для нового prod-подключения включает read-only", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Окружение"), "prod");
    expect(screen.getByLabelText("Только чтение")).toBeChecked();
  });

  it("невалидные доп. свойства показывают ошибку и не вызывают сохранение", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={{ ...newProfile(), name: "x" }} tabId="t" />);
    await user.type(screen.getByLabelText("Доп. свойства librdkafka"), "broken");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(await screen.findByText("Строка 1: ожидается key=value")).toBeInTheDocument();
    expect(api.saveConnection).not.toHaveBeenCalled();
  });

  it("удаление закрывает вкладки подключения и сбрасывает его кэш", async () => {
    vi.mocked(api.deleteConnection).mockResolvedValue(undefined);
    useWorkspace.setState({ tabs: [], activeId: null });
    const { open } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "c1" });
    open({ kind: "topic", connectionId: "c1", topic: "t" });
    open({ kind: "cluster", connectionId: "c2" });
    open({ kind: "connection-form", connectionId: "c1" });
    const user = userEvent.setup();
    const { client } = renderWithClient(
      <ConnectionForm initial={{ ...newProfile(), id: "c1", name: "x" }} tabId="connection-form:c1" />,
    );
    client.setQueryData(["topics", "c1"], []);
    client.setQueryData(["topics", "c2"], []);
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    // Подтверждение — внутри формы: нативный window.confirm в Tauri 2 не показывается.
    expect(api.deleteConnection).not.toHaveBeenCalled();
    expect(screen.getByText("Удалить подключение «x»?")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Да, удалить" }));
    await vi.waitFor(() => expect(useWorkspace.getState().tabs.map((t) => t.id)).toEqual(["cluster:c2"]));
    expect(api.deleteConnection).toHaveBeenCalledWith("c1");
    expect(client.getQueryData(["topics", "c1"])).toBeUndefined();
    expect(client.getQueryData(["topics", "c2"])).toEqual([]);
  });

  it("отмена удаления ничего не удаляет", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={{ ...newProfile(), id: "c1", name: "x" }} tabId="connection-form:c1" />);
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    await user.click(screen.getByRole("button", { name: "Отмена" }));
    expect(screen.queryByText("Удалить подключение «x»?")).not.toBeInTheDocument();
    expect(api.deleteConnection).not.toHaveBeenCalled();
  });
});
