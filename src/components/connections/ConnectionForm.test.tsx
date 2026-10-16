import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-dialog", () => ({ open: vi.fn() }));

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

import { open as openDialog } from "@tauri-apps/plugin-dialog";
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
        { name: "config", ok: true, message: { key: "messages.connectionTest.configOk", params: {}, text: "Configuration is valid" } },
        {
          name: "tcp",
          ok: true,
          message: {
            key: "messages.connectionTest.tcpOk",
            params: { address: "localhost:9094" },
            text: "TCP connection to localhost:9094 established",
          },
        },
        {
          name: "kafka",
          ok: false,
          message: { key: "errors.kafka.authFailed", params: {}, text: "Authentication failed" },
          error: {
            code: "AUTH_FAILED",
            key: "errors.kafka.authFailed",
            message: "Authentication failed",
            details: "SASL authentication error",
            retryable: false,
          },
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
          tls: { caLocation: null, caFormat: "pem", verifyHostname: true, clientCert: null },
          sasl: { mechanism: "PLAIN", username: "alice" },
        },
      }),
      "secret",
      { keyPassword: null, keystorePassword: null, truststorePassword: null },
    );
    // Тексты шагов приходят ключами и показываются на языке интерфейса.
    expect(await screen.findByText("Ошибка аутентификации")).toBeInTheDocument();
    expect(screen.getByText("Конфигурация корректна")).toBeInTheDocument();
    expect(screen.getByText("TCP-соединение с localhost:9094 установлено")).toBeInTheDocument();
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

  it("клиентский сертификат PKCS#12: выбор файла, формат по расширению, пароль хранилища", async () => {
    vi.mocked(openDialog).mockResolvedValue("/certs/client.p12");
    vi.mocked(api.saveConnection).mockResolvedValue({ ...newProfile(), id: "n", name: "m" });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.type(screen.getByLabelText("Имя"), "m");
    await user.selectOptions(screen.getByLabelText("Протокол"), "SSL");
    await user.click(screen.getByLabelText("Клиентский сертификат (mTLS)"));
    await user.click(screen.getByRole("button", { name: "Выбрать файл: Сертификат клиента" }));
    expect(screen.getByLabelText("Формат клиентского сертификата")).toHaveValue("pkcs12");
    expect(screen.queryByLabelText("Ключ клиента")).not.toBeInTheDocument();
    await user.type(screen.getByLabelText("Пароль хранилища клиента"), "storepass");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        security: {
          protocol: "SSL",
          tls: {
            caLocation: null,
            caFormat: "pem",
            verifyHostname: true,
            clientCert: { format: "pkcs12", certificatePath: "/certs/client.p12", keyPath: "", alias: null },
          },
        },
      }),
      undefined,
      { keyPassword: null, keystorePassword: "storepass", truststorePassword: null },
    );
  });

  it("PEM: поля сертификата и ключа, пароль ключа необязателен; у полей видимые подписи", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Протокол"), "SSL");
    await user.click(screen.getByLabelText("Клиентский сертификат (mTLS)"));
    for (const label of ["Сертификат клиента", "Ключ клиента", "Пароль ключа (если зашифрован)", "Доверенный CA"]) {
      expect(screen.getByText(label, { selector: "label" })).toBeVisible();
    }
  });

  it("существующий профиль: сохранённый пароль можно удалить", async () => {
    vi.mocked(api.saveConnection).mockResolvedValue({ ...newProfile(), id: "c1", name: "x" });
    const initial = {
      ...newProfile(),
      id: "c1",
      name: "x",
      security: {
        protocol: "SSL" as const,
        tls: {
          caLocation: "/ca.jks",
          caFormat: "jks" as const,
          verifyHostname: true,
          clientCert: { format: "jks" as const, certificatePath: "/c.jks", keyPath: null, alias: null },
        },
      },
    };
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    await user.click(screen.getByLabelText("Удалить сохранённый: Пароль ключа (если отличается)"));
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveConnection).toHaveBeenCalledWith(expect.anything(), undefined, {
      keyPassword: "",
      keystorePassword: null,
      truststorePassword: null,
    });
  });
});
