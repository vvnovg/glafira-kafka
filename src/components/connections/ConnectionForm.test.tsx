import { QueryClientProvider } from "@tanstack/react-query";
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
      listFolders: vi.fn().mockResolvedValue([]),
      moveItems: vi.fn(),
    },
  };
});

import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { api } from "../../api/client";
import { useWorkspace } from "../../state/workspace";
import type { ConnectionFolder } from "../../api/types";
import { ConnectionForm } from "./ConnectionForm";
import { ConnectionFormTab } from "./ConnectionFormTab";
import { newProfile } from "./profile";

const FOLDERS: ConnectionFolder[] = [
  { id: "a", name: "Проект А", parentId: null, order: 0 },
  { id: "b", name: "ИФТ", parentId: "a", order: 0 },
];

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listFolders).mockResolvedValue([]);
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

  it("prod: «Только чтение» включён по умолчанию, доступен и снимается без ключа", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Окружение"), "prod");
    const readOnly = screen.getByLabelText("Только чтение");
    expect(readOnly).toBeChecked();
    expect(readOnly).toBeEnabled();
    await user.click(readOnly);
    expect(readOnly).not.toBeChecked();
  });

  it("существующий prod-профиль: «Только чтение» и опция prod не заблокированы", () => {
    const initial = { ...newProfile(), id: "p", name: "p", environment: "prod" as const, readOnly: true };
    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    expect(screen.getByLabelText("Только чтение")).toBeEnabled();
    expect(screen.getByRole("option", { name: "prod" })).toBeEnabled();
  });

  it("флажок «Доступ для LLM» сохраняется в профиле", async () => {
    vi.mocked(api.saveConnection).mockResolvedValue({ ...newProfile(), id: "n", name: "m", llmAccess: "metadata" });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={{ ...newProfile(), name: "m" }} tabId="t" />);
    const box = screen.getByLabelText("Доступ для LLM (только метаданные)");
    expect(box).not.toBeChecked();
    await user.click(box);
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveConnection).toHaveBeenCalledWith(expect.objectContaining({ llmAccess: "metadata" }), undefined, undefined);
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

  it("подсказки «?» у полей формы; открытая подсказка показывает текст", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    for (const label of [
      "Окружение",
      "Только чтение",
      "Bootstrap-серверы",
      "Протокол",
      "Таймаут запроса, мс",
      "Доп. свойства librdkafka",
      "Проверить подключение",
    ]) {
      expect(screen.getByRole("button", { name: `Подсказка: ${label}` })).toBeInTheDocument();
    }
    // Значок рядом с подписью, а не внутри: доступное имя поля не меняется.
    expect(screen.getByLabelText("Bootstrap-серверы")).toHaveAttribute("id", "cf-bootstrap");
    await user.click(screen.getByRole("button", { name: "Подсказка: Bootstrap-серверы" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Один или несколько host:port через запятую. Остальные брокеры клиент найдёт сам.",
    );
  });

  it("подсказки SASL и TLS появляются вместе с полями", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    expect(screen.queryByRole("button", { name: "Подсказка: Доверенный CA" })).not.toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Протокол"), "SASL_SSL");
    await user.click(screen.getByLabelText("Клиентский сертификат (mTLS)"));
    for (const label of [
      "Механизм SASL",
      "Доверенный CA",
      "Проверять hostname",
      "Клиентский сертификат (mTLS)",
      "Пароль ключа (если зашифрован)",
    ]) {
      expect(screen.getByRole("button", { name: `Подсказка: ${label}` })).toBeInTheDocument();
    }
  });

  it("подсказки формата CA, alias и пароля ключа JKS", () => {
    const initial = {
      ...newProfile(),
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
    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    for (const label of ["Формат CA", "Alias ключа", "Пароль ключа (если отличается)"]) {
      expect(screen.getByRole("button", { name: `Подсказка: ${label}` })).toBeInTheDocument();
    }
  });

  it("поле «Папка»: новое подключение уходит с выбранной папкой", async () => {
    vi.mocked(api.listFolders).mockResolvedValue(FOLDERS);
    vi.mocked(api.saveConnection).mockResolvedValue({ ...newProfile(), id: "n", name: "dev", folderId: "b" });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    const select = screen.getByLabelText("Папка");
    await screen.findByRole("option", { name: "Проект А / ИФТ" });
    expect(select).toHaveValue("");
    expect(screen.getAllByRole("option", { name: /Проект А|корень/ }).map((o) => o.textContent)).toEqual([
      "(корень)",
      "Проект А",
      "Проект А / ИФТ",
    ]);
    await user.type(screen.getByLabelText("Имя"), "dev");
    await user.selectOptions(select, "Проект А / ИФТ");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(api.saveConnection).toHaveBeenCalledWith(expect.objectContaining({ name: "dev", folderId: "b" }), undefined, undefined);
    expect(api.moveItems).not.toHaveBeenCalled();
  });

  it("поле «Папка»: смена папки существующего подключения переносит его после сохранения", async () => {
    vi.mocked(api.listFolders).mockResolvedValue(FOLDERS);
    const initial = { ...newProfile(), id: "c1", name: "x", folderId: "b", order: 3 };
    vi.mocked(api.saveConnection).mockResolvedValue(initial);
    vi.mocked(api.moveItems).mockResolvedValue(undefined);
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    await screen.findByRole("option", { name: "Проект А / ИФТ" });
    expect(screen.getByLabelText("Папка")).toHaveValue("b");
    await user.selectOptions(screen.getByLabelText("Папка"), "(корень)");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await vi.waitFor(() => expect(api.moveItems).toHaveBeenCalledWith([{ kind: "connection", id: "c1" }], null, null));
    expect(vi.mocked(api.saveConnection).mock.invocationCallOrder[0]).toBeLessThan(vi.mocked(api.moveItems).mock.invocationCallOrder[0]);
  });

  it("поле «Папка»: без смены папки перенос не вызывается", async () => {
    vi.mocked(api.listFolders).mockResolvedValue(FOLDERS);
    const initial = { ...newProfile(), id: "c1", name: "x", folderId: "b", order: 3 };
    vi.mocked(api.saveConnection).mockResolvedValue(initial);
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    await screen.findByRole("option", { name: "Проект А / ИФТ" });
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await vi.waitFor(() => expect(api.saveConnection).toHaveBeenCalled());
    expect(api.moveItems).not.toHaveBeenCalled();
  });

  it("поле «Папка»: перенос в дереве при открытой форме не откатывается сохранением", async () => {
    vi.mocked(api.listFolders).mockResolvedValue(FOLDERS);
    const initial = { ...newProfile(), id: "c1", name: "x", folderId: "a", order: 0 };
    vi.mocked(api.saveConnection).mockResolvedValue({ ...initial, folderId: "b" });
    const user = userEvent.setup();
    const { rerender, client } = renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    await screen.findByRole("option", { name: "Проект А / ИФТ" });
    // Подключение перетащили в «ИФТ»: после обновления списка форма получает новый initial.
    rerender(
      <QueryClientProvider client={client}>
        <ConnectionForm initial={{ ...initial, folderId: "b" }} tabId="t" />
      </QueryClientProvider>,
    );
    expect(screen.getByLabelText("Папка")).toHaveValue("b");
    await user.clear(screen.getByLabelText("Имя"));
    await user.type(screen.getByLabelText("Имя"), "y");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await vi.waitFor(() => expect(api.saveConnection).toHaveBeenCalled());
    expect(api.moveItems).not.toHaveBeenCalled();
  });

  it("вкладка новой формы предвыбирает папку, из которой её открыли", async () => {
    vi.mocked(api.listFolders).mockResolvedValue(FOLDERS);
    renderWithClient(<ConnectionFormTab connectionId={null} folderId="b" tabId="t" />);
    await screen.findByRole("option", { name: "Проект А / ИФТ" });
    expect(screen.getByLabelText("Папка")).toHaveValue("b");
  });

  it("поле «Папка»: неизвестная папка считается корнем и не вызывает перенос", async () => {
    vi.mocked(api.listFolders).mockResolvedValue(FOLDERS);
    const initial = { ...newProfile(), id: "c1", name: "x", folderId: "gone", order: 0 };
    vi.mocked(api.saveConnection).mockResolvedValue(initial);
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
    await screen.findByRole("option", { name: "Проект А / ИФТ" });
    expect(screen.getByLabelText("Папка")).toHaveValue("");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    await vi.waitFor(() => expect(api.saveConnection).toHaveBeenCalledWith(expect.objectContaining({ folderId: null }), undefined, undefined));
    expect(api.moveItems).not.toHaveBeenCalled();
  });

  it("поле «Папка»: ошибка переноса после сохранения показывается, вкладка остаётся открытой", async () => {
    vi.mocked(api.listFolders).mockResolvedValue(FOLDERS);
    const initial = { ...newProfile(), id: "c1", name: "x", folderId: "b", order: 0 };
    vi.mocked(api.saveConnection).mockResolvedValue(initial);
    vi.mocked(api.moveItems).mockRejectedValue({
      code: "INVALID_INPUT",
      key: "errors.folders.notFound",
      params: { id: "a" },
      message: "Folder a not found",
      retryable: false,
    });
    useWorkspace.setState({ tabs: [], activeId: null });
    useWorkspace.getState().open({ kind: "connection-form", connectionId: "c1" });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={initial} tabId="connection-form:c1" />);
    await screen.findByRole("option", { name: "Проект А / ИФТ" });
    await user.selectOptions(screen.getByLabelText("Папка"), "Проект А");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(await screen.findByText("Папка a не найдена")).toBeInTheDocument();
    expect(useWorkspace.getState().tabs.map((t) => t.id)).toEqual(["connection-form:c1"]);
  });
});
