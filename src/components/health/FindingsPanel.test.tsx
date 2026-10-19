import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Annotation, Finding, HealthView, LicenseStatus } from "../../api/types";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getHealth: vi.fn(),
      runHealthCheck: vi.fn(),
      setFindingIgnored: vi.fn(),
      setAnnotationStatus: vi.fn(),
      deleteAnnotation: vi.fn(),
      deleteAllAnnotations: vi.fn(),
      getLicenseStatus: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { FindingsPanel } from "./FindingsPanel";

const rf1 = (name: string): Finding => ({
  key: `topic.replicationFactorOne:topic:${name}`,
  ruleId: "topic.replicationFactorOne",
  severity: "critical",
  target: { kind: "topic", name },
  params: { replicationFactor: 1 },
});
const abandoned: Finding = {
  key: "group.abandoned:group:billing",
  ruleId: "group.abandoned",
  severity: "warning",
  target: { kind: "group", name: "billing" },
  params: { lag: 12 },
};
const note = (id: string, status: Annotation["status"] = "open"): Annotation => ({
  id,
  target: { kind: "topic", name: "orders" },
  severity: "warning",
  title: `Note ${id}`,
  body: "line 1\nline 2",
  relatedRuleId: "topic.replicationFactorOne",
  client: "claude-ai",
  createdAtMs: 1,
  updatedAtMs: 1,
  status,
});

function view(p: Partial<HealthView> = {}): HealthView {
  return {
    lastCheck: {
      runId: "r1",
      startedAtMs: Date.now() - 5 * 60_000,
      durationMs: 1200,
      trigger: { kind: "mcp", client: "claude-ai" },
      environment: "prod",
      incomplete: true,
      scope: { brokers: 3, topicsChecked: 2000, topicsTotal: 2100, groupsChecked: 5, groupsTotal: 5 },
      findings: [rf1("orders"), rf1("legacy"), abandoned],
    },
    ignored: [],
    annotations: [],
    ...p,
  };
}

const license = (tier: "free" | "pro") => ({ tier }) as LicenseStatus;

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getLicenseStatus).mockResolvedValue(license("pro"));
});

describe("FindingsPanel", () => {
  it("запускает проверку и группирует находки по правилу", async () => {
    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] });
    vi.mocked(api.runHealthCheck).mockResolvedValue(view());
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    expect(await screen.findByText("Кластер ещё не проверялся.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Проверить кластер" }));
    expect(api.runHealthCheck).toHaveBeenCalledWith("c1");
    expect(await screen.findByText("Фактор репликации 1")).toBeInTheDocument();
    expect(screen.getByText("объектов: 2")).toBeInTheDocument();
    expect(screen.getByText("Фактор репликации 1 у orders")).toBeInTheDocument();
    expect(screen.getByText("Группа billing пуста, но у неё есть лаг")).toBeInTheDocument();
    expect(screen.getByText(/Последняя проверка: 5 минут назад · claude-ai/)).toBeInTheDocument();
    expect(screen.getByText(/проверено частично/)).toBeInTheDocument();
  });

  it("скрывает находку для объекта и показывает скрытые по запросу", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view());
    vi.mocked(api.setFindingIgnored).mockResolvedValue(view({ ignored: [rf1("legacy").key] }));
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    const item = (await screen.findByText("Фактор репликации 1 у legacy")).closest("li")!;
    await user.click(within(item).getByRole("button", { name: "Скрыть для этого объекта" }));
    expect(api.setFindingIgnored).toHaveBeenCalledWith("c1", rf1("legacy").key, true);
    await waitFor(() => expect(screen.queryByText("Фактор репликации 1 у legacy")).not.toBeInTheDocument());
    await user.click(screen.getByRole("checkbox", { name: /Показать скрытые \(1\)/ }));
    const hidden = (await screen.findByText("Фактор репликации 1 у legacy")).closest("li")!;
    expect(within(hidden).getByRole("button", { name: "Вернуть" })).toBeInTheDocument();
  });

  it("фильтрует по источнику и имени", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view({ annotations: [note("a")] }));
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    await screen.findByText("Note a");
    await user.selectOptions(screen.getByRole("combobox", { name: "Источник" }), "llm");
    expect(screen.queryByText("Фактор репликации 1 у orders")).not.toBeInTheDocument();
    expect(screen.getByText("Note a")).toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "Источник" }), "all");
    await user.type(screen.getByRole("textbox", { name: "Поиск по имени" }), "bill");
    expect(screen.getByText("Группа billing пуста, но у неё есть лаг")).toBeInTheDocument();
    expect(screen.queryByText("Фактор репликации 1 у orders")).not.toBeInTheDocument();
    expect(screen.getByText("Под фильтры ничего не подходит.")).toBeInTheDocument();
  });

  it("закрывает и удаляет замечания LLM", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view({ annotations: [note("a"), note("b", "resolved")] }));
    vi.mocked(api.setAnnotationStatus).mockResolvedValue(view({ annotations: [note("a", "dismissed"), note("b", "resolved")] }));
    vi.mocked(api.deleteAllAnnotations).mockResolvedValue(view());
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    const a = (await screen.findByText("Note a")).closest("li")!;
    expect(within(a).getByText("Замечание LLM · claude-ai · Фактор репликации 1")).toBeInTheDocument();
    expect(within(a).getByText(/line 1/)).toHaveClass("note-body");
    expect(screen.getByText("Закрытые (1)")).toBeInTheDocument();
    await user.click(within(a).getByRole("button", { name: "Не актуально" }));
    expect(api.setAnnotationStatus).toHaveBeenCalledWith("c1", "a", "dismissed");
    await waitFor(() => expect(screen.getByText("Закрытые (2)")).toBeInTheDocument());
    await user.click(screen.getByRole("button", { name: "Удалить все замечания LLM" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Удалить все замечания LLM у этого подключения?");
    await user.click(within(screen.getByRole("alert")).getByRole("button", { name: "Удалить все замечания LLM" }));
    expect(api.deleteAllAnnotations).toHaveBeenCalledWith("c1");
    await waitFor(() => expect(screen.getByText("Замечаний LLM нет.")).toBeInTheDocument());
  });

  it("во Free без замечаний показывает замок Pro", async () => {
    vi.mocked(api.getLicenseStatus).mockResolvedValue(license("free"));
    vi.mocked(api.getHealth).mockResolvedValue(view());
    renderWithClient(<FindingsPanel connectionId="c1" />);
    expect(await screen.findByText(/С Glafira Pro LLM-ассистенты могут оставлять здесь замечания/)).toBeInTheDocument();
    expect(screen.getByText("🔒 Pro")).toBeInTheDocument();
    expect(screen.getByText("Фактор репликации 1 у orders")).toBeInTheDocument();
  });
});
