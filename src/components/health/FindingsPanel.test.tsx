import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Annotation, Finding, HealthView, Proposal } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn().mockResolvedValue([]),
      getHealth: vi.fn(),
      runHealthCheck: vi.fn(),
      setFindingIgnored: vi.fn(),
      setAnnotationStatus: vi.fn(),
      deleteAnnotation: vi.fn(),
      deleteAllAnnotations: vi.fn(),
      setProposalStatus: vi.fn(),
      deleteProposal: vi.fn(),
      deleteAllProposals: vi.fn(),
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
    proposals: [],
    ...p,
  };
}

const proposal = (id: string, status: Proposal["status"] = "open"): Proposal => ({
  id,
  severity: "warning",
  title: `Proposal ${id}`,
  rationale: "acks=all\nneeds two replicas",
  target: { kind: "topic", name: "orders" },
  change: { kind: "alterTopicConfigs", topic: "orders", changes: [{ name: "min.insync.replicas", value: "2" }] },
  relatedRuleId: "topic.minIsrOne",
  client: "claude-code",
  createdAtMs: 1,
  updatedAtMs: 1,
  status,
  ...(status === "applied" ? { closedAtMs: Date.now() - 60_000, appliedAsProposed: false } : {}),
  ...(status === "rejected" ? { closedAtMs: Date.now() - 60_000, rejectReason: "позже" } : {}),
});

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "dev-1", environment: "dev" }]);
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("FindingsPanel", () => {
  it("запускает проверку и группирует находки по правилу", async () => {
    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] });
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
    // Подсказка «?» стоит рядом с подписью, а не внутри неё: имя флажка — только подпись.
    await user.click(screen.getByRole("checkbox", { name: "Показать скрытые (1)" }));
    const hidden = (await screen.findByText("Фактор репликации 1 у legacy")).closest("li")!;
    expect(within(hidden).getByRole("button", { name: "Вернуть" })).toBeInTheDocument();
  });

  it("когда все находки скрыты, говорит об этом, а не «находок нет»", async () => {
    const all = view().lastCheck!.findings.map((f) => f.key);
    vi.mocked(api.getHealth).mockResolvedValue(view({ ignored: all }));
    renderWithClient(<FindingsPanel connectionId="c1" />);
    expect(await screen.findByText("Все находки скрыты. Их видно с флажком «Показать скрытые».")).toBeInTheDocument();
    expect(screen.queryByText("Находок нет: правила не нашли проблем.")).not.toBeInTheDocument();
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
    // Пустое состояние под фильтры — у замечаний и у предложений.
    expect(screen.getAllByText("Под фильтры ничего не подходит.")).toHaveLength(2);
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

  it("раздел замечаний без замка: подсказка про MCP и пустой список", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view());
    renderWithClient(<FindingsPanel connectionId="c1" />);
    expect(await screen.findByText("Фактор репликации 1 у orders")).toBeInTheDocument();
    expect(screen.getByText(/LLM-ассистенты, подключённые по MCP, могут оставлять здесь замечания/)).toBeInTheDocument();
    expect(screen.getByText("Замечаний LLM нет.")).toBeInTheDocument();
    expect(screen.queryByText(/Pro/)).not.toBeInTheDocument();
  });

  it("показывает предложения LLM, открывает форму и отклоняет с причиной", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view({ proposals: [proposal("p1"), proposal("p2", "applied"), proposal("p3", "rejected")] }));
    vi.mocked(api.setProposalStatus).mockResolvedValue(view({ proposals: [proposal("p1", "rejected"), proposal("p2", "applied"), proposal("p3", "rejected")] }));
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    const card = (await screen.findByText("Proposal p1")).closest("li")!;
    expect(within(card).getByText("Конфиги топика: min.insync.replicas → 2")).toBeInTheDocument();
    expect(within(card).getByText(/acks=all/)).toHaveClass("note-body");
    expect(within(card).getByText("Предложение LLM · claude-code · min.insync.replicas равен 1")).toBeInTheDocument();
    expect(within(card).getByRole("button", { name: "orders" })).toBeInTheDocument();
    // Закрытые — в свёрнутом разделе со статусами.
    await user.click(screen.getByText("Закрытые (2)"));
    expect(screen.getByText(/применено с правками/)).toBeInTheDocument();
    expect(screen.getByText(/отклонено: позже/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Открыть снова" })).toBeInTheDocument();
    // «Открыть в форме» — вкладка топика с предложением.
    await user.click(within(card).getByRole("button", { name: "Открыть в форме" }));
    const tab = useWorkspace.getState().tabs.find((x) => x.id === "topic:c1:orders");
    expect(tab && "proposal" in tab && tab.proposal?.proposal.id).toBe("p1");
    expect(useWorkspace.getState().activeId).toBe("topic:c1:orders");
    // «Отклонить» спрашивает причину.
    await user.click(within(card).getByRole("button", { name: "Отклонить" }));
    await user.type(within(card).getByRole("textbox", { name: "Причина (необязательно)" }), "позже");
    await user.click(within(card).getByRole("button", { name: "Отклонить предложение" }));
    expect(api.setProposalStatus).toHaveBeenCalledWith("c1", "p1", "rejected", "позже");
    await waitFor(() => expect(screen.getByText("Закрытые (3)")).toBeInTheDocument());
  });

  it("у read-only подключения «Открыть в форме» неактивна", async () => {
    vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "ro", readOnly: true }]);
    vi.mocked(api.getHealth).mockResolvedValue(view({ proposals: [proposal("p1")] }));
    renderWithClient(<FindingsPanel connectionId="c1" />);
    const btn = await screen.findByRole("button", { name: "Открыть в форме" });
    await waitFor(() => expect(btn).toBeDisabled());
    expect(btn).toHaveAttribute("title", "Подключение только для чтения");
  });

  it("фильтр «Правила» прячет предложения; «Удалить все предложения LLM» спрашивает на странице", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(view({ proposals: [proposal("p1")] }));
    vi.mocked(api.deleteAllProposals).mockResolvedValue(view());
    const user = userEvent.setup();
    renderWithClient(<FindingsPanel connectionId="c1" />);
    await screen.findByText("Proposal p1");
    await user.selectOptions(screen.getByRole("combobox", { name: "Источник" }), "rules");
    expect(screen.queryByText("Proposal p1")).not.toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "Источник" }), "llm");
    expect(screen.getByText("Proposal p1")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Удалить все предложения LLM" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Удалить все предложения LLM у этого подключения?");
    await user.click(within(screen.getByRole("alert")).getByRole("button", { name: "Удалить все предложения LLM" }));
    expect(api.deleteAllProposals).toHaveBeenCalledWith("c1");
    await waitFor(() => expect(screen.getByText("Предложений LLM нет.")).toBeInTheDocument());
  });
});
