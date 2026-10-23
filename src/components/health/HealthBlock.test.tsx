import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactElement } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { HealthView } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn().mockResolvedValue([]),
      getHealth: vi.fn(),
      setFindingIgnored: vi.fn(),
      setAnnotationStatus: vi.fn(),
      deleteAnnotation: vi.fn(),
      setProposalStatus: vi.fn(),
      deleteProposal: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { HealthBadge, HealthBlock } from "./HealthBlock";

const view: HealthView = {
  lastCheck: {
    runId: "r1",
    startedAtMs: 1,
    durationMs: 1,
    trigger: { kind: "ui" },
    environment: "prod",
    incomplete: false,
    scope: { brokers: 1, topicsChecked: 2, topicsTotal: 2, groupsChecked: 0, groupsTotal: 0 },
    findings: [
      { key: "k1", ruleId: "topic.replicationFactorOne", severity: "critical", target: { kind: "topic", name: "orders" }, params: {} },
      { key: "k2", ruleId: "partition.notPreferredLeader", severity: "info", target: { kind: "cluster" }, params: { count: 1, examples: ["orders-0"] } },
      { key: "k3", ruleId: "topic.minIsrOne", severity: "warning", target: { kind: "topic", name: "audit" }, params: { replicationFactor: 3, source: "Default" } },
    ],
  },
  ignored: ["k3"],
  annotations: [
    {
      id: "n1",
      target: { kind: "topic", name: "orders" },
      severity: "warning",
      title: "Orders is not replicated",
      body: "Move it to 3 replicas before the release.",
      client: null,
      createdAtMs: 1,
      updatedAtMs: 1,
      status: "open",
    },
  ],
  proposals: [],
};

const isTopic = (name: string) => (x: { kind: string; name?: string }) => x.kind === "topic" && x.name === name;

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getHealth).mockResolvedValue(view);
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("HealthBlock", () => {
  it("показывает находки и замечания объекта и ведёт на вкладку «Замечания»", async () => {
    const user = userEvent.setup();
    renderWithClient(<HealthBlock connectionId="c1" match={isTopic("orders")} />);
    const block = await screen.findByRole("region", { name: "Замечания" });
    expect(within(block).getByText("Фактор репликации 1 у orders")).toBeInTheDocument();
    expect(within(block).getByText("Orders is not replicated")).toBeInTheDocument();
    expect(within(block).getByText("Замечание LLM · Неизвестный MCP-клиент")).toBeInTheDocument();
    await user.click(within(block).getByRole("button", { name: "Все замечания" }));
    expect(useWorkspace.getState().activeId).toBe("findings:c1");
  });

  it("не показывается, если по объекту ничего нет (скрытые не считаются)", async () => {
    // Два блока в одном дереве: первый дождётся данных, значит, ко второму они тоже дошли — и он остался пустым.
    renderWithClient(
      <>
        <HealthBlock connectionId="c1" match={isTopic("orders")} />
        <HealthBlock connectionId="c1" match={isTopic("audit")} />
      </>,
    );
    const block = await screen.findByRole("region", { name: "Замечания" });
    expect(within(block).getByText("Фактор репликации 1 у orders")).toBeInTheDocument();
    expect(screen.getAllByRole("region", { name: "Замечания" })).toHaveLength(1);
  });
});

describe("HealthBlock: предложения LLM", () => {
  const proposal = (id: string, name: string, status: "open" | "applied"): HealthView["proposals"][number] => ({
    id,
    severity: "critical",
    title: `More partitions for ${name}`,
    rationale: "r",
    target: { kind: "topic", name },
    change: { kind: "addPartitions", topic: name, newTotal: 6 },
    client: "claude-code",
    createdAtMs: 1,
    updatedAtMs: 1,
    status,
  });

  it("показывает открытые предложения объекта, закрытые — нет, и считает их в значке", async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      ...view,
      proposals: [proposal("p1", "orders", "open"), proposal("p2", "orders", "applied"), proposal("p3", "audit", "open")],
    });
    vi.mocked(api.deleteProposal).mockResolvedValue({ ...view, proposals: [] });
    const user = userEvent.setup();
    renderWithClient(
      <>
        <HealthBadge connectionId="c1" match={isTopic("orders")} />
        <HealthBlock connectionId="c1" match={isTopic("orders")} />
      </>,
    );
    const block = await screen.findByRole("region", { name: "Замечания" });
    expect(within(block).getByText("More partitions for orders")).toBeInTheDocument();
    expect(within(block).getByText("Партиций: до 6")).toBeInTheDocument();
    expect(within(block).getAllByText(/More partitions/)).toHaveLength(1);
    expect(within(block).queryByText("More partitions for audit")).not.toBeInTheDocument();
    // Значок: находка k1 (критично), замечание n1 (предупреждение), предложение p1 (критично).
    expect(screen.getByTitle(/Требуют внимания: 3/)).toHaveAttribute(
      "title",
      "Требуют внимания: 3\nФактор репликации 1 у orders\nMore partitions for orders\nOrders is not replicated",
    );
    await user.click(within(block).getAllByRole("button", { name: "Удалить" })[1]);
    expect(api.deleteProposal).toHaveBeenCalledWith("c1", "p1");
  });
});

describe("HealthBadge", () => {
  it("считает критичные и предупреждения, а info — нет", async () => {
    renderWithClient(
      <>
        <HealthBadge connectionId="c1" match={isTopic("orders")} />
        <HealthBadge connectionId="c1" match={(x) => x.kind === "cluster"} />
      </>,
    );
    const badge = await screen.findByTitle(/Требуют внимания: 2/);
    expect(within(badge).getByRole("img", { name: "Критично: 2" })).toBeInTheDocument();
    expect(screen.getAllByTitle(/Требуют внимания/)).toHaveLength(1);
  });

  it("в подсказке — число и заголовки находок и замечаний, самые строгие первыми; метка без своей подсказки", async () => {
    renderWithClient(<HealthBadge connectionId="c1" match={isTopic("orders")} />);
    const badge = await screen.findByTitle(/Требуют внимания: 2/);
    expect(badge).toHaveAttribute("title", "Требуют внимания: 2\nФактор репликации 1 у orders\nOrders is not replicated");
    // Своя подсказка метки («Критично») перекрыла бы подсказку значка.
    expect(within(badge).getByRole("img", { name: "Критично: 2" })).not.toHaveAttribute("title");
  });

  it("повторное появление значка не перечитывает проверку, пока данные свежие", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrap = (ui: ReactElement) => <QueryClientProvider client={client}>{ui}</QueryClientProvider>;
    const badge = <HealthBadge connectionId="c1" match={isTopic("orders")} />;
    const { rerender } = render(wrap(badge));
    await screen.findByRole("img", { name: "Критично: 2" });
    rerender(
      wrap(
        <>
          {badge}
          {badge}
        </>,
      ),
    );
    expect(await screen.findAllByRole("img", { name: "Критично: 2" })).toHaveLength(2);
    expect(api.getHealth).toHaveBeenCalledTimes(1);
  });
});
