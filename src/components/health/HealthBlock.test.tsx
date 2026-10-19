import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { HealthView } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getHealth: vi.fn(),
      setFindingIgnored: vi.fn(),
      setAnnotationStatus: vi.fn(),
      deleteAnnotation: vi.fn(),
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
    const { container } = renderWithClient(<HealthBlock connectionId="c1" match={isTopic("audit")} />);
    await vi.waitFor(() => expect(api.getHealth).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
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
    const badge = await screen.findByTitle("Требуют внимания: 2");
    expect(within(badge).getByRole("img", { name: "Критично: 2" })).toBeInTheDocument();
    expect(screen.getAllByTitle(/Требуют внимания/)).toHaveLength(1);
  });
});
