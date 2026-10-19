import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Finding, HealthView } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getHealth: vi.fn(), runHealthCheck: vi.fn() } };
});

import { api } from "../../api/client";
import { CheckBar } from "./CheckBar";

const checked = (findings: Finding[]): HealthView => ({
  lastCheck: {
    runId: "r1",
    startedAtMs: Date.now(),
    durationMs: 1,
    trigger: { kind: "ui" },
    environment: "prod",
    incomplete: false,
    scope: { brokers: 1, topicsChecked: 2, topicsTotal: 2, groupsChecked: 0, groupsTotal: 0 },
    findings,
  },
  ignored: [],
  annotations: [],
});

const problem: Finding = { key: "k1", ruleId: "topic.replicationFactorOne", severity: "critical", target: { kind: "topic", name: "orders" }, params: {} };

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("CheckBar: ссылка «Все замечания»", () => {
  it("с allLink открывает вкладку «Замечания»", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(checked([problem]));
    const user = userEvent.setup();
    renderWithClient(<CheckBar connectionId="c1" allLink />);
    await user.click(await screen.findByRole("button", { name: "Все замечания" }));
    expect(useWorkspace.getState().activeId).toBe("findings:c1");
  });

  it("показывается и без находок, раз проверка была", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(checked([]));
    renderWithClient(<CheckBar connectionId="c1" allLink />);
    expect(await screen.findByRole("button", { name: "Все замечания" })).toBeInTheDocument();
  });

  it("без allLink её нет (вкладка «Замечания» ссылается сама на себя)", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(checked([problem]));
    renderWithClient(<CheckBar connectionId="c1" />);
    await screen.findByText(/Последняя проверка/);
    expect(screen.queryByRole("button", { name: "Все замечания" })).not.toBeInTheDocument();
  });

  it("пока проверок не было, ссылки нет", async () => {
    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] });
    renderWithClient(<CheckBar connectionId="c1" allLink />);
    await screen.findByText("Кластер ещё не проверялся.");
    expect(screen.queryByRole("button", { name: "Все замечания" })).not.toBeInTheDocument();
  });
});
