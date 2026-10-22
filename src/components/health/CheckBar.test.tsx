import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Finding, HealthView } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getHealth: vi.fn(), runHealthCheck: vi.fn(), listConnections: vi.fn() } };
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
  vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "dev-1", environment: "prod" }]);
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("CheckBar: общее состояние и окружение проверки", () => {
  it("«Проверка…» видна во всех CheckBar подключения, пока идёт одна проверка", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(checked([]));
    let finish: (v: HealthView) => void = () => {};
    vi.mocked(api.runHealthCheck).mockReturnValue(new Promise<HealthView>((resolve) => (finish = resolve)));
    const user = userEvent.setup();
    renderWithClient(
      <>
        <CheckBar connectionId="c1" allLink />
        <CheckBar connectionId="c1" />
      </>,
    );
    const buttons = await screen.findAllByRole("button", { name: "Проверить кластер" });
    expect(buttons).toHaveLength(2);
    await user.click(buttons[0]);
    expect(await screen.findAllByRole("button", { name: "Проверка…" })).toHaveLength(2);
    expect(screen.getAllByRole("button", { name: "Проверка…" }).every((b) => b.hasAttribute("disabled"))).toBe(true);
    finish(checked([problem]));
    expect(await screen.findAllByRole("button", { name: "Проверить кластер" })).toHaveLength(2);
    expect(api.runHealthCheck).toHaveBeenCalledTimes(1);
  });

  it("помечает проверку, сделанную под другим окружением, чем у подключения сейчас", async () => {
    // Подключение теперь dev, а последняя проверка считалась как prod.
    vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "dev-1", environment: "dev" }]);
    vi.mocked(api.getHealth).mockResolvedValue(checked([problem]));
    renderWithClient(<CheckBar connectionId="c1" />);
    expect(await screen.findByText(/проверено как PROD/)).toBeInTheDocument();
  });

  it("без расхождения окружений пометки нет", async () => {
    vi.mocked(api.getHealth).mockResolvedValue(checked([problem]));
    renderWithClient(<CheckBar connectionId="c1" />);
    await screen.findByText(/Последняя проверка/);
    await waitFor(() => expect(api.listConnections).toHaveBeenCalled());
    expect(screen.queryByText(/проверено как/)).not.toBeInTheDocument();
  });
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
