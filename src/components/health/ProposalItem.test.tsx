import { screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Proposal } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { listConnections: vi.fn().mockResolvedValue([]) } };
});

import { ProposalItem } from "./ProposalItem";

const applied = (p: Partial<Proposal> = {}): Proposal => ({
  id: "p1",
  severity: "warning",
  title: "Больше партиций",
  rationale: "консьюмеров больше, чем партиций",
  target: { kind: "topic", name: "smoke-g2" },
  change: { kind: "addPartitions", topic: "smoke-g2", newTotal: 3 },
  client: "claude-code",
  createdAtMs: 1,
  updatedAtMs: 1,
  status: "applied",
  closedAtMs: Date.now() - 60_000,
  appliedAsProposed: false,
  ...p,
});

function renderItem(proposal: Proposal) {
  renderWithClient(
    <ul>
      <ProposalItem connectionId="c1" proposal={proposal} onStatus={vi.fn()} onDelete={vi.fn()} />
    </ul>,
  );
}

beforeEach(() => {
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("ProposalItem", () => {
  it("у применённого с правками показывает, что применено фактически", () => {
    renderItem(applied({ appliedChange: { kind: "addPartitions", topic: "smoke-g2", newTotal: 4 } }));
    expect(screen.getByText("Партиций: до 3")).toBeInTheDocument();
    const line = screen.getByText("Применено: Партиций: до 4");
    expect(line).toHaveClass("proposal-change");
    expect(screen.getByText(/применено с правками/)).toBeInTheDocument();
  });

  it("без фактически применённого строки нет", () => {
    renderItem(applied());
    expect(screen.getByText("Партиций: до 3")).toBeInTheDocument();
    expect(screen.queryByText(/^Применено:/)).not.toBeInTheDocument();
  });
});
