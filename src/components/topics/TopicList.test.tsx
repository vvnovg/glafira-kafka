import { fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 30 })),
    getTotalSize: () => o.count * 30,
  }),
}));

const topic = (name: string) => ({ name, partitionCount: 1, replicationFactor: 1, underReplicated: 0, internal: false });

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      listTopics: vi.fn(),
      getTopicStats: vi.fn().mockResolvedValue([]),
      getTopicPolicies: vi.fn().mockRejectedValue(new Error("boom")),
      deleteTopics: vi.fn(),
    },
  };
});

import { api } from "../../api/client";
import { TopicList } from "./TopicList";

beforeEach(() => {
  vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c", name: "dev-1", environment: "dev" }]);
  vi.mocked(api.listTopics).mockResolvedValue([topic("a"), topic("b"), topic("c")] as never);
});

describe("TopicList", () => {
  it("внутренние топики нельзя выбрать для удаления", async () => {
    vi.mocked(api.listTopics).mockResolvedValue([
      topic("a"),
      { ...topic("__consumer_offsets"), internal: true },
    ] as never);
    const user = userEvent.setup();
    renderWithClient(<TopicList connectionId="c" />);
    await user.click(await screen.findByLabelText("Внутренние"));
    await user.click(screen.getByRole("button", { name: "Выбрать" }));
    expect(screen.getByLabelText("Выбрать a")).toBeInTheDocument();
    expect(screen.queryByLabelText("Выбрать __consumer_offsets")).not.toBeInTheDocument();
    // Клик по строке внутреннего топика в режиме выбора не добавляет его в выбор.
    await user.click(screen.getByText("__consumer_offsets"));
    expect(screen.getByRole("button", { name: "Удалить (0)" })).toBeDisabled();
  });

  it("цель удаления фиксируется при открытии панели", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicList connectionId="c" />);
    await user.click(await screen.findByRole("button", { name: "Выбрать" }));
    await user.click(screen.getByLabelText("Выбрать a"));
    await user.click(screen.getByLabelText("Выбрать b"));
    await user.click(screen.getByRole("button", { name: "Удалить (2)" }));
    expect(screen.getByText("a, b")).toBeInTheDocument();
    expect(screen.getByLabelText("Подтверждение: число топиков")).toBeInTheDocument();

    expect(screen.getByLabelText("Выбрать c")).toBeDisabled();
    fireEvent.click(screen.getByLabelText("Выбрать c"));
    expect(screen.getByRole("button", { name: "Отменить выбор" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Создать топик" })).toBeDisabled();
    expect(screen.getByText("a, b")).toBeInTheDocument();
    expect(screen.getByText("Удалить 2 топиков?")).toBeInTheDocument();
  });

  it("ошибка загрузки политики: «—» с причиной в title", async () => {
    renderWithClient(<TopicList connectionId="c" />);
    const dashes = await screen.findAllByTitle(/Не удалось загрузить cleanup.policy: boom/);
    expect(dashes.length).toBe(3);
  });
});
