import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { ConfigEntry, ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { alterTopicConfigs: vi.fn().mockResolvedValue(undefined) } };
});

import { api } from "../../api/client";
import { ConfigEditor } from "./ConfigEditor";
import { TOPIC_CONFIG_HINTS } from "./topicConfigHints";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const entries: ConfigEntry[] = [
  { name: "retention.ms", value: "1000", source: "DynamicTopic", isDefault: false, isReadOnly: false, isSensitive: false },
  { name: "segment.bytes", value: "1", source: "Default", isDefault: true, isReadOnly: true, isSensitive: false },
];

describe("ConfigEditor", () => {
  it("показывает diff и применяет изменения", async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    renderWithClient(<ConfigEditor connection={dev} topic="t" entries={entries} onDone={onDone} />);
    expect(screen.getByLabelText("Значение segment.bytes")).toBeDisabled();
    const input = screen.getByLabelText("Значение retention.ms");
    await user.clear(input);
    await user.type(input, "2000");
    await user.click(screen.getByRole("button", { name: "Проверить изменения" }));
    expect(screen.getByText("1000 → 2000")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Применить" }));
    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], null, null);
    await vi.waitFor(() => expect(onDone).toHaveBeenCalled());
  });

  it("правки из предложения LLM уже заполнены, применение передаёт proposalId", async () => {
    const user = userEvent.setup();
    renderWithClient(
      <ConfigEditor
        connection={dev}
        topic="t"
        entries={entries}
        onDone={() => {}}
        initialEdits={{ "retention.ms": { kind: "set", value: "2000" } }}
        proposalId="p1"
      />,
    );
    expect(screen.getByLabelText("Значение retention.ms")).toHaveValue("2000");
    expect(screen.getByText("изменений: 1")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Проверить изменения" }));
    expect(screen.getByText("1000 → 2000")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Применить" }));
    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], null, "p1");
  });

  it("очищенное поле не отправляет пустой set", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConfigEditor connection={dev} topic="t" entries={entries} onDone={() => {}} />);
    await user.clear(screen.getByLabelText("Значение retention.ms"));
    expect(screen.getByText(/пустое значение; используйте «к default»/)).toBeInTheDocument();
    expect(screen.getByText("изменений: 0")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Проверить изменения" })).toBeDisabled();
  });

  it("сброс к default", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConfigEditor connection={dev} topic="t" entries={entries} onDone={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Сбросить retention.ms" }));
    await user.click(screen.getByRole("button", { name: "Проверить изменения" }));
    expect(screen.getByText("1000 → по умолчанию")).toBeInTheDocument();
  });

  it("на prod требует имя подключения", async () => {
    const user = userEvent.setup();
    vi.mocked(api.alterTopicConfigs).mockClear();
    const prod: ConnectionProfile = { ...dev, name: "prod-1", environment: "prod" };
    renderWithClient(<ConfigEditor connection={prod} topic="t" entries={entries} onDone={() => {}} />);
    const input = screen.getByLabelText("Значение retention.ms");
    await user.clear(input);
    await user.type(input, "2000");
    await user.click(screen.getByRole("button", { name: "Проверить изменения" }));
    expect(screen.getByRole("button", { name: "Применить" })).toBeDisabled();
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(screen.getByRole("button", { name: "Применить" }));
    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], "prod-1", null);
  });

  it("подсказка «?» рядом с «Проверить изменения»", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConfigEditor connection={dev} topic="t" entries={entries} onDone={() => {}} />);
    const review = screen.getByRole("button", { name: "Проверить изменения" });
    const hint = screen.getByRole("button", { name: "Подсказка: Проверить изменения" });
    expect(review.nextElementSibling).toBe(hint);
    await user.click(hint);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "«к default» удаляет переопределение — действует значение брокера. Ничего не меняется, пока вы не проверите и не подтвердите изменения.",
    );
  });

  it("подсказки параметров в режиме редактирования: значок только у известных", async () => {
    const user = userEvent.setup();
    const withUnknown: ConfigEntry[] = [
      ...entries,
      { name: "x.unknown", value: "1", source: "Default", isDefault: true, isReadOnly: false, isSensitive: false },
    ];
    renderWithClient(<ConfigEditor connection={dev} topic="t" entries={withUnknown} onDone={() => {}} hints={TOPIC_CONFIG_HINTS} />);
    expect(screen.getByRole("button", { name: "Подсказка: retention.ms" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Подсказка: segment.bytes" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Подсказка: x.unknown" })).toBeNull();
    await user.click(screen.getByRole("button", { name: "Подсказка: retention.ms" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Сколько хранить сообщения, в мс.");
  });

  it("без hints значков параметров нет", () => {
    renderWithClient(<ConfigEditor connection={dev} topic="t" entries={entries} onDone={() => {}} />);
    expect(screen.queryByRole("button", { name: "Подсказка: retention.ms" })).toBeNull();
  });
});
