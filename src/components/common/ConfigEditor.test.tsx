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
    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], null);
    await vi.waitFor(() => expect(onDone).toHaveBeenCalled());
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
    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], "prod-1");
  });
});
