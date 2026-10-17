import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConfigEntry } from "../../api/types";
import { ConfigTable, filterConfigs } from "./ConfigTable";
import { TOPIC_CONFIG_HINTS } from "./topicConfigHints";

const e = (name: string, value: string | null, isDefault: boolean, isSensitive = false): ConfigEntry => ({
  name,
  value,
  source: isDefault ? "Default" : "DynamicTopic",
  isDefault,
  isReadOnly: false,
  isSensitive,
});

const entries = [e("retention.ms", "1000", false), e("cleanup.policy", "delete", true), e("sasl.jaas.config", null, true, true)];

describe("ConfigTable", () => {
  it("фильтрует по подстроке и изменённым", () => {
    expect(filterConfigs(entries, "RET", false).map((x) => x.name)).toEqual(["retention.ms"]);
    expect(filterConfigs(entries, "", true).map((x) => x.name)).toEqual(["retention.ms"]);
  });

  it("скрывает sensitive и фильтрует через UI", async () => {
    const user = userEvent.setup();
    render(<ConfigTable entries={entries} />);
    expect(screen.getByText("••••••")).toBeInTheDocument();
    await user.click(screen.getByLabelText("Только изменённые"));
    expect(screen.queryByText("cleanup.policy")).not.toBeInTheDocument();
    expect(screen.getByText("retention.ms")).toBeInTheDocument();
  });

  it("подсказки параметров: значок только у известных, только при переданном hints", async () => {
    const entries: ConfigEntry[] = [
      { name: "retention.ms", value: "604800000", source: "DEFAULT_CONFIG", isDefault: true, isReadOnly: false, isSensitive: false },
      { name: "x.unknown", value: "1", source: "DEFAULT_CONFIG", isDefault: true, isReadOnly: false, isSensitive: false },
    ];
    const { unmount } = render(<ConfigTable entries={entries} hints={TOPIC_CONFIG_HINTS} />);
    expect(screen.getByRole("button", { name: "Подсказка: retention.ms" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Подсказка: x.unknown" })).toBeNull();
    await userEvent.click(screen.getByRole("button", { name: "Подсказка: retention.ms" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Сколько хранить сообщения, в мс.");
    unmount();
    render(<ConfigTable entries={entries} />);
    expect(screen.queryByRole("button", { name: /^Подсказка:/ })).toBeNull();
  });

  it("подсказки параметров: все 36 параметров, min.insync.replicas открывает текст", async () => {
    expect(Object.keys(TOPIC_CONFIG_HINTS)).toHaveLength(36);
    const entries: ConfigEntry[] = [
      { name: "min.insync.replicas", value: "1", source: "DEFAULT_CONFIG", isDefault: true, isReadOnly: false, isSensitive: false },
    ];
    render(<ConfigTable entries={entries} hints={TOPIC_CONFIG_HINTS} />);
    await userEvent.click(screen.getByRole("button", { name: "Подсказка: min.insync.replicas" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Сколько реплик должны подтвердить запись при acks=all, иначе producer получит ошибку. Обычно 2 при факторе репликации 3.",
    );
  });
});
