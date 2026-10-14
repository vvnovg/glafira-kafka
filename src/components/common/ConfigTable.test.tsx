import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConfigEntry } from "../../api/types";
import { ConfigTable, filterConfigs } from "./ConfigTable";

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
});
