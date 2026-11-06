import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    api: {
      ...actual.api,
      startBrowse: vi.fn(() => new Promise<string>(() => {})),
      stopSession: vi.fn(async () => true),
    },
  };
});

import { api } from "../../api/client";
import { BrowsePanel } from "./BrowsePanel";

beforeEach(() => {
  vi.mocked(api.startBrowse).mockClear();
});

describe("BrowsePanel: Enter", () => {
  it("Enter в поле формы запускает чтение, как кнопка «Читать» с подсказкой ⏎", async () => {
    const user = userEvent.setup();
    renderWithClient(<BrowsePanel connectionId="c" topic="t" />);
    const read = screen.getByRole("button", { name: "Читать" });
    expect(read.querySelector(".kbd")).toHaveTextContent("⏎");
    expect(read.querySelector('svg[data-icon="play"]')).not.toBeNull();

    await user.click(screen.getByLabelText("Значение содержит"));
    await user.keyboard("FAILED{Enter}");
    expect(api.startBrowse).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(vi.mocked(api.startBrowse).mock.calls[0][0])).toContain("FAILED");
  });

  it("Enter на флажке и вне формы чтение не запускает", async () => {
    const user = userEvent.setup();
    renderWithClient(<BrowsePanel connectionId="c" topic="t" />);
    await user.click(screen.getByLabelText("regex"));
    await user.keyboard("{Enter}");
    expect(api.startBrowse).not.toHaveBeenCalled();
  });
});
