import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      deleteTopics: vi.fn().mockResolvedValue([
        { name: "a" },
        { name: "b", error: { code: "NOT_AUTHORIZED", message: "Не удалось удалить топик", retryable: false } },
      ]),
    },
  };
});

import { api } from "../../api/client";
import { DeleteTopicsPanel } from "./DeleteTopicsPanel";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };

describe("DeleteTopicsPanel", () => {
  it("несколько топиков подтверждаются их числом, ошибки показываются по каждому", async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    renderWithClient(<DeleteTopicsPanel connection={dev} topics={["a", "b"]} onDone={onDone} onCancel={() => {}} />);
    await user.type(screen.getByLabelText("Подтверждение: число топиков"), "2");
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    expect(api.deleteTopics).toHaveBeenCalledWith("c", ["a", "b"], { confirm: null, typedName: "2" });
    expect(await screen.findByText(/b: Не удалось удалить топик/)).toBeInTheDocument();
    expect(onDone).toHaveBeenCalledWith(["a"]);
  });
});
