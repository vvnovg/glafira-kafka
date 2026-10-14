import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { BrowseForm } from "./BrowseForm";
import { defaultBrowseForm } from "./params";

describe("BrowseForm", () => {
  it("у полей фильтров есть видимые подписи, не исчезающие при вводе", () => {
    render(<BrowseForm form={{ ...defaultBrowseForm, jsonPath: '$.status == "FAILED"' }} onChange={() => {}} />);
    for (const label of ["Ключ содержит", "Значение содержит", "Заголовок", "Значение заголовка", "JSONPath"]) {
      expect(screen.getByText(label)).toBeVisible();
      expect(screen.getByLabelText(label)).toBeInTheDocument();
    }
    expect(screen.getByLabelText("JSONPath")).toHaveValue('$.status == "FAILED"');
  });
});
