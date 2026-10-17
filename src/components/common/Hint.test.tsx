import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { i18n } from "../../i18n";
import { Hint } from "./Hint";

beforeAll(() => {
  i18n.addResourceBundle("ru", "translation", { hints: { test: { a: "Первая <code>acks</code>", b: "Вторая" } } }, true, true);
});
beforeEach(() => vi.useFakeTimers({ shouldAdvanceTime: true }));
afterEach(() => vi.useRealTimers());

const setup = () => userEvent.setup({ advanceTimers: vi.advanceTimersByTime });

describe("Hint", () => {
  it("значок с aria-label из подписи", () => {
    render(<Hint k="hints.test.a" label="acks" />);
    expect(screen.getByRole("button", { name: "Подсказка: acks" })).toHaveTextContent("?");
  });

  it("наведение: показ через 300 мс, уход курсора скрывает", async () => {
    const user = setup();
    render(<Hint k="hints.test.a" label="acks" />);
    const btn = screen.getByRole("button", { name: "Подсказка: acks" });
    await user.hover(btn);
    expect(screen.queryByRole("tooltip")).toBeNull();
    act(() => vi.advanceTimersByTime(300));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Первая acks");
    await user.unhover(btn);
    act(() => vi.advanceTimersByTime(150));
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("фокус с клавиатуры — сразу; Esc закрывает", async () => {
    const user = setup();
    render(<Hint k="hints.test.a" label="acks" />);
    await user.tab();
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("клик закрепляет, второй клик закрывает", async () => {
    const user = setup();
    render(<Hint k="hints.test.a" label="acks" />);
    const btn = screen.getByRole("button", { name: "Подсказка: acks" });
    await user.click(btn);
    await user.unhover(btn);
    act(() => vi.advanceTimersByTime(500));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    await user.click(btn);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("открыта только одна подсказка", async () => {
    const user = setup();
    render(
      <>
        <Hint k="hints.test.a" label="A" />
        <Hint k="hints.test.b" label="B" />
      </>,
    );
    await user.click(screen.getByRole("button", { name: "Подсказка: A" }));
    await user.click(screen.getByRole("button", { name: "Подсказка: B" }));
    const tips = screen.getAllByRole("tooltip");
    expect(tips).toHaveLength(1);
    expect(tips[0]).toHaveTextContent("Вторая");
  });

  it("закреплённая A не гасится блюром: фокус на B — открыта одна, и это B (стор)", async () => {
    const user = setup();
    render(
      <>
        <Hint k="hints.test.a" label="A" />
        <Hint k="hints.test.b" label="B" />
      </>,
    );
    await user.click(screen.getByRole("button", { name: "Подсказка: A" }));
    expect(screen.getByRole("tooltip")).toHaveTextContent("Первая");
    await user.tab();
    expect(screen.getByRole("button", { name: "Подсказка: B" })).toHaveFocus();
    const tips = screen.getAllByRole("tooltip");
    expect(tips).toHaveLength(1);
    expect(tips[0]).toHaveTextContent("Вторая");
  });

  it("клик по значку и по подсказке не всплывает к предкам", async () => {
    const user = setup();
    const spy = vi.fn();
    render(
      <div onClick={spy}>
        <Hint k="hints.test.a" label="acks" />
      </div>,
    );
    await user.click(screen.getByRole("button", { name: "Подсказка: acks" }));
    await user.click(screen.getByRole("tooltip"));
    expect(spy).not.toHaveBeenCalled();
  });

  it("aria-describedby указывает на подсказку, <code> сохраняется", async () => {
    const user = setup();
    render(<Hint k="hints.test.a" label="acks" />);
    const btn = screen.getByRole("button", { name: "Подсказка: acks" });
    await user.click(btn);
    const tip = screen.getByRole("tooltip");
    expect(btn).toHaveAttribute("aria-describedby", tip.id);
    expect(tip.querySelector("code")).toHaveTextContent("acks");
  });
});
