import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { BrowseEvent, BrowseParams, BrowsedMessage } from "../../api/types";
import { defaultBrowseForm, buildBrowseParams } from "./params";

type OnEvent = (e: BrowseEvent) => void;
interface Pending {
  resolve: (id: string) => void;
  onEvent: OnEvent;
}
const pending: Pending[] = [];

vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    api: {
      ...actual.api,
      startBrowse: vi.fn(
        (_p: BrowseParams, onEvent: OnEvent) =>
          new Promise<string>((resolve) => {
            pending.push({ resolve, onEvent });
          }),
      ),
      stopSession: vi.fn(async () => true),
    },
  };
});

import { api } from "../../api/client";
import { useBrowseSession } from "./useBrowseSession";

const params = buildBrowseParams(defaultBrowseForm, "c", "t");
const msg = (offset: number): BrowsedMessage => ({
  topic: "t",
  partition: 0,
  offset,
  timestamp: null,
  key: null,
  value: null,
  headers: [],
  size: 0,
});

beforeEach(() => {
  pending.length = 0;
  vi.mocked(api.stopSession).mockClear();
  vi.mocked(api.startBrowse).mockClear();
});

describe("useBrowseSession", () => {
  it("stop до ответа startBrowse останавливает запоздавшую сессию", async () => {
    const { result } = renderHook(() => useBrowseSession());
    let p!: Promise<void>;
    await act(async () => {
      p = result.current.start(params);
    });
    await act(async () => {
      await result.current.stop();
    });
    await act(async () => {
      pending[0].resolve("s1");
      await p;
    });
    expect(api.stopSession).toHaveBeenCalledWith("s1");
  });

  it("размонтирование до ответа startBrowse останавливает сессию", async () => {
    const { result, unmount } = renderHook(() => useBrowseSession());
    let p!: Promise<void>;
    await act(async () => {
      p = result.current.start(params);
    });
    unmount();
    await act(async () => {
      pending[0].resolve("s1");
      await p;
    });
    expect(api.stopSession).toHaveBeenCalledWith("s1");
  });

  it("события старой сессии после перезапуска игнорируются", async () => {
    const { result } = renderHook(() => useBrowseSession());
    let p1!: Promise<void>;
    await act(async () => {
      p1 = result.current.start(params);
    });
    await act(async () => {
      pending[0].resolve("s1");
      await p1;
    });
    let p2!: Promise<void>;
    await act(async () => {
      p2 = result.current.start(params);
    });
    await act(async () => {
      await Promise.resolve();
    });
    await act(async () => {
      pending[1].resolve("s2");
      await p2;
    });
    act(() => {
      pending[0].onEvent({ type: "messages", messages: [msg(1)] });
      pending[1].onEvent({ type: "messages", messages: [msg(2)] });
    });
    expect(result.current.state.messages.map((m) => m.offset)).toEqual([2]);
  });

  it("обычный сценарий: события попадают в state, stop останавливает сессию", async () => {
    const { result } = renderHook(() => useBrowseSession());
    let p!: Promise<void>;
    await act(async () => {
      p = result.current.start(params);
    });
    await act(async () => {
      pending[0].resolve("s1");
      await p;
    });
    act(() => pending[0].onEvent({ type: "messages", messages: [msg(5)] }));
    expect(result.current.state.messages).toHaveLength(1);
    await act(async () => {
      await result.current.stop();
    });
    expect(api.stopSession).toHaveBeenCalledWith("s1");
    expect(result.current.state.status).toBe("finished");
    expect(result.current.state.finishReason).toBe("cancelled");
  });

  it("перезапуск не переводит состояние в finished", async () => {
    const { result } = renderHook(() => useBrowseSession());
    let p1!: Promise<void>;
    await act(async () => {
      p1 = result.current.start(params);
    });
    await act(async () => {
      pending[0].resolve("s1");
      await p1;
    });
    await act(async () => {
      void result.current.start(params);
      await Promise.resolve();
    });
    expect(result.current.state.status).toBe("running");
  });

  describe("стоп во время перезапуска", () => {
    const setup = async (): Promise<{
      hook: ReturnType<typeof renderHook<ReturnType<typeof useBrowseSession>, unknown>>;
      p2: Promise<void>;
      resolveStop: () => void;
    }> => {
      let resolveStop!: () => void;
      const hook = renderHook(() => useBrowseSession());
      let p1!: Promise<void>;
      await act(async () => {
        p1 = hook.result.current.start(params);
      });
      await act(async () => {
        pending[0].resolve("s0");
        await p1;
      });
      vi.mocked(api.stopSession).mockImplementationOnce(
        () =>
          new Promise<boolean>((resolve) => {
            resolveStop = () => resolve(true);
          }),
      );
      let p2!: Promise<void>;
      await act(async () => {
        p2 = hook.result.current.start(params);
      });
      return { hook, p2, resolveStop: () => resolveStop() };
    };

    it("размонтирование во время остановки не запускает новую сессию", async () => {
      const { hook, p2, resolveStop } = await setup();
      hook.unmount();
      await act(async () => {
        resolveStop();
        await p2;
      });
      expect(api.startBrowse).toHaveBeenCalledTimes(1);
    });

    it("stop() во время остановки не запускает новую сессию", async () => {
      const { hook, p2, resolveStop } = await setup();
      let ps!: Promise<void>;
      await act(async () => {
        ps = hook.result.current.stop();
      });
      await act(async () => {
        resolveStop();
        await p2;
        await ps;
      });
      expect(api.startBrowse).toHaveBeenCalledTimes(1);
    });
  });
});
