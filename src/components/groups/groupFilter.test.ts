import { describe, expect, it } from "vitest";
import type { GroupSummary } from "../../api/types";
import { filterGroups, groupStates } from "./groupFilter";

const g = (groupId: string, state: string): GroupSummary => ({ groupId, state, isSimple: false });
const groups = [g("orders-svc", "Stable"), g("billing", "Empty"), g("orders-batch", "Empty")];

describe("groupFilter", () => {
  it("поиск по подстроке без учёта регистра и фильтр по состоянию", () => {
    expect(filterGroups(groups, "ORDERS", "all").map((x) => x.groupId)).toEqual(["orders-svc", "orders-batch"]);
    expect(filterGroups(groups, "orders", "Empty").map((x) => x.groupId)).toEqual(["orders-batch"]);
    expect(filterGroups(groups, "", "all")).toHaveLength(3);
  });

  it("список состояний", () => {
    expect(groupStates(groups)).toEqual(["Empty", "Stable"]);
  });
});
