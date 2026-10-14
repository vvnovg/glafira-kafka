import { describe, expect, it } from "vitest";
import type { TopicSummary } from "../../api/types";
import { filterTopics, visibleChunks } from "./topicFilter";

const t = (name: string, internal = false): TopicSummary => ({
  name,
  internal,
  partitionCount: 1,
  replicationFactor: 1,
  underReplicated: 0,
});
const topics = [t("orders"), t("orders-dlq"), t("payments"), t("__consumer_offsets", true)];

describe("filterTopics", () => {
  it("подстрока без учёта регистра, внутренние скрыты", () => {
    expect(filterTopics(topics, "ORD", false).map((x) => x.name)).toEqual(["orders", "orders-dlq"]);
    expect(filterTopics(topics, "", false)).toHaveLength(3);
    expect(filterTopics(topics, "", true)).toHaveLength(4);
  });

  it("regex в слэшах; невалидный regex → пусто", () => {
    expect(filterTopics(topics, "/-dlq$/", false).map((x) => x.name)).toEqual(["orders-dlq"]);
    expect(filterTopics(topics, "/(/", false)).toEqual([]);
  });
});

describe("visibleChunks", () => {
  it("номера чанков для видимого диапазона", () => {
    expect(visibleChunks(0, 10)).toEqual([0]);
    expect(visibleChunks(190, 410)).toEqual([0, 1, 2]);
    expect(visibleChunks(0, -1)).toEqual([]);
  });
});
