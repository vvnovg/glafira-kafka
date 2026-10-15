import { describe, expect, it } from "vitest";
import { buildCreateCommand, TOPIC_PRESETS, topicNameWarning, validateTopicName } from "./topicName";

describe("topicName", () => {
  it("валидирует имя по правилам Kafka", () => {
    expect(validateTopicName("orders.v1_x-2")).toBeNull();
    expect(validateTopicName("")).toBe("Введите имя топика");
    expect(validateTopicName("..")).toBe("Имя топика не может быть «.» или «..»");
    expect(validateTopicName("a b")).toBe("Недопустимый символ « »: разрешены латинские буквы, цифры, «.», «_» и «-»");
    expect(validateTopicName("a".repeat(250))).toBe("Имя топика длиннее 249 символов");
  });

  it("предупреждает о коллизии метрик", () => {
    expect(topicNameWarning("a.b_c")).toContain("метрик");
    expect(topicNameWarning("a.b")).toBeNull();
  });

  it("строит команду kafka-topics", () => {
    const cmd = buildCreateCommand("localhost:9092", {
      name: "orders",
      partitions: 3,
      replicationFactor: -1,
      configs: { "cleanup.policy": "compact", "retention.ms": "1000" },
    });
    expect(cmd).toBe(
      "kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 3 " +
        "--config cleanup.policy=compact --config retention.ms=1000",
    );
    expect(buildCreateCommand("b:1", { name: "t", partitions: 1, replicationFactor: 2, configs: {} })).toContain(
      "--replication-factor 2",
    );
  });

  it("пресеты", () => {
    expect(TOPIC_PRESETS.map((p) => p.id)).toEqual(["delete", "compact", "compact-delete", "test"]);
    expect(TOPIC_PRESETS.find((p) => p.id === "test")?.configs).toEqual({ "retention.ms": "3600000" });
  });
});
