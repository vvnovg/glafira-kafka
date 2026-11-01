#!/usr/bin/env bash
# Ждёт готовности dev-стенда: брокер отвечает и одноразовая инициализация SCRAM завершилась.
set -euo pipefail
for _ in $(seq 1 60); do
  if docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null
code=$(docker wait ke-kafka-init)
if [ "$code" != "0" ]; then
  docker logs ke-kafka-init
  exit 1
fi
# Координатор групп готов: пока нет `__consumer_offsets`, описание группы приходит в состоянии
# Unknown, а не Dead, и тесты групп падают на свежем брокере.
for _ in $(seq 1 60); do
  if docker exec ke-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group ke-wait-coordinator 2>&1 | grep -q "does not exist"; then
    exit 0
  fi
  sleep 2
done
echo "group coordinator is not ready" >&2
exit 1
