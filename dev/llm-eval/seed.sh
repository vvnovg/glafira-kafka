#!/bin/sh
# Данные для трёх сценариев на dev-стенде (контейнер ke-kafka, dev/docker-compose.yml).
# Время записи payment_failed заказа 123 пишется в out/seed.env для сценария 3.
set -e
mkdir -p "$(dirname "$0")/out"
K="docker exec -i ke-kafka"
B="--bootstrap-server localhost:9092"

# Сценарий 1: заведомые проблемы конфигурации.
$K kafka-topics $B --create --if-not-exists --topic payments --partitions 3 --replication-factor 1 \
  --config min.insync.replicas=2
$K kafka-topics $B --create --if-not-exists --topic audit-log --partitions 1 --replication-factor 1 \
  --config retention.ms=60000

# Сценарии 2 и 3: топик заказов.
$K kafka-topics $B --create --if-not-exists --topic orders --partitions 3 --replication-factor 1

gen() { # $1..$2 — номера заказов, $3 — событие
  i=$1
  while [ "$i" -le "$2" ]; do
    printf '%s|{"orderId":%s,"event":"%s","customerEmail":"user%s@example.com","amount":%s}\n' \
      "$i" "$i" "$3" "$i" "$((i * 7 % 900 + 100))"
    i=$((i + 1))
  done
}
P="kafka-console-producer $B --topic orders --property parse.key=true --property key.separator=|"

gen 1 1500 created | $K $P
# Группа order-service обработала первую порцию и остановилась.
$K kafka-console-consumer $B --topic orders --group order-service --from-beginning \
  --max-messages 1200 --timeout-ms 20000 >/dev/null 2>&1 || true

sleep 5
date -u +"MARK_123_UTC=%Y-%m-%dT%H:%M:%SZ" > "$(dirname "$0")/out/seed.env"
printf '123|{"orderId":123,"event":"payment_failed","reason":"card declined","customerEmail":"user123@example.com","amount":961}\n' | $K $P
sleep 5
gen 1501 3000 created | $K $P
date -u +"DONE_UTC=%Y-%m-%dT%H:%M:%SZ"
$K kafka-consumer-groups $B --describe --group order-service
