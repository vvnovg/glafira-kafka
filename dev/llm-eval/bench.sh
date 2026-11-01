#!/bin/sh
# bench.sh <tag> <model-id> [повторов] — 3 сценария на уже запущенном сервере модели.
# Окружение: BASE_URL (http://127.0.0.1:8100/v1), API_KEY (sk-dev), MAXTOK (8192), PY (python3), GLAFIRA.
set -e
cd "$(dirname "$0")"
TAG=$1; MODEL=$2; REPS=${3:-3}; PY=${PY:-python3}
[ -f out/seed.env ] || { echo "сначала ./seed.sh" >&2; exit 1; }
. out/seed.env
# Сценарий 3 называет время по Москве: модель должна сама перевести его в UTC.
WHEN=$($PY -I -c 'import sys,datetime as d;t=d.datetime.fromisoformat(sys.argv[1].replace("Z","+00:00"))+d.timedelta(hours=3);print(t.strftime("%d.%m.%Y в %H:%M"))' "$MARK_123_UTC")
P1="Запусти проверку кластера на подключении dev и объясни находки: что опасно, что нормально для dev-стенда и что исправить."
P2="Почему у группы order-service на подключении dev большой лаг? Разберись и скажи, что делать."
P3="$WHEN по Москве (UTC+3) сервис заказов записал в лог ошибку по заказу 123. Выясни на подключении dev, что было в топике orders про этот заказ и успела ли группа order-service его обработать."
run() {
  $PY -I agent.py --model "$MODEL" --base-url "${BASE_URL:-http://127.0.0.1:8100/v1}" \
    --api-key "${API_KEY:-sk-dev}" --max-tokens "${MAXTOK:-8192}" --out "out/$TAG-$1-r$2.json" \
    --prompt "$3" > /dev/null 2>&1 || echo "$TAG $1 r$2: agent failed" >&2
}
r=1
while [ "$r" -le "$REPS" ]; do
  run s1 "$r" "$P1"
  run s2 "$r" "$P2"
  run s3 "$r" "$P3"
  echo "$TAG rep $r done $(date +%H:%M:%S)"
  r=$((r + 1))
done
$PY -I grade.py "$TAG"
