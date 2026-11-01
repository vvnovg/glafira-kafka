# Проверка моделей на MCP Glafira (только для разработки)

Скриптовый прогон трёх сценариев: модель за OpenAI-совместимым API (vLLM, Ollama и т. п.) вызывает
инструменты Glafira через `glafira-kafka mcp` и отвечает по-русски. Нужен, чтобы сравнивать модели
и проверять правки описаний и схем инструментов.

| Сценарий | Вопрос | Верный ответ |
|---|---|---|
| s1 | проверка кластера dev | у `payments` `min.insync.replicas` больше фактора репликации |
| s2 | почему у `order-service` лаг | у группы нет участников: потребитель не запущен |
| s3 | что было по заказу 123 в момент из лога (время по Москве) | `payment_failed` в партиции 2 не обработан: группа закоммитила раньше его оффсета |

## Подготовка

1. Стенд: `docker compose -f dev/docker-compose.yml up -d`, затем `dev/llm-eval/seed.sh` — топики
   `payments`, `audit-log`, `orders` и группа `order-service`. Seed рассчитан на чистый стенд.
2. Glafira: подключение `dev` к `localhost:9092`, доступ для LLM — «Метаданные и сообщения»;
   в «Настройках» включён MCP-сервер. Сборка не из `/Applications` — через `GLAFIRA=<путь к
   glafira-kafka>`.
3. Модель, например Qwen3.6-35B-A3B на vLLM (Apple Silicon — vllm-metal):

   ```bash
   vllm serve mlx-community/Qwen3.6-35B-A3B-4bit --host 127.0.0.1 --port 8100 --api-key sk-dev \
     --enable-auto-tool-choice --tool-call-parser qwen3_coder --reasoning-parser qwen3 \
     --max-model-len 65536
   ```

4. Python: `pip install -r dev/llm-eval/requirements.txt`.

Первый запуск агента Glafira покажет как нового клиента `mcp`: подтвердите его в диалоге, пока
агент подключён (диалог закрывается вместе с сессией).

## Прогон

```bash
dev/llm-eval/bench.sh q36 mlx-community/Qwen3.6-35B-A3B-4bit
```

Третий аргумент — число повторов (по умолчанию 3). Переменные: `BASE_URL`, `API_KEY`, `MAXTOK`
(предел токенов на шаг, 8192: модели с рассуждением на 4096 не успевают), `PY`, `GLAFIRA`.
Журналы прогонов — `out/<tag>-s<N>-r<R>.json` (шаги, вызовы, ответ), сводка — `grade.py <tag>`:
время, шаги, ошибки инструментов и эвристики по ключу ответов. Ответы s3 стоит прочитать.

Агент — `agent.py`: системная подсказка, инструкции сервера, до 15 шагов, температура 0,2.
