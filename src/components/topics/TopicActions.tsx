import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, toAppError } from "../../api/client";
import type { ConnectionProfile, TopicDetails } from "../../api/types";
import { tr } from "../../i18n/text";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { DeleteTopicsPanel } from "./DeleteTopicsPanel";

type Action = "partitions" | "purge" | "delete" | null;

interface Props {
  connection: ConnectionProfile;
  topic: TopicDetails;
  onDeleted: () => void;
}

function useInvalidateTopic(connectionId: string, topic: string) {
  const queryClient = useQueryClient();
  return () => {
    void queryClient.invalidateQueries({ queryKey: ["topic", connectionId, topic] });
    void queryClient.invalidateQueries({ queryKey: ["topicConfigs", connectionId, topic] });
    void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
    void queryClient.invalidateQueries({ queryKey: ["topicStats", connectionId] });
    void queryClient.invalidateQueries({ queryKey: ["topicPolicies", connectionId] });
  };
}

/** Брокер принимает DeleteRecords, только если в cleanup.policy есть delete. */
export function purgeBlockReason(cleanup: string | null): string | null {
  if (cleanup === null || cleanup.trim() === "") return null;
  const parts = cleanup.split(",").map((x) => x.trim());
  if (parts.includes("delete")) return null;
  return `Очистка невозможна: у топика cleanup.policy=${cleanup} (нужно compact,delete или delete)`;
}

export function TopicActions({ connection, topic, onDeleted }: Props) {
  const [action, setAction] = useState<Action>(null);
  const policy = useQuery({
    queryKey: ["topicPolicies", connection.id, [topic.name]],
    queryFn: () => api.getTopicPolicies(connection.id, [topic.name]),
  });
  const cleanup = policy.data?.[0]?.cleanupPolicy ?? null;
  const purgeBlock = purgeBlockReason(cleanup);
  return (
    <>
      <div className="toolbar">
        <button type="button" onClick={() => setAction("partitions")}>
          Добавить партиции
        </button>
        <button
          type="button"
          className="danger"
          disabled={purgeBlock !== null}
          title={purgeBlock ?? undefined}
          onClick={() => setAction("purge")}
        >
          Очистить
        </button>
        <button type="button" className="danger" onClick={() => setAction("delete")}>
          Удалить топик
        </button>
      </div>
      {purgeBlock && <p className="warn">{purgeBlock}</p>}
      {action === "partitions" && <AddPartitions connection={connection} topic={topic} onClose={() => setAction(null)} />}
      {action === "purge" && <Purge connection={connection} topic={topic} cleanup={cleanup} policyError={policy.error} onClose={() => setAction(null)} />}
      {action === "delete" && (
        <DeleteTopicsPanel
          connection={connection}
          topics={[topic.name]}
          onCancel={() => setAction(null)}
          onDone={(deleted) => {
            if (deleted.includes(topic.name)) onDeleted();
          }}
        />
      )}
    </>
  );
}

function AddPartitions({ connection, topic, onClose }: { connection: ConnectionProfile; topic: TopicDetails; onClose: () => void }) {
  const invalidate = useInvalidateTopic(connection.id, topic.name);
  const current = topic.partitions.length;
  const [value, setValue] = useState(String(current + 1));
  // Снимок цели на момент открытия подтверждения.
  const [confirming, setConfirming] = useState<number | null>(null);
  const n = Number(value);
  const valid = Number.isInteger(n) && n > current;
  const run = useMutation({
    mutationFn: (v: { total: number; confirm: string | null }) => api.addPartitions(connection.id, topic.name, v.total, v.confirm),
    onSuccess: () => {
      invalidate();
      onClose();
    },
  });
  return (
    <div className="panel">
      <label>
        Новое число партиций (сейчас {current})
        <input
          aria-label="Новое число партиций"
          type="number"
          min={current + 1}
          value={value}
          disabled={confirming !== null || run.isPending}
          onChange={(e) => setValue(e.target.value)}
        />
      </label>
      <p className="muted">
        Уменьшить число партиций нельзя. Сообщения с одним ключом после увеличения могут попадать в другую партицию — порядок
        по ключу нарушится.
      </p>
      {run.error && <ErrorBanner error={run.error} />}
      {confirming !== null ? (
        <ConfirmDialog
          connection={connection}
          title={`Увеличить число партиций «${topic.name}» до ${confirming}?`}
          confirmLabel="Подтвердить"
          busy={run.isPending}
          onConfirm={(c) => run.mutate({ total: confirming, confirm: c.confirm })}
          onCancel={() => setConfirming(null)}
        />
      ) : (
        <div className="toolbar">
          <button
            type="button"
            className="primary"
            disabled={!valid || run.isPending}
            onClick={() => (needsConfirmation(connection) ? setConfirming(n) : run.mutate({ total: n, confirm: null }))}
          >
            Увеличить
          </button>
          <button type="button" onClick={onClose}>
            Отмена
          </button>
        </div>
      )}
    </div>
  );
}

function Purge({
  connection,
  topic,
  cleanup,
  policyError,
  onClose,
}: {
  connection: ConnectionProfile;
  topic: TopicDetails;
  cleanup: string | null;
  policyError: unknown;
  onClose: () => void;
}) {
  const invalidate = useInvalidateTopic(connection.id, topic.name);
  // Список партиций фиксируется при открытии: обновление топика не меняет цель под подтверждением.
  const [ids] = useState(() => topic.partitions.map((p) => p.id));
  const [selected, setSelected] = useState<Set<number>>(new Set(ids));
  const compact = cleanup?.includes("compact") ?? false;
  const run = useMutation({
    mutationFn: (v: { parts: number[]; c: Parameters<typeof api.purgeTopic>[3] }) =>
      api.purgeTopic(connection.id, topic.name, v.parts, v.c),
    onSettled: invalidate,
  });
  const toggle = (id: number) =>
    setSelected((s) => {
      const next = new Set(s);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  if (run.data) {
    return (
      <div className="panel">
        <ul>
          {run.data.map((p) => (
            <li key={p.partition} className={p.error ? "warn" : ""}>
              {p.error
                ? `Партиция ${p.partition}: ${tr(p.error)}`
                : `Партиция ${p.partition}: новый low watermark ${p.lowWatermark ?? "—"}`}
            </li>
          ))}
        </ul>
        <button type="button" onClick={onClose}>
          Закрыть
        </button>
      </div>
    );
  }

  const sortedSel = [...selected].sort((a, b) => a - b);
  const allSelected = selected.size === ids.length;
  return (
    <div className="panel">
      {compact && (
        <p className="warn">cleanup.policy = {cleanup}: purge удалит и последние значения ключей (compact).</p>
      )}
      {policyError != null && (
        <p className="warn" title={tr(toAppError(policyError))}>
          Не удалось определить cleanup.policy — для compact-топика purge удалит и последние значения ключей.
        </p>
      )}
      <div className="toolbar">
        {ids.map((id) => (
          <label key={id}>
            <input type="checkbox" aria-label={`Партиция ${id}`} checked={selected.has(id)} disabled={run.isPending} onChange={() => toggle(id)} /> {id}
          </label>
        ))}
      </div>
      {run.error && <ErrorBanner error={run.error} />}
      <ConfirmDialog
        key={sortedSel.join(",")}
        connection={connection}
        title={
          allSelected
            ? `Очистить все партиции топика «${topic.name}»?`
            : `Очистить партиции [${sortedSel.join(", ")}] топика «${topic.name}»?`
        }
        objectName={topic.name}
        objectLabel="имя топика"
        confirmLabel="Очистить партиции"
        danger
        busy={run.isPending || selected.size === 0}
        onConfirm={(c) => run.mutate({ parts: sortedSel, c })}
        onCancel={onClose}
      >
        <p className="muted">Сообщения до текущего конца выбранных партиций будут удалены безвозвратно.</p>
      </ConfirmDialog>
    </div>
  );
}
