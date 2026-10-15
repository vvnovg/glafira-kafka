import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConnectionProfile, NewTopicSpec } from "../../api/types";
import { parseProps } from "../connections/profile";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { buildCreateCommand, TOPIC_PRESETS, topicNameWarning, validateTopicName } from "./topicName";

interface Props {
  connection: ConnectionProfile;
  onCreated: (name: string) => void;
  onCancel: () => void;
}

export function CreateTopicForm({ connection, onCreated, onCancel }: Props) {
  const cluster = useQuery({
    queryKey: ["cluster", connection.id],
    queryFn: () => api.getClusterOverview(connection.id),
  });
  const brokers = cluster.data?.brokers.length;
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState("1");
  const [rf, setRf] = useState<string | null>(null);
  const [preset, setPreset] = useState("delete");
  const [extra, setExtra] = useState("");
  const [confirming, setConfirming] = useState(false);

  // RF по умолчанию — min(3, число брокеров), пока пользователь не ввёл своё значение.
  const rfValue = rf ?? String(Math.min(3, brokers ?? 1));
  const nameError = validateTopicName(name);
  let configs: Record<string, string> = {};
  let extraError: string | null = null;
  try {
    configs = { ...TOPIC_PRESETS.find((p) => p.id === preset)?.configs, ...parseProps(extra) };
  } catch (e) {
    extraError = (e as Error).message;
  }
  const p = Number(partitions);
  const r = Number(rfValue);
  const numbersError =
    !Number.isInteger(p) || p < 1
      ? "Партиций должно быть не меньше 1"
      : !Number.isInteger(r) || (r < 1 && r !== -1)
        ? "Фактор репликации — целое ≥ 1 (или -1 — по умолчанию брокера)"
        : null;
  const spec: NewTopicSpec = { name, partitions: p, replicationFactor: r, configs };
  const valid = !nameError && !extraError && !numbersError;

  const create = useMutation({
    mutationFn: (confirm: string | null) => api.createTopic(connection.id, spec, confirm),
    onSuccess: () => onCreated(name),
  });

  const submit = () => {
    if (needsConfirmation(connection)) setConfirming(true);
    else create.mutate(null);
  };

  return (
    <div className="panel">
      <h3>Новый топик</h3>
      <fieldset className="form" disabled={confirming} style={{ border: 0, padding: 0, margin: 0 }}>
        <label htmlFor="ct-name">Имя топика</label>
        <input id="ct-name" value={name} onChange={(e) => setName(e.target.value)} />
        {name && nameError && <p className="warn full">{nameError}</p>}
        {topicNameWarning(name) && <p className="muted full">{topicNameWarning(name)}</p>}
        <label htmlFor="ct-partitions">Партиции</label>
        <input id="ct-partitions" type="number" min={1} value={partitions} onChange={(e) => setPartitions(e.target.value)} />
        <label htmlFor="ct-rf">Фактор репликации</label>
        <input id="ct-rf" type="number" value={rfValue} onChange={(e) => setRf(e.target.value)} />
        {brokers !== undefined && <p className="muted full">Брокеров в кластере: {brokers}</p>}
        <label htmlFor="ct-preset">Пресет</label>
        <select id="ct-preset" value={preset} onChange={(e) => setPreset(e.target.value)}>
          {TOPIC_PRESETS.map((pr) => (
            <option key={pr.id} value={pr.id}>
              {pr.label}
            </option>
          ))}
        </select>
        <label htmlFor="ct-configs">Доп. конфиги (key=value)</label>
        <textarea id="ct-configs" rows={3} value={extra} onChange={(e) => setExtra(e.target.value)} />
      </fieldset>
      {(extraError || numbersError) && <p className="warn">{extraError ?? numbersError}</p>}
      {valid && <pre className="muted">{buildCreateCommand(connection.bootstrapServers, spec)}</pre>}
      {create.error && <ErrorBanner error={create.error} />}
      {confirming ? (
        <ConfirmDialog
          connection={connection}
          title={`Создать топик «${name}»?`}
          confirmLabel="Создать топик"
          busy={create.isPending}
          onConfirm={(c) => create.mutate(c.confirm)}
          onCancel={() => setConfirming(false)}
        />
      ) : (
        <div className="toolbar">
          <button type="button" className="primary" disabled={!valid || create.isPending} onClick={submit}>
            Создать
          </button>
          <button type="button" onClick={onCancel}>
            Отмена
          </button>
        </div>
      )}
    </div>
  );
}
