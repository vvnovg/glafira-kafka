import { type KeyboardEvent, type ReactNode, useEffect, useRef, useState } from "react";
import type { Confirmation, ConnectionProfile } from "../../api/types";

/** Нужен ли диалог: prod-подключение или операция с вводом имени объекта (удаление, purge). */
export function needsConfirmation(connection: ConnectionProfile, objectName?: string): boolean {
  return connection.environment === "prod" || objectName !== undefined;
}

interface Props {
  connection: ConnectionProfile;
  title: string;
  confirmLabel: string;
  /** Имя объекта, которое нужно ввести для подтверждения. */
  objectName?: string;
  /** Что именно вводится: «имя топика», «число топиков», «id группы». */
  objectLabel?: string;
  danger?: boolean;
  busy?: boolean;
  children?: ReactNode;
  onConfirm: (c: Confirmation) => void;
  onCancel: () => void;
}

/**
 * Подтверждение внутри страницы (нативный window.confirm в Tauri 2 не показывается).
 * Проверка дублируется в Rust: без верных значений команда вернёт CONFIRMATION_REQUIRED.
 */
export function ConfirmDialog({
  connection,
  title,
  confirmLabel,
  objectName,
  objectLabel = "имя",
  danger,
  busy,
  children,
  onConfirm,
  onCancel,
}: Props) {
  const [connName, setConnName] = useState("");
  const [typed, setTyped] = useState("");
  const rootRef = useRef<HTMLDivElement>(null);
  const isProd = connection.environment === "prod";
  const ok =
    (!isProd || connName.trim() === connection.name.trim()) &&
    (objectName === undefined || typed.trim() === objectName);

  useEffect(() => {
    rootRef.current?.querySelector("input")?.focus();
  }, []);

  const submit = () =>
    onConfirm({
      confirm: isProd ? connName.trim() : null,
      typedName: objectName !== undefined ? typed.trim() : null,
    });

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Escape") {
      if (!busy) onCancel();
    } else if (e.key === "Enter" && !(e.target instanceof HTMLButtonElement)) {
      e.preventDefault();
      if (ok && !busy) submit();
    }
  };

  return (
    <div className="confirm" role="group" aria-label={title} ref={rootRef} onKeyDown={onKeyDown}>
      <strong>{title}</strong>
      {children}
      {objectName !== undefined && (
        <label>
          Для подтверждения введите {objectLabel} <code>{objectName}</code>
          <input aria-label={`Подтверждение: ${objectLabel}`} value={typed} disabled={busy} onChange={(e) => setTyped(e.target.value)} />
        </label>
      )}
      {isProd && (
        <label>
          Это prod-подключение. Введите его имя <code>{connection.name}</code>
          <input aria-label="Имя подключения" value={connName} disabled={busy} onChange={(e) => setConnName(e.target.value)} />
        </label>
      )}
      <div className="toolbar">
        <button
          type="button"
          className={danger ? "danger" : "primary"}
          disabled={!ok || busy}
          onClick={submit}
        >
          {confirmLabel}
        </button>
        <button type="button" disabled={busy} onClick={onCancel}>
          Отмена
        </button>
      </div>
    </div>
  );
}
