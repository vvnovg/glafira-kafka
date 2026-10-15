import { useEffect, useId, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import { ErrorBanner } from "../common/ErrorBanner";
import { formToMessage, messageToForm, type ProducerForm } from "./form";

interface Props {
  connectionId: string;
  form: ProducerForm;
  onLoad: (form: ProducerForm) => void;
  /** Меняется, когда форму заменили не шаблоном (очистка, история) — выбор шаблона снимается. */
  resetKey?: number;
}

export function TemplatesBar({ connectionId, form, onLoad, resetKey }: Props) {
  const queryClient = useQueryClient();
  const uid = useId();
  const templates = useQuery({ queryKey: ["templates", connectionId], queryFn: () => api.listTemplates(connectionId) });
  const [selected, setSelected] = useState("");
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [name, setName] = useState("");
  const [global, setGlobal] = useState(false);
  useEffect(() => {
    setSelected("");
    setDeleting(false);
  }, [resetKey]);
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ["templates"] });
  const save = useMutation({
    mutationFn: () => {
      // В шаблоне топик может быть пустым — проверяем только структуру формы.
      const message = formToMessage({ ...form, topic: form.topic.trim() || "-" });
      return api.saveTemplate({
        id: "",
        name: name.trim(),
        connectionId: global ? null : connectionId,
        message: { ...message, topic: form.topic.trim() },
        settings: form.settings,
      });
    },
    onSuccess: (t) => {
      setSaving(false);
      setName("");
      setGlobal(false);
      setSelected(t.id);
      invalidate();
    },
  });
  const remove = useMutation({
    mutationFn: (id: string) => api.deleteTemplate(id),
    onSuccess: () => {
      setSelected("");
      setDeleting(false);
      invalidate();
    },
  });
  const current = templates.data?.find((t) => t.id === selected);

  return (
    <div className="toolbar">
      <label htmlFor={`${uid}-tpl`}>Шаблон</label>
      <select
        id={`${uid}-tpl`}
        value={selected}
        onChange={(e) => {
          setSelected(e.target.value);
          setDeleting(false);
          const t = templates.data?.find((x) => x.id === e.target.value);
          if (t) onLoad(messageToForm(t.message, t.settings));
        }}
      >
        <option value="">—</option>
        {templates.data?.map((t) => (
          <option key={t.id} value={t.id}>
            {t.name}
            {t.connectionId === null ? " (все подключения)" : ""}
          </option>
        ))}
      </select>
      {current &&
        (deleting ? (
          <span role="group" aria-label={`Удалить шаблон «${current.name}»?`}>
            Удалить шаблон «{current.name}»?{" "}
            <button type="button" className="danger" disabled={remove.isPending} onClick={() => remove.mutate(current.id)}>
              Удалить
            </button>{" "}
            <button type="button" disabled={remove.isPending} onClick={() => setDeleting(false)}>
              Отмена
            </button>
          </span>
        ) : (
          <button type="button" className="link" onClick={() => setDeleting(true)}>
            удалить шаблон
          </button>
        ))}
      {saving ? (
        <>
          <label htmlFor={`${uid}-name`}>Имя шаблона</label>
          <input id={`${uid}-name`} value={name} onChange={(e) => setName(e.target.value)} />
          <label>
            <input type="checkbox" checked={global} onChange={(e) => setGlobal(e.target.checked)} /> Для всех подключений
          </label>
          <button type="button" className="primary" disabled={!name.trim() || save.isPending} onClick={() => save.mutate()}>
            Сохранить
          </button>
          <button type="button" onClick={() => setSaving(false)}>
            Отмена
          </button>
        </>
      ) : (
        <button type="button" onClick={() => setSaving(true)}>
          Сохранить как шаблон
        </button>
      )}
      {templates.error && <ErrorBanner error={templates.error} onRetry={() => templates.refetch()} />}
      {(save.error || remove.error) && <ErrorBanner error={save.error ?? remove.error} />}
    </div>
  );
}
