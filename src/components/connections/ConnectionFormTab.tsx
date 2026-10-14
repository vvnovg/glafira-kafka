import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import { ErrorBanner } from "../common/ErrorBanner";
import { ConnectionForm } from "./ConnectionForm";
import { newProfile } from "./profile";

export function ConnectionFormTab({ connectionId, tabId }: { connectionId: string | null; tabId: string }) {
  const q = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  if (connectionId === null) return <ConnectionForm initial={newProfile()} tabId={tabId} />;
  if (q.isPending) return <p className="empty">Загрузка…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const profile = q.data.find((c) => c.id === connectionId);
  if (!profile) return <p className="empty">Подключение удалено.</p>;
  return <ConnectionForm key={profile.id} initial={profile} tabId={tabId} />;
}
