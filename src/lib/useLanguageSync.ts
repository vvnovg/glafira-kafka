import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { setLanguage } from "../i18n";

/** Применяет язык интерфейса, выбранный в Rust; запрос повторяется при смене тарифа лицензии. */
export function useLanguageSync() {
  const license = useQuery({ queryKey: ["license"], queryFn: api.getLicenseStatus, staleTime: 60_000 });
  const q = useQuery({ queryKey: ["language", license.data?.tier], queryFn: api.getLanguageSettings });
  useEffect(() => {
    if (q.data) void setLanguage(q.data.effective);
  }, [q.data]);
}
