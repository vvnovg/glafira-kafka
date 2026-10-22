import { useTranslation } from "react-i18next";
import type { ProposalPrefill } from "../../state/workspace";

/** Строка над формой, открытой из предложения LLM (спец. G3 §8): какое предложение применяется. */
export function ProposalBanner({ prefill }: { prefill: ProposalPrefill | undefined }) {
  const { t } = useTranslation();
  if (!prefill) return null;
  return <p className="muted">{t("health.proposals.applying", { title: prefill.proposal.title })}</p>;
}
