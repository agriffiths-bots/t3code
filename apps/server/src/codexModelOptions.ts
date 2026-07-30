import type { ModelSelection } from "@t3tools/contracts";
import {
  getModelSelectionBooleanOptionValue,
  getModelSelectionStringOptionValue,
} from "@t3tools/shared/model";

/** Explicit Standard routing sentinel used by Codex 0.145+ app-server and CLI config. */
export const CODEX_STANDARD_SERVICE_TIER = "default";
/** Canonical model-catalog/request id for Codex Fast routing. */
export const CODEX_FAST_SERVICE_TIER = "priority";

export function normalizeCodexServiceTier(value: string | undefined): string | undefined {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) return undefined;
  // `fast` is the legacy config/T3 value; current model catalogs advertise `priority`.
  return normalized === "fast" ? CODEX_FAST_SERVICE_TIER : normalized;
}

export function getCodexServiceTierOptionValue(
  modelSelection: ModelSelection | null | undefined,
): string | undefined {
  const selected = normalizeCodexServiceTier(
    getModelSelectionStringOptionValue(modelSelection, "serviceTier"),
  );
  if (selected) return selected;

  const legacyFastMode = getModelSelectionBooleanOptionValue(modelSelection, "fastMode");
  if (legacyFastMode === true) return CODEX_FAST_SERVICE_TIER;
  if (legacyFastMode === false) return CODEX_STANDARD_SERVICE_TIER;
  return undefined;
}
