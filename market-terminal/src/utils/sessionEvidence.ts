import type { SymbolState } from '@/types/market'
import { runtimeStatus } from '@/utils/runtimeStatus'

export function sessionEvidence(state: SymbolState | null) {
  const freshness = state?.freshness
  const degradedReasons = freshness?.degradedReasons?.length
    ? freshness.degradedReasons
    : state?.subscriptionError
      ? [state.subscriptionError]
      : []

  return {
    status: runtimeStatus(state),
    requestedDate: freshness?.requestedTradeDate ?? state?.snapshot?.requestedTradeDate ?? '--',
    effectiveDate: freshness?.effectiveTradeDate ?? state?.snapshot?.tradeDate ?? '--',
    sourceDates: freshness?.sourceDates ?? {},
    degradedReasons,
  }
}
