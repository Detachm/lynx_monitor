import type { SymbolState } from '@/types/market'

export type RuntimeStatusKey = 'loading' | 'warm' | 'live' | 'closed' | 'degraded'

export interface RuntimeStatus {
  key: RuntimeStatusKey
  label: 'Loading' | 'Warm' | 'Live' | 'Closed' | 'Degraded'
}

export function runtimeStatus(state: SymbolState | null | undefined): RuntimeStatus {
  if (state?.subscriptionStatus === 'degraded') {
    return { key: 'degraded', label: 'Degraded' }
  }
  if (state?.subscriptionStatus === 'loading' && !state.snapshotLoaded) {
    return { key: 'loading', label: 'Loading' }
  }
  if (!state?.snapshotLoaded) {
    return { key: 'loading', label: 'Loading' }
  }

  const freshness = state.freshness
  if (freshness?.degraded || freshness?.runtimeState === 'DEGRADED') {
    return { key: 'degraded', label: 'Degraded' }
  }

  const requested = freshness?.requestedTradeDate ?? state.snapshot?.requestedTradeDate
  const effective = freshness?.effectiveTradeDate ?? state.snapshot?.tradeDate
  if (requested && effective && requested !== effective) {
    return { key: 'closed', label: 'Closed' }
  }

  if (freshness?.runtimeState === 'LIVE') {
    return { key: 'live', label: 'Live' }
  }
  if (state.subscriptionStatus === 'closed') {
    return { key: 'closed', label: 'Closed' }
  }
  if (state.subscriptionStatus === 'live') {
    return { key: 'live', label: 'Live' }
  }
  return { key: 'warm', label: 'Warm' }
}
