import { describe, expect, it } from 'vitest'

import { runtimeStatus } from '@/utils/runtimeStatus'
import type { SymbolState } from '@/types/market'

describe('runtimeStatus', () => {
  it('treats requested/effective date mismatch as closed even when runtime is warm', () => {
    expect(
      runtimeStatus(
        makeState({
          requestedTradeDate: '20260525',
          effectiveTradeDate: '20260522',
          runtimeState: 'WARM',
        }),
      ),
    ).toEqual({ key: 'closed', label: 'Closed' })
  })

  it('does not label same-day warm snapshots as live', () => {
    expect(runtimeStatus(makeState({ runtimeState: 'WARM' }))).toEqual({ key: 'warm', label: 'Warm' })
  })

  it('labels only explicit live freshness as live', () => {
    expect(runtimeStatus(makeState({ runtimeState: 'LIVE' }))).toEqual({ key: 'live', label: 'Live' })
  })

  it('prioritizes degraded and loading states over date evidence', () => {
    expect(
      runtimeStatus(
        makeState({
          requestedTradeDate: '20260525',
          effectiveTradeDate: '20260522',
          runtimeState: 'DEGRADED',
          degraded: true,
        }),
      ),
    ).toEqual({ key: 'degraded', label: 'Degraded' })
    expect(runtimeStatus({ ...makeState(), snapshotLoaded: false, subscriptionStatus: 'loading' })).toEqual({
      key: 'loading',
      label: 'Loading',
    })
  })
})

function makeState(
  freshness: Partial<NonNullable<SymbolState['freshness']>> = {},
): SymbolState {
  return {
    snapshot: {
      symbol: '00700.HK',
      name: '00700.HK',
      currency: 'HKD',
      tradeDate: freshness.effectiveTradeDate ?? '20260522',
      requestedTradeDate: freshness.requestedTradeDate ?? '20260522',
      price: 388.4,
      previousClose: 386.2,
      open: 386.2,
      high: 388.4,
      low: 386.2,
      volume: 1000,
      turnover: 388400,
      change: 2.2,
      changePercent: 0.57,
      updatedAt: '2026-05-22T09:30:00+08:00',
    },
    ticks: [],
    alerts: [],
    askQueues: [],
    bidQueues: [],
    holding: [],
    holdingHistoryByParticipant: {},
    freshness: {
      updatedAt: '2026-05-22T09:30:00+08:00',
      degradedReasons: [],
      sourceDates: {},
      requestedTradeDate: '20260522',
      effectiveTradeDate: '20260522',
      ...freshness,
    },
    subscriptionStatus: 'warm',
    subscriptionError: null,
    snapshotLoaded: true,
    lastUpdatedAt: '2026-05-22T09:30:00+08:00',
    unreadAlerts: 0,
  }
}
