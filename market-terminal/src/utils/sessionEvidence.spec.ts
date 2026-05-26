import { describe, expect, it } from 'vitest'

import { sessionEvidence } from '@/utils/sessionEvidence'
import type { SymbolState } from '@/types/market'

describe('sessionEvidence', () => {
  it('exposes closed-market effective date and source-date evidence', () => {
    const evidence = sessionEvidence(
      makeState({
        requestedTradeDate: '20260525',
        effectiveTradeDate: '20260522',
        runtimeState: 'WARM',
        sourceDates: {
          minute_bars: '20260522',
          ccass_current: '20260521',
        },
      }),
    )

    expect(evidence.status).toEqual({ key: 'closed', label: 'Closed' })
    expect(evidence.requestedDate).toBe('20260525')
    expect(evidence.effectiveDate).toBe('20260522')
    expect(evidence.sourceDates.minute_bars).toBe('20260522')
    expect(evidence.sourceDates.ccass_current).toBe('20260521')
  })

  it('falls back to subscription error when freshness has no degraded reasons', () => {
    const state = makeState({ runtimeState: 'DEGRADED', degraded: true, degradedReasons: [] })
    state.subscriptionStatus = 'degraded'
    state.subscriptionError = 'redis_down'

    const evidence = sessionEvidence(state)

    expect(evidence.status).toEqual({ key: 'degraded', label: 'Degraded' })
    expect(evidence.degradedReasons).toEqual(['redis_down'])
  })
})

function makeState(freshness: Partial<NonNullable<SymbolState['freshness']>>): SymbolState {
  return {
    snapshot: {
      symbol: '00700.HK',
      name: 'Tencent',
      currency: 'HKD',
      price: 388.4,
      previousClose: 386,
      open: 387,
      high: 389,
      low: 386.5,
      change: 2.4,
      changePercent: 0.62,
      volume: 1000,
      turnover: 388400,
      tradeDate: freshness.effectiveTradeDate ?? '20260522',
      requestedTradeDate: freshness.requestedTradeDate ?? '20260522',
      updatedAt: '2026-05-25T09:30:00+08:00',
    },
    ticks: [],
    alerts: [],
    askQueues: [],
    bidQueues: [],
    holding: [],
    holdingHistoryByParticipant: {},
    freshness: {
      updatedAt: '2026-05-25T09:30:00+08:00',
      degraded: false,
      degradedReasons: [],
      requestedTradeDate: '20260522',
      effectiveTradeDate: '20260522',
      sourceDates: {},
      ...freshness,
    },
    subscriptionStatus: 'warm',
    subscriptionError: null,
    snapshotLoaded: true,
    lastUpdatedAt: '2026-05-25T09:30:00+08:00',
    unreadAlerts: 0,
  }
}
