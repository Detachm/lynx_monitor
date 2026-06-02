import { createPinia, setActivePinia } from 'pinia'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  SMOKE_MACHINE_ID_STORAGE_KEY,
  WATCHLIST_STORAGE_KEY,
  holdingHistoryKey,
  useMarketStore,
} from '@/stores/market'
import type {
  BigTradeAlert,
  BrokerQueueEntry,
  HoldingEntry,
  MarketMessage,
  MarketPerformanceSample,
  MarketSnapshot,
  PriceTick,
} from '@/types/market'

describe('market store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  afterEach(async () => {
    const store = useMarketStore()
    await store.disconnectDataSource()
    store.clearPerformanceSamples()
    vi.unstubAllGlobals()
  })

  it('tracks multi-symbol subscription, active tab switching, and unsubscribe state', async () => {
    const store = useMarketStore()

    await store.subscribeSymbol('00700')
    await store.subscribeSymbol('939')
    store.setActiveSymbol('00939.HK')

    expect(store.subscribedSymbols).toEqual(['00700.HK', '00939.HK'])
    expect(store.activeSymbol).toBe('00939.HK')
    expect(store.symbols['00939.HK']!.subscriptionStatus).toBe('loading')

    await store.unsubscribeSymbol('00939.HK')

    expect(store.subscribedSymbols).toEqual(['00700.HK'])
    expect(store.activeSymbol).toBe('00700.HK')
    expect(store.symbols['00939.HK']).toBeDefined()
    expect(store.symbols['00939.HK']!.subscriptionStatus).toBe('idle')
  })

  it('loads local watchlist on startup and persists add/remove changes', async () => {
    const storage = new MemoryStorage({
      [WATCHLIST_STORAGE_KEY]: JSON.stringify(['700', '00939.HK', '700', 'BAD']),
    })
    vi.stubGlobal('localStorage', storage)
    const store = useMarketStore()
    const subscribed: string[] = []
    const unsubscribed: string[] = []
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async (symbol: string) => {
        subscribed.push(symbol)
      },
      unsubscribe: async (symbol: string) => {
        unsubscribed.push(symbol)
      },
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    await store.initialize('mock', { defaultSymbols: ['00005.HK'] }, source)
    await store.subscribeSymbol('5')
    await store.unsubscribeSymbol('00939.HK')

    expect(subscribed).toEqual(['00700.HK', '00939.HK', '00005.HK'])
    expect(unsubscribed).toEqual(['00939.HK'])
    expect(store.subscribedSymbols).toEqual(['00700.HK', '00005.HK'])
    expect(JSON.parse(storage.getItem(WATCHLIST_STORAGE_KEY) ?? '[]')).toEqual([
      '00700.HK',
      '00005.HK',
    ])
  })

  it('does not let a slow startup subscription block later watchlist snapshots', async () => {
    const store = useMarketStore()
    const started: string[] = []
    let releaseFirst = () => {}
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async (symbol: string) => {
        started.push(symbol)
        if (symbol === '00700.HK') {
          await new Promise<void>((resolve) => {
            releaseFirst = resolve
          })
        }
        store.handleMessage({
          type: 'snapshot',
          symbol,
          snapshot: makeSnapshot(symbol, symbol === '00939.HK' ? 7.4 : 388),
          ticks: [makeTick(symbol === '00939.HK' ? 7.4 : 388)],
          alerts: [],
          askQueues: [],
          bidQueues: [],
          holding: [],
          freshness: {
            updatedAt: '2026-05-22T09:30:00+08:00',
            runtimeState: 'WARM',
            degraded: false,
            degradedReasons: [],
            sourceDates: { minute_bars: '20260522' },
          },
        })
      },
      unsubscribe: async () => {},
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    const initialized = store.initialize(
      'mock',
      { defaultSymbols: ['00700.HK', '00939.HK'] },
      source,
    )

    await new Promise((resolve) => setTimeout(resolve, 0))
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(started).toEqual(['00700.HK', '00939.HK'])
    expect(store.connectionStatus).toBe('connected')
    expect(store.symbols['00939.HK']!.snapshotLoaded).toBe(true)
    expect(store.symbols['00939.HK']!.ticks).toHaveLength(1)
    expect(store.symbols['00700.HK']!.snapshotLoaded).toBe(false)

    releaseFirst?.()
    await initialized

    expect(store.symbols['00700.HK']!.snapshotLoaded).toBe(true)
  })

  it('stores snapshot, tick, alert, queue, and holding history messages by symbol', async () => {
    const store = useMarketStore()
    await store.subscribeSymbol('00700')
    await store.subscribeSymbol('00939')

    const snapshot = makeSnapshot('00700.HK', 388)
    const tick = makeTick(389)
    const alert = makeAlert('alert-1')
    const askQueues = [makeQueue('ask-1', 'ask')]
    const holding = [makeHolding('JPMorgan Chase Bank, N.A.')]

    const messages: MarketMessage[] = [
      {
        type: 'snapshot',
        symbol: '00700.HK',
        snapshot,
        ticks: [tick],
        alerts: [alert],
        askQueues,
        bidQueues: [],
        holding,
        freshness: {
          updatedAt: '2026-05-22T09:30:00+08:00',
          requestedTradeDate: '20260525',
          effectiveTradeDate: '20260522',
          runtimeState: 'WARM',
          degraded: false,
          degradedReasons: [],
          sourceDates: { minute_bars: '20260522' },
        },
      },
      {
        type: 'tick_realtime',
        symbol: '00939.HK',
        tick: makeTick(7.4),
        snapshot: makeSnapshot('00939.HK', 7.4),
      },
      {
        type: 'queue_realtime',
        symbol: '00939.HK',
        side: 'bid',
        bidQueues: [makeQueue('bid-1', 'bid')],
      },
      {
        type: 'holding_name_click_response',
        symbol: '00700.HK',
        participantName: 'JPMorgan Chase Bank, N.A.',
        days: 7,
        history: [{ date: '2026-05-22', shares: 1000, percent: 1.1, change: 200 }],
      },
    ]

    for (const message of messages) {
      store.handleMessage(message)
    }

    const tencent = store.symbols['00700.HK']!
    const ccb = store.symbols['00939.HK']!

    expect(tencent.snapshot?.price).toBe(388)
    expect(tencent.holding).toEqual(holding)
    expect(tencent.freshness?.effectiveTradeDate).toBe('20260522')
    expect(tencent.subscriptionStatus).toBe('closed')
    expect(ccb.ticks).toHaveLength(1)
    expect(ccb.subscriptionStatus).toBe('live')
    expect(ccb.bidQueues[0]?.id).toBe('bid-1')
    expect(
      tencent.holdingHistoryByParticipant[holdingHistoryKey('JPMorgan Chase Bank, N.A.', 7)],
    ).toHaveLength(1)
  })

  it('increments unread alerts for inactive symbols and clears them on tab activation', async () => {
    const store = useMarketStore()
    await store.subscribeSymbol('00700')
    await store.subscribeSymbol('00939')

    store.handleMessage({
      type: 'alert_realtime',
      symbol: '00939.HK',
      alert: makeAlert('alert-inactive'),
    })

    expect(store.symbols['00939.HK']!.unreadAlerts).toBe(1)

    store.setActiveSymbol('00939.HK')

    expect(store.symbols['00939.HK']!.unreadAlerts).toBe(0)
  })

  it('merges realtime ticks into the current minute bar for chart state', async () => {
    const store = useMarketStore()
    await store.subscribeSymbol('00700')
    store.handleMessage({
      type: 'snapshot',
      symbol: '00700.HK',
      snapshot: makeSnapshot('00700.HK', 388),
      ticks: [makeTick(388.4, '2026-05-22T09:30:00+08:00')],
      alerts: [],
      askQueues: [],
      bidQueues: [],
      holding: [],
      freshness: {
        updatedAt: '2026-05-22T09:30:00+08:00',
        runtimeState: 'WARM',
        degraded: false,
        degradedReasons: [],
        sourceDates: { minute_bars: '20260522' },
      },
    })

    store.handleMessage({
      type: 'tick_realtime',
      symbol: '00700.HK',
      tick: makeTick(388.8, '2026-05-22T09:30:05+08:00'),
    })
    store.handleMessage({
      type: 'tick_realtime',
      symbol: '00700.HK',
      tick: makeTick(388.2, '2026-05-22T09:30:45+08:00'),
    })
    store.handleMessage({
      type: 'tick_realtime',
      symbol: '00700.HK',
      tick: makeTick(388.5, '2026-05-22T09:31:01+08:00'),
    })

    const ticks = store.symbols['00700.HK']!.ticks
    expect(ticks).toHaveLength(2)
    expect(ticks[0]).toMatchObject({
      timestamp: '2026-05-22T09:30:00+08:00',
      price: 388.2,
      volume: 3000,
    })
    expect(ticks[1]).toMatchObject({
      timestamp: '2026-05-22T09:31:00+08:00',
      price: 388.5,
      volume: 1000,
    })
  })

  it('clears stale symbol state on runtime epoch change and waits for the new snapshot', async () => {
    const store = useMarketStore()
    await store.subscribeSymbol('00700')
    store.handleMessage({
      type: 'snapshot',
      symbol: '00700.HK',
      runtimeEpoch: 'epoch-1',
      snapshot: makeSnapshot('00700.HK', 388),
      ticks: [makeTick(388.4)],
      alerts: [makeAlert('alert-1')],
      askQueues: [makeQueue('ask-1', 'ask')],
      bidQueues: [],
      holding: [],
      freshness: {
        updatedAt: '2026-05-22T09:30:00+08:00',
        runtimeState: 'LIVE',
        degraded: false,
        degradedReasons: [],
        sourceDates: { minute_bars: '20260522' },
      },
    })

    store.handleMessage({
      type: 'queue_realtime',
      symbol: '00700.HK',
      runtimeEpoch: 'epoch-2',
      askQueues: [makeQueue('ask-new', 'ask')],
    })

    expect(store.symbols['00700.HK']!).toMatchObject({
      runtimeEpoch: 'epoch-2',
      snapshot: null,
      ticks: [],
      alerts: [],
      askQueues: [],
      snapshotLoaded: false,
      subscriptionStatus: 'loading',
    })

    store.handleMessage({
      type: 'snapshot',
      symbol: '00700.HK',
      runtimeEpoch: 'epoch-2',
      snapshot: makeSnapshot('00700.HK', 389),
      ticks: [makeTick(389)],
      alerts: [],
      askQueues: [],
      bidQueues: [],
      holding: [],
      freshness: {
        updatedAt: '2026-05-22T09:31:00+08:00',
        runtimeState: 'LIVE',
        degraded: false,
        degradedReasons: [],
        sourceDates: { minute_bars: '20260522' },
      },
    })

    expect(store.symbols['00700.HK']!.snapshot?.price).toBe(389)
    expect(store.symbols['00700.HK']!.ticks).toHaveLength(1)
    expect(store.symbols['00700.HK']!.snapshotLoaded).toBe(true)
  })

  it('ignores anomalous non-replace realtime tick volume above the current snapshot volume', async () => {
    const store = useMarketStore()
    const samples: MarketPerformanceSample[] = []
    store.setPerformanceSampleHandler((sample) => samples.push(sample))
    await store.subscribeSymbol('00700')
    store.handleMessage({
      type: 'snapshot',
      symbol: '00700.HK',
      snapshot: makeSnapshot('00700.HK', 388),
      ticks: [makeTick(388.4, '2026-05-22T09:30:00+08:00')],
      alerts: [],
      askQueues: [],
      bidQueues: [],
      holding: [],
      freshness: {
        updatedAt: '2026-05-22T09:30:00+08:00',
        runtimeState: 'LIVE',
        degraded: false,
        degradedReasons: [],
        requestedTradeDate: '20260522',
        effectiveTradeDate: '20260522',
        sourceDates: { minute_bars: '20260522' },
      },
    })

    store.handleMessage({
      type: 'tick_realtime',
      symbol: '00700.HK',
      tick: {
        ...makeTick(389, '2026-05-22T09:30:05+08:00'),
        volume: 4_000_000_000,
        turnover: 1_556_000_000_000,
      },
    })

    expect(store.symbols['00700.HK']!.ticks[0]?.volume).toBe(1000)
    expect(samples.some((sample) => sample.key === 'realtime_tick_volume_anomaly')).toBe(true)
  })

  it('sorts and deduplicates snapshot minute bars with mixed timezone encodings', async () => {
    const store = useMarketStore()
    await store.subscribeSymbol('00700')

    store.handleMessage({
      type: 'snapshot',
      symbol: '00700.HK',
      snapshot: makeSnapshot('00700.HK', 388),
      ticks: [
        makeTick(388.8, '2026-05-22T09:32:05+08:00'),
        makeTick(388.4, '2026-05-22T01:30:10Z'),
        makeTick(388.6, '2026-05-22T09:31:45+08:00'),
        makeTick(388.5, '2026-05-22T09:30:25+08:00'),
      ],
      alerts: [],
      askQueues: [],
      bidQueues: [],
      holding: [],
      freshness: {
        updatedAt: '2026-05-22T09:32:00+08:00',
        runtimeState: 'WARM',
        degraded: false,
        degradedReasons: [],
        sourceDates: { minute_bars: '20260522' },
      },
    })

    const ticks = store.symbols['00700.HK']!.ticks
    expect(ticks.map((tick) => tick.timestamp)).toEqual([
      '2026-05-22T09:30:00+08:00',
      '2026-05-22T09:31:00+08:00',
      '2026-05-22T09:32:00+08:00',
    ])
    expect(ticks[0]).toMatchObject({
      price: 388.5,
      volume: 2000,
    })
  })

  it('filters cross-day and preopen minute bars from snapshots and realtime ticks', async () => {
    const store = useMarketStore()
    await store.subscribeSymbol('00700')

    store.handleMessage({
      type: 'snapshot',
      symbol: '00700.HK',
      snapshot: makeSnapshot('00700.HK', 388),
      ticks: [
        makeTick(387.8, '2026-05-21T16:08:00+08:00'),
        makeTick(388.1, '2026-05-22T09:20:00+08:00'),
        makeTick(388.5, '2026-05-22T09:30:25+08:00'),
      ],
      alerts: [],
      askQueues: [],
      bidQueues: [],
      holding: [],
      freshness: {
        updatedAt: '2026-05-22T09:30:00+08:00',
        runtimeState: 'WARM',
        degraded: false,
        degradedReasons: [],
        requestedTradeDate: '20260522',
        effectiveTradeDate: '20260522',
        sourceDates: { minute_bars: '20260522' },
      },
    })

    store.handleMessage({
      type: 'tick_realtime',
      symbol: '00700.HK',
      tick: makeTick(388.2, '2026-05-22T09:29:59+08:00'),
      freshness: {
        updatedAt: '2026-05-22T09:29:59+08:00',
        runtimeState: 'LIVE',
        degraded: false,
        degradedReasons: [],
        requestedTradeDate: '20260522',
        effectiveTradeDate: '20260522',
        sourceDates: { minute_bars: '20260522' },
      },
    })

    expect(store.symbols['00700.HK']!.ticks.map((tick) => tick.timestamp)).toEqual([
      '2026-05-22T09:30:00+08:00',
    ])
  })

  it('records frontend store update performance samples for shadow-run evidence', () => {
    const store = useMarketStore()
    const samples: MarketPerformanceSample[] = []
    store.setPerformanceSampleHandler((sample) => samples.push(sample))

    store.handleMessage({
      type: 'tick_realtime',
      symbol: '00700.HK',
      tick: makeTick(389),
    })

    expect(samples).toHaveLength(1)
    expect(samples[0]).toMatchObject({
      key: 'frontend_store_update_ms',
      symbol: '00700.HK',
      messageType: 'tick_realtime',
    })
    expect(samples[0]!.valueMs).toBeGreaterThanOrEqual(0)
    expect(samples[0]!.recordedAt).toContain('T')
    expect(store.getPerformanceSamples()[0]).toMatchObject({ key: 'frontend_store_update_ms' })
  })

  it('records subscribe snapshot latency and exports client smoke observation', async () => {
    const storage = new MemoryStorage()
    vi.stubGlobal('localStorage', storage)
    vi.stubGlobal('location', { href: 'http://192.168.1.10:5173/' })
    const store = useMarketStore()
    const samples: MarketPerformanceSample[] = []
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async () => {},
      unsubscribe: async () => {},
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }
    await store.initialize(
      'mock',
      { defaultSymbols: ['00700.HK'], performanceSampleHandler: (sample) => samples.push(sample) },
      source,
    )
    store.handleMessage({
      type: 'snapshot',
      symbol: '00700.HK',
      snapshot: makeSnapshot('00700.HK', 388),
      ticks: [],
      alerts: [],
      askQueues: [],
      bidQueues: [],
      holding: [],
      freshness: {
        updatedAt: '2026-05-22T09:30:00+08:00',
        requestedTradeDate: '20260522',
        effectiveTradeDate: '20260522',
        runtimeState: 'WARM',
        degraded: false,
        degradedReasons: [],
        sourceDates: {},
      },
    })

    const subscribeSamples = store
      .getPerformanceSamples()
      .filter((sample) => sample.key === 'subscribe_snapshot_ms')
    expect(subscribeSamples).toHaveLength(1)
    expect(subscribeSamples[0]).toMatchObject({ symbol: '00700.HK' })
    expect(subscribeSamples[0]!.valueMs).toBeGreaterThanOrEqual(0)
    expect(samples.some((sample) => sample.key === 'subscribe_snapshot_ms')).toBe(true)
    expect(store.smokeClientObservation('desk-a')).toEqual({
      machine_id: 'desk-a',
      data_source_mode: 'mock',
      page_url: 'http://192.168.1.10:5173/',
      gateway_url: '',
      connected: true,
      watchlist: ['00700.HK'],
      refresh_recovered: true,
      symbol_statuses: {
        '00700.HK': {
          status: 'warm',
          snapshot_loaded: true,
          requested_trade_date: '20260522',
          effective_trade_date: '20260522',
          source_dates: {},
          degraded_reasons: [],
        },
      },
    })
    const generatedObservation = store.smokeClientObservation()
    expect(generatedObservation.machine_id).toMatch(/^desk-/)
    expect(storage.getItem(SMOKE_MACHINE_ID_STORAGE_KEY)).toBe(generatedObservation.machine_id)
  })

  it('exposes and resets the persisted smoke machine id for operator checks', () => {
    const storage = new MemoryStorage({
      [SMOKE_MACHINE_ID_STORAGE_KEY]: 'desk-existing',
    })
    vi.stubGlobal('localStorage', storage)
    const store = useMarketStore()

    expect(store.smokeMachineId()).toBe('desk-existing')

    const resetId = store.resetSmokeMachineId()

    expect(resetId).toMatch(/^desk-/)
    expect(resetId).not.toBe('desk-existing')
    expect(storage.getItem(SMOKE_MACHINE_ID_STORAGE_KEY)).toBe(resetId)
  })

  it('normalizes persisted smoke machine id before exporting client evidence', () => {
    const storage = new MemoryStorage({
      [SMOKE_MACHINE_ID_STORAGE_KEY]: '  desk-a  ',
    })
    vi.stubGlobal('localStorage', storage)
    const store = useMarketStore()

    expect(store.smokeMachineId()).toBe('desk-a')
    expect(storage.getItem(SMOKE_MACHINE_ID_STORAGE_KEY)).toBe('desk-a')
    expect(store.smokeClientObservation().machine_id).toBe('desk-a')
  })

  it('exports backend-accepted loading status for subscribed symbols without a snapshot yet', async () => {
    const store = useMarketStore()
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async () => {},
      unsubscribe: async () => {},
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    await store.initialize('mock', { defaultSymbols: ['00700.HK'] }, source)

    expect(store.liveGatewayUrl).toBeNull()
    expect(store.smokeClientObservation('desk-a').symbol_statuses['00700.HK']).toMatchObject({
      status: 'loading',
      snapshot_loaded: false,
    })
  })

  it('surfaces invalid live runtime config as a connection error', async () => {
    const store = useMarketStore()

    await store.initialize('live', {
      liveUrl: 'https://gateway.internal/ws',
      protocol: 'terminal-message-v1',
      validationErrors: ['live_url_invalid'],
    })

    expect(store.mode).toBe('live')
    expect(store.liveGatewayUrl).toBe('https://gateway.internal/ws')
    expect(store.connectionStatus).toBe('error')
    expect(store.connectionError).toContain('Live data source configuration is invalid: live_url_invalid')
  })

  it('tracks symbol-level loading, subscribe errors, and degraded snapshots', async () => {
    const storage = new MemoryStorage()
    vi.stubGlobal('localStorage', storage)
    const store = useMarketStore()
    let failSubscribe = false
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async () => {
        if (failSubscribe) {
          throw new Error('hydrate timeout')
        }
      },
      unsubscribe: async () => {},
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    await store.initialize('mock', { defaultSymbols: ['00005.HK'] }, source)
    failSubscribe = true
    await expect(store.subscribeSymbol('00700')).rejects.toThrow('hydrate timeout')

    expect(store.symbols['00700.HK']!.subscriptionStatus).toBe('degraded')
    expect(store.symbols['00700.HK']!.subscriptionError).toBe('hydrate timeout')
    expect(store.subscribedSymbols).toEqual(['00005.HK', '00700.HK'])
    expect(JSON.parse(storage.getItem(WATCHLIST_STORAGE_KEY) ?? '[]')).toEqual(['00005.HK'])

    store.handleMessage({
      type: 'snapshot',
      symbol: '00700.HK',
      snapshot: makeSnapshot('00700.HK', 388),
      ticks: [],
      alerts: [],
      askQueues: [],
      bidQueues: [],
      holding: [],
      freshness: {
        updatedAt: '2026-05-22T09:30:00+08:00',
        runtimeState: 'DEGRADED',
        degraded: true,
        degradedReasons: ['hydration_capacity_exceeded'],
        sourceDates: {},
      },
    })

    expect(store.symbols['00700.HK']!.snapshotLoaded).toBe(true)
    expect(store.symbols['00700.HK']!.subscriptionStatus).toBe('degraded')
    expect(store.symbols['00700.HK']!.subscriptionError).toBe('hydration_capacity_exceeded')
  })

  it('shows a cold symbol in the watchlist while subscribe is pending', async () => {
    const store = useMarketStore()
    const resolveSubscribe: Array<() => void> = []
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async (symbol: string) => {
        if (symbol !== '02643.HK') {
          return
        }
        return new Promise<void>((resolve) => {
          resolveSubscribe.push(resolve)
        })
      },
      unsubscribe: async () => {},
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    await store.initialize('mock', { defaultSymbols: ['00005.HK'] }, source)
    const pending = store.subscribeSymbol('2643')

    expect(store.subscribedSymbols).toContain('02643.HK')
    expect(store.symbols['02643.HK']!.subscriptionStatus).toBe('loading')

    resolveSubscribe[0]?.()
    await pending

    expect(store.subscribedSymbols).toContain('02643.HK')
  })

  it('exports degraded smoke reasons from subscription error when freshness reasons are empty', async () => {
    const store = useMarketStore()
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async (symbol: string) => {
        store.handleMessage({
          type: 'snapshot',
          symbol,
          snapshot: makeSnapshot(symbol, 388),
          ticks: [],
          alerts: [],
          askQueues: [],
          bidQueues: [],
          holding: [],
          freshness: {
            updatedAt: '2026-05-22T09:30:00+08:00',
            runtimeState: 'WARM',
            degraded: false,
            degradedReasons: [],
            sourceDates: {},
          },
        })
      },
      unsubscribe: async () => {},
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    await store.initialize('mock', { defaultSymbols: ['00700.HK'] }, source)
    store.handleMessage({
      type: 'snapshot',
      symbol: '00700.HK',
      snapshot: makeSnapshot('00700.HK', 388),
      ticks: [],
      alerts: [],
      askQueues: [],
      bidQueues: [],
      holding: [],
      freshness: {
        updatedAt: '2026-05-22T09:31:00+08:00',
        runtimeState: 'DEGRADED',
        degraded: true,
        degradedReason: 'redis_down',
        degradedReasons: [],
        sourceDates: {},
      },
    })

    expect(store.smokeClientObservation('desk-a').symbol_statuses['00700.HK']).toMatchObject({
      status: 'degraded',
      degraded_reasons: ['redis_down'],
    })
  })

  it('keeps but does not persist a symbol when subscribe returns a degraded snapshot', async () => {
    const storage = new MemoryStorage()
    vi.stubGlobal('localStorage', storage)
    const store = useMarketStore()
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async (symbol: string) => {
        if (symbol === '00700.HK') {
          store.handleMessage({
            type: 'snapshot',
            symbol,
            snapshot: makeSnapshot(symbol, 388),
            ticks: [],
            alerts: [],
            askQueues: [],
            bidQueues: [],
            holding: [],
            freshness: {
              updatedAt: '2026-05-22T09:30:00+08:00',
              runtimeState: 'DEGRADED',
              degraded: true,
              degradedReasons: ['hydration_capacity_exceeded'],
              sourceDates: {},
            },
          })
        }
      },
      unsubscribe: async () => {},
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    await store.initialize('mock', { defaultSymbols: ['00005.HK'] }, source)
    await expect(store.subscribeSymbol('00700')).rejects.toThrow('hydration_capacity_exceeded')

    expect(store.subscribedSymbols).toEqual(['00005.HK', '00700.HK'])
    expect(store.symbols['00700.HK']!.subscriptionStatus).toBe('degraded')
    expect(JSON.parse(storage.getItem(WATCHLIST_STORAGE_KEY) ?? '[]')).toEqual(['00005.HK'])
  })

  it('keeps initialization connected when one startup watchlist symbol is degraded', async () => {
    const storage = new MemoryStorage({
      [WATCHLIST_STORAGE_KEY]: JSON.stringify(['00700.HK', '00939.HK']),
    })
    vi.stubGlobal('localStorage', storage)
    const store = useMarketStore()
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async (symbol: string) => {
        if (symbol === '00700.HK') {
          store.handleMessage({
            type: 'snapshot',
            symbol,
            snapshot: makeSnapshot(symbol, 388),
            ticks: [],
            alerts: [],
            askQueues: [],
            bidQueues: [],
            holding: [],
            freshness: {
              updatedAt: '2026-05-22T09:30:00+08:00',
              runtimeState: 'DEGRADED',
              degraded: true,
              degradedReasons: ['hydration_capacity_exceeded'],
              sourceDates: {},
            },
          })
          return
        }
        store.handleMessage({
          type: 'snapshot',
          symbol,
          snapshot: makeSnapshot(symbol, 7.4),
          ticks: [],
          alerts: [],
          askQueues: [],
          bidQueues: [],
          holding: [],
          freshness: {
            updatedAt: '2026-05-22T09:30:00+08:00',
            runtimeState: 'WARM',
            degraded: false,
            degradedReasons: [],
            sourceDates: { minute_bars: '20260522' },
          },
        })
      },
      unsubscribe: async () => {},
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    await store.initialize('mock', { defaultSymbols: [] }, source)

    expect(store.connectionStatus).toBe('connected')
    expect(store.connectionError).toBeNull()
    expect(store.subscribedSymbols).toEqual(['00700.HK', '00939.HK'])
    expect(store.symbols['00700.HK']!.subscriptionStatus).toBe('degraded')
    expect(JSON.parse(storage.getItem(WATCHLIST_STORAGE_KEY) ?? '[]')).toEqual(['00939.HK'])
  })

  it('unsubscribes active symbols before disconnecting the data source', async () => {
    const store = useMarketStore()
    const unsubscribed: string[] = []
    let disconnected = false
    const source = {
      connect: async () => {},
      disconnect: async () => {
        disconnected = true
      },
      subscribe: async () => {},
      unsubscribe: async (symbol: string) => {
        unsubscribed.push(symbol)
      },
      requestHoldingHistory: async () => {},
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    await store.initialize('mock', { defaultSymbols: ['00700.HK', '00939.HK'] }, source)
    await store.disconnectDataSource()

    expect(unsubscribed).toEqual(['00700.HK', '00939.HK'])
    expect(disconnected).toBe(true)
    expect(store.connectionStatus).toBe('disconnected')
    expect(store.subscribedSymbols).toEqual(['00700.HK', '00939.HK'])
  })

  it('normalizes holding history request days before calling the data source', async () => {
    const store = useMarketStore()
    let requestedDays: number | null = null
    const source = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async () => {},
      unsubscribe: async () => {},
      requestHoldingHistory: async (_symbol: string, _participantName: string, days: number) => {
        requestedDays = days
      },
      onMessage: () => () => {},
      onHealth: () => () => {},
    }

    await store.initialize('mock', {}, source)
    await store.requestHoldingHistory('00700', 'JPMorgan Chase Bank, N.A.', 0)

    expect(requestedDays).toBe(30)
  })
})

function makeSnapshot(symbol: string, price: number): MarketSnapshot {
  return {
    symbol,
    name: symbol,
    currency: 'HKD',
    price,
    previousClose: price - 1,
    open: price - 0.5,
    high: price + 1,
    low: price - 1,
    volume: 1000,
    turnover: 1000 * price,
    change: 1,
    changePercent: 0.25,
    updatedAt: '2026-05-22T00:00:00.000Z',
  }
}

function makeTick(price: number, timestamp = '2026-05-22T09:30:00+08:00'): PriceTick {
  return {
    timestamp,
    price,
    volume: 1000,
    turnover: price * 1000,
    direction: 'up',
  }
}

function makeAlert(id: string): BigTradeAlert {
  return {
    id,
    timestamp: '2026-05-22T00:00:00.000Z',
    price: 388,
    volume: 200000,
    turnover: 77600000,
    side: 'buy',
    participantName: 'JPMorgan Chase Bank, N.A.',
    brokerName: 'JPMorgan Chase Bank, N.A.',
    isHighlighted: true,
  }
}

function makeQueue(id: string, side: 'ask' | 'bid'): BrokerQueueEntry {
  return {
    id,
    position: 1,
    side,
    participantName: 'JPMorgan Chase Bank, N.A.',
    brokerCode: 'JPM',
    price: 388,
    volume: 100000,
  }
}

function makeHolding(participantName: string): HoldingEntry {
  return {
    participantName,
    participantCode: 'C00010',
    shares: 1000,
    percent: 1.1,
    change: 200,
    isHighlighted: true,
  }
}

class MemoryStorage implements Storage {
  private readonly values = new Map<string, string>()

  constructor(seed: Record<string, string> = {}) {
    for (const [key, value] of Object.entries(seed)) {
      this.values.set(key, value)
    }
  }

  get length() {
    return this.values.size
  }

  clear(): void {
    this.values.clear()
  }

  getItem(key: string): string | null {
    return this.values.get(key) ?? null
  }

  key(index: number): string | null {
    return Array.from(this.values.keys())[index] ?? null
  }

  removeItem(key: string): void {
    this.values.delete(key)
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value)
  }
}
