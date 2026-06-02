import { defineStore } from 'pinia'

import {
  DEFAULT_SYMBOLS,
  createMarketDataSource,
  type DataSourceFactoryOptions,
} from '@/services/dataSourceFactory'
import { createInitialTerminalHealth } from '@/services/health'
import type {
  DataSourceMode,
  MarketDataSource,
  MarketMessage,
  MarketPerformanceSample,
  MarketPerformanceSampleHandler,
  MarketSnapshot,
  MarketState,
  PriceTick,
  StockSymbol,
  SymbolState,
  SymbolSubscriptionStatus,
} from '@/types/market'
import { normalizeHoldingHistoryDays } from '@/utils/holdingHistory'
import { isNormalizedStockSymbol, normalizeStockSymbol } from '@/utils/symbol'

const MAX_TICKS_PER_SYMBOL = 420
const MAX_ALERTS_PER_SYMBOL = 500
const MAX_PERFORMANCE_SAMPLES = 1000
const STARTUP_SUBSCRIPTION_CONCURRENCY = 8
export const WATCHLIST_STORAGE_KEY = 'market-terminal.watchlist.v1'
export const SMOKE_MACHINE_ID_STORAGE_KEY = 'market-terminal.smoke.machine-id.v1'

let activeSource: MarketDataSource | null = null
let removeMessageHandler: (() => void) | null = null
let removeHealthHandler: (() => void) | null = null
let removeConnectionStatusHandler: (() => void) | null = null
let performanceSampleHandler: MarketPerformanceSampleHandler | null = null
let performanceSamples: MarketPerformanceSample[] = []

export function createEmptySymbolState(): SymbolState {
  return {
    runtimeEpoch: null,
    snapshot: null,
    ticks: [],
    alerts: [],
    askQueues: [],
    bidQueues: [],
    holding: [],
    holdingHistoryByParticipant: {},
    freshness: null,
    subscriptionStatus: 'idle',
    subscriptionError: null,
    snapshotLoaded: false,
    lastUpdatedAt: null,
    unreadAlerts: 0,
  }
}

export function holdingHistoryKey(participantName: string, days: number): string {
  return `${participantName}::${days}`
}

export const useMarketStore = defineStore('market', {
  state: (): MarketState => ({
    mode: 'mock',
    liveGatewayUrl: null,
    connectionStatus: 'disconnected',
    connectionError: null,
    health: createInitialTerminalHealth(),
    subscribedSymbols: [],
    activeSymbol: '',
    symbols: {},
  }),

  getters: {
    activeSymbolState(state): SymbolState | null {
      return state.activeSymbol ? state.symbols[state.activeSymbol] ?? null : null
    },
    getSymbolState:
      (state) =>
      (rawSymbol: string): SymbolState | null => {
        const symbol = normalizeStockSymbol(rawSymbol)
        return state.symbols[symbol] ?? null
      },
    getHoldingHistory:
      (state) =>
      (rawSymbol: string, participantName: string, days: number) => {
        const symbol = normalizeStockSymbol(rawSymbol)
        return state.symbols[symbol]?.holdingHistoryByParticipant[holdingHistoryKey(participantName, days)] ?? []
      },
  },

  actions: {
    async initialize(
      mode?: DataSourceMode,
      options: DataSourceFactoryOptions = {},
      sourceOverride?: MarketDataSource,
    ) {
      await this.disconnectDataSource()

      const targetMode = mode ?? this.mode
      this.mode = targetMode
      const sourceOptions =
        targetMode === 'live' && !options.clientId
          ? { ...options, clientId: getOrCreateSmokeMachineId() }
          : options
      this.liveGatewayUrl = targetMode === 'live' ? sourceOptions.liveUrl ?? null : null
      this.connectionStatus = 'connecting'
      this.connectionError = null
      performanceSampleHandler = sourceOptions.performanceSampleHandler ?? null

      try {
        const source = sourceOverride ?? createMarketDataSource(targetMode, sourceOptions)
        activeSource = source
        removeMessageHandler = source.onMessage((message) => this.handleMessage(message))
        removeHealthHandler = source.onHealth?.((health) => {
          this.health = health
        }) ?? null
        removeConnectionStatusHandler = source.onConnectionStatus?.((status, error) => {
          this.connectionStatus = status
          this.connectionError = error
        }) ?? null

        const persistedSymbols = this.subscribedSymbols.length > 0 ? [] : loadPersistedWatchlist()
        const defaultSymbols = sourceOptions.defaultSymbols?.length ? sourceOptions.defaultSymbols : DEFAULT_SYMBOLS
        const symbolsToSubscribe =
          this.subscribedSymbols.length > 0
            ? [...this.subscribedSymbols]
            : persistedSymbols.length > 0
              ? persistedSymbols
              : [...defaultSymbols]

        for (const symbol of symbolsToSubscribe) {
          this.stageSubscriptionSymbol(symbol)
        }

        await source.connect()
        this.connectionStatus = 'connected'

        await runStartupSubscriptions(symbolsToSubscribe, (symbol) => this.subscribeSymbol(symbol))
      } catch (error) {
        this.connectionStatus = 'error'
        this.connectionError = error instanceof Error ? error.message : String(error)
      }
    },

    async disconnectDataSource() {
      removeMessageHandler?.()
      removeMessageHandler = null
      removeHealthHandler?.()
      removeHealthHandler = null
      removeConnectionStatusHandler?.()
      removeConnectionStatusHandler = null

      const source = activeSource
      if (source) {
        await Promise.allSettled(this.subscribedSymbols.map((symbol) => source.unsubscribe(symbol)))
        await source.disconnect()
      }

      activeSource = null
      performanceSampleHandler = null
      this.connectionStatus = 'disconnected'
      this.health = {
        ...this.health,
        process: 'stopped',
        updatedAt: new Date().toISOString(),
      }
    },

    async setMode(mode: DataSourceMode, options: DataSourceFactoryOptions = {}) {
      if (this.mode === mode && this.connectionStatus === 'connected') {
        return
      }

      await this.initialize(mode, options)
    },

    setPerformanceSampleHandler(handler: MarketPerformanceSampleHandler | null) {
      performanceSampleHandler = handler
    },

    async subscribeSymbol(rawSymbol: string) {
      const symbol = normalizeStockSymbol(rawSymbol)
      const state = this.ensureSymbol(symbol)
      state.subscriptionStatus = 'loading'
      state.subscriptionError = null
      const wasSubscribed = this.subscribedSymbols.includes(symbol)

      if (!this.activeSymbol) {
        this.activeSymbol = symbol
      }
      if (!wasSubscribed) {
        this.subscribedSymbols.push(symbol)
      }

      if (activeSource && this.connectionStatus === 'connected') {
        const subscribeStartedAt = nowMs()
        try {
          await activeSource.subscribe(symbol)
          recordPerformanceSample({
            key: 'subscribe_snapshot_ms',
            valueMs: Math.max(0, nowMs() - subscribeStartedAt),
            symbol,
            recordedAt: new Date().toISOString(),
          })
          const subscribeError = subscriptionErrorMessage(state)
          if (subscribeError) {
            throw new Error(subscribeError)
          }
        } catch (error) {
          state.subscriptionStatus = 'degraded'
          state.subscriptionError = error instanceof Error ? error.message : String(error)
          persistWatchlist(
            this.subscribedSymbols.filter((item) => this.symbols[item]?.subscriptionStatus !== 'degraded'),
          )
          throw error
        }
      } else if (state.snapshotLoaded) {
        setRuntimeStatusFromFreshness(state)
      }

      if (!wasSubscribed || this.connectionStatus === 'connected') {
        persistWatchlist(
          this.subscribedSymbols.filter((item) => this.symbols[item]?.subscriptionStatus !== 'degraded'),
        )
      }
    },

    stageSubscriptionSymbol(rawSymbol: string) {
      const symbol = normalizeStockSymbol(rawSymbol)
      const state = this.ensureSymbol(symbol)
      state.subscriptionStatus = 'loading'
      state.subscriptionError = null
      if (!this.activeSymbol) {
        this.activeSymbol = symbol
      }
      if (!this.subscribedSymbols.includes(symbol)) {
        this.subscribedSymbols.push(symbol)
      }
    },

    async unsubscribeSymbol(rawSymbol: string) {
      const symbol = normalizeStockSymbol(rawSymbol)

      if (activeSource && this.connectionStatus === 'connected') {
        await activeSource.unsubscribe(symbol)
      }

      this.subscribedSymbols = this.subscribedSymbols.filter((item) => item !== symbol)
      persistWatchlist(this.subscribedSymbols)
      if (this.symbols[symbol]) {
        this.symbols[symbol].subscriptionStatus = 'idle'
        this.symbols[symbol].subscriptionError = null
      }

      if (this.activeSymbol === symbol) {
        this.activeSymbol = this.subscribedSymbols[0] ?? ''
      }
    },

    setActiveSymbol(rawSymbol: string) {
      const symbol = normalizeStockSymbol(rawSymbol)
      if (!this.symbols[symbol]) {
        return
      }

      this.activeSymbol = symbol
      this.symbols[symbol].unreadAlerts = 0
    },

    async requestHoldingHistory(rawSymbol: string, participantName: string, days: number) {
      const symbol = normalizeStockSymbol(rawSymbol)
      const normalizedDays = normalizeHoldingHistoryDays(days)
      this.ensureSymbol(symbol)

      if (activeSource && this.connectionStatus === 'connected') {
        await activeSource.requestHoldingHistory(symbol, participantName, normalizedDays)
      }
    },

    handleMessage(message: MarketMessage) {
      const startedAt = nowMs()
      try {
        const state = this.ensureSymbol(message.symbol)
        state.lastUpdatedAt = new Date().toISOString()
        if (message.runtimeEpoch) {
          if (state.runtimeEpoch && state.runtimeEpoch !== message.runtimeEpoch) {
            resetSymbolForRuntimeEpoch(state, message.runtimeEpoch)
            if (message.type !== 'snapshot') {
              return
            }
          } else {
            state.runtimeEpoch = message.runtimeEpoch
          }
        }
        this.health = {
          ...this.health,
          latestEventAtBySymbol: {
            ...this.health.latestEventAtBySymbol,
            [message.symbol]: state.lastUpdatedAt,
          },
          updatedAt: state.lastUpdatedAt,
        }

        const snapshotVolumeBeforeMessage = state.snapshot?.volume ?? 0
        switch (message.type) {
          case 'snapshot':
            state.snapshot = message.snapshot
            state.ticks = canonicalizeMinuteTicks(message.ticks, tradeDateForMessage(message.snapshot, message.freshness))
            state.alerts = message.alerts.slice(0, MAX_ALERTS_PER_SYMBOL)
            state.askQueues = message.askQueues
            state.bidQueues = message.bidQueues
            state.holding = message.holding
            state.freshness = message.freshness
            state.snapshotLoaded = true
            setRuntimeStatusFromFreshness(state)
            break
          case 'tick_realtime':
            if (message.snapshot) {
              state.snapshot = message.snapshot
            }
            if (
              shouldApplyChartTick(message.tick, snapshotVolumeBeforeMessage, message.symbol) &&
              isRegularSessionTick(
                message.tick,
                tradeDateForMessage(message.snapshot ?? state.snapshot, message.freshness ?? state.freshness ?? undefined),
              )
            ) {
              state.ticks = upsertMinuteTick(state.ticks, message.tick)
            }
            if (message.freshness) {
              state.freshness = message.freshness
            }
            state.snapshotLoaded = true
            if (message.freshness) {
              setRuntimeStatusFromFreshness(state)
            } else {
              state.subscriptionStatus = 'live'
              state.subscriptionError = null
            }
            break
          case 'alert_realtime':
            state.alerts = [message.alert, ...state.alerts].slice(0, MAX_ALERTS_PER_SYMBOL)
            if (message.freshness) {
              state.freshness = message.freshness
              setRuntimeStatusFromFreshness(state)
            }
            if (message.symbol !== this.activeSymbol) {
              state.unreadAlerts += 1
            }
            break
          case 'queue_realtime':
            if (message.askQueues) {
              state.askQueues = message.askQueues
            }
            if (message.bidQueues) {
              state.bidQueues = message.bidQueues
            }
            if (message.freshness) {
              state.freshness = message.freshness
              setRuntimeStatusFromFreshness(state)
            }
            break
          case 'holding_name_click_response':
            state.holdingHistoryByParticipant[
              holdingHistoryKey(message.participantName, message.days)
            ] = message.history
            if (message.freshness) {
              state.freshness = message.freshness
              setRuntimeStatusFromFreshness(state)
            }
            break
        }
      } finally {
        recordStoreUpdatePerformanceSample(startedAt, message)
      }
    },

    ensureSymbol(rawSymbol: string): SymbolState {
      const symbol = normalizeStockSymbol(rawSymbol)
      if (!this.symbols[symbol]) {
        this.symbols[symbol] = createEmptySymbolState()
      }
      return this.symbols[symbol]
    },

    getPerformanceSamples(): MarketPerformanceSample[] {
      return performanceSamples.map((sample) => ({ ...sample }))
    },

    clearPerformanceSamples() {
      performanceSamples = []
    },

    smokeMachineId() {
      return getOrCreateSmokeMachineId()
    },

    resetSmokeMachineId() {
      const storage = browserLocalStorage()
      storage?.removeItem(SMOKE_MACHINE_ID_STORAGE_KEY)
      return getOrCreateSmokeMachineId()
    },

    smokeClientObservation(machineId?: string) {
      return {
        machine_id: machineId?.trim() || getOrCreateSmokeMachineId(),
        data_source_mode: this.mode,
        page_url: currentPageUrl(),
        gateway_url: this.liveGatewayUrl ?? '',
        connected: this.connectionStatus === 'connected',
        watchlist: [...this.subscribedSymbols],
        refresh_recovered:
          this.subscribedSymbols.length > 0 &&
          this.subscribedSymbols.every((symbol) => this.symbols[symbol]?.snapshotLoaded === true),
        symbol_statuses: Object.fromEntries(
          this.subscribedSymbols.map((symbol) => {
            const state = this.symbols[symbol]
            return [
              symbol,
              {
                status: smokeStatusFromSymbolState(state),
                snapshot_loaded: state?.snapshotLoaded === true,
                requested_trade_date: state?.freshness?.requestedTradeDate ?? state?.snapshot?.requestedTradeDate,
                effective_trade_date: state?.freshness?.effectiveTradeDate ?? state?.snapshot?.tradeDate,
                source_dates: state?.freshness?.sourceDates ?? {},
                degraded_reasons: degradedReasonsForSmoke(state),
              },
            ]
          }),
        ),
      }
    },
  },
})

function nowMs(): number {
  return typeof performance !== 'undefined' && typeof performance.now === 'function'
    ? performance.now()
    : Date.now()
}

function recordStoreUpdatePerformanceSample(startedAt: number, message: MarketMessage) {
  recordPerformanceSample({
    key: 'frontend_store_update_ms',
    valueMs: Math.max(0, nowMs() - startedAt),
    symbol: message.symbol,
    messageType: message.type,
    recordedAt: new Date().toISOString(),
  })
}

function resetSymbolForRuntimeEpoch(state: SymbolState, runtimeEpoch: string) {
  state.runtimeEpoch = runtimeEpoch
  state.snapshot = null
  state.ticks = []
  state.alerts = []
  state.askQueues = []
  state.bidQueues = []
  state.freshness = null
  state.snapshotLoaded = false
  state.subscriptionStatus = 'loading'
  state.subscriptionError = null
  state.unreadAlerts = 0
}

function recordPerformanceSample(sample: MarketPerformanceSample) {
  performanceSamples = [...performanceSamples, sample].slice(-MAX_PERFORMANCE_SAMPLES)
  performanceSampleHandler?.(sample)
}

async function runStartupSubscriptions(
  symbols: StockSymbol[],
  subscribe: (symbol: StockSymbol) => Promise<void>,
): Promise<void> {
  const queue = [...symbols]
  const workerCount = Math.min(STARTUP_SUBSCRIPTION_CONCURRENCY, queue.length)
  await Promise.all(
    Array.from({ length: workerCount }, async () => {
      while (queue.length > 0) {
        const symbol = queue.shift()
        if (!symbol) {
          continue
        }
        try {
          await subscribe(symbol)
        } catch {
          continue
        }
      }
    }),
  )
}

function upsertMinuteTick(existing: PriceTick[], tick: PriceTick): PriceTick[] {
  const minuteTimestamp = minuteBucket(tick.timestamp)
  if (!isRegularSessionMinute(minuteTimestamp)) {
    return existing.slice(-MAX_TICKS_PER_SYMBOL)
  }
  const index = existing.findIndex((item) => minuteBucket(item.timestamp) === minuteTimestamp)
  if (index < 0) {
    return [...existing, { ...tick, timestamp: minuteTimestamp }]
      .sort((left, right) => minuteBucket(left.timestamp).localeCompare(minuteBucket(right.timestamp)))
      .slice(-MAX_TICKS_PER_SYMBOL)
  }

  const previous = existing[index]!
  if (tick.replace) {
    return [
      ...existing.slice(0, index),
      {
        ...previous,
        ...tick,
        timestamp: minuteTimestamp,
      },
      ...existing.slice(index + 1),
    ]
      .sort((left, right) => minuteBucket(left.timestamp).localeCompare(minuteBucket(right.timestamp)))
      .slice(-MAX_TICKS_PER_SYMBOL)
  }
  const updated = [
    ...existing.slice(0, index),
    {
      ...previous,
      ...tick,
      timestamp: minuteTimestamp,
      volume: previous.volume + tick.volume,
      turnover: previous.turnover + tick.turnover,
    },
    ...existing.slice(index + 1),
  ]
  return updated
    .sort((left, right) => minuteBucket(left.timestamp).localeCompare(minuteBucket(right.timestamp)))
    .slice(-MAX_TICKS_PER_SYMBOL)
}

function shouldApplyChartTick(tick: PriceTick, snapshotVolume: number, symbol: StockSymbol): boolean {
  if (tick.chartUpdate === false) {
    return false
  }
  if (!tick.replace && snapshotVolume > 0 && tick.volume > snapshotVolume) {
    recordPerformanceSample({
      key: 'realtime_tick_volume_anomaly',
      valueMs: 0,
      symbol,
      messageType: 'tick_realtime',
      recordedAt: new Date().toISOString(),
    })
    return false
  }
  return true
}

function canonicalizeMinuteTicks(ticks: PriceTick[], tradeDate?: string): PriceTick[] {
  return ticks
    .filter((tick) => isRegularSessionTick(tick, tradeDate))
    .reduce<PriceTick[]>((merged, tick) => upsertMinuteTick(merged, tick), [])
}

function minuteBucket(timestamp: string): string {
  const normalizedInput = hasExplicitTimezone(timestamp) ? timestamp : `${timestamp}+08:00`
  const parsed = new Date(normalizedInput)
  if (!Number.isNaN(parsed.getTime())) {
    return formatHongKongMinute(parsed)
  }
  const match = timestamp.match(/^(.+T\d{2}:\d{2}):\d{2}(?:\.\d+)?(.*)$/)
  return match ? `${match[1]}:00${match[2]}` : timestamp
}

function hasExplicitTimezone(timestamp: string): boolean {
  return /(?:Z|[+-]\d{2}:\d{2})$/i.test(timestamp)
}

function formatHongKongMinute(date: Date): string {
  const shifted = new Date(date.getTime() + 8 * 60 * 60 * 1000)
  const year = shifted.getUTCFullYear()
  const month = String(shifted.getUTCMonth() + 1).padStart(2, '0')
  const day = String(shifted.getUTCDate()).padStart(2, '0')
  const hour = String(shifted.getUTCHours()).padStart(2, '0')
  const minute = String(shifted.getUTCMinutes()).padStart(2, '0')
  return `${year}-${month}-${day}T${hour}:${minute}:00+08:00`
}

function tradeDateForMessage(
  snapshot: MarketSnapshot | null | undefined,
  freshness: { effectiveTradeDate?: string; requestedTradeDate?: string } | null | undefined,
): string | undefined {
  return freshness?.effectiveTradeDate ?? freshness?.requestedTradeDate ?? snapshot?.tradeDate ?? snapshot?.requestedTradeDate
}

function isRegularSessionTick(tick: PriceTick, tradeDate?: string): boolean {
  return isRegularSessionMinute(minuteBucket(tick.timestamp), tradeDate)
}

function isRegularSessionMinute(timestamp: string, tradeDate?: string): boolean {
  const match = minuteBucket(timestamp).match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/)
  if (!match) {
    return false
  }
  const [, year, month, day, hourText, minuteText] = match
  if (tradeDate && `${year}${month}${day}` !== tradeDate) {
    return false
  }
  const minuteOfDay = Number(hourText) * 60 + Number(minuteText)
  return (
    (9 * 60 + 30 <= minuteOfDay && minuteOfDay < 12 * 60) ||
    (13 * 60 <= minuteOfDay && minuteOfDay <= 16 * 60)
  )
}

function loadPersistedWatchlist(): StockSymbol[] {
  const storage = browserLocalStorage()
  if (!storage) {
    return []
  }
  try {
    const raw = storage.getItem(WATCHLIST_STORAGE_KEY)
    if (!raw) {
      return []
    }
    const decoded = JSON.parse(raw)
    if (!Array.isArray(decoded)) {
      return []
    }
    return dedupeSymbols(
      decoded
        .filter((item): item is string => typeof item === 'string')
        .flatMap((item) => {
          try {
            return [normalizeStockSymbol(item)]
          } catch {
            return []
          }
        })
        .filter(isNormalizedStockSymbol),
    )
  } catch {
    return []
  }
}

function persistWatchlist(symbols: StockSymbol[]) {
  const storage = browserLocalStorage()
  if (!storage) {
    return
  }
  storage.setItem(WATCHLIST_STORAGE_KEY, JSON.stringify(dedupeSymbols(symbols)))
}

function dedupeSymbols(symbols: StockSymbol[]): StockSymbol[] {
  return Array.from(new Set(symbols.map((symbol) => normalizeStockSymbol(symbol)).filter(isNormalizedStockSymbol)))
}

function setRuntimeStatusFromFreshness(state: SymbolState) {
  const freshness = state.freshness
  if (freshness?.degraded || freshness?.runtimeState === 'DEGRADED') {
    state.subscriptionStatus = 'degraded'
    state.subscriptionError = freshness.degradedReasons[0] ?? freshness.degradedReason ?? 'degraded'
    return
  }

  const requested = freshness?.requestedTradeDate ?? state.snapshot?.requestedTradeDate
  const effective = freshness?.effectiveTradeDate ?? state.snapshot?.tradeDate
  if (requested && effective && requested !== effective) {
    state.subscriptionStatus = 'closed'
    state.subscriptionError = null
    return
  }

  if (freshness?.runtimeState === 'LIVE') {
    state.subscriptionStatus = 'live'
    state.subscriptionError = null
    return
  }

  state.subscriptionStatus = 'warm'
  state.subscriptionError = null
}

function browserLocalStorage(): Storage | null {
  const candidate = globalThis as typeof globalThis & { localStorage?: Storage }
  return candidate.localStorage ?? null
}

function getOrCreateSmokeMachineId(): string {
  const storage = browserLocalStorage()
  const existing = storage?.getItem(SMOKE_MACHINE_ID_STORAGE_KEY)
  if (existing?.trim()) {
    const normalized = existing.trim()
    if (normalized !== existing) {
      storage?.setItem(SMOKE_MACHINE_ID_STORAGE_KEY, normalized)
    }
    return normalized
  }
  const generated = `desk-${Math.random().toString(36).slice(2, 10)}`
  storage?.setItem(SMOKE_MACHINE_ID_STORAGE_KEY, generated)
  return generated
}

function smokeStatusFromSymbolState(state: SymbolState | undefined): Exclude<SymbolSubscriptionStatus, 'idle'> {
  if (!state || state.subscriptionStatus === 'idle') {
    return 'loading'
  }
  return state.subscriptionStatus
}

function degradedReasonsForSmoke(state: SymbolState | undefined): string[] {
  if (!state) {
    return []
  }
  const reasons = state.freshness?.degradedReasons ?? []
  if (reasons.length > 0) {
    return reasons
  }
  if (state.freshness?.degradedReason) {
    return [state.freshness.degradedReason]
  }
  return state.subscriptionError ? [state.subscriptionError] : []
}

function currentPageUrl(): string {
  return typeof globalThis.location === 'object' && typeof globalThis.location?.href === 'string'
    ? globalThis.location.href
    : ''
}

function subscriptionErrorMessage(state: SymbolState): string | null {
  return state.subscriptionStatus === 'degraded' && state.subscriptionError ? state.subscriptionError : null
}
