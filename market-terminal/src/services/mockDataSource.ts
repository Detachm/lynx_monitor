import type {
  BigTradeAlert,
  BrokerQueueEntry,
  ConnectionStatusHandler,
  HoldingEntry,
  HoldingHistoryPoint,
  MarketDataSource,
  MarketMessage,
  MarketMessageHandler,
  MarketSnapshot,
  PriceTick,
  QueueSide,
  StockSymbol,
  TerminalHealthHandler,
  TerminalMessage,
  TerminalMessagePayloadByType,
  TerminalMessageType,
  TradeSide,
} from '@/types/market'
import { TerminalHealthTracker } from '@/services/health'
import { normalizeTerminalMessage } from '@/services/normalizers'
import { normalizeHoldingHistoryDays } from '@/utils/holdingHistory'
import { normalizeStockSymbol } from '@/utils/symbol'

export interface MockDataSourceOptions {
  tickIntervalMs?: number
  autoRealtime?: boolean
}

interface MockProfile {
  name: string
  currency: string
  basePrice: number
  previousClose: number
  lotSize: number
  floatShares: number
}

interface ParticipantProfile {
  name: string
  code: string
  brokerCode: string
  highlighted: boolean
}

interface MockRuntime {
  symbol: StockSymbol
  profile: MockProfile
  rng: () => number
  price: number
  sequence: number
  cumulativeVolume: number
  askQueues: BrokerQueueEntry[]
  bidQueues: BrokerQueueEntry[]
  holding: HoldingEntry[]
}

const SYMBOL_PROFILES: Record<string, MockProfile> = {
  '00700.HK': {
    name: 'Tencent Holdings',
    currency: 'HKD',
    basePrice: 388.4,
    previousClose: 386.2,
    lotSize: 100,
    floatShares: 9_450_000_000,
  },
  '00939.HK': {
    name: 'China Construction Bank',
    currency: 'HKD',
    basePrice: 7.42,
    previousClose: 7.36,
    lotSize: 1_000,
    floatShares: 240_000_000_000,
  },
  '02643.HK': {
    name: 'Horizon Robotics',
    currency: 'HKD',
    basePrice: 5.18,
    previousClose: 5.05,
    lotSize: 600,
    floatShares: 11_200_000_000,
  },
  '00108.HK': {
    name: 'GR Properties',
    currency: 'HKD',
    basePrice: 0.58,
    previousClose: 0.56,
    lotSize: 2_000,
    floatShares: 1_830_000_000,
  },
}

const PARTICIPANTS: ParticipantProfile[] = [
  {
    name: 'Hong Kong Securities Clearing Company Limited',
    code: 'C00019',
    brokerCode: 'CCASS',
    highlighted: true,
  },
  {
    name: 'JPMorgan Chase Bank, N.A.',
    code: 'C00010',
    brokerCode: 'JPM',
    highlighted: true,
  },
  {
    name: 'Citibank N.A.',
    code: 'C00039',
    brokerCode: 'CITI',
    highlighted: true,
  },
  {
    name: 'HSBC Broking Securities (Asia) Limited',
    code: 'B01234',
    brokerCode: 'HSBC',
    highlighted: true,
  },
  {
    name: 'UBS Securities Hong Kong Limited',
    code: 'B00085',
    brokerCode: 'UBS',
    highlighted: true,
  },
  {
    name: 'Merrill Lynch Far East Limited',
    code: 'B00024',
    brokerCode: 'MLFE',
    highlighted: true,
  },
  {
    name: 'Bright Smart Securities International',
    code: 'B01988',
    brokerCode: 'BS',
    highlighted: false,
  },
  {
    name: 'Futu Securities International (Hong Kong) Limited',
    code: 'B02138',
    brokerCode: 'FUTU',
    highlighted: false,
  },
  {
    name: 'BOCI Securities Limited',
    code: 'B00066',
    brokerCode: 'BOCI',
    highlighted: false,
  },
  {
    name: 'Phillip Securities (Hong Kong) Limited',
    code: 'B00173',
    brokerCode: 'PHIL',
    highlighted: false,
  },
]

export class MockDataSource implements MarketDataSource {
  private readonly handlers = new Set<MarketMessageHandler>()
  private readonly connectionHandlers = new Set<ConnectionStatusHandler>()
  private readonly health = new TerminalHealthTracker()
  private readonly runtimes = new Map<StockSymbol, MockRuntime>()
  private readonly intervals = new Map<StockSymbol, ReturnType<typeof globalThis.setInterval>>()
  private connected = false

  constructor(private readonly options: MockDataSourceOptions = {}) {}

  async connect() {
    this.connected = true
    this.notifyConnectionStatus('connected')
    this.health.update({
      process: 'running',
      kafka: 'connected',
      redis: 'connected',
      kafkaLag: 0,
    })
  }

  disconnect() {
    this.connected = false
    for (const interval of this.intervals.values()) {
      globalThis.clearInterval(interval)
    }
    this.intervals.clear()
    this.notifyConnectionStatus('disconnected')
    this.health.update({ process: 'stopped' })
  }

  subscribe(rawSymbol: StockSymbol) {
    const symbol = normalizeStockSymbol(rawSymbol)
    const runtime = this.getRuntime(symbol)

    this.emitTerminal('snapshot', symbol, runtime, {
      snapshot: this.createSnapshot(runtime),
      minute_bars: this.createHistoricTicks(runtime),
      alerts: this.createInitialAlerts(runtime),
      broker_queue: {
        ask: runtime.askQueues,
        bid: runtime.bidQueues,
      },
      ccass_holdings: runtime.holding,
      freshness: this.createFreshness(runtime, 'WARM'),
    })

    if (this.options.autoRealtime === false || this.intervals.has(symbol)) {
      return
    }

    const interval = globalThis.setInterval(() => {
      if (this.connected) {
        this.emitNext(symbol)
      }
    }, this.options.tickIntervalMs ?? 1_200)

    this.intervals.set(symbol, interval)
  }

  unsubscribe(rawSymbol: StockSymbol) {
    const symbol = normalizeStockSymbol(rawSymbol)
    const interval = this.intervals.get(symbol)
    if (interval) {
      globalThis.clearInterval(interval)
      this.intervals.delete(symbol)
    }
  }

  requestHoldingHistory(rawSymbol: StockSymbol, participantName: string, days: number) {
    const symbol = normalizeStockSymbol(rawSymbol)
    const runtime = this.getRuntime(symbol)
    const normalizedDays = normalizeHoldingHistoryDays(days)

    this.emitTerminal('holding_name_click_response', symbol, runtime, {
      participant_name: participantName,
      days: normalizedDays,
      history: this.createHoldingHistory(runtime, participantName, normalizedDays),
      freshness: this.createFreshness(runtime, 'WARM'),
    })
  }

  onMessage(handler: MarketMessageHandler) {
    this.handlers.add(handler)
    return () => this.handlers.delete(handler)
  }

  onHealth(handler: TerminalHealthHandler) {
    return this.health.onHealth(handler)
  }

  onConnectionStatus(handler: ConnectionStatusHandler) {
    this.connectionHandlers.add(handler)
    return () => this.connectionHandlers.delete(handler)
  }

  private notifyConnectionStatus(status: 'connected' | 'disconnected') {
    for (const handler of this.connectionHandlers) {
      handler(status, null)
    }
  }

  emitNext(rawSymbol: StockSymbol) {
    const symbol = normalizeStockSymbol(rawSymbol)
    const runtime = this.getRuntime(symbol)
    const tick = this.createRealtimeTick(runtime)

    this.emitTerminal('tick_realtime', symbol, runtime, {
      tick,
      snapshot: this.createSnapshot(runtime),
      freshness: this.createFreshness(runtime, 'LIVE'),
    })

    if (runtime.rng() > 0.58) {
      this.emitTerminal('alert_realtime', symbol, runtime, {
        alert: this.createAlert(runtime),
        freshness: this.createFreshness(runtime, 'LIVE'),
      })
    }

    if (runtime.rng() > 0.68) {
      const side: QueueSide = runtime.rng() > 0.5 ? 'ask' : 'bid'
      const queues = this.createQueues(runtime, side)
      if (side === 'ask') {
        runtime.askQueues = queues
      } else {
        runtime.bidQueues = queues
      }

      this.emitTerminal('queue_realtime', symbol, runtime, {
        side,
        broker_queue: {
          ask: side === 'ask' ? queues : undefined,
          bid: side === 'bid' ? queues : undefined,
        },
        freshness: this.createFreshness(runtime, 'LIVE'),
      })
    }
  }

  private createFreshness(runtime: MockRuntime, runtimeState: 'WARM' | 'LIVE') {
    const tradeDate = this.createSnapshot(runtime).tradeDate ?? new Date().toISOString().slice(0, 10).replace(/-/g, '')
    return {
      updatedAt: new Date().toISOString(),
      degraded: false,
      degradedReasons: [],
      requestedTradeDate: tradeDate,
      effectiveTradeDate: tradeDate,
      runtimeState,
      sourceDates: {
        minute_bars: tradeDate,
        ccass_current: tradeDate,
      },
    }
  }

  private getRuntime(symbol: StockSymbol): MockRuntime {
    const existing = this.runtimes.get(symbol)
    if (existing) {
      return existing
    }

    const profile = SYMBOL_PROFILES[symbol] ?? createFallbackProfile(symbol)
    const rng = mulberry32(hashSymbol(symbol))
    const runtime: MockRuntime = {
      symbol,
      profile,
      rng,
      price: profile.basePrice,
      sequence: 0,
      cumulativeVolume: 0,
      askQueues: [],
      bidQueues: [],
      holding: [],
    }

    runtime.askQueues = this.createQueues(runtime, 'ask')
    runtime.bidQueues = this.createQueues(runtime, 'bid')
    runtime.holding = this.createHolding(runtime)
    this.runtimes.set(symbol, runtime)
    return runtime
  }

  private createSnapshot(runtime: MockRuntime): MarketSnapshot {
    const change = runtime.price - runtime.profile.previousClose

    return {
      symbol: runtime.symbol,
      name: runtime.profile.name,
      currency: runtime.profile.currency,
      price: roundPrice(runtime.price),
      previousClose: runtime.profile.previousClose,
      open: roundPrice(runtime.profile.basePrice * 0.997),
      high: roundPrice(Math.max(runtime.price, runtime.profile.basePrice * 1.012)),
      low: roundPrice(Math.min(runtime.price, runtime.profile.basePrice * 0.988)),
      volume: runtime.cumulativeVolume,
      turnover: roundPrice(runtime.cumulativeVolume * runtime.price),
      change: roundPrice(change),
      changePercent: roundPrice((change / runtime.profile.previousClose) * 100),
      updatedAt: new Date().toISOString(),
    }
  }

  private createHistoricTicks(runtime: MockRuntime): PriceTick[] {
    const ticks: PriceTick[] = []
    const start = new Date()
    start.setHours(9, 30, 0, 0)

    let price = runtime.profile.basePrice * (0.994 + runtime.rng() * 0.012)

    for (let index = 0; index < 96; index += 1) {
      const previous = price
      price = Math.max(0.01, price + (runtime.rng() - 0.48) * runtime.profile.basePrice * 0.0025)
      const volume = roundLot(30_000 + runtime.rng() * 420_000, runtime.profile.lotSize)
      runtime.cumulativeVolume += volume

      ticks.push({
        timestamp: new Date(start.getTime() + index * 60_000).toISOString(),
        price: roundPrice(price),
        volume,
        turnover: roundPrice(price * volume),
        direction: price > previous ? 'up' : price < previous ? 'down' : 'flat',
      })
    }

    runtime.price = roundPrice(price)
    return ticks
  }

  private createRealtimeTick(runtime: MockRuntime): PriceTick {
    const previous = runtime.price
    runtime.sequence += 1
    runtime.price = Math.max(
      0.01,
      roundPrice(runtime.price + (runtime.rng() - 0.5) * runtime.profile.basePrice * 0.003),
    )

    const volume = roundLot(20_000 + runtime.rng() * 520_000, runtime.profile.lotSize)
    runtime.cumulativeVolume += volume

    return {
      timestamp: new Date().toISOString(),
      price: runtime.price,
      volume,
      turnover: roundPrice(runtime.price * volume),
      direction: runtime.price > previous ? 'up' : runtime.price < previous ? 'down' : 'flat',
    }
  }

  private createInitialAlerts(runtime: MockRuntime): BigTradeAlert[] {
    return Array.from({ length: 14 }, () => this.createAlert(runtime))
  }

  private createAlert(runtime: MockRuntime): BigTradeAlert {
    const participant = pick(runtime.rng, PARTICIPANTS)
    const side: TradeSide = runtime.rng() > 0.5 ? 'buy' : 'sell'
    const volume = roundLot(250_000 + runtime.rng() * 2_800_000, runtime.profile.lotSize)
    const price = roundPrice(runtime.price + (runtime.rng() - 0.5) * runtime.profile.basePrice * 0.004)
    const sourceEventId = `${runtime.symbol}-mock-trade-${runtime.sequence}-${Math.floor(runtime.rng() * 1_000_000)}`

    return {
      id: `${runtime.symbol}-alert-${sourceEventId}`,
      timestamp: new Date(Date.now() - runtime.rng() * 3_600_000).toISOString(),
      price,
      volume,
      turnover: roundPrice(price * volume),
      side,
      participantName: participant.name,
      brokerName: participant.name,
      sourceKind: 'canonical_trade_tick',
      sourceTable: 'trade_ticks',
      sourceEventId,
      tradeId: sourceEventId,
      remark: side === 'buy' ? 'Aggressive buy' : 'Aggressive sell',
      isHighlighted: participant.highlighted,
    }
  }

  private createQueues(runtime: MockRuntime, side: QueueSide): BrokerQueueEntry[] {
    const tickSize = runtime.profile.basePrice >= 20 ? 0.2 : runtime.profile.basePrice >= 1 ? 0.01 : 0.001
    const participants = [...PARTICIPANTS].sort(() => runtime.rng() - 0.5)

    return Array.from({ length: 10 }, (_, index) => {
      const participant = (participants[index % participants.length] ?? PARTICIPANTS[0])!
      const priceOffset = tickSize * (index + 1) * (side === 'ask' ? 1 : -1)
      return {
        id: `${runtime.symbol}-${side}-${runtime.sequence}-${index}`,
        position: index + 1,
        side,
        participantName: participant.name,
        brokerCode: participant.brokerCode,
        price: roundPrice(Math.max(0.001, runtime.price + priceOffset)),
        volume: roundLot(40_000 + runtime.rng() * 1_400_000, runtime.profile.lotSize),
      }
    })
  }

  private createHolding(runtime: MockRuntime): HoldingEntry[] {
    let remainingPercent = 42 + runtime.rng() * 18

    return PARTICIPANTS.map((participant, index) => {
      const percent =
        index === PARTICIPANTS.length - 1
          ? remainingPercent
          : Math.max(0.24, Math.min(remainingPercent, 2 + runtime.rng() * 8))
      remainingPercent = Math.max(0, remainingPercent - percent)

      return {
        participantName: participant.name,
        participantCode: participant.code,
        shares: Math.round((runtime.profile.floatShares * percent) / 100),
        percent: roundPrice(percent),
        change: roundLot((runtime.rng() - 0.46) * 6_000_000, runtime.profile.lotSize),
        isHighlighted: participant.highlighted,
      }
    }).sort((left, right) => right.shares - left.shares)
  }

  private createHoldingHistory(
    runtime: MockRuntime,
    participantName: string,
    days: number,
  ): HoldingHistoryPoint[] {
    const holding =
      runtime.holding.find((item) => item.participantName === participantName) ?? runtime.holding[0]
    if (!holding) {
      return []
    }

    let shares = holding.shares
    const percentBase = holding.percent

    return Array.from({ length: days }, (_, index) => {
      const date = new Date()
      date.setDate(date.getDate() - (days - index - 1))
      const change = roundLot((runtime.rng() - 0.5) * 2_600_000, runtime.profile.lotSize)
      shares = Math.max(0, shares + change)

      return {
        date: date.toISOString().slice(0, 10),
        shares,
        percent: roundPrice(Math.max(0, percentBase + (runtime.rng() - 0.5) * 0.7)),
        change,
      }
    })
  }

  private emitTerminal<TType extends TerminalMessageType>(
    type: TType,
    symbol: StockSymbol,
    runtime: MockRuntime,
    payload: TerminalMessagePayloadByType[TType],
  ) {
    const now = new Date().toISOString()
    runtime.sequence += 1
    const terminalMessage = {
      schema_version: 1,
      type,
      event_id: `${type}-${symbol}-${runtime.sequence}`,
      symbol,
      source: 'mock',
      source_ts: now,
      ingest_ts: now,
      seq: runtime.sequence,
      payload,
    } satisfies TerminalMessage<TType>

    const message = normalizeTerminalMessage(terminalMessage)
    if (!message) {
      return
    }

    this.health.recordEvent(symbol, terminalMessage.source_ts)
    this.emit(message)
  }

  private emit(message: MarketMessage) {
    for (const handler of this.handlers) {
      handler(message)
    }
  }
}

function createFallbackProfile(symbol: StockSymbol): MockProfile {
  const seed = hashSymbol(symbol)
  const basePrice = roundPrice(1 + (seed % 9000) / 100)

  return {
    name: symbol,
    currency: 'HKD',
    basePrice,
    previousClose: roundPrice(basePrice * 0.992),
    lotSize: 500,
    floatShares: 3_000_000_000 + (seed % 7_000_000_000),
  }
}

function hashSymbol(symbol: string): number {
  let hash = 2166136261
  for (let index = 0; index < symbol.length; index += 1) {
    hash ^= symbol.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return hash >>> 0
}

function mulberry32(seed: number) {
  return () => {
    let next = (seed += 0x6d2b79f5)
    next = Math.imul(next ^ (next >>> 15), next | 1)
    next ^= next + Math.imul(next ^ (next >>> 7), next | 61)
    return ((next ^ (next >>> 14)) >>> 0) / 4294967296
  }
}

function pick<T>(rng: () => number, items: T[]): T {
  const item = items[Math.floor(rng() * items.length)]
  if (item === undefined) {
    throw new Error('Cannot pick from an empty list')
  }
  return item
}

function roundPrice(value: number): number {
  if (Math.abs(value) >= 10) {
    return Number(value.toFixed(2))
  }
  if (Math.abs(value) >= 1) {
    return Number(value.toFixed(3))
  }
  return Number(value.toFixed(4))
}

function roundLot(value: number, lotSize: number): number {
  return Math.max(lotSize, Math.round(value / lotSize) * lotSize)
}
