export type StockSymbol = string

export type DataSourceMode = 'mock' | 'live'

export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error'

export type SymbolSubscriptionStatus = 'idle' | 'loading' | 'warm' | 'live' | 'closed' | 'degraded'

export type TradeSide = 'buy' | 'sell' | 'neutral'

export type QueueSide = 'ask' | 'bid'

export type MarketEventSchemaVersion = 1

export type RawMarketEventSource = 'xtquant' | 'mammoth' | 'gateway' | 'mock' | string

export type ProcessedMarketEventSource = 'octopus' | 'gateway' | 'mock' | string

export type TerminalMessageSource = 'gateway' | 'mock' | string

export type RawMarketEventKind =
  | 'tick'
  | 'broker_queue'
  | 'l2_order_book'
  | string

export type ProcessedMarketResultType =
  | 'snapshot'
  | 'big_trade_alert'
  | 'broker_queue'
  | 'l2_order_book'
  | string

export type TerminalMessageType =
  | 'snapshot'
  | 'tick_realtime'
  | 'alert_realtime'
  | 'queue_realtime'
  | 'holding_name_click_response'

export interface MarketEventEnvelope<TPayload, TSource extends string = string> {
  schema_version: MarketEventSchemaVersion
  event_id: string
  symbol: StockSymbol
  source: TSource
  source_ts: string
  ingest_ts: string
  seq: number
  payload: TPayload
}

export interface RawMarketEvent<TPayload = Record<string, unknown>>
  extends MarketEventEnvelope<TPayload, RawMarketEventSource> {
  kind: RawMarketEventKind
  period?: string
}

export interface ProcessedMarketEvent<TPayload = Record<string, unknown>>
  extends MarketEventEnvelope<TPayload, ProcessedMarketEventSource> {
  result_type: ProcessedMarketResultType
  period?: string
}

export interface FreshnessPayload {
  updatedAt: string
  sourceTs?: string
  ingestTs?: string
  degraded?: boolean
  degradedReason?: string
  degradedReasons: string[]
  requestedTradeDate?: string
  effectiveTradeDate?: string
  runtimeState?: 'COLD' | 'HYDRATING' | 'WARM' | 'LIVE' | 'DEGRADED' | 'EVICTING' | string
  sourceDates: Record<string, string>
}

export interface MarketSnapshot {
  symbol: StockSymbol
  name: string
  currency: string
  tradeDate?: string
  requestedTradeDate?: string
  isHistoricalSession?: boolean
  price: number
  previousClose: number
  open: number
  high: number
  low: number
  volume: number
  turnover: number
  change: number
  changePercent: number
  updatedAt: string
}

export interface PriceTick {
  timestamp: string
  price: number
  volume: number
  turnover: number
  direction: 'up' | 'down' | 'flat'
  replace?: boolean
}

export interface BigTradeAlert {
  id: string
  timestamp: string
  price: number
  volume: number
  turnover: number
  side: TradeSide
  participantName: string
  brokerName: string
  brokerCode?: string
  remark?: string
  isHighlighted: boolean
}

export interface BrokerQueueEntry {
  id: string
  position: number
  side: QueueSide
  participantName: string
  brokerCode: string
  price: number
  volume: number
}

export interface HoldingEntry {
  participantName: string
  participantCode: string
  shares: number
  percent: number
  change: number
  date?: string
  currentDate?: string
  previousDate?: string
  isHighlighted: boolean
}

export interface HoldingHistoryPoint {
  date: string
  shares: number
  percent: number
  change: number
}

export interface SnapshotTerminalPayload {
  snapshot: MarketSnapshot
  minute_bars: PriceTick[]
  alerts: BigTradeAlert[]
  broker_queue: {
    ask: BrokerQueueEntry[]
    bid: BrokerQueueEntry[]
  }
  ccass_holdings: HoldingEntry[]
  freshness: FreshnessPayload
}

export interface TickRealtimeTerminalPayload {
  tick: PriceTick
  snapshot?: MarketSnapshot
  freshness?: FreshnessPayload
}

export interface AlertRealtimeTerminalPayload {
  alert: BigTradeAlert
  freshness?: FreshnessPayload
}

export interface QueueRealtimeTerminalPayload {
  side?: QueueSide
  broker_queue: {
    ask?: BrokerQueueEntry[]
    bid?: BrokerQueueEntry[]
  }
  freshness?: FreshnessPayload
}

export interface HoldingHistoryTerminalPayload {
  participant_name: string
  days: number
  history: HoldingHistoryPoint[]
  freshness?: FreshnessPayload
}

export type TerminalMessagePayloadByType = {
  snapshot: SnapshotTerminalPayload
  tick_realtime: TickRealtimeTerminalPayload
  alert_realtime: AlertRealtimeTerminalPayload
  queue_realtime: QueueRealtimeTerminalPayload
  holding_name_click_response: HoldingHistoryTerminalPayload
}

export type TerminalMessage<TType extends TerminalMessageType = TerminalMessageType> = {
  [K in TType]: MarketEventEnvelope<TerminalMessagePayloadByType[K], TerminalMessageSource> & {
    type: K
  }
}[TType]

export interface SymbolState {
  snapshot: MarketSnapshot | null
  ticks: PriceTick[]
  alerts: BigTradeAlert[]
  askQueues: BrokerQueueEntry[]
  bidQueues: BrokerQueueEntry[]
  holding: HoldingEntry[]
  holdingHistoryByParticipant: Record<string, HoldingHistoryPoint[]>
  freshness: FreshnessPayload | null
  subscriptionStatus: SymbolSubscriptionStatus
  subscriptionError: string | null
  snapshotLoaded: boolean
  lastUpdatedAt: string | null
  unreadAlerts: number
}

export interface TerminalHealthStatus {
  process: 'starting' | 'running' | 'degraded' | 'stopped'
  kafka: 'unknown' | 'connected' | 'degraded' | 'down'
  redis: 'unknown' | 'connected' | 'degraded' | 'down'
  kafkaLag: number | null
  latestEventAtBySymbol: Record<StockSymbol, string>
  symbolFreshness: Record<
    StockSymbol,
    {
      subscribed: boolean
      latestEventAt: string | null
      latestIngestAt: string | null
      queueBacklog: number
      degraded: boolean
      degradedReason: string | null
      resubscribeRequested: boolean
    }
  >
  updatedAt: string
}

export interface SnapshotMessage {
  type: 'snapshot'
  symbol: StockSymbol
  snapshot: MarketSnapshot
  ticks: PriceTick[]
  alerts: BigTradeAlert[]
  askQueues: BrokerQueueEntry[]
  bidQueues: BrokerQueueEntry[]
  holding: HoldingEntry[]
  freshness: FreshnessPayload
}

export interface TickRealtimeMessage {
  type: 'tick_realtime'
  symbol: StockSymbol
  tick: PriceTick
  snapshot?: MarketSnapshot
  freshness?: FreshnessPayload
}

export interface AlertRealtimeMessage {
  type: 'alert_realtime'
  symbol: StockSymbol
  alert: BigTradeAlert
  freshness?: FreshnessPayload
}

export interface QueueRealtimeMessage {
  type: 'queue_realtime'
  symbol: StockSymbol
  side?: QueueSide
  askQueues?: BrokerQueueEntry[]
  bidQueues?: BrokerQueueEntry[]
  freshness?: FreshnessPayload
}

export interface HoldingHistoryMessage {
  type: 'holding_name_click_response'
  symbol: StockSymbol
  participantName: string
  days: number
  history: HoldingHistoryPoint[]
  freshness?: FreshnessPayload
}

export type MarketMessage =
  | SnapshotMessage
  | TickRealtimeMessage
  | AlertRealtimeMessage
  | QueueRealtimeMessage
  | HoldingHistoryMessage

export type MarketMessageHandler = (message: MarketMessage) => void

export type TerminalHealthHandler = (health: TerminalHealthStatus) => void

export type ConnectionStatusHandler = (status: ConnectionStatus, error: string | null) => void

export interface MarketPerformanceSample {
  key: 'frontend_store_update_ms' | string
  valueMs: number
  symbol?: StockSymbol
  messageType?: MarketMessage['type']
  recordedAt: string
}

export type MarketPerformanceSampleHandler = (sample: MarketPerformanceSample) => void

export interface MarketDataSource {
  connect(): Promise<void>
  disconnect(): Promise<void> | void
  subscribe(symbol: StockSymbol): Promise<void> | void
  unsubscribe(symbol: StockSymbol): Promise<void> | void
  requestHoldingHistory(symbol: StockSymbol, participantName: string, days: number): Promise<void> | void
  onMessage(handler: MarketMessageHandler): () => void
  onHealth?(handler: TerminalHealthHandler): () => void
  onConnectionStatus?(handler: ConnectionStatusHandler): () => void
}

export interface MarketState {
  mode: DataSourceMode
  liveGatewayUrl: string | null
  connectionStatus: ConnectionStatus
  connectionError: string | null
  health: TerminalHealthStatus
  subscribedSymbols: StockSymbol[]
  activeSymbol: StockSymbol
  symbols: Record<StockSymbol, SymbolState>
}
