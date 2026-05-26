import type {
  AlertRealtimeMessage,
  BigTradeAlert,
  BrokerQueueEntry,
  FreshnessPayload,
  HoldingEntry,
  HoldingHistoryMessage,
  HoldingHistoryPoint,
  MarketMessage,
  MarketSnapshot,
  PriceTick,
  QueueRealtimeMessage,
  QueueSide,
  SnapshotMessage,
  TerminalMessage,
  TerminalMessageType,
  TickRealtimeMessage,
  TradeSide,
} from '@/types/market'
import { normalizeStockSymbol } from '@/utils/symbol'

type JsonRecord = Record<string, unknown>

const TERMINAL_MESSAGE_TYPES = [
  'snapshot',
  'tick_realtime',
  'alert_realtime',
  'queue_realtime',
  'holding_name_click_response',
] as const

const HIGHLIGHTED_PARTICIPANTS = [
  'JPMORGAN',
  'MORGAN STANLEY',
  'CITIBANK',
  'HSBC',
  'UBS',
  'MERRILL',
  'GOLDMAN',
  'CCASS',
]

export function normalizeTerminalMessage(rawInput: unknown): MarketMessage | null {
  if (!isTerminalMessageV1(rawInput)) {
    return null
  }

  return normalizeMarketMessage(rawInput)
}

export function isTerminalMessageV1(rawInput: unknown): rawInput is TerminalMessage {
  const raw = parseRecord(rawInput)
  if (!raw) {
    return false
  }

  const type = raw.type
  const payload = recordValue(raw.payload)
  return (
    raw.schema_version === 1 &&
    typeof type === 'string' &&
    TERMINAL_MESSAGE_TYPES.includes(type as TerminalMessageType) &&
    isNonEmptyString(raw.event_id) &&
    isTerminalSymbol(raw.symbol) &&
    isNonEmptyString(raw.source) &&
    isIsoDateTime(raw.source_ts) &&
    isIsoDateTime(raw.ingest_ts) &&
    isPositiveInteger(raw.seq) &&
    payload !== null &&
    isTerminalPayloadV1(type as TerminalMessageType, payload)
  )
}

function isTerminalPayloadV1(type: TerminalMessageType, payload: JsonRecord): boolean {
  switch (type) {
    case 'snapshot': {
      const brokerQueue = recordValue(payload.broker_queue)
      return (
        recordValue(payload.snapshot) !== null &&
        Array.isArray(payload.minute_bars) &&
        Array.isArray(payload.alerts) &&
        brokerQueue !== null &&
        Array.isArray(brokerQueue.ask) &&
        Array.isArray(brokerQueue.bid) &&
        Array.isArray(payload.ccass_holdings) &&
        recordValue(payload.freshness) !== null
      )
    }
    case 'tick_realtime':
      return recordValue(payload.tick) !== null
    case 'alert_realtime':
      return recordValue(payload.alert) !== null
    case 'queue_realtime':
      return recordValue(payload.broker_queue) !== null
    case 'holding_name_click_response':
      return (
        isNonEmptyString(payload.participant_name) &&
        isPositiveInteger(payload.days) &&
        Array.isArray(payload.history)
      )
    default:
      return true
  }
}

export function normalizeMarketMessage(rawInput: unknown): MarketMessage | null {
  const raw = parseRecord(rawInput)
  if (!raw) {
    return null
  }

  const rawType = stringValue(raw.type ?? raw.event ?? raw.message_type)
  const symbolValue = stringValue(raw.symbol ?? raw.code ?? raw.stock)
  if (!rawType || !symbolValue) {
    return null
  }

  const symbol = normalizeStockSymbol(symbolValue)
  const payload = recordValue(raw.payload)
  const messageRecord = payload ? { ...raw, ...payload } : raw

  switch (rawType) {
    case 'snapshot':
      return normalizeSnapshotMessage(symbol, messageRecord)
    case 'tick_realtime':
    case 'tick':
      return normalizeTickMessage(symbol, messageRecord)
    case 'alert_realtime':
    case 'large_trade':
      return normalizeAlertMessage(symbol, messageRecord)
    case 'queue_realtime':
    case 'broker_queue':
      return normalizeQueueMessage(symbol, messageRecord)
    case 'holding_name_click_response':
    case 'holding_history':
      return normalizeHoldingHistoryMessage(symbol, messageRecord)
    default:
      return null
  }
}

function normalizeSnapshotMessage(symbol: string, raw: JsonRecord): SnapshotMessage {
  const snapshotRecord = recordValue(raw.snapshot) ?? raw
  const brokerQueue = recordValue(raw.broker_queue)
  return {
    type: 'snapshot',
    symbol,
    snapshot: normalizeSnapshot(symbol, snapshotRecord),
    ticks: arrayValue(raw.minute_bars ?? raw.ticks ?? raw.minute_ticks).map((item, index) =>
      normalizeTick(recordValue(item) ?? {}, index),
    ),
    alerts: arrayValue(raw.alerts ?? raw.large_trades).map((item, index) =>
      normalizeAlert(recordValue(item) ?? {}, index),
    ),
    askQueues: normalizeQueueArray(raw.askQueues ?? raw.ask_queues ?? brokerQueue?.ask, 'ask'),
    bidQueues: normalizeQueueArray(raw.bidQueues ?? raw.bid_queues ?? brokerQueue?.bid, 'bid'),
    holding: arrayValue(raw.ccass_holdings ?? raw.holding ?? raw.holdings).map((item, index) =>
      normalizeHolding(recordValue(item) ?? {}, index),
    ),
    freshness: normalizeFreshness(recordValue(raw.freshness) ?? {}),
  }
}

function normalizeTickMessage(symbol: string, raw: JsonRecord): TickRealtimeMessage {
  const tickRecord = recordValue(raw.tick) ?? raw
  const snapshotRecord = recordValue(raw.snapshot)
  return {
    type: 'tick_realtime',
    symbol,
    tick: normalizeTick(tickRecord, 0),
    snapshot: snapshotRecord ? normalizeSnapshot(symbol, snapshotRecord) : undefined,
    freshness: recordValue(raw.freshness) ? normalizeFreshness(recordValue(raw.freshness) ?? {}) : undefined,
  }
}

function normalizeAlertMessage(symbol: string, raw: JsonRecord): AlertRealtimeMessage {
  const alertRecord = recordValue(raw.alert) ?? raw
  return {
    type: 'alert_realtime',
    symbol,
    alert: normalizeAlert(alertRecord, 0),
    freshness: recordValue(raw.freshness) ? normalizeFreshness(recordValue(raw.freshness) ?? {}) : undefined,
  }
}

function normalizeQueueMessage(symbol: string, raw: JsonRecord): QueueRealtimeMessage {
  const side = normalizeQueueSide(raw.side)
  const brokerQueue = recordValue(raw.broker_queue)
  const message: QueueRealtimeMessage = {
    type: 'queue_realtime',
    symbol,
    side,
    freshness: recordValue(raw.freshness) ? normalizeFreshness(recordValue(raw.freshness) ?? {}) : undefined,
  }

  if (raw.askQueues || raw.ask_queues || brokerQueue?.ask || side === 'ask') {
    message.askQueues = normalizeQueueArray(
      raw.askQueues ?? raw.ask_queues ?? brokerQueue?.ask ?? raw.queues,
      'ask',
    )
  }

  if (raw.bidQueues || raw.bid_queues || brokerQueue?.bid || side === 'bid') {
    message.bidQueues = normalizeQueueArray(
      raw.bidQueues ?? raw.bid_queues ?? brokerQueue?.bid ?? raw.queues,
      'bid',
    )
  }

  return message
}

function normalizeHoldingHistoryMessage(symbol: string, raw: JsonRecord): HoldingHistoryMessage {
  const participantName = stringValue(raw.participant_name ?? raw.participantName ?? raw.name) ?? ''
  const days = numberValue(raw.days) || arrayValue(raw.history).length || 30

  return {
    type: 'holding_name_click_response',
    symbol,
    participantName,
    days,
    history: arrayValue(raw.history ?? raw.data).map((item, index) =>
      normalizeHistoryPoint(recordValue(item) ?? {}, index),
    ),
    freshness: recordValue(raw.freshness) ? normalizeFreshness(recordValue(raw.freshness) ?? {}) : undefined,
  }
}

function normalizeSnapshot(symbol: string, raw: JsonRecord): MarketSnapshot {
  const price = numberValue(raw.price ?? raw.last_price ?? raw.last) || 0
  const previousClose = numberValue(raw.previousClose ?? raw.previous_close ?? raw.prev_close) || price
  const change = numberValue(raw.change) || price - previousClose
  const changePercent =
    numberValue(raw.changePercent ?? raw.change_percent) ||
    (previousClose === 0 ? 0 : (change / previousClose) * 100)

  return {
    symbol,
    name: stringValue(raw.name ?? raw.stock_name) ?? symbol,
    currency: stringValue(raw.currency) ?? 'HKD',
    tradeDate: stringValue(raw.tradeDate ?? raw.trade_date ?? raw.displayTradeDate ?? raw.display_trade_date) ?? undefined,
    requestedTradeDate: stringValue(raw.requestedTradeDate ?? raw.requested_trade_date) ?? undefined,
    isHistoricalSession: booleanValue(raw.isHistoricalSession ?? raw.is_historical_session) ?? undefined,
    price,
    previousClose,
    open: numberValue(raw.open) || price,
    high: numberValue(raw.high) || price,
    low: numberValue(raw.low) || price,
    volume: numberValue(raw.volume) || 0,
    turnover: numberValue(raw.turnover) || 0,
    change,
    changePercent,
    updatedAt: stringValue(raw.updatedAt ?? raw.updated_at ?? raw.timestamp) ?? new Date().toISOString(),
  }
}

function normalizeTick(raw: JsonRecord, index: number): PriceTick {
  const price = numberValue(raw.price ?? raw.last_price) || 0
  const volume = numberValue(raw.volume ?? raw.qty ?? raw.quantity) || 0

  return {
    timestamp:
      stringValue(raw.timestamp ?? raw.time ?? raw.datetime) ??
      new Date(Date.now() + index * 1000).toISOString(),
    price,
    volume,
    turnover: numberValue(raw.turnover) || price * volume,
    direction: normalizeDirection(raw.direction),
  }
}

function normalizeAlert(raw: JsonRecord, index: number): BigTradeAlert {
  const participantName = participantDisplayName(raw.participantName ?? raw.participant_name ?? raw.participant)
  const price = numberValue(raw.price ?? raw.last_price) || 0
  const volume = numberValue(raw.volume ?? raw.qty ?? raw.quantity) || 0

  return {
    id: stringValue(raw.id) ?? `alert-${Date.now()}-${index}`,
    timestamp:
      stringValue(raw.timestamp ?? raw.time ?? raw.datetime) ??
      new Date(Date.now() + index * 1000).toISOString(),
    price,
    volume,
    turnover: numberValue(raw.turnover ?? raw.amount) || price * volume,
    side: normalizeTradeSide(raw.side),
    participantName,
    brokerName: stringValue(raw.brokerName ?? raw.broker_name ?? raw.broker) ?? participantName,
    brokerCode: stringValue(raw.brokerCode ?? raw.broker_code) ?? undefined,
    remark: stringValue(raw.remark) ?? undefined,
    isHighlighted: booleanValue(raw.isHighlighted ?? raw.highlighted) ?? isHighlightedParticipant(participantName),
  }
}

function normalizeQueueArray(input: unknown, side: QueueSide): BrokerQueueEntry[] {
  return arrayValue(input).map((item, index) => {
    const raw = recordValue(item) ?? {}
    return {
      id: stringValue(raw.id) ?? `${side}-${Date.now()}-${index}`,
      position: numberValue(raw.position ?? raw.rank) || index + 1,
      side,
      participantName: participantDisplayName(
        raw.participantName ?? raw.participant_name ?? raw.participant ?? raw.brokerName ?? raw.broker_name,
      ),
      brokerCode: queueBrokerCode(raw.brokerCode ?? raw.broker_code ?? raw.code),
      price: numberValue(raw.price) || 0,
      volume: numberValue(raw.volume ?? raw.qty ?? raw.quantity) || 0,
    }
  })
}

function participantDisplayName(input: unknown): string {
  const value = stringValue(input)
  if (!value || value === '--' || value.startsWith('Broker ')) {
    return '未披露'
  }
  return value
}

function queueBrokerCode(input: unknown): string {
  const value = stringValue(input)
  if (value) {
    return value
  }
  if (typeof input === 'number' && Number.isFinite(input)) {
    return String(input)
  }
  return '--'
}

function normalizeHolding(raw: JsonRecord, index: number): HoldingEntry {
  const participantName =
    stringValue(raw.participantName ?? raw.participant_name ?? raw.name) ?? `Participant ${index + 1}`

  return {
    participantName,
    participantCode: stringValue(raw.participantCode ?? raw.participant_code ?? raw.code) ?? '--',
    shares: numberValue(raw.shares ?? raw.holding ?? raw.volume) || 0,
    percent: numberValue(raw.percent ?? raw.ratio) || 0,
    change: numberValue(raw.change ?? raw.delta) || 0,
    date: stringValue(raw.date) ?? undefined,
    currentDate: stringValue(raw.currentDate ?? raw.current_date) ?? undefined,
    previousDate: stringValue(raw.previousDate ?? raw.previous_date) ?? undefined,
    isHighlighted: booleanValue(raw.isHighlighted ?? raw.highlighted) ?? isHighlightedParticipant(participantName),
  }
}

function normalizeHistoryPoint(raw: JsonRecord, index: number): HoldingHistoryPoint {
  return {
    date: stringValue(raw.date) ?? new Date(Date.now() - index * 86_400_000).toISOString().slice(0, 10),
    shares: numberValue(raw.shares ?? raw.holding ?? raw.volume) || 0,
    percent: numberValue(raw.percent ?? raw.ratio) || 0,
    change: numberValue(raw.change ?? raw.delta) || 0,
  }
}

function normalizeFreshness(raw: JsonRecord): FreshnessPayload {
  const degradedReason = stringValue(raw.degradedReason ?? raw.degraded_reason) ?? undefined
  const degradedReasons = arrayValue(raw.degradedReasons ?? raw.degraded_reasons)
    .filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
    .map((item) => item.trim())
  if (degradedReason && degradedReasons.length === 0) {
    degradedReasons.push(degradedReason)
  }
  const sourceDates = recordValue(raw.sourceDates ?? raw.source_dates)

  return {
    updatedAt: stringValue(raw.updatedAt ?? raw.updated_at) ?? new Date().toISOString(),
    sourceTs: stringValue(raw.sourceTs ?? raw.source_ts) ?? undefined,
    ingestTs: stringValue(raw.ingestTs ?? raw.ingest_ts) ?? undefined,
    degraded: booleanValue(raw.degraded) ?? degradedReasons.length > 0,
    degradedReason,
    degradedReasons,
    requestedTradeDate:
      stringValue(raw.requestedTradeDate ?? raw.requested_trade_date) ?? undefined,
    effectiveTradeDate:
      stringValue(raw.effectiveTradeDate ?? raw.effective_trade_date) ?? undefined,
    runtimeState: stringValue(raw.runtimeState ?? raw.runtime_state) ?? undefined,
    sourceDates: sourceDates
      ? Object.fromEntries(
          Object.entries(sourceDates).flatMap(([key, value]) => {
            const normalized = stringValue(value)
            return normalized ? [[key, normalized]] : []
          }),
        )
      : {},
  }
}

function parseRecord(input: unknown): JsonRecord | null {
  if (typeof input === 'string') {
    try {
      return recordValue(JSON.parse(input))
    } catch {
      return null
    }
  }

  return recordValue(input)
}

function recordValue(input: unknown): JsonRecord | null {
  return input && typeof input === 'object' && !Array.isArray(input) ? (input as JsonRecord) : null
}

function arrayValue(input: unknown): unknown[] {
  return Array.isArray(input) ? input : []
}

function stringValue(input: unknown): string | null {
  return typeof input === 'string' && input.trim() ? input.trim() : null
}

function isNonEmptyString(input: unknown): input is string {
  return typeof input === 'string' && input.trim().length > 0
}

function isPositiveInteger(input: unknown): input is number {
  return typeof input === 'number' && Number.isInteger(input) && input > 0
}

function isIsoDateTime(input: unknown): input is string {
  if (typeof input !== 'string' || !input.includes('T')) {
    return false
  }

  const parsed = Date.parse(input)
  return Number.isFinite(parsed)
}

function isTerminalSymbol(input: unknown): input is string {
  return typeof input === 'string' && /^\d{5}\.HK$/.test(input)
}

function numberValue(input: unknown): number {
  if (typeof input === 'number' && Number.isFinite(input)) {
    return input
  }

  if (typeof input === 'string') {
    const parsed = Number(input.replace(/,/g, ''))
    return Number.isFinite(parsed) ? parsed : 0
  }

  return 0
}

function booleanValue(input: unknown): boolean | null {
  return typeof input === 'boolean' ? input : null
}

function normalizeTradeSide(input: unknown): TradeSide {
  const value = stringValue(input)?.toLowerCase()
  if (value === 'buy' || value === 'b') {
    return 'buy'
  }
  if (value === 'sell' || value === 's') {
    return 'sell'
  }
  return 'neutral'
}

function normalizeQueueSide(input: unknown): QueueSide | undefined {
  const value = stringValue(input)?.toLowerCase()
  if (value === 'ask' || value === 'sell') {
    return 'ask'
  }
  if (value === 'bid' || value === 'buy') {
    return 'bid'
  }
  return undefined
}

function normalizeDirection(input: unknown): PriceTick['direction'] {
  const value = stringValue(input)?.toLowerCase()
  if (value === 'up' || value === '+') {
    return 'up'
  }
  if (value === 'down' || value === '-') {
    return 'down'
  }
  return 'flat'
}

function isHighlightedParticipant(name: string): boolean {
  const normalized = name.toUpperCase()
  return HIGHLIGHTED_PARTICIPANTS.some((participant) => normalized.includes(participant))
}
