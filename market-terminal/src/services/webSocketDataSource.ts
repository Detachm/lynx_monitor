import type {
  ConnectionStatus,
  ConnectionStatusHandler,
  MarketDataSource,
  MarketMessageHandler,
  StockSymbol,
  TerminalHealthHandler,
  TerminalHealthStatus,
} from '@/types/market'
import { TERMINAL_MESSAGE_PROTOCOL } from '@/config/dataSource'
import { TerminalHealthTracker } from '@/services/health'
import { normalizeTerminalMessage } from '@/services/normalizers'
import { normalizeHoldingHistoryDays } from '@/utils/holdingHistory'
import { normalizeStockSymbol } from '@/utils/symbol'

export class WebSocketDataSource implements MarketDataSource {
  private socket: WebSocket | null = null
  private readonly handlers = new Set<MarketMessageHandler>()
  private readonly connectionHandlers = new Set<ConnectionStatusHandler>()
  private readonly health = new TerminalHealthTracker()
  private readonly desiredSubscriptions = new Set<StockSymbol>()
  private readonly pendingSubscribes = new Map<
    StockSymbol,
    {
      resolve: () => void
      reject: (error: Error) => void
      timer: ReturnType<typeof globalThis.setTimeout>
    }
  >()
  private connectPromise: Promise<void> | null = null
  private reconnectTimer: ReturnType<typeof globalThis.setTimeout> | null = null
  private reconnectAttempts = 0
  private manuallyDisconnected = false

  constructor(
    private readonly url: string,
    private readonly protocol: string = TERMINAL_MESSAGE_PROTOCOL,
    private readonly subscribeAckTimeoutMs = 120_000,
    private readonly clientId: string = '',
    private readonly reconnectBaseDelayMs = 1_000,
    private readonly reconnectMaxDelayMs = 30_000,
  ) {}

  connect(): Promise<void> {
    if (this.socket?.readyState === WebSocket.OPEN) {
      return Promise.resolve()
    }
    if (this.connectPromise) {
      return this.connectPromise
    }
    this.manuallyDisconnected = false
    this.clearReconnectTimer()
    this.notifyConnectionStatus('connecting')

    this.connectPromise = new Promise((resolve, reject) => {
      const socket = new WebSocket(this.url)
      this.socket = socket

      socket.addEventListener(
        'open',
        () => {
          this.connectPromise = null
          this.reconnectAttempts = 0
          this.health.update({ process: 'running' })
          this.notifyConnectionStatus('connected')
          this.restoreSubscriptions()
          resolve()
        },
        { once: true },
      )
      socket.addEventListener(
        'error',
        () => {
          this.connectPromise = null
          const error = new Error(`Unable to connect to ${this.url}`)
          this.health.update({ process: 'degraded' })
          this.notifyConnectionStatus('error', error.message)
          reject(error)
          this.scheduleReconnect()
        },
        { once: true },
      )
      socket.addEventListener('message', (event) => {
        const error = parseGatewayError(event.data)
        if (error) {
          this.rejectOldestPendingSubscribe(new Error(error))
          this.health.update({ process: 'degraded' })
          return
        }

        const health = parseHealthStatus(event.data)
        if (health) {
          this.health.update(health)
          return
        }

        const message = normalizeTerminalMessage(event.data)
        if (!message) {
          return
        }
        if (message.type === 'snapshot') {
          this.resolvePendingSubscribe(message.symbol)
        }
        this.health.recordEvent(message.symbol)
        for (const handler of this.handlers) {
          handler(message)
        }
      })
      socket.addEventListener('close', () => {
        if (this.socket === socket) {
          this.socket = null
        }
        this.connectPromise = null
        this.rejectPendingSubscribes(new Error('WebSocket connection closed'))
        if (this.manuallyDisconnected) {
          this.health.update({ process: 'stopped' })
          this.notifyConnectionStatus('disconnected')
        } else {
          this.health.update({ process: 'degraded' })
          this.notifyConnectionStatus('error', 'WebSocket connection closed')
          this.scheduleReconnect()
        }
      })
    })
    return this.connectPromise
  }

  disconnect() {
    this.manuallyDisconnected = true
    this.clearReconnectTimer()
    this.socket?.close()
    this.socket = null
    this.rejectPendingSubscribes(new Error('WebSocket connection closed'))
    this.notifyConnectionStatus('disconnected')
  }

  subscribe(rawSymbol: StockSymbol) {
    const symbol = normalizeStockSymbol(rawSymbol)
    this.desiredSubscriptions.add(symbol)
    if (this.pendingSubscribes.has(symbol)) {
      return new Promise<void>((resolve, reject) => {
        const previous = this.pendingSubscribes.get(symbol)!
        const previousResolve = previous.resolve
        const previousReject = previous.reject
        previous.resolve = () => {
          previousResolve()
          resolve()
        }
        previous.reject = (error: Error) => {
          previousReject(error)
          reject(error)
        }
      })
    }

    return new Promise<void>((resolve, reject) => {
      const timer = globalThis.setTimeout(() => {
        this.pendingSubscribes.delete(symbol)
        reject(new Error(`Timed out waiting for subscribe snapshot: ${symbol}`))
      }, this.subscribeAckTimeoutMs)
      this.pendingSubscribes.set(symbol, { resolve, reject, timer })
      try {
        if (this.socket?.readyState === WebSocket.OPEN) {
          this.send({ action: 'subscribe', symbol })
        } else {
          void this.connect().catch((error) => {
            this.rejectPendingSubscribe(symbol, error instanceof Error ? error : new Error(String(error)))
          })
        }
      } catch (error) {
        globalThis.clearTimeout(timer)
        this.pendingSubscribes.delete(symbol)
        reject(error instanceof Error ? error : new Error(String(error)))
      }
    })
  }

  unsubscribe(rawSymbol: StockSymbol) {
    const symbol = normalizeStockSymbol(rawSymbol)
    this.desiredSubscriptions.delete(symbol)
    this.rejectPendingSubscribe(symbol, new Error(`Subscribe cancelled: ${symbol}`))
    this.send({ action: 'unsubscribe', symbol })
  }

  requestHoldingHistory(rawSymbol: StockSymbol, participantName: string, days = 30) {
    const symbol = normalizeStockSymbol(rawSymbol)
    this.send({
      action: 'holding_name_click',
      symbol,
      participant_name: participantName,
      days: normalizeHoldingHistoryDays(days),
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

  private send(payload: object) {
    if (this.socket?.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket is not open')
    }

    this.socket.send(JSON.stringify(gatewayRequestPayload(payload, this.protocol, this.clientId)))
  }

  private restoreSubscriptions() {
    for (const symbol of this.desiredSubscriptions) {
      try {
        this.send({ action: 'subscribe', symbol })
      } catch (error) {
        this.rejectPendingSubscribe(symbol, error instanceof Error ? error : new Error(String(error)))
      }
    }
  }

  private scheduleReconnect() {
    if (this.manuallyDisconnected || this.reconnectTimer || this.desiredSubscriptions.size === 0) {
      return
    }
    const delay = Math.min(
      this.reconnectMaxDelayMs,
      this.reconnectBaseDelayMs * 2 ** this.reconnectAttempts,
    )
    this.reconnectAttempts += 1
    this.health.update({ process: 'degraded' })
    this.reconnectTimer = globalThis.setTimeout(() => {
      this.reconnectTimer = null
      void this.connect().catch(() => {
        this.scheduleReconnect()
      })
    }, delay)
  }

  private clearReconnectTimer() {
    if (!this.reconnectTimer) {
      return
    }
    globalThis.clearTimeout(this.reconnectTimer)
    this.reconnectTimer = null
  }

  private resolvePendingSubscribe(symbol: StockSymbol) {
    const pending = this.pendingSubscribes.get(symbol)
    if (!pending) {
      return
    }
    globalThis.clearTimeout(pending.timer)
    this.pendingSubscribes.delete(symbol)
    pending.resolve()
  }

  private rejectPendingSubscribes(error: Error) {
    for (const [symbol, pending] of this.pendingSubscribes) {
      globalThis.clearTimeout(pending.timer)
      this.pendingSubscribes.delete(symbol)
      pending.reject(error)
    }
  }

  private rejectPendingSubscribe(symbol: StockSymbol, error: Error) {
    const pending = this.pendingSubscribes.get(symbol)
    if (!pending) {
      return
    }
    globalThis.clearTimeout(pending.timer)
    this.pendingSubscribes.delete(symbol)
    pending.reject(error)
  }

  private rejectOldestPendingSubscribe(error: Error) {
    const firstSymbol = this.pendingSubscribes.keys().next().value
    if (firstSymbol) {
      this.rejectPendingSubscribe(firstSymbol, error)
    }
  }

  private notifyConnectionStatus(status: ConnectionStatus, error: string | null = null) {
    for (const handler of this.connectionHandlers) {
      handler(status, error)
    }
  }
}

export function gatewayRequestPayload(payload: object, protocol = TERMINAL_MESSAGE_PROTOCOL, clientId = '') {
  const request = {
    schema_version: 1,
    protocol,
    ...payload,
  }
  return clientId.trim() ? { ...request, client_id: clientId.trim() } : request
}

export function parseHealthStatus(input: unknown): Partial<Omit<TerminalHealthStatus, 'updatedAt'>> | null {
  const raw = parseRecord(input)
  if (!raw) {
    return null
  }

  if (raw.schema_version !== 1 || !stringValue(raw.source)) {
    return null
  }

  const type = stringValue(raw.type)
  if (type !== 'health') {
    return null
  }

  const payload = recordValue(raw.payload)
  if (!payload) {
    return null
  }

  const process = enumValue(payload.process, ['starting', 'running', 'degraded', 'stopped'])
  const kafka = enumValue(payload.kafka, ['unknown', 'connected', 'degraded', 'down'])
  const redis = enumValue(payload.redis, ['unknown', 'connected', 'degraded', 'down'])
  const kafkaLag = nonNegativeIntegerOrNull(payload.kafka_lag ?? payload.kafkaLag)
  const latestEventAtBySymbol = recordValue(
    payload.latest_event_at_by_symbol ?? payload.latestEventAtBySymbol,
  )
  const symbolFreshness = recordValue(payload.symbol_freshness ?? payload.symbolFreshness)
  const realtimeRecovery = recordValue(payload.realtime_recovery ?? payload.realtimeRecovery)
  const tradeTickSourceAvailable = booleanValue(
    payload.trade_tick_source_available ?? payload.tradeTickSourceAvailable,
  )
  const tradeTickSourceAvailableBySymbol = recordValue(
    payload.trade_tick_source_available_by_symbol ??
      payload.tradeTickSourceAvailableBySymbol ??
      realtimeRecovery?.trade_tick_source_available_by_symbol ??
      realtimeRecovery?.tradeTickSourceAvailableBySymbol,
  )

  return {
    ...(process ? { process } : {}),
    ...(kafka ? { kafka } : {}),
    ...(redis ? { redis } : {}),
    ...(kafkaLag !== undefined ? { kafkaLag } : {}),
    ...(latestEventAtBySymbol ? { latestEventAtBySymbol: stringRecord(latestEventAtBySymbol) } : {}),
    ...(tradeTickSourceAvailable !== null ? { tradeTickSourceAvailable } : {}),
    ...(tradeTickSourceAvailableBySymbol
      ? { tradeTickSourceAvailableBySymbol: booleanSymbolRecord(tradeTickSourceAvailableBySymbol) }
      : {}),
    ...(symbolFreshness ? { symbolFreshness: normalizeSymbolFreshness(symbolFreshness) } : {}),
  }
}

export function parseGatewayError(input: unknown): string | null {
  const raw = parseRecord(input)
  if (!raw) {
    return null
  }
  if (raw.schema_version !== 1 || stringValue(raw.type) !== 'error') {
    return null
  }
  const payload = recordValue(raw.payload)
  return stringValue(payload?.message) ?? 'Gateway error'
}

function parseRecord(input: unknown): Record<string, unknown> | null {
  if (typeof input === 'string') {
    try {
      return recordValue(JSON.parse(input))
    } catch {
      return null
    }
  }

  return recordValue(input)
}

function recordValue(input: unknown): Record<string, unknown> | null {
  return input && typeof input === 'object' && !Array.isArray(input) ? (input as Record<string, unknown>) : null
}

function stringValue(input: unknown): string | null {
  return typeof input === 'string' && input.trim() ? input.trim() : null
}

function enumValue<TValue extends string>(input: unknown, values: readonly TValue[]): TValue | null {
  const value = stringValue(input)
  return value && values.includes(value as TValue) ? (value as TValue) : null
}

function nonNegativeIntegerOrNull(input: unknown): number | null | undefined {
  if (input === null) {
    return null
  }
  return typeof input === 'number' && Number.isInteger(input) && input >= 0 ? input : undefined
}

function stringRecord(input: Record<string, unknown>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(input).filter(
      (entry): entry is [string, string] =>
        isTerminalSymbol(entry[0]) && typeof entry[1] === 'string' && isIsoDateTime(entry[1]),
    ),
  )
}

function booleanSymbolRecord(input: Record<string, unknown>): Record<string, boolean> {
  return Object.fromEntries(
    Object.entries(input).flatMap(([symbol, value]) => {
      if (!isTerminalSymbol(symbol) || typeof value !== 'boolean') {
        return []
      }
      return [[symbol, value]]
    }),
  )
}

function normalizeSymbolFreshness(input: Record<string, unknown>): TerminalHealthStatus['symbolFreshness'] {
  return Object.fromEntries(
    Object.entries(input).flatMap(([symbol, value]) => {
      if (!isTerminalSymbol(symbol)) {
        return []
      }
      const raw = recordValue(value)
      if (!raw) {
        return []
      }
      const queueBacklog = nonNegativeIntegerOrNull(raw.queue_backlog ?? raw.queueBacklog)

      return [
        [
          symbol,
          {
            subscribed: booleanValue(raw.subscribed) ?? false,
            latestEventAt: isoStringOrNull(raw.latest_event_at ?? raw.latestEventAt),
            latestIngestAt: isoStringOrNull(raw.latest_ingest_at ?? raw.latestIngestAt),
            queueBacklog: queueBacklog ?? 0,
            degraded: booleanValue(raw.degraded) ?? false,
            degradedReason: nullableString(raw.degraded_reason ?? raw.degradedReason),
            resubscribeRequested: booleanValue(
              raw.resubscribe_requested ?? raw.resubscribeRequested,
            ) ?? false,
          },
        ],
      ]
    }),
  )
}

function nullableString(input: unknown): string | null {
  return typeof input === 'string' && input.trim() ? input.trim() : null
}

function booleanValue(input: unknown): boolean | null {
  return typeof input === 'boolean' ? input : null
}

function isoStringOrNull(input: unknown): string | null {
  const value = nullableString(input)
  return value && isIsoDateTime(value) ? value : null
}

function isIsoDateTime(value: string): boolean {
  if (!value.includes('T')) {
    return false
  }
  const time = Date.parse(value)
  return Number.isFinite(time)
}

function isTerminalSymbol(value: string): boolean {
  return /^\d{5}\.HK$/.test(value)
}
