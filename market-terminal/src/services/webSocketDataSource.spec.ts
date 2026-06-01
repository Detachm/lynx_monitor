import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  WebSocketDataSource,
  gatewayRequestPayload,
  parseGatewayError,
  parseHealthStatus,
} from '@/services/webSocketDataSource'

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('parseHealthStatus', () => {
  it('normalizes backend symbol freshness health payloads', () => {
    const health = parseHealthStatus(
      JSON.stringify({
        schema_version: 1,
        type: 'health',
        source: 'gateway',
        payload: {
          process: 'degraded',
          kafka: 'connected',
          redis: 'connected',
          kafka_lag: 2,
          latest_event_at_by_symbol: {
            '00700.HK': '2026-05-22T09:30:00+08:00',
          },
          symbol_freshness: {
            '00700.HK': {
              subscribed: true,
              latest_event_at: '2026-05-22T09:30:00+08:00',
              latest_ingest_at: '2026-05-22T09:30:00.010+08:00',
              queue_backlog: 4,
              degraded: true,
              degraded_reason: 'queue_backlog_exceeded',
              resubscribe_requested: true,
            },
          },
        },
      }),
    )

    expect(health).toMatchObject({
      process: 'degraded',
      kafkaLag: 2,
      latestEventAtBySymbol: {
        '00700.HK': '2026-05-22T09:30:00+08:00',
      },
      symbolFreshness: {
        '00700.HK': {
          subscribed: true,
          queueBacklog: 4,
          degradedReason: 'queue_backlog_exceeded',
          resubscribeRequested: true,
        },
      },
    })
  })

  it('rejects weak health metrics and non-canonical freshness symbols', () => {
    const health = parseHealthStatus({
      schema_version: 1,
      type: 'health',
      source: 'gateway',
      payload: {
        kafka_lag: '2',
        latest_event_at_by_symbol: {
          '00700.HK': '2026-05-22T09:30:00+08:00',
          '700.HK': '2026-05-22T09:30:00+08:00',
          '00939.HK': '20260522 093000',
        },
        symbol_freshness: {
          '00700.HK': {
            subscribed: true,
            latest_event_at: '20260522 093000',
            latest_ingest_at: '2026-05-22T09:30:00.010+08:00',
            queue_backlog: '4',
            degraded: false,
          },
          '700.HK': {
            subscribed: true,
            latest_event_at: '2026-05-22T09:30:00+08:00',
            queue_backlog: 1,
          },
        },
      },
    })

    expect(health?.kafkaLag).toBeUndefined()
    expect(health?.latestEventAtBySymbol).toEqual({
      '00700.HK': '2026-05-22T09:30:00+08:00',
    })
    expect(health?.symbolFreshness).toEqual({
      '00700.HK': expect.objectContaining({
        latestEventAt: null,
        latestIngestAt: '2026-05-22T09:30:00.010+08:00',
        queueBacklog: 0,
      }),
    })
  })

  it('rejects non-v1 or legacy health envelopes', () => {
    expect(
      parseHealthStatus({
        type: 'health',
        source: 'gateway',
        payload: { process: 'running' },
      }),
    ).toBeNull()
    expect(
      parseHealthStatus({
        schema_version: 1,
        type: 'health_status',
        source: 'gateway',
        payload: { process: 'running' },
      }),
    ).toBeNull()
    expect(
      parseHealthStatus({
        schema_version: 1,
        type: 'health',
        source: '',
        payload: { process: 'running' },
      }),
    ).toBeNull()
    expect(
      parseHealthStatus({
        schema_version: 1,
        type: 'health',
        source: 'gateway',
        process: 'running',
      }),
    ).toBeNull()
  })
})

describe('parseGatewayError', () => {
  it('normalizes gateway error frames', () => {
    expect(
      parseGatewayError({
        schema_version: 1,
        type: 'error',
        source: 'gateway',
        payload: { message: 'hydrate failed' },
      }),
    ).toBe('hydrate failed')
    expect(parseGatewayError({ schema_version: 1, type: 'health', source: 'gateway' })).toBeNull()
  })
})

describe('gatewayRequestPayload', () => {
  it('marks live websocket requests with the v1 terminal protocol', () => {
    expect(gatewayRequestPayload({ action: 'subscribe', symbol: '00700.HK' })).toEqual({
      schema_version: 1,
      protocol: 'terminal-message-v1',
      action: 'subscribe',
      symbol: '00700.HK',
    })
    expect(gatewayRequestPayload({ action: 'subscribe', symbol: '00700.HK' }, 'terminal-message-v1', 'desk-a')).toEqual({
      schema_version: 1,
      protocol: 'terminal-message-v1',
      action: 'subscribe',
      symbol: '00700.HK',
      client_id: 'desk-a',
    })
  })

  it('normalizes holding history days before sending live websocket requests', () => {
    const sent: string[] = []
    vi.stubGlobal('WebSocket', { OPEN: 1 })
    const source = new WebSocketDataSource('ws://localhost:9020/ws')
    ;(source as unknown as { socket: { readyState: number; send: (message: string) => void } }).socket = {
      readyState: 1,
      send: (message: string) => sent.push(message),
    }

    source.requestHoldingHistory('700', 'JPMorgan', 0)

    expect(JSON.parse(sent[0]!)).toMatchObject({
      schema_version: 1,
      protocol: 'terminal-message-v1',
      action: 'holding_name_click',
      symbol: '00700.HK',
      participant_name: 'JPMorgan',
      days: 30,
    })
  })
})

describe('WebSocketDataSource subscribe acknowledgements', () => {
  it('resolves subscribe only after the matching snapshot arrives', async () => {
    const sockets = installFakeWebSocket()
    const source = new WebSocketDataSource('ws://localhost:9020/ws', 'terminal-message-v1', 50, 'desk-a')
    const messages: string[] = []
    source.onMessage((message) => messages.push(message.type))
    const connected = source.connect()
    sockets[0]!.open()
    await connected

    let resolved = false
    const subscribed = source.subscribe('700').then(() => {
      resolved = true
    })

    expect(JSON.parse(sockets[0]!.sent[0]!)).toMatchObject({
      action: 'subscribe',
      symbol: '00700.HK',
      client_id: 'desk-a',
    })
    await Promise.resolve()
    expect(resolved).toBe(false)

    sockets[0]!.message(snapshotFrame('00700.HK'))
    await subscribed

    expect(resolved).toBe(true)
    expect(messages).toEqual(['snapshot'])
  })

  it('rejects subscribe when the gateway returns an error', async () => {
    const sockets = installFakeWebSocket()
    const source = new WebSocketDataSource('ws://localhost:9020/ws', 'terminal-message-v1', 50)
    const connected = source.connect()
    sockets[0]!.open()
    await connected

    const subscribed = source.subscribe('700')
    sockets[0]!.message(
      JSON.stringify({
        schema_version: 1,
        type: 'error',
        source: 'gateway',
        payload: { message: 'hydrate failed' },
      }),
    )

    await expect(subscribed).rejects.toThrow('hydrate failed')
  })

  it('only rejects the oldest pending subscribe when the gateway returns a subscribe error', async () => {
    const sockets = installFakeWebSocket()
    const source = new WebSocketDataSource('ws://localhost:9020/ws', 'terminal-message-v1', 50)
    const connected = source.connect()
    sockets[0]!.open()
    await connected

    const firstSubscribed = source.subscribe('700')
    const secondSubscribed = source.subscribe('939')
    sockets[0]!.message(
      JSON.stringify({
        schema_version: 1,
        type: 'error',
        source: 'gateway',
        payload: { message: 'hydrate failed' },
      }),
    )

    await expect(firstSubscribed).rejects.toThrow('hydrate failed')

    let secondResolved = false
    const trackedSecondSubscribe = secondSubscribed.then(() => {
      secondResolved = true
    })
    await Promise.resolve()
    expect(secondResolved).toBe(false)

    sockets[0]!.message(snapshotFrame('00939.HK'))
    await trackedSecondSubscribe
    expect(secondResolved).toBe(true)
  })

  it('rejects a pending subscribe when the symbol is unsubscribed before snapshot ack', async () => {
    const sockets = installFakeWebSocket()
    const source = new WebSocketDataSource('ws://localhost:9020/ws', 'terminal-message-v1', 50)
    const connected = source.connect()
    sockets[0]!.open()
    await connected

    const subscribed = source.subscribe('700')
    source.unsubscribe('700')

    await expect(subscribed).rejects.toThrow('Subscribe cancelled: 00700.HK')
    expect(JSON.parse(sockets[0]!.sent[0]!)).toMatchObject({
      action: 'subscribe',
      symbol: '00700.HK',
    })
    expect(JSON.parse(sockets[0]!.sent[1]!)).toMatchObject({
      action: 'unsubscribe',
      symbol: '00700.HK',
    })
  })

  it('reconnects and restores desired subscriptions after an unexpected close', async () => {
    vi.useFakeTimers()
    const sockets = installFakeWebSocket()
    const source = new WebSocketDataSource('ws://localhost:9020/ws', 'terminal-message-v1', 50, 'desk-a', 10, 100)
    const statuses: Array<[string, string | null]> = []
    source.onConnectionStatus((status, error) => statuses.push([status, error]))
    const connected = source.connect()
    sockets[0]!.open()
    await connected
    const subscribed = source.subscribe('700')
    sockets[0]!.message(snapshotFrame('00700.HK'))
    await subscribed

    sockets[0]!.close()
    await vi.advanceTimersByTimeAsync(10)
    expect(sockets).toHaveLength(2)
    sockets[1]!.open()
    await Promise.resolve()

    expect(statuses).toEqual([
      ['connecting', null],
      ['connected', null],
      ['error', 'WebSocket connection closed'],
      ['connecting', null],
      ['connected', null],
    ])
    expect(JSON.parse(sockets[1]!.sent[0]!)).toMatchObject({
      action: 'subscribe',
      symbol: '00700.HK',
      client_id: 'desk-a',
    })
  })

  it('allows subscribe before the websocket has opened', async () => {
    const sockets = installFakeWebSocket()
    const source = new WebSocketDataSource('ws://localhost:9020/ws', 'terminal-message-v1', 50, 'desk-a')

    const subscribed = source.subscribe('700')
    expect(sockets).toHaveLength(1)
    expect(sockets[0]!.sent).toEqual([])

    sockets[0]!.open()
    await Promise.resolve()
    expect(JSON.parse(sockets[0]!.sent[0]!)).toMatchObject({
      action: 'subscribe',
      symbol: '00700.HK',
      client_id: 'desk-a',
    })

    sockets[0]!.message(snapshotFrame('00700.HK'))
    await subscribed
  })

  it('does not reconnect after an explicit disconnect', async () => {
    vi.useFakeTimers()
    const sockets = installFakeWebSocket()
    const source = new WebSocketDataSource('ws://localhost:9020/ws', 'terminal-message-v1', 50, 'desk-a', 10, 100)
    const connected = source.connect()
    sockets[0]!.open()
    await connected
    const subscribed = source.subscribe('700')
    sockets[0]!.message(snapshotFrame('00700.HK'))
    await subscribed

    source.disconnect()
    await vi.advanceTimersByTimeAsync(100)

    expect(sockets).toHaveLength(1)
  })
})

function installFakeWebSocket() {
  const sockets: FakeWebSocket[] = []
  const WebSocketConstructor = function (url: string) {
    const socket = new FakeWebSocket(url)
    sockets.push(socket)
    return socket
  }
  ;(WebSocketConstructor as unknown as { OPEN: number }).OPEN = 1
  vi.stubGlobal('WebSocket', WebSocketConstructor)
  return sockets
}

type Listener = (event: { data?: string }) => void

class FakeWebSocket {
  static OPEN = 1
  readonly sent: string[] = []
  readyState = 0
  private readonly listeners: Record<string, Listener[]> = {}

  constructor(readonly url: string) {}

  addEventListener(type: string, listener: Listener) {
    this.listeners[type] ??= []
    this.listeners[type]!.push(listener)
  }

  send(message: string) {
    this.sent.push(message)
  }

  close() {
    this.readyState = 3
    this.emit('close')
  }

  open() {
    this.readyState = 1
    this.emit('open')
  }

  message(data: string) {
    this.emit('message', { data })
  }

  private emit(type: string, event: { data?: string } = {}) {
    for (const listener of this.listeners[type] ?? []) {
      listener(event)
    }
  }
}

function snapshotFrame(symbol: string) {
  return JSON.stringify({
    schema_version: 1,
    type: 'snapshot',
    event_id: `snapshot-${symbol}`,
    symbol,
    source: 'gateway',
    source_ts: '2026-05-22T09:30:00+08:00',
    ingest_ts: '2026-05-22T09:30:00.010+08:00',
    seq: 1,
    payload: {
      snapshot: {
        symbol,
        name: symbol,
        currency: 'HKD',
        price: 388.4,
        previousClose: 386.2,
        open: 386.8,
        high: 389,
        low: 385.4,
        volume: 1000,
        turnover: 388400,
        change: 2.2,
        changePercent: 0.56,
        updatedAt: '2026-05-22T09:30:00+08:00',
      },
      minute_bars: [],
      alerts: [],
      broker_queue: { ask: [], bid: [] },
      ccass_holdings: [],
      freshness: {
        updated_at: '2026-05-22T09:30:00+08:00',
        runtime_state: 'WARM',
        degraded_reasons: [],
        source_dates: {},
      },
    },
  })
}
