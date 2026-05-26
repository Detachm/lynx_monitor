import { describe, expect, it } from 'vitest'

import { isTerminalMessageV1, normalizeMarketMessage, normalizeTerminalMessage } from '@/services/normalizers'
import type { QueueRealtimeMessage, SnapshotMessage } from '@/types/market'

describe('normalizeMarketMessage', () => {
  it('projects TerminalMessage v1 snapshot payloads into frontend state messages', () => {
    const message = normalizeMarketMessage({
      schema_version: 1,
      type: 'snapshot',
      event_id: 'snapshot-00700.HK-1',
      symbol: '700',
      source: 'gateway',
      source_ts: '2026-05-22T09:30:00.000+08:00',
      ingest_ts: '2026-05-22T09:30:00.020+08:00',
      seq: 1,
      payload: {
        snapshot: {
          price: 388.4,
          previousClose: 386.2,
          tradeDate: '20260522',
          requestedTradeDate: '20260525',
          isHistoricalSession: true,
          updatedAt: '2026-05-22T09:30:00.020+08:00',
        },
        minute_bars: [{ timestamp: '2026-05-22T09:30:00.000+08:00', price: 388.4 }],
        alerts: [],
        broker_queue: {
          ask: [{ position: 1, broker_code: 'JPM', participant_name: 'JPMorgan', price: 388.6 }],
          bid: [{ position: 1, broker_code: 'CITI', participant_name: 'Citibank', price: 388.2 }],
        },
        ccass_holdings: [{ participant_name: 'JPMorgan', participant_code: 'C00010', shares: 1000 }],
        freshness: {
          updated_at: '2026-05-22T09:30:00.020+08:00',
          requested_trade_date: '20260525',
          effective_trade_date: '20260522',
          runtime_state: 'WARM',
          source_dates: {
            minute_bars: '20260522',
            ccass_current: '20260522',
          },
          degraded_reasons: ['missing_realtime'],
        },
      },
    })

    expect(message?.type).toBe('snapshot')
    const snapshot = message as SnapshotMessage
    expect(snapshot.symbol).toBe('00700.HK')
    expect(snapshot.ticks).toHaveLength(1)
    expect(snapshot.askQueues[0]?.brokerCode).toBe('JPM')
    expect(snapshot.askQueues[0]?.participantName).toBe('JPMorgan')
    expect(snapshot.bidQueues[0]?.brokerCode).toBe('CITI')
    expect(snapshot.bidQueues[0]?.participantName).toBe('Citibank')
    expect(snapshot.holding[0]?.participantName).toBe('JPMorgan')
    expect(snapshot.snapshot.tradeDate).toBe('20260522')
    expect(snapshot.snapshot.requestedTradeDate).toBe('20260525')
    expect(snapshot.snapshot.isHistoricalSession).toBe(true)
    expect(snapshot.freshness).toMatchObject({
      updatedAt: '2026-05-22T09:30:00.020+08:00',
      requestedTradeDate: '20260525',
      effectiveTradeDate: '20260522',
      runtimeState: 'WARM',
      degraded: true,
      degradedReasons: ['missing_realtime'],
      sourceDates: {
        minute_bars: '20260522',
        ccass_current: '20260522',
      },
    })
  })

  it('projects TerminalMessage v1 queue payloads by side', () => {
    const message = normalizeMarketMessage({
      schema_version: 1,
      type: 'queue_realtime',
      event_id: 'queue-00700.HK-1',
      symbol: '00700.HK',
      source: 'gateway',
      source_ts: '2026-05-22T09:30:00.000+08:00',
      ingest_ts: '2026-05-22T09:30:00.020+08:00',
      seq: 2,
      payload: {
        side: 'bid',
        broker_queue: {
          bid: [{ position: 1, broker_code: 'UBS', participant_name: 'UBS', price: 388.2 }],
        },
      },
    })

    expect(message?.type).toBe('queue_realtime')
    const queue = message as QueueRealtimeMessage
    expect(queue.side).toBe('bid')
    expect(queue.bidQueues?.[0]?.brokerCode).toBe('UBS')
    expect(queue.bidQueues?.[0]?.participantName).toBe('UBS')
    expect(queue.askQueues).toBeUndefined()
  })

  it('normalizes queue participant placeholders to undisclosed display', () => {
    const message = normalizeMarketMessage({
      schema_version: 1,
      type: 'queue_realtime',
      event_id: 'queue-00700.HK-2',
      symbol: '00700.HK',
      source: 'gateway',
      source_ts: '2026-05-22T09:30:00.000+08:00',
      ingest_ts: '2026-05-22T09:30:00.020+08:00',
      seq: 3,
      payload: {
        broker_queue: {
          ask: [
            { position: 1, broker_code: '0', participant_name: '--', price: 388.6 },
            { position: 2, broker_code: '-1', participant_name: 'Broker -1', price: 388.7 },
          ],
          bid: [{ position: 1, broker_code: 'UBS', participant_name: 'UBS Securities', price: 388.2 }],
        },
      },
    })

    const queue = message as QueueRealtimeMessage

    expect(queue.askQueues?.map((entry) => entry.participantName)).toEqual(['未披露', '未披露'])
    expect(queue.bidQueues?.[0]?.participantName).toBe('UBS Securities')
  })

  it('falls back to disclosed taxonomy for alerts without participant fields', () => {
    const message = normalizeMarketMessage({
      schema_version: 1,
      type: 'alert_realtime',
      event_id: 'alert-00700.HK-1',
      symbol: '00700.HK',
      source: 'gateway',
      source_ts: '2026-05-22T09:30:00.000+08:00',
      ingest_ts: '2026-05-22T09:30:00.020+08:00',
      seq: 3,
      payload: {
        alert: {
          id: 'alert-1',
          timestamp: '2026-05-22T09:30:00.000+08:00',
          price: 388.4,
          volume: 1000,
        },
      },
    })

    expect(message?.type).toBe('alert_realtime')
    expect(message).toMatchObject({
      alert: {
        participantName: '未披露',
        brokerName: '未披露',
      },
    })
  })

})

describe('normalizeTerminalMessage', () => {
  const terminalMessage = {
    schema_version: 1,
    type: 'tick_realtime',
    event_id: 'tick-00700.HK-1',
    symbol: '00700.HK',
    source: 'gateway',
    source_ts: '2026-05-22T09:30:00.000+08:00',
    ingest_ts: '2026-05-22T09:30:00.020+08:00',
    seq: 1,
    payload: {
      tick: {
        timestamp: '2026-05-22T09:30:00.000+08:00',
        price: 388.4,
        volume: 1000,
        turnover: 388400,
      },
    },
  }

  const holdingHistoryMessage = {
    schema_version: 1,
    type: 'holding_name_click_response',
    event_id: 'holding-history-00700.HK-1',
    symbol: '00700.HK',
    source: 'gateway',
    source_ts: '2026-05-22T09:30:00.000+08:00',
    ingest_ts: '2026-05-22T09:30:00.020+08:00',
    seq: 2,
    payload: {
      participant_name: 'JPMorgan',
      days: 7,
      history: [],
    },
  }

  const snapshotMessage = {
    schema_version: 1,
    type: 'snapshot',
    event_id: 'snapshot-00700.HK-1',
    symbol: '00700.HK',
    source: 'gateway',
    source_ts: '2026-05-22T09:30:00.000+08:00',
    ingest_ts: '2026-05-22T09:30:00.020+08:00',
    seq: 3,
    payload: {
      snapshot: { price: 388.4 },
      minute_bars: [],
      alerts: [],
      broker_queue: { ask: [], bid: [] },
      ccass_holdings: [],
      freshness: { updated_at: '2026-05-22T09:30:00.020+08:00' },
    },
  }

  it('accepts only strict TerminalMessage v1 envelopes at data-source boundaries', () => {
    expect(isTerminalMessageV1(terminalMessage)).toBe(true)
    expect(normalizeTerminalMessage(terminalMessage)).toMatchObject({
      type: 'tick_realtime',
      symbol: '00700.HK',
    })
    expect(
      normalizeTerminalMessage({
        event: 'tick',
        symbol: '00700.HK',
        price: 388.4,
      }),
    ).toBeNull()
    expect(normalizeTerminalMessage({ ...terminalMessage, payload: null })).toBeNull()
  })

  it('rejects weak TerminalMessage v1 envelopes at data-source boundaries', () => {
    const invalidMessages = [
      { ...terminalMessage, event_id: '' },
      { ...terminalMessage, symbol: '700' },
      { ...terminalMessage, symbol: '00700.hk' },
      { ...terminalMessage, source: '' },
      { ...terminalMessage, source_ts: '2026-05-22 09:30:00' },
      { ...terminalMessage, source_ts: 'not-a-date' },
      { ...terminalMessage, ingest_ts: '2026-05-22 09:30:00' },
      { ...terminalMessage, seq: 0 },
      { ...terminalMessage, seq: -1 },
      { ...terminalMessage, seq: 1.5 },
      { ...terminalMessage, seq: '1' },
    ]

    for (const invalidMessage of invalidMessages) {
      expect(isTerminalMessageV1(invalidMessage)).toBe(false)
      expect(normalizeTerminalMessage(invalidMessage)).toBeNull()
    }
  })

  it('rejects weak TerminalMessage v1 holding history payloads at data-source boundaries', () => {
    expect(isTerminalMessageV1(holdingHistoryMessage)).toBe(true)
    expect(normalizeTerminalMessage(holdingHistoryMessage)).toMatchObject({
      type: 'holding_name_click_response',
      symbol: '00700.HK',
      participantName: 'JPMorgan',
      days: 7,
    })

    const invalidMessages = [
      {
        ...holdingHistoryMessage,
        payload: { ...holdingHistoryMessage.payload, participant_name: '' },
      },
      {
        ...holdingHistoryMessage,
        payload: { ...holdingHistoryMessage.payload, days: 0 },
      },
      {
        ...holdingHistoryMessage,
        payload: { ...holdingHistoryMessage.payload, days: true },
      },
      {
        ...holdingHistoryMessage,
        payload: { ...holdingHistoryMessage.payload, history: { date: '2026-05-22' } },
      },
    ]

    for (const invalidMessage of invalidMessages) {
      expect(isTerminalMessageV1(invalidMessage)).toBe(false)
      expect(normalizeTerminalMessage(invalidMessage)).toBeNull()
    }
  })

  it('rejects weak TerminalMessage v1 realtime payload shapes at data-source boundaries', () => {
    const alertMessage = {
      ...terminalMessage,
      type: 'alert_realtime',
      event_id: 'alert-00700.HK-1',
      payload: { alert: { id: 'alert-1' } },
    }
    const queueMessage = {
      ...terminalMessage,
      type: 'queue_realtime',
      event_id: 'queue-00700.HK-1',
      payload: { broker_queue: { ask: [], bid: [] } },
    }

    for (const validMessage of [terminalMessage, alertMessage, queueMessage, snapshotMessage]) {
      expect(isTerminalMessageV1(validMessage)).toBe(true)
    }

    const invalidMessages = [
      { ...terminalMessage, payload: { tick: 'bad' } },
      { ...alertMessage, payload: { alert: 'bad' } },
      { ...queueMessage, payload: { broker_queue: [] } },
      { ...snapshotMessage, payload: { ...snapshotMessage.payload, minute_bars: {} } },
      { ...snapshotMessage, payload: { ...snapshotMessage.payload, broker_queue: { ask: [], bid: {} } } },
      { ...snapshotMessage, payload: { ...snapshotMessage.payload, freshness: null } },
    ]

    for (const invalidMessage of invalidMessages) {
      expect(isTerminalMessageV1(invalidMessage)).toBe(false)
      expect(normalizeTerminalMessage(invalidMessage)).toBeNull()
    }
  })
})
