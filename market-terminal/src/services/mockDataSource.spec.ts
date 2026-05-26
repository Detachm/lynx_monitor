import { describe, expect, it } from 'vitest'

import { MockDataSource } from '@/services/mockDataSource'
import type { HoldingHistoryMessage, MarketMessage, SnapshotMessage } from '@/types/market'

describe('MockDataSource', () => {
  it('pushes independent snapshot and realtime messages for multiple symbols', async () => {
    const source = new MockDataSource({ autoRealtime: false })
    const messages: MarketMessage[] = []
    source.onMessage((message) => messages.push(message))

    await source.connect()
    source.subscribe('00700')
    source.subscribe('00939')
    source.emitNext('00700.HK')
    source.emitNext('00939.HK')

    const snapshots = messages.filter((message): message is SnapshotMessage => message.type === 'snapshot')
    const tickSymbols = messages
      .filter((message) => message.type === 'tick_realtime')
      .map((message) => message.symbol)

    expect(snapshots.map((snapshot) => snapshot.symbol)).toEqual(['00700.HK', '00939.HK'])
    expect(snapshots[0]!.snapshot.price).not.toBe(snapshots[1]!.snapshot.price)
    expect(tickSymbols).toEqual(expect.arrayContaining(['00700.HK', '00939.HK']))

    source.disconnect()
  })

  it('emits holding history for participant clicks', async () => {
    const source = new MockDataSource({ autoRealtime: false })
    const messages: MarketMessage[] = []
    source.onMessage((message) => messages.push(message))

    await source.connect()
    source.subscribe('00700')

    const snapshot = messages.find(
      (message): message is SnapshotMessage => message.type === 'snapshot' && message.symbol === '00700.HK',
    )

    expect(snapshot).toBeDefined()

    const participantName = snapshot?.holding[0]?.participantName ?? ''
    source.requestHoldingHistory('00700', participantName, 7)

    const historyMessage = messages.find(
      (message): message is HoldingHistoryMessage =>
        message.type === 'holding_name_click_response' && message.symbol === '00700.HK',
    )

    expect(historyMessage).toMatchObject({
      participantName,
      days: 7,
    })
    expect(historyMessage?.history).toHaveLength(7)

    source.disconnect()
  })

  it('normalizes invalid holding history days before emitting TerminalMessage v1', async () => {
    const source = new MockDataSource({ autoRealtime: false })
    const messages: MarketMessage[] = []
    source.onMessage((message) => messages.push(message))

    await source.connect()
    source.subscribe('00700')

    const snapshot = messages.find(
      (message): message is SnapshotMessage => message.type === 'snapshot' && message.symbol === '00700.HK',
    )
    const participantName = snapshot?.holding[0]?.participantName ?? ''

    source.requestHoldingHistory('00700', participantName, 0)

    const historyMessage = messages.find(
      (message): message is HoldingHistoryMessage =>
        message.type === 'holding_name_click_response' && message.symbol === '00700.HK',
    )

    expect(historyMessage?.days).toBe(30)
    expect(historyMessage?.history).toHaveLength(30)

    source.disconnect()
  })
})
