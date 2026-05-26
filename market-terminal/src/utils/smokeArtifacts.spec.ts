import { describe, expect, it } from 'vitest'

import {
  clientSmokeArtifact,
  clientSmokeArtifactDownload,
  type ClientSmokeObservation,
  performanceSmokeArtifact,
  performanceSmokeArtifactDownload,
  smokeArtifactFilename,
} from '@/utils/smokeArtifacts'

describe('smokeArtifacts', () => {
  it('builds client and performance artifacts for backend smoke observation assembly', () => {
    const client: ClientSmokeObservation = {
      machine_id: 'desk-a',
      data_source_mode: 'live',
      page_url: 'http://192.168.1.10:5173/',
      gateway_url: 'ws://192.168.1.10:9020/ws',
      connected: true,
      watchlist: ['00700.HK', '00939.HK'],
      refresh_recovered: true,
      symbol_statuses: {
        '00700.HK': {
          status: 'live',
          snapshot_loaded: true,
          requested_trade_date: '20260522',
          effective_trade_date: '20260522',
          source_dates: { minute_bars: '20260522' },
          degraded_reasons: [],
        },
        '00939.HK': {
          status: 'closed',
          snapshot_loaded: true,
          requested_trade_date: '20260525',
          effective_trade_date: '20260522',
          source_dates: { minute_bars: '20260522' },
          degraded_reasons: [],
        },
      },
    }
    const samples = [
      {
        key: 'subscribe_snapshot_ms',
        valueMs: 120,
        symbol: '00700.HK',
        recordedAt: '2026-05-25T11:00:00+08:00',
      },
      {
        key: 'frontend_store_update_ms',
        valueMs: 2,
        symbol: '00700.HK',
        messageType: 'tick_realtime' as const,
        recordedAt: '2026-05-25T11:00:01+08:00',
      },
    ]

    expect(clientSmokeArtifact(client, samples, '2026-05-25T11:00:02+08:00')).toEqual({
      schema_version: 1,
      exported_at: '2026-05-25T11:00:02+08:00',
      clients: [client],
      performance_samples: { subscribe_snapshot_ms: [120] },
      raw_samples: samples,
    })
    expect(performanceSmokeArtifact(samples, 'desk-a', '2026-05-25T11:00:02+08:00')).toMatchObject({
      schema_version: 1,
      machine_id: 'desk-a',
      performance_samples: { subscribe_snapshot_ms: [120] },
      raw_samples: samples,
    })
    expect(performanceSmokeArtifact(samples, 'desk-a', '2026-05-25T11:00:02+08:00')).toMatchObject({
      schema_version: 1,
      machine_id: 'desk-a',
      performance_samples: { subscribe_snapshot_ms: [120] },
    })
  })

  it('uses stable filesystem-safe smoke artifact filenames', () => {
    expect(smokeArtifactFilename('client', 'desk-a', new Date('2026-05-25T11:00:02.123Z'))).toBe(
      'multi-trader-smoke-client-desk-a-2026-05-25-110002123.json',
    )
    expect(smokeArtifactFilename('performance', 'desk/a b', new Date('2026-05-25T11:00:02.123Z'))).toBe(
      'multi-trader-smoke-performance-desk-a-b-2026-05-25-110002123.json',
    )
  })

  it('uses one export timestamp for smoke payloads and filenames', () => {
    const exportedAt = new Date('2026-05-25T11:00:02.123Z')
    const client: ClientSmokeObservation = {
      machine_id: 'desk/a b',
      data_source_mode: 'live',
      page_url: 'http://192.168.1.10:5173/',
      gateway_url: 'ws://192.168.1.10:9020/ws',
      connected: true,
      watchlist: ['00700.HK'],
      refresh_recovered: true,
      symbol_statuses: {
        '00700.HK': {
          status: 'live',
          snapshot_loaded: true,
          requested_trade_date: '20260522',
          effective_trade_date: '20260522',
          source_dates: { minute_bars: '20260522' },
          degraded_reasons: [],
        },
      },
    }

    const clientExport = clientSmokeArtifactDownload(client, [], exportedAt)
    const performanceExport = performanceSmokeArtifactDownload([], client.machine_id, exportedAt)

    expect(clientExport.payload.exported_at).toBe('2026-05-25T11:00:02.123Z')
    expect(clientExport.filename).toBe('multi-trader-smoke-client-desk-a-b-2026-05-25-110002123.json')
    expect(performanceExport.payload.exported_at).toBe('2026-05-25T11:00:02.123Z')
    expect(performanceExport.filename).toBe('multi-trader-smoke-performance-desk-a-b-2026-05-25-110002123.json')
  })

  it('drops invalid performance samples before exporting smoke artifacts', () => {
    const client: ClientSmokeObservation = {
      machine_id: 'desk-a',
      data_source_mode: 'live',
      page_url: 'http://192.168.1.10:5173/',
      gateway_url: 'ws://192.168.1.10:9020/ws',
      connected: true,
      watchlist: ['00700.HK'],
      refresh_recovered: true,
      symbol_statuses: {
        '00700.HK': {
          status: 'live',
          snapshot_loaded: true,
          requested_trade_date: '20260522',
          effective_trade_date: '20260522',
          source_dates: { minute_bars: '20260522' },
          degraded_reasons: [],
        },
      },
    }
    const samples = [
      { key: 'subscribe_snapshot_ms', valueMs: 120, symbol: '00700.HK', recordedAt: '2026-05-25T11:00:00+08:00' },
      { key: 'subscribe_snapshot_ms', valueMs: Number.NaN, symbol: '00700.HK', recordedAt: '2026-05-25T11:00:01+08:00' },
      { key: 'frontend_store_update_ms', valueMs: -1, symbol: '00700.HK', recordedAt: '2026-05-25T11:00:02+08:00' },
      { key: 'frontend_store_update_ms', valueMs: 2, symbol: '00700.HK', recordedAt: '2026-05-25T11:00:03+08:00' },
    ]

    expect(clientSmokeArtifact(client, samples).performance_samples).toEqual({ subscribe_snapshot_ms: [120] })
    expect(clientSmokeArtifact(client, samples).raw_samples).toEqual([samples[0], samples[3]])
    expect(performanceSmokeArtifact(samples, 'desk-a', '2026-05-25T11:00:04+08:00').performance_samples).toEqual({
      subscribe_snapshot_ms: [120],
    })
    expect(performanceSmokeArtifact(samples, 'desk-a', '2026-05-25T11:00:04+08:00').raw_samples).toEqual([
      samples[0],
      samples[3],
    ])
  })
})
