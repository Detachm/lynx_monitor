import type { DataSourceMode, MarketPerformanceSample, StockSymbol, SymbolSubscriptionStatus } from '@/types/market'

export type ClientSmokeSymbolStatus = Exclude<SymbolSubscriptionStatus, 'idle'>

export interface ClientSymbolSmokeStatus {
  status: ClientSmokeSymbolStatus
  snapshot_loaded: boolean
  requested_trade_date?: string
  effective_trade_date?: string
  source_dates: Record<string, string>
  degraded_reasons: string[]
}

export interface ClientSmokeObservation {
  machine_id: string
  data_source_mode: DataSourceMode
  page_url: string
  gateway_url: string
  connected: boolean
  watchlist: StockSymbol[]
  refresh_recovered: boolean
  symbol_statuses: Record<StockSymbol, ClientSymbolSmokeStatus>
}

export function clientSmokeArtifact(
  client: ClientSmokeObservation,
  performanceSamples: MarketPerformanceSample[] = [],
  exportedAt = new Date().toISOString(),
) {
  const validSamples = validPerformanceSamples(performanceSamples)
  return {
    schema_version: 1,
    exported_at: exportedAt,
    clients: [client],
    performance_samples: performanceSampleSummary(validSamples),
    raw_samples: validSamples,
  }
}

export function clientSmokeArtifactDownload(
  client: ClientSmokeObservation,
  performanceSamples: MarketPerformanceSample[] = [],
  exportedAt = new Date(),
) {
  return {
    payload: clientSmokeArtifact(client, performanceSamples, exportedAt.toISOString()),
    filename: smokeArtifactFilename('client', client.machine_id, exportedAt),
  }
}

export function performanceSmokeArtifact(
  samples: MarketPerformanceSample[],
  machineId: string,
  exportedAt = new Date().toISOString(),
) {
  const payload: {
    schema_version: number
    exported_at: string
    machine_id: string
    performance_samples: ReturnType<typeof performanceSampleSummary>
    raw_samples: MarketPerformanceSample[]
  } = {
    schema_version: 1,
    exported_at: exportedAt,
    machine_id: machineId.trim(),
    performance_samples: performanceSampleSummary(samples),
    raw_samples: samples,
  }
  const validSamples = validPerformanceSamples(samples)
  payload.performance_samples = performanceSampleSummary(validSamples)
  payload.raw_samples = validSamples
  return payload
}

export function performanceSmokeArtifactDownload(
  samples: MarketPerformanceSample[],
  machineId: string,
  exportedAt = new Date(),
) {
  return {
    payload: performanceSmokeArtifact(samples, machineId, exportedAt.toISOString()),
    filename: smokeArtifactFilename('performance', machineId, exportedAt),
  }
}

function performanceSampleSummary(samples: MarketPerformanceSample[]) {
  return {
    subscribe_snapshot_ms: samples
      .filter((sample) => sample.key === 'subscribe_snapshot_ms')
      .map((sample) => sample.valueMs),
  }
}

function validPerformanceSamples(samples: MarketPerformanceSample[]) {
  return samples.filter((sample) => Number.isFinite(sample.valueMs) && sample.valueMs >= 0)
}

export function smokeArtifactFilename(kind: 'client' | 'performance', machineId: string, exportedAt = new Date()) {
  const timestamp = exportedAt.toISOString().replace(/[:.]/g, '').replace('T', '-').replace('Z', '')
  return `multi-trader-smoke-${kind}-${safeFilenameSegment(machineId, 'unknown')}-${timestamp}.json`
}

function safeFilenameSegment(value: string, fallback: string) {
  const sanitized = value.trim().replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '')
  return sanitized || fallback
}
