import type { DataSourceMode } from '@/types/market'
import type { DataSourceFactoryOptions } from '@/services/dataSourceFactory'

export type ConfiguredDataSourceMode = DataSourceMode | 'auto'

export interface CutoverReadiness {
  schema_version?: number
  passed?: boolean
  frontend_default_v2_allowed?: boolean
  legacy_retirement_allowed?: boolean
  blockers?: string[]
  legacy_retirement_blockers?: string[]
  report_count?: number
  accepted_report_ids?: string[]
  rejected_reports?: unknown[]
  policy?: Partial<CutoverPolicy>
}

interface CutoverPolicy {
  min_parallel_session_count: number
  min_session_duration_seconds: number
  min_stream_coverage_ratio: number
  require_non_empty_streams: boolean
  require_no_failed_symbols: boolean
  allow_legacy_retirement: boolean
}

export interface DataSourceRuntimeConfig {
  defaultMode: DataSourceMode
  liveUrl: string
  protocol: string
  defaultSymbols: string[]
  configuredMode: ConfiguredDataSourceMode
  cutoverReadiness: CutoverReadiness | null
  validationErrors: string[]
}

type RuntimeEnv = Record<string, string | boolean | undefined>

const DEFAULT_LIVE_URL = 'ws://127.0.0.1:9020/ws'
const DEFAULT_GATEWAY_PORT = '9020'
const DEFAULT_GATEWAY_PATH = '/ws'
const DEFAULT_SYMBOLS = ['00700.HK', '00939.HK', '02643.HK', '00108.HK']
export const TERMINAL_MESSAGE_PROTOCOL = 'terminal-message-v1'
const DEFAULT_CUTOVER_POLICY: CutoverPolicy = {
  min_parallel_session_count: 1,
  min_session_duration_seconds: 14400,
  min_stream_coverage_ratio: 0.9,
  require_non_empty_streams: true,
  require_no_failed_symbols: true,
  allow_legacy_retirement: true,
}

export function resolveDataSourceRuntimeConfig(env: RuntimeEnv = import.meta.env): DataSourceRuntimeConfig {
  const configuredMode = parseConfiguredMode(env.VITE_MARKET_DATA_MODE)
  const cutoverReadiness = parseCutoverReadiness(env.VITE_MARKET_CUTOVER_READINESS)
  const liveUrl = parseLiveUrl(env.VITE_MARKET_WS_URL)
  const protocol = parseProtocol(env.VITE_MARKET_PROTOCOL)
  const defaultSymbols = parseDefaultSymbols(env.VITE_MARKET_SYMBOLS)
  const validationErrors = runtimeConfigValidationErrors({
    configuredMode,
    liveUrl,
    protocol,
    rawLiveUrl: env.VITE_MARKET_WS_URL,
    rawProtocol: env.VITE_MARKET_PROTOCOL,
  })

  return {
    defaultMode: selectDefaultDataSourceMode(configuredMode, cutoverReadiness, validationErrors),
    liveUrl,
    protocol,
    defaultSymbols,
    configuredMode,
    cutoverReadiness,
    validationErrors,
  }
}

export function dataSourceOptionsFromRuntimeConfig(
  config: Pick<DataSourceRuntimeConfig, 'liveUrl' | 'protocol' | 'validationErrors' | 'defaultSymbols'>,
): DataSourceFactoryOptions {
  return {
    liveUrl: config.liveUrl,
    protocol: config.protocol,
    validationErrors: config.validationErrors,
    defaultSymbols: config.defaultSymbols,
  }
}

export function selectDefaultDataSourceMode(
  configuredMode: ConfiguredDataSourceMode,
  cutoverReadiness: CutoverReadiness | null,
  validationErrors: string[] = [],
): DataSourceMode {
  if (configuredMode === 'auto') {
    return 'live'
  }

  return configuredMode === 'mock' ? 'live' : configuredMode
}

function isFrontendV2ReadinessAllowed(cutoverReadiness: CutoverReadiness | null): boolean {
  return (
    cutoverReadiness?.schema_version === 1 &&
    cutoverReadiness.passed === true &&
    cutoverReadiness.frontend_default_v2_allowed === true &&
    Array.isArray(cutoverReadiness.blockers) &&
    cutoverReadiness.blockers.length === 0 &&
    isPositiveInteger(cutoverReadiness.report_count) &&
    acceptedReportIdsValid(cutoverReadiness.accepted_report_ids) &&
    Array.isArray(cutoverReadiness.rejected_reports) &&
    policyMatchesFrontendCutoverDefaults(cutoverReadiness.policy)
  )
}

function acceptedReportIdsValid(acceptedReportIds: unknown): acceptedReportIds is string[] {
  if (
    !Array.isArray(acceptedReportIds) ||
    acceptedReportIds.length === 0 ||
    acceptedReportIds.some((reportId) => typeof reportId !== 'string' || reportId.trim().length === 0)
  ) {
    return false
  }
  return new Set(acceptedReportIds).size === acceptedReportIds.length
}

function policyMatchesFrontendCutoverDefaults(policy: unknown): boolean {
  if (!policy || typeof policy !== 'object' || Array.isArray(policy)) {
    return false
  }
  const candidate = policy as Partial<CutoverPolicy>
  return (
    candidate.min_parallel_session_count === DEFAULT_CUTOVER_POLICY.min_parallel_session_count &&
    candidate.min_session_duration_seconds === DEFAULT_CUTOVER_POLICY.min_session_duration_seconds &&
    candidate.min_stream_coverage_ratio === DEFAULT_CUTOVER_POLICY.min_stream_coverage_ratio &&
    candidate.require_non_empty_streams === DEFAULT_CUTOVER_POLICY.require_non_empty_streams &&
    candidate.require_no_failed_symbols === DEFAULT_CUTOVER_POLICY.require_no_failed_symbols
  )
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value > 0
}

function parseConfiguredMode(input: unknown): ConfiguredDataSourceMode {
  return input === 'mock' || input === 'live' || input === 'auto' ? input : 'live'
}

function parseLiveUrl(input: unknown): string {
  return typeof input === 'string' && input.trim() ? input.trim() : defaultLiveUrl()
}

function defaultLiveUrl(): string {
  if (typeof globalThis.location === 'object' && globalThis.location?.hostname) {
    const protocol = globalThis.location.protocol === 'https:' ? 'wss:' : 'ws:'
    return `${protocol}//${hostForWebSocket(globalThis.location.hostname)}:${DEFAULT_GATEWAY_PORT}${DEFAULT_GATEWAY_PATH}`
  }
  return DEFAULT_LIVE_URL
}

function hostForWebSocket(hostname: string): string {
  return hostname.includes(':') && !hostname.startsWith('[') ? `[${hostname}]` : hostname
}

function parseProtocol(input: unknown): string {
  return input === TERMINAL_MESSAGE_PROTOCOL ? input : TERMINAL_MESSAGE_PROTOCOL
}

function parseDefaultSymbols(input: unknown): string[] {
  if (typeof input !== 'string' || !input.trim()) {
    return [...DEFAULT_SYMBOLS]
  }

  const symbols = input
    .split(',')
    .map((symbol) => normalizeConfiguredSymbol(symbol))
    .filter((symbol): symbol is string => symbol !== null)

  return symbols.length > 0 ? Array.from(new Set(symbols)) : [...DEFAULT_SYMBOLS]
}

function normalizeConfiguredSymbol(input: string): string | null {
  const value = input.trim().toUpperCase()
  if (!value) {
    return null
  }
  const symbol = value.includes('.') ? value : `${value.padStart(5, '0')}.HK`
  return /^\d{5}\.HK$/.test(symbol) ? symbol : null
}

function runtimeConfigValidationErrors(input: {
  configuredMode: ConfiguredDataSourceMode
  liveUrl: string
  protocol: string
  rawLiveUrl: unknown
  rawProtocol: unknown
}): string[] {
  const errors: string[] = []
  if (!isGatewayWebSocketUrl(input.liveUrl)) {
    errors.push('live_url_invalid')
  }
  if (input.rawProtocol !== undefined && input.rawProtocol !== TERMINAL_MESSAGE_PROTOCOL) {
    errors.push('protocol_invalid')
  }
  if (input.protocol !== TERMINAL_MESSAGE_PROTOCOL) {
    errors.push('protocol_invalid')
  }
  return Array.from(new Set(errors))
}

function isGatewayWebSocketUrl(value: string): boolean {
  try {
    const parsed = new URL(value)
    return (
      (parsed.protocol === 'ws:' || parsed.protocol === 'wss:') &&
      parsed.pathname === '/ws' &&
      hasExplicitOrDefaultPort(value, parsed.protocol)
    )
  } catch {
    return false
  }
}

function hasExplicitOrDefaultPort(value: string, protocol: string): boolean {
  const authority = value.match(/^[a-z][a-z\d+.-]*:\/\/([^/?#]*)/i)?.[1]
  if (!authority) {
    return false
  }
  if (authority.startsWith('[')) {
    return /\]:\d+$/.test(authority) || protocol === 'wss:' || protocol === 'ws:'
  }
  return /:\d+$/.test(authority) || protocol === 'wss:' || protocol === 'ws:'
}

function parseCutoverReadiness(input: unknown): CutoverReadiness | null {
  if (typeof input !== 'string' || !input.trim()) {
    return null
  }

  try {
    const parsed = JSON.parse(input)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? (parsed as CutoverReadiness)
      : null
  } catch {
    return null
  }
}

export const DEFAULT_DATA_SOURCE_CONFIG = resolveDataSourceRuntimeConfig()
