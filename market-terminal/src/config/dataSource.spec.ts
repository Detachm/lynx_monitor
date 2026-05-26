import { describe, expect, it } from 'vitest'

import {
  dataSourceOptionsFromRuntimeConfig,
  resolveDataSourceRuntimeConfig,
  selectDefaultDataSourceMode,
} from '@/config/dataSource'

describe('data source runtime config', () => {
  const passingReadiness = {
    schema_version: 1,
    passed: true,
    frontend_default_v2_allowed: true,
    legacy_retirement_allowed: false,
    blockers: [],
    legacy_retirement_blockers: ['legacy_retirement_requires_operator_approval'],
    report_count: 1,
    accepted_report_ids: ['session-1'],
    rejected_reports: [],
    policy: {
      min_parallel_session_count: 1,
      min_session_duration_seconds: 14400,
      min_stream_coverage_ratio: 0.9,
      require_non_empty_streams: true,
      require_no_failed_symbols: true,
      allow_legacy_retirement: false,
    },
  }

  it('keeps mock as the default when no cutover gate is configured', () => {
    const config = resolveDataSourceRuntimeConfig({})

    expect(config.defaultMode).toBe('mock')
    expect(config.liveUrl).toBe('ws://127.0.0.1:9020/ws')
    expect(config.protocol).toBe('terminal-message-v1')
    expect(config.defaultSymbols).toEqual(['00700.HK', '00939.HK', '02643.HK', '00108.HK'])
  })

  it('derives default live websocket URL from the page hostname for LAN access', () => {
    const originalLocation = globalThis.location
    Object.defineProperty(globalThis, 'location', {
      configurable: true,
      value: { protocol: 'http:', hostname: '192.168.1.20' },
    })

    const config = resolveDataSourceRuntimeConfig({})

    expect(config.liveUrl).toBe('ws://192.168.1.20:9020/ws')
    Object.defineProperty(globalThis, 'location', {
      configurable: true,
      value: originalLocation,
    })
  })

  it('uses secure websocket defaults from HTTPS pages and brackets IPv6 hosts', () => {
    const originalLocation = globalThis.location
    Object.defineProperty(globalThis, 'location', {
      configurable: true,
      value: { protocol: 'https:', hostname: 'fd00::1' },
    })

    const config = resolveDataSourceRuntimeConfig({})

    expect(config.liveUrl).toBe('wss://[fd00::1]:9020/ws')
    Object.defineProperty(globalThis, 'location', {
      configurable: true,
      value: originalLocation,
    })
  })

  it('selects live in auto mode only when cutover readiness allows frontend v2', () => {
    expect(
      selectDefaultDataSourceMode('auto', {
        ...passingReadiness,
      }),
    ).toBe('live')

    expect(
      selectDefaultDataSourceMode('auto', {
        ...passingReadiness,
        frontend_default_v2_allowed: false,
      }),
    ).toBe('mock')

    expect(
      selectDefaultDataSourceMode('auto', {
        frontend_default_v2_allowed: true,
      }),
    ).toBe('mock')
  })

  it('parses live websocket URL and cutover readiness from Vite env values', () => {
    const config = resolveDataSourceRuntimeConfig({
      VITE_MARKET_DATA_MODE: 'auto',
      VITE_MARKET_WS_URL: 'ws://gateway.internal:9020/ws',
      VITE_MARKET_PROTOCOL: 'terminal-message-v1',
      VITE_MARKET_SYMBOLS: '68,02476.HK,1879,02476.HK',
      VITE_MARKET_CUTOVER_READINESS: JSON.stringify(passingReadiness),
    })

    expect(config.configuredMode).toBe('auto')
    expect(config.defaultMode).toBe('live')
    expect(config.liveUrl).toBe('ws://gateway.internal:9020/ws')
    expect(config.protocol).toBe('terminal-message-v1')
    expect(config.defaultSymbols).toEqual(['00068.HK', '02476.HK', '01879.HK'])
    expect(config.validationErrors).toEqual([])
  })

  it('builds store initialization options with an explicit Gateway port and protocol', () => {
    const config = resolveDataSourceRuntimeConfig({
      VITE_MARKET_WS_URL: 'wss://gateway.internal:443/ws',
      VITE_MARKET_PROTOCOL: 'terminal-message-v1',
    })

    expect(dataSourceOptionsFromRuntimeConfig(config)).toEqual({
      liveUrl: 'wss://gateway.internal:443/ws',
      protocol: 'terminal-message-v1',
      validationErrors: [],
      defaultSymbols: ['00700.HK', '00939.HK', '02643.HK', '00108.HK'],
    })
  })

  it('keeps mock when live config has a non-Gateway URL or invalid protocol', () => {
    const invalidUrl = resolveDataSourceRuntimeConfig({
      VITE_MARKET_DATA_MODE: 'live',
      VITE_MARKET_WS_URL: 'https://gateway.internal/ws',
      VITE_MARKET_PROTOCOL: 'terminal-message-v1',
    })

    expect(invalidUrl.defaultMode).toBe('mock')
    expect(invalidUrl.validationErrors).toEqual(['live_url_invalid'])

    const invalidProtocol = resolveDataSourceRuntimeConfig({
      VITE_MARKET_DATA_MODE: 'auto',
      VITE_MARKET_WS_URL: 'wss://gateway.internal:443/ws',
      VITE_MARKET_PROTOCOL: 'legacy-message',
      VITE_MARKET_CUTOVER_READINESS: JSON.stringify(passingReadiness),
    })

    expect(invalidProtocol.defaultMode).toBe('mock')
    expect(invalidProtocol.protocol).toBe('terminal-message-v1')
    expect(invalidProtocol.validationErrors).toEqual(['protocol_invalid'])

    const missingPort = resolveDataSourceRuntimeConfig({
      VITE_MARKET_DATA_MODE: 'live',
      VITE_MARKET_WS_URL: 'wss://gateway.internal/ws',
      VITE_MARKET_PROTOCOL: 'terminal-message-v1',
    })

    expect(missingPort.defaultMode).toBe('mock')
    expect(missingPort.validationErrors).toEqual(['live_url_invalid'])
  })

  it('records invalid live config even when default mode is mock', () => {
    const config = resolveDataSourceRuntimeConfig({
      VITE_MARKET_DATA_MODE: 'mock',
      VITE_MARKET_WS_URL: 'https://gateway.internal/ws',
      VITE_MARKET_PROTOCOL: 'legacy-message',
    })

    expect(config.defaultMode).toBe('mock')
    expect(config.validationErrors).toEqual(['live_url_invalid', 'protocol_invalid'])
  })

  it('keeps auto on mock when cutover readiness lacks audit fields', () => {
    expect(
      selectDefaultDataSourceMode('auto', {
        schema_version: 1,
        passed: true,
        frontend_default_v2_allowed: true,
        accepted_report_ids: ['session-1'],
      }),
    ).toBe('mock')

    expect(
      selectDefaultDataSourceMode('auto', {
        ...passingReadiness,
        accepted_report_ids: ['session-1', 'session-1'],
      }),
    ).toBe('mock')

    expect(
      selectDefaultDataSourceMode('auto', {
        ...passingReadiness,
        blockers: ['shadow_run_reports_fail_default_cutover_policy'],
      }),
    ).toBe('mock')

    expect(
      selectDefaultDataSourceMode('auto', {
        ...passingReadiness,
        policy: {
          ...passingReadiness.policy,
          min_session_duration_seconds: 60,
        },
      }),
    ).toBe('mock')
  })
})
