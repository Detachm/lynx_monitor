import type { DataSourceMode, MarketDataSource, MarketPerformanceSampleHandler } from '@/types/market'
import { DEFAULT_DATA_SOURCE_CONFIG } from '@/config/dataSource'
import { MockDataSource, type MockDataSourceOptions } from '@/services/mockDataSource'
import { WebSocketDataSource } from '@/services/webSocketDataSource'

export const DEFAULT_SYMBOLS = ['00700.HK', '00939.HK', '02643.HK', '00108.HK'] as const

export interface DataSourceFactoryOptions {
  liveUrl?: string
  protocol?: string
  clientId?: string
  validationErrors?: string[]
  defaultSymbols?: readonly string[]
  performanceSampleHandler?: MarketPerformanceSampleHandler
  mock?: MockDataSourceOptions
}

export function createMarketDataSource(
  mode: DataSourceMode,
  options: DataSourceFactoryOptions = {},
): MarketDataSource {
  if (mode === 'live') {
    const validationErrors = options.validationErrors ?? DEFAULT_DATA_SOURCE_CONFIG.validationErrors
    if (validationErrors.length > 0) {
      throw new Error(`Live data source configuration is invalid: ${validationErrors.join(', ')}`)
    }

    return new WebSocketDataSource(
      options.liveUrl ?? DEFAULT_DATA_SOURCE_CONFIG.liveUrl,
      options.protocol ?? DEFAULT_DATA_SOURCE_CONFIG.protocol,
      undefined,
      options.clientId,
    )
  }

  return new MockDataSource(options.mock)
}
