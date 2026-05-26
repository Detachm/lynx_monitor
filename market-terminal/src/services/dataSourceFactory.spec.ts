import { describe, expect, it } from 'vitest'

import { createMarketDataSource } from '@/services/dataSourceFactory'
import { WebSocketDataSource } from '@/services/webSocketDataSource'

describe('data source factory', () => {
  it('rejects live data source creation when runtime config has validation errors', () => {
    expect(() =>
      createMarketDataSource('live', {
        liveUrl: 'https://gateway.internal/ws',
        protocol: 'terminal-message-v1',
        validationErrors: ['live_url_invalid'],
      }),
    ).toThrow('Live data source configuration is invalid: live_url_invalid')
  })

  it('creates a WebSocket data source for valid live runtime config', () => {
    const source = createMarketDataSource('live', {
      liveUrl: 'wss://gateway.internal/ws',
      protocol: 'terminal-message-v1',
      validationErrors: [],
    })

    expect(source).toBeInstanceOf(WebSocketDataSource)
  })
})
