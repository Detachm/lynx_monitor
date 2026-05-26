import { describe, expect, it } from 'vitest'

import { isNormalizedStockSymbol, normalizeStockSymbol } from '@/utils/symbol'

describe('normalizeStockSymbol', () => {
  it('normalizes short Hong Kong numeric symbols', () => {
    expect(normalizeStockSymbol('00700')).toBe('00700.HK')
    expect(normalizeStockSymbol('700')).toBe('00700.HK')
    expect(normalizeStockSymbol('939.hk')).toBe('00939.HK')
  })

  it('keeps non-HK symbols uppercase', () => {
    expect(normalizeStockSymbol('aapl')).toBe('AAPL')
  })

  it('validates normalized Hong Kong symbols', () => {
    expect(isNormalizedStockSymbol('00700.HK')).toBe(true)
    expect(isNormalizedStockSymbol('700')).toBe(false)
  })
})
