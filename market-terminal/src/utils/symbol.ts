import type { StockSymbol } from '@/types/market'

export function normalizeStockSymbol(input: string): StockSymbol {
  const value = input.trim().toUpperCase().replace(/\s+/g, '')

  if (!value) {
    throw new Error('Stock symbol is required')
  }

  const hkMatch = value.match(/^(\d{1,5})(?:\.HK)?$/)
  if (hkMatch) {
    const code = hkMatch[1] ?? ''
    return `${code.padStart(5, '0')}.HK`
  }

  return value
}

export function isNormalizedStockSymbol(symbol: string): boolean {
  return /^\d{5}\.HK$/.test(symbol.trim().toUpperCase())
}
