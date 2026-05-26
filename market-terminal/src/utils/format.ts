export function formatCompactNumber(value: number): string {
  return new Intl.NumberFormat('zh-HK', {
    notation: 'compact',
    maximumFractionDigits: 2,
  }).format(value)
}

export function formatPrice(value: number): string {
  return new Intl.NumberFormat('zh-HK', {
    minimumFractionDigits: value >= 10 ? 2 : 3,
    maximumFractionDigits: value >= 10 ? 2 : 3,
  }).format(value)
}

export function formatPercent(value: number): string {
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`
}

export function formatSignedNumber(value: number): string {
  return `${value >= 0 ? '+' : ''}${formatCompactNumber(value)}`
}
