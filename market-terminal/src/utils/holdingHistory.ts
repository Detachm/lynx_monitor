export const DEFAULT_HOLDING_HISTORY_DAYS = 30

export function normalizeHoldingHistoryDays(days: unknown): number {
  return typeof days === 'number' && Number.isInteger(days) && days >= 1
    ? days
    : DEFAULT_HOLDING_HISTORY_DAYS
}
