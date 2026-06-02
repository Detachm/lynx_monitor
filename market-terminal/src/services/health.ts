import type { StockSymbol, TerminalHealthHandler, TerminalHealthStatus } from '@/types/market'

export function createInitialTerminalHealth(): TerminalHealthStatus {
  return {
    process: 'stopped',
    kafka: 'unknown',
    redis: 'unknown',
    kafkaLag: null,
    latestEventAtBySymbol: {},
    tradeTickSourceAvailable: undefined,
    tradeTickSourceAvailableBySymbol: {},
    symbolFreshness: {},
    updatedAt: new Date(0).toISOString(),
  }
}

export class TerminalHealthTracker {
  private readonly handlers = new Set<TerminalHealthHandler>()
  private status: TerminalHealthStatus = createInitialTerminalHealth()

  snapshot(): TerminalHealthStatus {
    return cloneStatus(this.status)
  }

  update(patch: Partial<Omit<TerminalHealthStatus, 'updatedAt'>>) {
    this.status = {
      ...this.status,
      ...patch,
      latestEventAtBySymbol: {
        ...this.status.latestEventAtBySymbol,
        ...patch.latestEventAtBySymbol,
      },
      tradeTickSourceAvailableBySymbol: {
        ...this.status.tradeTickSourceAvailableBySymbol,
        ...patch.tradeTickSourceAvailableBySymbol,
      },
      symbolFreshness: {
        ...this.status.symbolFreshness,
        ...patch.symbolFreshness,
      },
      updatedAt: new Date().toISOString(),
    }
    this.emit()
  }

  recordEvent(symbol: StockSymbol, sourceTs?: string) {
    this.update({
      latestEventAtBySymbol: {
        [symbol]: sourceTs ?? new Date().toISOString(),
      },
    })
  }

  onHealth(handler: TerminalHealthHandler) {
    this.handlers.add(handler)
    handler(this.snapshot())
    return () => this.handlers.delete(handler)
  }

  private emit() {
    const snapshot = this.snapshot()
    for (const handler of this.handlers) {
      handler(snapshot)
    }
  }
}

function cloneStatus(status: TerminalHealthStatus): TerminalHealthStatus {
  return {
    ...status,
    latestEventAtBySymbol: {
      ...status.latestEventAtBySymbol,
    },
    tradeTickSourceAvailableBySymbol: {
      ...status.tradeTickSourceAvailableBySymbol,
    },
    symbolFreshness: {
      ...status.symbolFreshness,
    },
  }
}
