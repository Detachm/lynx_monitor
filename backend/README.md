# Beast Market Backend

This directory contains the backend implementation for the Beast market terminal.

Authoritative development documents:

- Roadmap and todo: `../BEAST_MARKET_TERMINAL_BACKEND_UPGRADE_PLAN.md`
- Runtime architecture: `../docs/terminal-runtime-architecture-guidance.md`
- LAN multi-trader smoke runbook: `../docs/lan-multi-trader-smoke-runbook.md`
- Wire contracts: `../docs/market-event-contracts-v1.md`
- Historical schema: `../docs/mammoth-silver-schema-v1.md`

The current code still contains parts of the previous backend v2 baseline. Treat those modules as implementation inventory for migration, not as the target architecture. New work must follow the roadmap and symbol-runtime architecture.

## Development Commands

Run backend tests:

```bash
PYTHONPATH=backend python -m pytest backend/tests -q
```

Verify frontend types and unit tests:

```bash
cd market-terminal
npx vue-tsc --build
npx vitest run
```

If Corepack/pnpm is available, `corepack pnpm exec vue-tsc --build` and `corepack pnpm exec vitest run` are equivalent. The npm/npx commands match the default tools available on this workstation.

## Runtime Notes

- Live terminal data must enter the frontend through `TerminalMessage v1`.
- Historical business reads must go through `MammothAPI`.
- Redis is a rebuildable read model, not the source of truth.
- Kafka raw topics are durable fact streams keyed by canonical symbol.
- The target runtime owner is `SymbolRuntime`, one active lifecycle per symbol.
