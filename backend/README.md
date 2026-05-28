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

Install production backend dependencies:

```bash
python -m pip install -r backend/requirements-production.txt
```

Start local single-node infrastructure:

```bash
docker compose -f infra/docker-compose.production.yml up -d
```

Start the persistent production container stack:

```bash
cp infra/production.env.example infra/production.env
# edit SILVER_ROOT, XTQUANT_SDK_PATH, XTQUANT_DATA_HOME, and RUNTIME_TRADE_DATE
docker compose --env-file infra/production.env -f infra/docker-compose.production.yml up -d --build
```

See `../docs/container-production-runbook.md` for the 09:30 auto-pull runbook.

Start the supervised production runtime from a verified config artifact:

```bash
PYTHONPATH=backend /home/hliu/miniconda3/envs/mammoth/bin/python -m beast_market.production_runtime \
  --config-path artifacts/runtime-config.json \
  --symbols 00700.HK,00939.HK,00005.HK \
  --xtquant-sdk-path /home/hliu/xtbackend/vendor/xtquant_251211_interim-release_cp36m-37m-38-39-310-311-312_linux-gnu_x86_64 \
  --health-snapshot-path artifacts/runtime-health.json
```

The vendored xtquant SDK on this workstation is built for CPython 3.12. Use the `mammoth` Python 3.12 environment for live xtquant runs; the default Python 3.13 environment can run tests and CSV/Redis/Kafka-only tooling, but cannot import that xtquant binary extension.

Use `../docs/runtime-config.production.template.json` as the no-secret config shape. Copy it to an artifact path, set `silver_root` to a real absolute Mammoth silver path, then verify it with:

```bash
PYTHONPATH=backend python -m beast_market.ops_cli verify-runtime-config \
  --config-path artifacts/runtime-config.json \
  --output-path artifacts/runtime-config-verification.json
```

`backend.tools.real_data_runner` remains a smoke/dev/debug helper. Production startup should use `beast_market.production_runtime`.

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
