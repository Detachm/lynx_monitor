# Container Production Runbook

This runbook describes the current local-backend production shape:

- Redis and Redpanda/Kafka run persistently on the local workstation.
- The production backend runtime runs as a persistent Docker container on the local workstation because xtquant and its data directory are host-specific.
- The public frontend is deployed on Vercel.
- External WebSocket access reaches the local backend through a Cloudflare tunnel to `http://127.0.0.1:9020`.

The Docker Compose file can still run a full local stack, including a static frontend container, but the current external user-facing path is Vercel plus Cloudflare Tunnel.

## Market-Day Startup

Create or update `infra/production.env` from the example:

```bash
cp infra/production.env.example infra/production.env
```

Set these required host paths and runtime values in `infra/production.env`:

```bash
SILVER_ROOT=/absolute/path/to/mammoth-silver
XTQUANT_SDK_PATH=/home/hliu/xtbackend/vendor/xtquant_251211_interim-release_cp36m-37m-38-39-310-311-312_linux-gnu_x86_64
XTQUANT_DATA_HOME=/home/hliu/xtbackend/.runtime/xtquant
RUNTIME_TRADE_DATE=<YYYYMMDD>
RUNTIME_START_AT=09:25
WAIT_FOR_MARKET_START=true
RUNTIME_SYMBOLS=00700.HK,00939.HK,00005.HK,00108.HK,02643.HK
VITE_MARKET_SYMBOLS=00700.HK,00939.HK,00005.HK,00108.HK,02643.HK
KAFKA_POLL_TIMEOUT_MS=1
RAW_QUEUE_MAX_SIZE=100000
MAX_RAW_RECORDS_PER_TICK=50
STARTUP_INTRADAY_RECOVERY=false
PERSIST_REALTIME_EVENTS=false
COMMIT_RUNTIME_OWNED_RAW_OFFSETS=false
HEALTH_SNAPSHOT_EVERY_TICKS=20
RUNTIME_HEALTH_MAX_AGE_SECONDS=30
TICK_INTERVAL_SECONDS=0.1
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092
KAFKA_RETENTION_MS=86400000
KAFKA_RAW_RETENTION_BYTES=5368709120
KAFKA_PROCESSED_RETENTION_BYTES=5368709120
ALLOW_KAFKA_DEGRADED=true
REDIS_MAXMEMORY=1gb
REDIS_MAXMEMORY_POLICY=volatile-ttl
REDIS_HISTORY_TTL_SECONDS=604800
MIN_ARTIFACT_FREE_BYTES=21474836480
WARN_ARTIFACT_FREE_BYTES=107374182400
```

For same-day manual recovery or afternoon debugging, set `WAIT_FOR_MARKET_START=false` and keep `RUNTIME_TRADE_DATE` on the current HK trade date. Do not advance `RUNTIME_TRADE_DATE` to tomorrow while expecting today's realtime data.

Start Redis/Redpanda and the backend. The current deployment uses host networking for the backend so xtquant and local broker addresses resolve exactly as they do on the workstation:

```bash
docker build -t thousand-backend:production -f backend/Dockerfile.production .

infra/run-live-backend.sh
```

The backend entrypoint waits until `RUNTIME_START_AT` on `RUNTIME_TRADE_DATE` in `Asia/Shanghai` when `WAIT_FOR_MARKET_START=true`, writes and verifies `artifacts/runtime-config.json`, then starts `beast_market.production_runtime`. With `RUNTIME_START_AT=09:25`, the runtime should be connected and subscribed before 09:30.

For compose-managed Redpanda, `infra/configure-redpanda-retention.sh` creates or updates the raw and processed topics with 24h retention and 5GiB per-topic byte caps. For the manual live backend path, the default Kafka endpoint is `127.0.0.1:19092`, matching the compose external listener.

## Cloudflare Tunnel

Start a tunnel from the public internet to the local backend:

```bash
setsid -f env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY \
  /home/hliu/thousand/artifacts/tunnels/bin/cloudflared tunnel \
  --url http://127.0.0.1:9020 \
  --edge-ip-version 4 \
  --no-autoupdate \
  > /home/hliu/thousand/artifacts/tunnels/logs/cloudflared-9020.log 2>&1
```

Record the emitted `https://<host>.trycloudflare.com` URL. The frontend WebSocket URL is:

```text
wss://<host>.trycloudflare.com:443/ws
```

Quick tunnels are account-less and can change or become unavailable. If the URL changes, redeploy Vercel with the new `VITE_MARKET_WS_URL`.

## Vercel Frontend

Deploy the frozen frontend with live build-time settings:

```bash
cd market-terminal
npx --yes vercel --prod --yes \
  --build-env VITE_MARKET_DATA_MODE=live \
  --build-env VITE_MARKET_PROTOCOL=terminal-message-v1 \
  --build-env VITE_MARKET_WS_URL=wss://<host>.trycloudflare.com:443/ws \
  --build-env VITE_MARKET_SYMBOLS=00700.HK,00939.HK,00005.HK,00108.HK,02643.HK
```

The production alias is:

```text
https://market-terminal-psi.vercel.app
```

## Checks

```bash
docker ps --format '{{.Names}} {{.Status}}' | grep -E 'thousand-backend|thousand-redis'
docker logs --tail 80 thousand-backend-${RUNTIME_TRADE_DATE}
```

Runtime health is persisted at:

```bash
artifacts/runtime-health.json
```

Validate one symbol in Redis:

```bash
docker exec thousand-redis redis-cli GET terminal:${RUNTIME_TRADE_DATE}:snapshot:00700.HK
```

Validate local WebSocket:

```bash
python - <<'PY'
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://127.0.0.1:9020/ws", max_size=8_000_000) as ws:
        print(json.loads(await ws.recv())["type"])
        await ws.send(json.dumps({
            "schema_version": 1,
            "protocol": "terminal-message-v1",
            "action": "subscribe",
            "symbol": "00700.HK",
            "client_id": "runbook-check",
        }))
        message = json.loads(await ws.recv())
        payload = message.get("payload", {})
        snapshot = payload.get("snapshot", {})
        print(message.get("type"), snapshot.get("name"), snapshot.get("tradeDate"), snapshot.get("isHistoricalSession"))

asyncio.run(main())
PY
```

## Persistence

- Redis uses a Docker volume with AOF enabled when started through compose.
- Redis is capped by default at `1gb` with `volatile-ttl`; CCASS history TTL defaults to 7 days.
- Redpanda uses a Docker volume when started through compose, with topic retention capped by `KAFKA_RETENTION_MS` and per-topic bytes.
- Backend runtime state, Kafka spool, degraded Kafka audit logs, generated config, tunnel logs, and health snapshots are under the host `artifacts/` directory.
- xtquant runtime data is mounted from `XTQUANT_DATA_HOME`.

Run cleanup in dry-run mode first:

```bash
infra/cleanup-production-artifacts.sh
```

Apply cleanup after review:

```bash
CONFIRM=true PRUNE_DOCKER_BUILD_CACHE=true infra/cleanup-production-artifacts.sh
```

This gzips old runtime JSONL, deletes runtime-state date directories older than 7 days, and prunes oversized validation data.

## Stop Or Restart

```bash
docker restart thousand-backend-${RUNTIME_TRADE_DATE}
docker rm -f thousand-backend-${RUNTIME_TRADE_DATE}
```

To auto-restart a wedged live backend from host health evidence:

```bash
START_BACKEND_WATCHDOG=true infra/run-live-backend.sh
```

Use `docker compose ... down -v` only when you intentionally want to delete Redis/Redpanda data from the compose-managed stack.
