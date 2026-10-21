import argparse
import asyncio
import logging
import signal
import time

from app.incident_agent import detect_and_investigate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("prod-inc-commander.detector")

DEFAULT_REQUEST = (
    "Autonomously detect actionable production incidents using available MCP tools. "
    "Evaluate HTTP/API errors and latency, Kafka consumer availability and lag, "
    "outbox backlog and retry failures, database availability, failed payments, "
    "and telemetry collection failures where relevant metrics and diagnostics "
    "are available. Use dynamic lookback windows appropriate to the age of each "
    "symptom, widening beyond 5 minutes when necessary and up to 24 hours when "
    "justified. For point-in-time database diagnostics, inspect timestamps, "
    "record ages, retry counts, and status snapshots instead of treating them "
    "as time-series queries. Check active incidents before creating one, avoid "
    "duplicates, investigate correlated symptoms, and persist diagnostic "
    "evidence when an incident is warranted. Never invent metrics or claim "
    "incident creation or evidence persistence unless the tools confirm success. "
    "If tools or the database are unavailable, report the limitation explicitly."
)

async def run_once(request: str) -> str:
    log.info("Starting detection cycle")
    result = await detect_and_investigate(request)
    log.info("Detection cycle finished")
    return result

def main() -> None:
    parser = argparse.ArgumentParser(description="Periodic autonomous incident detector")
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--request", default=DEFAULT_REQUEST)
    args = parser.parse_args()
    if args.interval_seconds < 60:
        parser.error("--interval-seconds must be at least 60")
    if args.once:
        print(asyncio.run(run_once(args.request)))
        return

    stopping = False
    def stop(signum, frame):
        nonlocal stopping
        stopping = True
        log.info("Stop requested; finish current cycle and exit")

    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop)

    log.info("Detection loop started; interval=%s seconds", args.interval_seconds)
    while not stopping:
        started = time.monotonic()
        try:
            print(asyncio.run(run_once(args.request)), flush=True)
        except Exception:
            log.exception("Detection cycle failed")
        delay = max(0, args.interval_seconds - (time.monotonic() - started))
        end = time.monotonic() + delay
        while not stopping and time.monotonic() < end:
            time.sleep(min(1, end - time.monotonic()))
    log.info("Detection loop stopped")

if __name__ == "__main__":
    main()
