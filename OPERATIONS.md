# BTC candle feed operations

Repository: `lxiaomng-lgtm/btc-kline-data`; branch: `main`; data path: `latest.json`.

## Automatic collection

`.github/workflows/update-btc.yml` runs at UTC minutes 11, 26, 41 and 56. A scheduled runner waits until the upcoming quarter-hour boundary before requesting Bitstamp. Late starts collect the latest fully closed quarter-hour. This prewarm reduces some queue delay but is not a timing guarantee. A push changing the workflow or collector performs a bootstrap update; this is not a scheduled-cycle acceptance test.

The standard-library collector `scripts/update_btc.py` obtains 192 closed 15-minute BTC/USD candles directly from Bitstamp, checks all fields, finite positive OHLC prices, nonnegative volume, timestamp alignment, strict ordering, duplicates, 900-second continuity, and the expected most recent close. Invalid or stale responses are retried within a bounded window. Failed validation preserves the previous data. A concurrent edit produces a SHA conflict rather than overwriting another writer.

The workflow uses its repository-scoped GITHUB_TOKEN with contents:write. No personal token or exchange credentials are stored. It writes the existing data path through the GitHub Contents API and verifies the returned blob SHA by a separate readback.

## Consumer rules

Read `latest.json` through GitHub fetch_file with repository_full_name, path, ref=main and encoding=utf-8. Preserve original field meanings:
- `candles[].timestamp`: Unix seconds of candle open.
- `candles[].beijing_time`: candle open in Asia/Shanghai.
- OHLC in USD; volume in BTC. Decimal strings preserve source precision.
- `snapshot_received_at`: time the source response finished.
- `automatic_updates`: true indicates that automatic publication is configured; it does not prove that data is fresh.
- `mode`: automatic_snapshot after the first automatic workflow publication.
- `collected_at`, `latest_candle_close_at`, `update_status` and `validation`: collection and quality metadata.

Check actual latest candle close against the current quarter-hour before analysis. If a run fails, the previous successful file remains and must be reported as stale. Never treat automatic_updates=true as evidence of fresh data or of 60-second compliance.

## Evidence and latency

Each run prints and retains `run-report.json` as a workflow artifact for 30 days and in the job summary. It records source request start/finish, validation attempts, publication completion, remote readback, commit/blob SHA, event type, and seconds from candle close to publication completion. A bootstrap push, workflow_dispatch or local dry run does not count as an unattended scheduled cycle.

Collection delay is not publication delay. File publication is not Pages/CDN refresh. The acceptance path is a GitHub repository read. Only event=schedule runs for distinct consecutive closes count toward the two-cycle test.

GitHub documents that scheduled jobs may be delayed or dropped, and public schedules can be disabled after 60 days without repository activity:
https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule

## Free fallback researched, not deployed

Cloudflare Workers Free plus Cron Triggers can call public APIs and write this same GitHub path. This requires a Cloudflare account and a narrowly scoped GitHub write credential in a Worker secret. It would retain GitHub as the reader-facing source and would need its own two-cycle latency test. No sub-60-second guarantee has been established.

https://developers.cloudflare.com/workers/configuration/cron-triggers/
https://developers.cloudflare.com/workers/platform/pricing/
