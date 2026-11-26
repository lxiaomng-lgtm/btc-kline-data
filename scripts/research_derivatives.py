"""Collect venue-specific BTC derivatives and spot premium; report actual coverage."""

import csv
import io
import json
import math
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zipfile import ZipFile

from research_etf import UTC, lookup_prices, relationship
from research_prices import OUT, stamp, write_json

VISION = "https://data.binance.vision/data"
FAPI = "https://fapi.binance.com"
OKX_FUNDING = "https://www.okx.com/api/v5/public/funding-rate-history"
COINBASE = "https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD/candles"
DERIBIT = "https://history.deribit.com/api/v2/public"
HOUR = 3600
DAY = 24 * HOUR
FIELDS = ("open_utc", "futures_delta_btc", "futures_volume_btc", "cvd_segment_btc",
          "oi_btc", "oi_usdt",
          "spot_binance_close_usdt", "coinbase_close_usd", "bitstamp_close_usd",
          "coinbase_vs_binance_bps", "coinbase_vs_bitstamp_bps", "first_observed_utc")


def request(url, **query):
    if query:
        url += "?" + urlencode(query)
    for attempt in range(3):
        try:
            with urlopen(Request(url, headers={"User-Agent": "BTC historical research/1.0"}), timeout=14) as r:
                return r.read()
        except HTTPError as error:
            if error.code < 429 or error.code == 451 or attempt == 2:
                raise
        except OSError:
            if attempt == 2:
                raise
        time.sleep(attempt + 1)


def api(url, **query):
    return json.loads(request(url, **query))


def archive(url):
    with ZipFile(io.BytesIO(request(url))) as z:
        path, = z.namelist()
        return list(csv.reader(io.StringIO(z.read(path).decode("utf-8-sig"))))


def utc(t):
    return datetime.fromtimestamp(t, UTC).isoformat()


def stamp_utc(s):
    return int(datetime.fromisoformat(s).timestamp())


def add_klines(rows, records, futures):
    for row in records:
        if not str(row[0]).isdigit():
            continue
        t = int(row[0])
        scale = 10**6 if t >= 10**15 else 1000
        ts = t // scale
        # Check pair fixtures' source timestamps before accepting a complete interval.
        if ts % HOUR or (int(row[6])+1)//scale != ts + HOUR:
            raise ValueError(f"Unaligned Binance candle at {ts}")
        if ts not in rows:
            continue
        close = float(row[4])
        if not math.isfinite(close) or close <= 0:
            raise ValueError(f"Invalid Binance close at {ts}")
        if futures:
            v, buy = float(row[5]), float(row[9])
            if not math.isfinite(v) or not 0 <= buy <= v:
                raise ValueError(f"Invalid Binance taker volume at {ts}")
            rows[ts].update(futures_delta_btc=2*buy-v, futures_volume_btc=v)
        else:
            rows[ts]["spot_binance_close_usdt"] = close


def run_jobs(jobs, errors):
    with ThreadPoolExecutor(max_workers=8) as ex:
        work = {key: ex.submit(fn) for key, fn in jobs.items()}
        for key, result in work.items():
            try:
                yield key, result.result()
            except (OSError, ValueError, KeyError) as e:
                errors[key] = str(e)


def read_previous(path, rows, fields):
    if path.exists():
        with path.open() as f:
            for row in csv.DictReader(f):
                t = stamp_utc(row["open_utc"])
                prior = rows.setdefault(t, {})
                prior.update({k: row[k] if k == "first_observed_utc" else float(row[k])
                              for k in fields[1:] if row.get(k)})


def write_rows(path, rows, fields, observed):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for t, row in sorted(rows.items()):
            if row:
                writer.writerow({"open_utc": utc(t), **row,
                                 "first_observed_utc": row.get("first_observed_utc", observed)})
    tmp.replace(path)


def complete_deribit_window(begin_ms, end_ms):
    result = api(f"{DERIBIT}/get_last_trades_by_instrument_and_time",
                 instrument_name="BTC-PERPETUAL", start_timestamp=begin_ms, end_timestamp=end_ms-1,
                 sorting="asc", count=10000)["result"]
    if result["has_more"]:
        mid = (begin_ms + end_ms)//2
        if mid == begin_ms:
            raise ValueError(f"Liquidation sampling exceeded page capacity at {begin_ms}")
        yield from complete_deribit_window(begin_ms, mid)
        yield from complete_deribit_window(mid, end_ms)
    else:
        for trade in result["trades"]:
            if not begin_ms <= trade["timestamp"] < end_ms:
                raise ValueError(f"Deribit returned an out-of-window trade: {trade['timestamp']}")
            yield trade


def deribit_window(start, stop):
    ids = set()
    flags = Counter()
    buy_usd = sell_usd = 0.
    for trade in complete_deribit_window(start*1000, stop*1000):
        key = trade["trade_id"]
        if key in ids:
            raise ValueError(f"Duplicate Deribit trade {key}")
        ids.add(key)
        flag = trade.get("liquidation")
        if flag:
            flags[flag] += 1
            amount = float(trade["amount"])
            side = trade["direction"]
            if not math.isfinite(amount) or amount <= 0 or side not in ("buy", "sell"):
                raise ValueError("Invalid liquidation amount or side")
            if side == "buy":
                buy_usd += amount
            else:
                sell_usd += amount
    return len(ids), dict(flags), buy_usd+sell_usd, buy_usd, sell_usd


def okx_settlements(start, now):
    out = {}
    cursor = (now+1)*1000
    for _ in range(8):
        res = api(OKX_FUNDING, instId="BTC-USDT-SWAP", after=cursor, limit=100)
        if res.get("code") != "0":
            raise ValueError(f"OKX funding API error: {res.get('code')}")
        page = res["data"]
        if not page:
            break
        times = [int(r["fundingTime"]) for r in page]
        if max(times) >= cursor or len(set(times)) != len(times):
            raise ValueError("Invalid OKX funding pagination")
        for r, millis in zip(page, times):
            t = millis//1000
            if t < start or not r.get("realizedRate"):
                continue
            rate = float(r["realizedRate"])
            if r.get("instId") != "BTC-USDT-SWAP" or not math.isfinite(rate) or abs(rate)>1:
                raise ValueError("Invalid OKX funding record")
            out[t] = rate
        cursor = min(times)
        if cursor < start*1000:
            break
    return out


def funding_study(data, prices, start):
    pairs = []
    for t, row in data.items():
        h = (t//HOUR+1)*HOUR
        if start <= t and h in prices and h+HOUR-900 in prices:
            pairs.append((row["funding_rate"], prices[h+HOUR-900][1]/prices[h][0]-1))
    return relationship(pairs)


def main():
    OUT.mkdir(exist_ok=True)
    now = int(time.time())
    today = now // DAY * DAY
    end = now // HOUR * HOUR
    start = today - 90*DAY
    observed = utc(now)
    prices = lookup_prices()
    rows = {t: {} for t in range(start, end, HOUR)}
    hourly = OUT / "market_hourly.csv"
    read_previous(hourly, rows, FIELDS)
    errors = {}
    jobs = {}
    for begin in range(start, end, 240*HOUR):
        stop = min(begin+240*HOUR, end)
        if any("coinbase_close_usd" not in rows[t] for t in range(begin, stop, HOUR)):
            jobs[("coinbase", begin)] = (
                lambda b=begin, s=stop: api(COINBASE, start=b, end=s-1, granularity="ONE_HOUR")["candles"])
    for day in range(start, today, DAY):
        tag = utc(day)[:10]
        if any("oi_btc" not in rows[t] for t in range(day, day+DAY, HOUR)):
            jobs[("oi", day)] = lambda tag=tag: archive(
                f"{VISION}/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-{tag}.zip")
        for label, prefix in (("futures", "futures/um"), ("spot", "spot")):
            field = "futures_delta_btc" if label == "futures" else "spot_binance_close_usdt"
            if any(field not in rows[t] for t in range(day, day+DAY, HOUR)):
                jobs[(label, day)] = lambda tag=tag,prefix=prefix: archive(
                    f"{VISION}/{prefix}/daily/klines/BTCUSDT/1h/BTCUSDT-1h-{tag}.zip")
    jobs[("funding_api", 0)] = lambda: api(f"{FAPI}/fapi/v1/fundingRate",
                                          symbol="BTCUSDT", startTime=start*1000, limit=1000)
    for month in sorted({utc(d)[:7] for d in range(start, today, DAY)} - {utc(today)[:7]}):
        jobs[("funding_file", month)] = lambda m=month: archive(
            f"{VISION}/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-{m}.zip")
    jobs[("oi_api", 0)] = lambda: api(f"{FAPI}/futures/data/openInterestHist",
                                     symbol="BTCUSDT", period="1h", limit=72)
    jobs[("futures_api", 0)] = lambda: api(f"{FAPI}/fapi/v1/klines",
                                          symbol="BTCUSDT", interval="1h", limit=72)
    jobs[("spot_api", 0)] = lambda: api("https://data-api.binance.vision/api/v3/klines",
                                       symbol="BTCUSDT", interval="1h", limit=72)

    okx_funding = {}
    okx_path = OUT / "funding_okx.csv"
    jobs[("okx_funding", 0)] = lambda: okx_settlements(start, now)
    funding = {}
    funding_path = OUT / "funding.csv"
    funding_fields = ("open_utc", "funding_rate", "first_observed_utc")
    read_previous(funding_path, funding, funding_fields)
    read_previous(okx_path, okx_funding, funding_fields)

    for (source, _), values in run_jobs(jobs, errors):
        if source == "okx_funding":
            for t, rate in values.items():
                okx_funding.setdefault(t, {})["funding_rate"] = rate
        elif source == "coinbase":
            for bar in values:
                t = int(bar["start"])
                p = float(bar["close"])
                if t % HOUR or not math.isfinite(p) or p <= 0:
                    raise ValueError("Invalid Coinbase close")
                if t in rows:
                    rows[t]["coinbase_close_usd"] = p
        elif source in ("futures", "spot", "futures_api", "spot_api"):
            add_klines(rows, values, source.startswith("futures"))
        elif source in ("funding_api", "funding_file"):
            vals = ((int(r["fundingTime"]), float(r["fundingRate"])) for r in values) if source == "funding_api" else (
                (int(r[0]), float(r[2])) for r in values if r[0].isdigit())
            for millis, rate in vals:
                t = millis // 1000
                if abs(rate) > 1 or not math.isfinite(rate):
                    raise ValueError("Invalid funding rate")
                if start <= t <= now:
                    existing = funding.setdefault(t, {})
                    if "funding_rate" in existing and abs(existing["funding_rate"]-rate) > 1e-10:
                        raise ValueError(f"Conflicting Binance funding settlement at {utc(t)}")
                    existing["funding_rate"] = rate
        elif source in ("oi", "oi_api"):
            vals = ((int(r["timestamp"])//1000, float(r["sumOpenInterest"]), float(r["sumOpenInterestValue"]))
                    for r in values) if source == "oi_api" else (
                    (int(datetime.fromisoformat(r[0]).replace(tzinfo=UTC).timestamp()), float(r[2]), float(r[3]))
                    for r in values if r[0] != "create_time" and r[2] and r[3])
            for t, oi, notional in vals:
                if t in rows and math.isfinite(oi) and math.isfinite(notional) and min(oi, notional) > 0:
                    rows[t].update(oi_btc=oi, oi_usdt=notional)
    previous_cvd_t = None
    cvd = 0.
    for t, row in sorted(rows.items()):
        delta = row.get("futures_delta_btc")
        if delta is not None:
            cvd = cvd + delta if previous_cvd_t == t - HOUR else delta
            row["cvd_segment_btc"] = cvd
            previous_cvd_t = t
        if t+HOUR-900 in prices:
            row["bitstamp_close_usd"] = prices[t+HOUR-900][1]
        cb = row.get("coinbase_close_usd")
        if cb:
            for venue, field in (("binance", "spot_binance_close_usdt"), ("bitstamp", "bitstamp_close_usd")):
                if row.get(field):
                    row[f"coinbase_vs_{venue}_bps"] = 10000*(cb/row[field]-1)
    write_rows(hourly, rows, FIELDS, observed)
    write_rows(funding_path, funding, funding_fields, observed)
    write_rows(okx_path, okx_funding, funding_fields, observed)

    liq_file = OUT / "liquidation_sample.json"
    saved = json.loads(liq_file.read_text()) if liq_file.exists() else {}
    liq_jobs = {}
    for day in range(today-3*DAY, today, DAY):
        for hour in (0, 12):
            a = day + hour*HOUR
            if utc(a) not in saved:
                liq_jobs[a] = lambda a=a: deribit_window(a, a+HOUR)
    for a, (count, flags, volume, buy, sell) in run_jobs(liq_jobs, errors):
        if count:
            saved[utc(a)] = {"through_exclusive": utc(a+HOUR), "first_observed_utc": observed,
                             "verified_trades": count, "flags": flags, "liquidated_trade_usd": volume,
                             "buy_trade_usd": buy, "sell_trade_usd": sell}
    write_json(liq_file, dict(sorted(saved.items())))
    coverage = {}
    studies = {}
    recent = {t:r for t,r in rows.items() if start <= t < end}
    for name in FIELDS[1:-1]:
        times = sorted(t for t,r in recent.items() if name in r)
        coverage[name] = {"n": len(times), "missing": len(recent)-len(times),
                          "first": utc(times[0]) if times else None, "last": utc(times[-1]) if times else None}
        if name in ("futures_delta_btc", "oi_btc", "coinbase_vs_binance_bps", "coinbase_vs_bitstamp_bps"):
            pairs = []
            for t in times:
                if t+HOUR not in prices or t+2*HOUR-900 not in prices:
                    continue
                value = rows[t][name]
                if name == "oi_btc":
                    prior = rows.get(t-HOUR, {}).get(name)
                    if not prior:
                        continue
                    value = value/prior-1
                pairs.append((value, prices[t+2*HOUR-900][1] / prices[t+HOUR][0] - 1))
            studies[name] = relationship(pairs)
    studies["funding_rate"] = funding_study(funding, prices, start)
    studies["okx_funding_rate"] = funding_study(okx_funding, prices, start)
    pairs = [(row["liquidated_trade_usd"], prices[t+2*HOUR-900][1]/prices[t+HOUR][0]-1)
             for s,row in saved.items() if (t:=stamp_utc(s))+HOUR in prices and t+2*HOUR-900 in prices]
    studies["liquidation_sample"] = relationship(pairs)
    write_json(OUT / "market.json", {
        "observed_at_utc": observed, "coverage_utc": {"from": utc(start), "through": utc(end)},
        "definitions": {"oi": "Binance USD-M BTCUSDT, hourly snapshots in BTC and USDT",
                        "cvd": "Binance USD-M BTCUSDT hourly signed CVD increment = 2*taker_buy_base - total_base; the cumulative CVD resets after a missing hour",
                        "funding": "Separate Binance BTCUSDT and OKX BTC-USDT-SWAP settled funding rates. Binance archive lags when its API rejects the runner.",
                        "liquidation": "Deribit BTC-PERPETUAL trades bearing liquidation flags; fixed 00:00 and 12:00 UTC one-hour samples, not full-market liquidations",
                        "premium": "Matched UTC hourly Coinbase BTC/USD close vs Binance spot BTC/USDT (includes USD/USDT basis) and vs Bitstamp BTC/USD"},
        "coverage": coverage, "funding_events_last_90_days": sum(t>=start for t in funding),
        "okx_funding_events_last_90_days": sum(t>=start for t in okx_funding),
        "liquidation_hours_verified": len(saved),
        "liquidation_trades_flagged": sum(sum(r["flags"].values()) for r in saved.values()),
        "next_hour_price_study": studies,
        "research_caveat": "Descriptive only. first_observed_utc records when the row was first stored, and may precede later field backfills. Historical file arrival times are unknown and Deribit may delay public liquidation flags. These inputs were not used in simulated signals.",
        "source_error_counts": dict(Counter(str(k[0] if isinstance(k, tuple) else "liquidation") for k in errors)),
        "source_error_examples": {str(k): v for k,v in list(errors.items())[:12]},
    })
    print(json.dumps({"coverage":coverage, "funding":len(funding), "liq_hours":len(saved),
                      "errors":len(errors)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
