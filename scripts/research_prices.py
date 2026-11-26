"""Causal BTC spot research on audited Bitstamp bars. Run from repository root."""

import csv
import json
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta
from hashlib import sha256
from os import getenv
from decimal import Decimal
from itertools import groupby
from pathlib import Path
from statistics import mean, median

from archive_btc import FIELDS, STEP, TZ, fetch

OUT = Path("research")
PERIODS = {"15m": 900, "45m": 2700, "3h": 10800, "6h": 21600,
           "1d": 86400, "1w": "week", "1M": "month"}
NAMES = ("cash", "hold", "fixed10", "trend", "breakout", "chan", "regime", "combined")
COSTS = (0.0015, 0.003, 0.006)
LENGTHS = {"1w": (4, 13), "1M": (3, 10)}


def local(t):
    return datetime.fromtimestamp(t, TZ)


def stamp(t):
    return local(t).isoformat()


def write_json(path, data):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    tmp.replace(path)


def read_bar(x):
    t = int(x["timestamp"])
    o, h, l, c, v = (Decimal(x[k]) for k in FIELDS[2:])
    if (t % STEP or int(datetime.fromisoformat(x["beijing_time"]).timestamp()) != t
            or not all(z.is_finite() for z in (o, h, l, c, v))
            or min(o, h, l, c) <= 0 or v < 0 or h < max(o, c, l) or l > min(o, c, h)):
        raise ValueError(f"Invalid OHLCV at {t}")
    return t, o, h, l, c, v


def load_prices():
    archive = json.loads(Path("history/index.json").read_text())
    snap = json.loads(Path("latest.json").read_text())
    rows = []
    for month in archive["months"]:
        with open(month["path"]) as f:
            bars = [read_bar(x) for x in csv.DictReader(f)]
        if (len(bars) != month["count"] or not bars or bars[0][0] != month["first_open"]
                or bars[-1][0] + STEP != month["last_close"]):
            raise ValueError(f"Invalid index entry {month['path']}")
        rows.extend(bars)
    if len(rows) != archive["count"] or rows[-1][0] + STEP != archive["latest_close"]:
        raise ValueError("Archive does not match the manifest")
    if (snap["symbol"] != "BITSTAMP:BTCUSD" or snap["interval_seconds"] != STEP
            or len(snap["candles"]) != snap["count"] or snap["count"] != 192):
        raise ValueError("Unexpected snapshot")
    live = [read_bar(x) for x in snap["candles"]]
    if any(b[0] - a[0] != STEP for a, b in zip(live, live[1:])):
        raise ValueError("Snapshot has gaps or duplicates")

    archive_end = rows[-1][0] + STEP
    first = rows[0][0]
    overlap = [r for r in live if first <= r[0] < archive_end]
    for row in overlap:
        if row != rows[(row[0] - first) // STEP]:
            raise ValueError(f"Archive/snapshot OHLCV disagreement at {stamp(row[0])}")
    target = int(datetime.now(TZ).timestamp()) // STEP * STEP
    if max(archive_end, live[-1][0] + STEP) > target:
        raise ValueError("Input contains a candle whose close is in the future")
    additions = [r for r in live if r[0] >= archive_end]
    anchor = additions[0][0] if additions else archive_end
    missing_count = 0

    def fill(start, end):
        nonlocal missing_count
        for begin in range(start, end, 999 * STEP):
            stop = min(begin + 999 * STEP, end)
            block = fetch(begin, stop)
            rows.extend(read_bar(block[t]) for t in range(begin, stop, STEP))
            missing_count += len(block)

    fill(archive_end, anchor)
    rows.extend(additions)
    fill(rows[-1][0] + STEP, target)
    if any(b[0] - a[0] != STEP for a, b in zip(rows, rows[1:])):
        raise ValueError("Merged series has gaps or duplicates")
    return rows, {"archived_bars": archive["count"], "snapshot_bars": len(live),
                  "overlap_exact_ohlcv": len(overlap), "snapshot_appended": len(additions),
                  "bitstamp_gap_or_tail_bars": missing_count, "total_bars": len(rows),
                  "from": stamp(first), "through_close": stamp(target),
                  "snapshot_close": stamp(live[-1][0] + STEP),
                  "archive_close": stamp(archive_end), "missing": 0, "duplicates": 0,
                  "invalid_ohlcv": 0}


def key(t, period):
    if isinstance(period, int):
        return (t + 28800) // period * period - 28800
    day = local(t).replace(hour=0, minute=0, second=0)
    start = day - timedelta(days=day.weekday()) if period == "week" else day.replace(day=1)
    return int(start.timestamp())


def end(t, period):
    if isinstance(period, int):
        return t + period
    if period == "week":
        return t + 604800
    d = local(t)
    return int(d.replace(year=d.year + (d.month == 12), month=d.month % 12 + 1).timestamp())


def aggregate(rows, period):
    for start, group in groupby(rows, key=lambda r: key(r[0], period)):
        g = list(group)
        if g[0][0] == start and g[-1][0] + STEP == end(start, period):
            yield (start, float(g[0][1]), float(max(r[2] for r in g)),
                   float(min(r[3] for r in g)), float(g[-1][4]))


class Chan:
    """Containment-adjusted fractals and confirmed pens, based on the V7 pen rules."""

    def __init__(self):
        self.active = None
        self.done = deque(maxlen=3)
        self.position = 0
        self.direction = 1
        self.endpoint = None
        self.locked = deque(maxlen=4)
        self.zone = None
        self.side = 0

    def step(self, high, low, close):
        if self.active is None:
            self.active = (high, low)
            return 0
        h, l = self.active
        if (high <= h and low >= l) or (high >= h and low <= l):
            merge = max if self.direction > 0 else min
            self.active = (merge(h, high), merge(l, low))
        else:
            self.done.append(self.active)
            self.active = (high, low)
            self.direction = 1 if high > h else -1
            self.position += 1
            if len(self.done) == 3:
                self.fractal()
        if self.zone:
            bottom, top = self.zone
            if close > top:
                self.side = 1
            elif close < bottom:
                self.side = 0
        return self.side

    def fractal(self):
        left, mid, right = self.done
        top = all(m > max(a, b) for a, m, b in zip(left, mid, right))
        bottom = all(m < min(a, b) for a, m, b in zip(left, mid, right))
        if not (top or bottom):
            return
        price, kind = (mid[0], 1) if top else (mid[1], -1)
        position = self.position - 2
        if self.endpoint is None:
            self.endpoint = (position, price, kind)
            return
        last, previous, previous_kind = self.endpoint
        if kind == previous_kind:
            if (price - previous) * kind > 0:
                self.endpoint = (position, price, kind)
        elif position - last >= 4 and (price - previous) * kind > 0:
            self.locked.append(previous)
            self.endpoint = (position, price, kind)
            if len(self.locked) == 4:
                legs = list(zip(self.locked, list(self.locked)[1:]))
                lo = max(min(pair) for pair in legs)
                hi = min(max(pair) for pair in legs)
                if lo < hi:
                    self.zone = (lo, hi)


def signals(bars, horizon):
    fast, slow = LENGTHS.get(horizon, (20, 60))
    closes = []
    chan = Chan()
    breakout = 0
    result = {name: [] for name in NAMES}
    regimes = Counter()
    for i, (_, _, high, low, close) in enumerate(bars):
        closes.append(close)
        structural = chan.step(high, low, close)
        trend = state = combo = 0
        regime = "warmup"
        if i >= slow:
            avg_fast = mean(closes[-fast:])
            avg_slow = mean(closes[-slow:])
            if close > max(closes[-fast-1:-1]):
                breakout = 1
            elif close < min(closes[-max(2, fast//2)-1:-1]):
                breakout = 0
            trend = int(avg_fast > avg_slow)
            # Direction and efficiency compare only completed closes.
            window = closes[-fast-1:]
            change = close - window[0]
            distance = sum(abs(b-a) for a, b in zip(window, window[1:]))
            efficiency = abs(change) / distance if distance else 0
            if trend and change > 0 and efficiency >= 0.25:
                regime, state = "bull_trend", trend
                combo = (trend + breakout + structural) / 3
            elif not trend and change < 0 and efficiency >= 0.25:
                regime = "bear_trend"
            else:
                regime, state = "range", breakout * 0.25
                combo = 0.25 * structural
        regimes[regime] += 1
        values = (0, 1, 0.1, trend, breakout, structural, state, combo)
        for name, weight in zip(NAMES, values):
            result[name].append(weight)
    return result, regimes, regime


def simulate(bars, target, a, b, fee, name, lag=1):
    if b - a < 2:
        return None
    cash, qty, peak, drawdown, expenses, orders, previous = 1., 0., 1., 0., 0., 0, None
    invested = 0
    for i in range(a, b):
        _, open_price, _, _, close = bars[i]
        weight = target[i-lag]
        invested += weight > 0
        if name == "fixed10" or weight != previous:
            nav = cash + qty * open_price
            delta = weight * nav - qty * open_price
            if abs(delta) > 1e-12:
                flow = delta / (1 + (fee if delta > 0 else -fee) * weight)
                qty += flow / open_price
                cash -= flow + abs(flow) * fee
                orders += 1
                expenses += abs(flow) * fee
        previous = weight
        nav = cash + qty * close
        peak = max(peak, nav)
        drawdown = max(drawdown, 1 - nav / peak)
    fee_at_exit = qty * bars[b-1][4] * fee
    final = nav - fee_at_exit
    drawdown = max(drawdown, 1 - final / peak)
    return {"net_return_pct": round(100*(final-1), 4), "max_close_drawdown_pct": round(100*drawdown, 4),
            "orders_including_terminal_exit": orders + int(qty > 1e-12),
            "cost_pct_of_starting_capital": round(100*(expenses+fee_at_exit), 4),
            "position_bar_pct": round(100*invested/(b-a), 3), "bars": b-a}


def seasonal(bars, start, stop):
    weekday, month, streak = (defaultdict(list) for _ in range(3))
    direction = 0
    run = 0
    for t, o, _, _, c in bars:
        if not start <= t < stop:
            continue
        d = local(t)
        r = c/o - 1
        weekday[d.strftime("%a")].append(r)
        month[d.month].append(r)
        if direction:
            suffix = "+" if run >= 4 else ""
            streak[f"{direction:+d}_{min(run, 4)}{suffix}"].append(r)
        change = (r > 0) - (r < 0)
        run = run + 1 if change and direction == change else 1
        direction = change

    def summarize(groups):
        return {k: {"n": len(v), "up_pct": round(100*sum(r > 0 for r in v)/len(v), 3),
                    "mean_pct": round(100*mean(v), 5), "median_pct": round(100*median(v), 5)}
                for k, v in sorted(groups.items())}

    return {"weekdays": summarize(weekday), "calendar_month_daily_returns": summarize(month),
            "following_day_by_prior_streak": summarize(streak)}


def multiframe_targets(daily, six_hour, daily_signals, six_hour_signals):
    closes = [t + 21600 for t, *_ in six_hour]
    weights = []
    for i, (t, *_) in enumerate(daily):
        j = bisect_right(closes, t + 86400) - 1
        if j >= 0 and six_hour_signals["trend"][j]:
            weights.append(daily_signals["combined"][i])
        else:
            weights.append(0.)
    return weights


def main():
    OUT.mkdir(exist_ok=True)
    rows, audit = load_prices()
    tail = OUT / "bitstamp_tail_15m.csv"
    tmp_tail = tail.with_suffix(".tmp")
    with tmp_tail.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(FIELDS)
        for t, *values in rows[audit["archived_bars"]:]:
            writer.writerow((t, stamp(t), *values))
    tmp_tail.replace(tail)
    report = {"audit": audit, "cost_per_side": COSTS,
              "execution": "Prior completed bar target; next open, long-only cash/spot, paid terminal liquidation. Drawdown at each strategy bar close including liquidation. Fixed10 rebalances each bar. Costs include assumed fees plus slippage, not taxes.",
              "split_note": "Historical holdout begins 2025. This is retrospective, not prospective. Validation and holdout start flat. Price rules fixed across costs; do not select models from the holdout table.",
              "periods": {}}
    six_hour, six_hour_signals = None, None
    prospective = None
    for name, period in PERIODS.items():
        bars = list(aggregate(rows, period))
        dates = [local(t).strftime("%Y-%m-%d") for t, *_ in bars]
        validation = bisect_left(dates, "2024-01-01")
        holdout = bisect_left(dates, "2025-01-01")
        targets, regimes, regime = signals(bars, name)
        if name == "6h":
            six_hour, six_hour_signals = bars, targets
        if name == "1d":
            targets["combined_mtf"] = multiframe_targets(bars, six_hour, targets, six_hour_signals)
            prospective = (end(bars[-1][0], period), regime, targets["regime"][-1])
        # A prefix run must emit the exact same signals as the same segment of the full run.
        for cutoff in (validation, holdout, len(bars)-7):
            prefix, _, _ = signals(bars[:cutoff], name)
            if name == "1d":
                available = bisect_right(six_hour, (end(bars[cutoff-1][0], period) - 21600, float("inf")))
                partial_six, _, _ = signals(six_hour[:available], "6h")
                prefix["combined_mtf"] = multiframe_targets(bars[:cutoff], six_hour[:available], prefix, partial_six)
            if any(prefix[n] != targets[n][:cutoff] for n in targets):
                raise ValueError(f"Noncausal signal: {name}, cutoff {cutoff}")
        second_year = bisect_left(dates, "2026-01-01")
        warmup = LENGTHS.get(name, (20, 60))[1] + 1
        splits = {"development": (warmup, validation),
                  "validation": (validation, holdout), "historical_holdout": (holdout, len(bars)),
                  "holdout_2025": (holdout, second_year), "holdout_2026": (second_year, len(bars))}
        comparisons = {}
        for split, (a, b) in splits.items():
            a = max(a, 1)
            comparisons[split] = {
                "start": stamp(bars[a][0]) if a < b else None,
                "through_close": stamp(end(bars[b-1][0], period)) if a < b else None,
                "cost_scenarios": {str(fee): {n: simulate(bars, targets[n], a, b, fee, n)
                                               for n in targets} for fee in COSTS},
                "one_extra_bar_delay_at_0.15pct": {
                    n: simulate(bars, targets[n], max(a, 2), b, COSTS[0], n, lag=2)
                    for n in targets},
            }
        report["periods"][name] = {"bars": len(bars), "regime_counts": dict(regimes),
                                   "latest_closed_regime": regime, "splits": comparisons}
    report["promotion"] = {"status": "shadow_research_only",
        "reason": "The historical holdout is retrospective, prior market outcomes were public, and no point-in-time live paper trading evidence has accumulated.",
        "rule": "Keep formal A/B/C frozen. Candidates are fixed ahead of prospective paper evaluation."}
    daily = list(aggregate(rows, PERIODS["1d"]))
    seasonal_start = int(datetime(2021, 11, 1, tzinfo=TZ).timestamp())
    seasonal_stop = int(datetime(2026, 11, 1, tzinfo=TZ).timestamp())
    season = seasonal(daily, seasonal_start, seasonal_stop)
    hourly = defaultdict(list)
    for t, o, _, _, c in aggregate(rows, 3600):
        if seasonal_start <= t < seasonal_stop:
            hourly[local(t).hour].append(c/o - 1)
    season["hour_bjt"] = {h: {"n": len(v), "up_pct": round(100*sum(x > 0 for x in v)/len(v), 3),
                              "mean_pct": round(100*mean(v), 5)} for h,v in sorted(hourly.items())}
    report["five_year_descriptive_statistics"] = {"from": stamp(seasonal_start),
                                                   "through_exclusive": stamp(seasonal_stop), **season}
    write_json(OUT / "prices.json", report)
    if getenv("GITHUB_ACTIONS") == "true" and getenv("GITHUB_REF_NAME") == "main":
        seal_shadow(*prospective, audit)
    print(json.dumps(audit, ensure_ascii=False))


def seal_shadow(source_close, regime, target, audit):
    now = datetime.now(TZ)
    if now - local(source_close) > timedelta(hours=24):
        raise ValueError("Daily source bar is stale for shadow observation")
    log = OUT / "shadow_signals"
    log.mkdir(exist_ok=True)
    path = log / f"{local(source_close).date()}.json"
    if path.exists():
        return
    write_json(path, {"observed_at_bjt": now.isoformat(), "closed_daily_bar_at_bjt": stamp(source_close),
                      "regime": regime, "target_spot_weight": target,
                      "source_data_through": audit["through_close"],
                      "code_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
                      "pipeline_run_id": getenv("GITHUB_RUN_ID"),
                      "purpose": "Prospective research observation. No fills and no ledger activity."})


if __name__ == "__main__":
    main()
