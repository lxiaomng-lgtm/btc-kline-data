"""Multi-timeframe BTC/USD spot research using closed Bitstamp candles."""
import argparse
import csv
import json
import math
import os
from bisect import bisect_left
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from itertools import groupby, product
from pathlib import Path
from statistics import mean

from research_etf import classification
from research_prices import Chan, OUT, STEP, TZ, aggregate, load_prices, signals, simulate, stamp, write_json

DAY = 86400
H3 = 10800
M45 = 2700
FAMILIES = ("daily", "daily_3h", "daily_45m", "3h_45m", "three", "three_etf",
            "three_etf_derivatives", "three_chan")
CAPS = (0.05, 0.10, 0.20, 0.35)
COSTS = (0.0015, 0.003, 0.006)
EXITS = ("signal", "atr2_hard", "atr3_trailing")
REGIMES = ("bull", "range", "bear")
RISK_BUDGET = 12.0
BASE_FEE = COSTS[0]
DATA_DIR = OUT / "multiframe_observations"


def epoch(value):
    return int(datetime.fromisoformat(value).timestamp())


def day_start(value):
    return epoch(value + "T08:00:00+08:00")


def closed_bars(rows, duration):
    for t, group in groupby(rows, key=lambda r: r[0] // duration * duration):
        bars = list(group)
        if len(bars) == duration // STEP and bars[0][0] == t and bars[-1][0] + STEP == t + duration:
            yield t, float(bars[0][1]), float(max(r[2] for r in bars)), float(min(r[3] for r in bars)), float(bars[-1][4])


def daily_inputs(bars):
    c = [r[4] for r in bars]
    legacy = signals(bars, "1d_08")[0]["regime10"]
    for i, row in enumerate(bars):
        state = "warmup"
        if i >= 60:
            window = c[i-20:i+1]
            change = window[-1] - window[0]
            distance = sum(abs(b-a) for a, b in zip(window, window[1:]))
            trend = mean(c[i-19:i+1]) > mean(c[i-59:i+1])
            if distance and abs(change)/distance >= 0.25 and (trend and change > 0 or not trend and change < 0):
                state = "bull" if trend else "bear"
            else:
                state = "range"
        yield row[0] + DAY, state, legacy[i]


def direction_inputs(bars):
    closes = [r[4] for r in bars]
    atr = 0.
    previous = None
    for i, (t, _, high, low, c) in enumerate(bars):
        span = max(high-low, abs(high-previous), abs(low-previous)) if previous is not None else high-low
        atr = span if i == 0 else atr+(span-atr)/14
        previous = c
        direction = 0.
        if i >= 35:
            fast = mean(closes[i-11:i+1])
            slow = mean(closes[i-35:i+1])
            if fast > slow and c >= slow:
                direction = 1. if c >= fast else 0.5
        yield t + H3, direction, c, atr


def trigger_inputs(bars):
    fast = slow = previous = None
    position = 0.
    atr = 0.
    chan = Chan()
    for i, (t, o, h, l, c) in enumerate(bars):
        fast = c if fast is None else fast + (c-fast)*2/9
        slow = c if slow is None else slow + (c-slow)*2/22
        span = max(h-l, abs(h-previous), abs(l-previous)) if previous is not None else h-l
        atr = span if i == 0 else atr+(span-atr)/14
        entry = False
        breakout = False
        if i >= 21:
            breakout = c > max(r[2] for r in bars[i-12:i])
            if fast <= slow or c < slow or c < min(r[3] for r in bars[i-8:i]):
                position = 0.
            elif c > fast:
                entry = position == 0.
                position = 1. if breakout or position == 1. else 0.5
            else:
                position = min(position, 0.5)
        previous = c
        yield t + M45, c, position, entry or breakout, atr, chan.step(h, l, c)


def latest_on_clock(events, clock, default):
    events = iter(events)
    pending = next(events, None)
    value = default
    for t in clock:
        while pending is not None and pending[0] <= t:
            value = pending
            pending = next(events, None)
        yield value


def read_csv(path):
    if path.exists():
        with path.open() as stream:
            yield from csv.DictReader(stream)


def etf_observations():
    for file in sorted((OUT / "etf_observations").glob("*.json")):
        doc = json.loads(file.read_text())
        seen = epoch(doc["observed_at_utc"])
        final = []
        for entry in doc["observations"]:
            day = datetime.fromisoformat(entry["date"]).date()
            if entry["history"] is None or entry["recent"] is None:
                continue
            h, r = (tuple(None if x in ("None", None) else Decimal(x) for x in entry[name])
                    for name in ("history", "recent"))
            if day_start(str(day)) + DAY <= seen and classification(day, h, r) == "FINAL":
                final.append((day, float(h[-1])))
        if final:
            day, total = max(final)
            yield seen + STEP, total, str(day), file.name


def capture_derivatives(now):
    """Persist values at capture, since a merged CSV row can acquire new fields later."""
    if os.getenv("DERIVATIVES_OUTCOME") != "success":
        return None
    path = OUT / "market.json"
    report = json.loads(path.read_text())
    observed = epoch(report["observed_at_utc"])
    if not 0 <= now-observed <= 1800:
        return None
    market = [(epoch(r["open_utc"])+3600, r) for r in read_csv(OUT / "market_hourly.csv")]
    by_close = {close: r for close, r in market if close <= observed}
    out = {}
    for t in sorted(by_close, reverse=True):
        recent = [by_close.get(t-i*3600, {}) for i in range(3)]
        if now-t <= 36*3600 and all(r.get("futures_delta_btc") and r.get("futures_volume_btc") for r in recent):
            volume = sum(float(r["futures_volume_btc"]) for r in recent)
            if volume > 0:
                out["cvd"] = {"time": t, "value": sum(float(r["futures_delta_btc"]) for r in recent)/volume}
                break
    for t in sorted(by_close, reverse=True):
        r = by_close[t]
        before = by_close.get(t-3*3600, {})
        if now-t <= 30*3600 and r.get("oi_btc") and before.get("oi_btc") and float(before["oi_btc"]) > 0:
            out["oi"] = {"time": t, "value": float(r["oi_btc"])/float(before["oi_btc"])-1}
            break
    for t in sorted(by_close, reverse=True):
        r = by_close[t]
        if now-t <= 3*3600 and r.get("coinbase_vs_bitstamp_bps"):
            out["premium"] = {"time": t, "value": float(r["coinbase_vs_bitstamp_bps"])}
            break
    for r in read_csv(OUT / "funding_okx.csv"):
        t = epoch(r["open_utc"])
        if 0 <= observed-t <= 16*3600:
            out["funding"] = {"time": t, "value": float(r["funding_rate"])}
    if not out:
        return None
    return {"captured_at": now, "eligible_at": now+STEP, "source_observed_at": observed,
            "run_id": os.getenv("GITHUB_RUN_ID"), "fields": out}


def derivative_observations():
    for file in sorted(DATA_DIR.glob("*.json")):
        doc = json.loads(file.read_text())
        yield max(doc["eligible_at"], doc["captured_at"]+STEP), doc


def overlays(t, etf, deriv):
    flow = None if etf is None or t-etf[0] > 3*DAY or t-day_start(etf[2]) > 5*DAY else etf[1]
    fields = deriv[1]["fields"] if deriv else {}
    limits = {"oi": 30, "cvd": 36, "premium": 3, "funding": 16}
    valid = {k: d["value"] for k, d in fields.items()
             if 0 <= t-d["time"] <= limits[k]*3600 and math.isfinite(d["value"])}
    if flow is None or len(valid) < 3:
        return flow, None
    stress = ((valid.get("cvd", 0) < -0.06 and valid.get("oi", 0) > 0.003)
              or valid.get("funding", 0) > 0.0003 or valid.get("premium", 0) < -6)
    return flow, 0.5 if stress else 1.


def features(rows):
    day = list(closed_bars(rows, DAY))
    h3 = list(closed_bars(rows, H3))
    m45 = list(closed_bars(rows, M45))
    t45 = [r[0]+M45 for r in m45]
    ds = latest_on_clock(daily_inputs(day), t45, (0, "warmup", 0.))
    hs = latest_on_clock(direction_inputs(h3), t45, (0, 0., 0., 0.))
    es = latest_on_clock(sorted(etf_observations()), t45, None)
    ms = latest_on_clock(sorted(derivative_observations(), key=lambda row: row[0]), t45, None)
    return [(*q, d[1], d[2], h[1], *overlays(q[0], e, m), d[0], h[0], h[2], h[3])
            for q, d, h, e, m in zip(trigger_inputs(m45), ds, hs, es, ms)]


@dataclass(frozen=True)
class Rule:
    family: str
    bear: str = "ban"
    cap: float = .10
    exit: str = "signal"

    @property
    def key(self):
        return f"{self.family}:{self.bear}:{self.cap:.2f}:{self.exit}"


def raw_target(rule, point):
    _, _, timing, trigger, _, chan, regime, legacy, direction, etf, deriv, _, _ = point[:13]
    family = rule.family
    if family == "daily":
        return legacy*rule.cap/.1
    if family in ("hold100", "hold10"):
        return 1. if family == "hold100" else .1
    uses_daily = family != "3h_45m"
    uses_h3 = family != "daily_45m"
    uses_m45 = family != "daily_3h"
    top = (1. if regime == "bull" else .5) if uses_daily else 1.
    if regime == "warmup" and uses_daily:
        return 0.
    if regime == "bear" and uses_daily:
        if rule.bear == "ban":
            return 0.
        # Bounce positions require full-strength lower-timeframe confirmation.
        if (uses_h3 and direction < 1) or (uses_m45 and timing < 1):
            return 0.
        top = .25
    weight = rule.cap * top * (direction if uses_h3 else 1.) * (timing if uses_m45 else 1.)
    if family == "three_chan" and not chan:
        weight *= .5
    if family in ("three_etf", "three_etf_derivatives") and etf is not None and etf < -100:
        weight *= .5
    if family == "three_etf_derivatives" and deriv is not None:
        weight *= deriv
    return weight

@dataclass
class Position:
    cash: float = 1.
    qty: float = 0.
    target: float = -1.
    fees: float = 0.
    orders: int = 0
    trips: int = 0
    wins: int = 0
    losses: int = 0
    gains: float = 0.
    losses_abs: float = 0.
    campaign_cash: float = 0.

    def trade(self, target, price, fee):
        if target == self.target:
            return
        self.target = target
        before = self.qty
        nav = self.cash + before * price
        delta = target*nav - before*price
        if abs(delta) <= 1e-12:
            return
        flow = delta/(1 + (fee if delta > 0 else -fee)*target)
        charge = abs(flow)*fee
        cashflow = -flow-charge
        self.cash += cashflow
        self.qty += flow/price
        self.campaign_cash += cashflow
        self.orders += 1
        self.fees += charge
        if target == 0.:
            self.qty = 0.
            self.trips += 1
            if self.campaign_cash > 1e-12:
                self.wins += 1
                self.gains += self.campaign_cash
            elif self.campaign_cash < -1e-12:
                self.losses += 1
                self.losses_abs -= self.campaign_cash
            self.campaign_cash = 0.


def run(points, opens, a, b, rule, fee, latency, targets, observer=None):
    p = Position()
    peak = last = 1.
    dd = exposure = 0.
    active = stops = 0
    frozen = False
    entry_price = entry_atr = high_close = 0.
    regime_log = [0., 0., 0.]
    regime_bars = [0, 0, 0]
    bear_add_orders = 0
    stop_style = rule.exit
    is_h3 = rule.family == "daily_3h"
    is_daily = rule.family == "daily"
    for i in range(a, b):
        point = points[i]
        t, close, _, trigger, atr = point[:5]
        regime = point[6]
        h3_close_at = point[12]
        target = targets[i]
        reason = 'signal'
        eligible = (not is_h3 or t == h3_close_at or i == a)
        if is_h3:
            close, atr = point[13:15]
            trigger = t == h3_close_at and target > targets[i-1]
        if not eligible:
            target = p.target
        elif stop_style != "signal":
            if frozen and (target == 0. or trigger):
                frozen = False
            if frozen:
                target = 0.
            if p.qty > 0:
                high_close = max(high_close, close)
                floor = (entry_price-2*entry_atr if stop_style == "atr2_hard"
                         else high_close-3*atr)
                if close <= floor and target > 0:
                    target = 0.
                    frozen = True
                    stops += 1
                    reason = 'atr_stop'
        if is_daily and i != a and t != point[11]:
            target = p.target
        previous = p.qty
        if target != p.target:
            price = opens[t+latency]
            pre_trade = p.cash + p.qty*price
            peak = max(peak, pre_trade)
            dd = max(dd, 1-pre_trade/peak)
            before = (p.qty, p.cash, p.fees, p.orders) if observer else None
            p.trade(target, price, fee)
            if observer:
                observer(t+latency, price, p, *before, reason, regime)
            post_trade = p.cash + p.qty*price
            dd = max(dd, 1-post_trade/peak)
            if p.qty > previous and regime == "bear":
                bear_add_orders += 1
            if previous == 0 and p.qty > 0:
                entry_price, entry_atr, high_close = price, atr, close
        mark = points[i+1][1]
        value = p.cash + p.qty*mark
        marked = value
        market_qty = p.qty
        if i+1 == b:
            before = (p.qty, p.cash, p.fees, p.orders) if observer else None
            p.trade(0., mark, fee)
            if observer:
                observer(points[i+1][0], mark, p, *before, 'terminal', regime)
            value = p.cash
        ri = 0 if regime == "bull" else 1 if regime == "range" else 2
        regime_log[ri] += math.log(value/last)
        regime_bars[ri] += 1
        peak = max(peak, marked)
        dd = max(dd, 1-value/peak)
        last = value
        # Mark exposure before terminal sale, once per complete decision bar.
        exposure += market_qty*mark/marked
        active += market_qty > 0
    years = (points[b][0]-points[a][0])/(365.2425*DAY)
    return {
        "net_pct": round((last-1)*100, 5),
        "cagr_pct": round((last**(1/years)-1)*100, 5),
        "drawdown_pct": round(dd*100, 5),
        "orders": p.orders, "campaigns": p.trips,
        "win_pct": round(100*p.wins/p.trips, 3) if p.trips else None,
        "payoff": round((p.gains/p.wins)/(p.losses_abs/p.losses), 5) if p.wins and p.losses else None,
        "avg_exposure_pct": round(100*exposure/(b-a), 5),
        "time_invested_pct": round(active/(b-a)*100, 4),
        "cost_pct": round(p.fees*100, 5),
        "stops": stops, "bear_add_orders": bear_add_orders,
        "regime_log_pct": {k: round(v*100, 5) for k, v in zip(REGIMES, regime_log)},
        "regime_bars": dict(zip(REGIMES, regime_bars))
    }


def variant_grid():
    for f in FAMILIES:
        policies = ("ban",) if f in ("daily", "3h_45m") else ("ban", "bounce")
        exits = ("signal",) if f == "daily" else EXITS
        for bear, cap, exit in product(policies, CAPS, exits):
            yield Rule(f, bear, cap, exit)


def anchored(f, bear="ban"):
    return Rule(f, bear)


def winning_rule(candidate_rows, minimum_campaigns=5):
    viable = [(rule, row) for rule, row in candidate_rows
              if row["drawdown_pct"] <= RISK_BUDGET and row["campaigns"] >= minimum_campaigns
              and row["net_pct"] > 0]
    if not viable:
        return None
    return max(viable, key=lambda x: (x[1]["net_pct"], -x[1]["drawdown_pct"], -x[0].cap, x[0].key))[0]


def sweep(points, opens, report_path):
    clock = [row[0] for row in points]
    definitions = list(variant_grid())
    # Targets are independent of costs and stop choice.
    target_cache = {}

    def targets(rule):
        key = (rule.family, rule.bear, rule.cap)
        if key not in target_cache:
            target_cache[key] = [raw_target(rule, point) for point in points]
        return target_cache[key]

    def boundary(date):
        return bisect_left(clock, day_start(date))

    last_full_day = max(i for i,t in enumerate(clock) if t % DAY == 0)
    cut = {"development": (boundary("2022-02-01"), boundary("2024-01-01")),
           "validation": (boundary("2024-01-01"), boundary("2025-01-01")),
           "historical_holdout": (boundary("2025-01-01"), last_full_day)}
    frozen = Rule("daily")
    h100, h10 = Rule("hold100"), Rule("hold10")
    freeze = {}
    static = {}
    stresses = {}
    tmp = report_path.with_suffix(".tmp")
    fields = ("split", "family", "bear", "cap", "exit", "fee", "delay_min", "net_pct", "cagr_pct",
              "drawdown_pct", "orders", "campaigns", "win_pct", "payoff", "avg_exposure_pct",
              "time_invested_pct", "cost_pct", "stops", "bear_add_orders", "bull_log_pct", "range_log_pct",
              "bear_log_pct", "excess_hold100_pp", "excess_hold10_pp", "excess_daily_pp")

    def evaluate(a, b, rule, fee=BASE_FEE, lag=STEP):
        return run(points, opens, a, b, rule, fee, lag, targets(rule))

    with tmp.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for split, (a, b) in cut.items():
            static[split] = {}
            stresses[split] = {}
            for fee in COSTS:
                compare = {r.family: evaluate(a, b, r, fee) for r in (h100, h10, frozen)}
                for family, val in compare.items():
                    if fee == BASE_FEE:
                        static[split][family] = val
                candidate = {}
                for rule in definitions:
                    row = evaluate(a, b, rule, fee)
                    row.update({f"excess_{name}_pp": round(row["net_pct"]-v["net_pct"], 5)
                                for name, v in compare.items()})
                    candidate[rule] = row
                    writer.writerow({
                        "split": split, **asdict(rule), "fee": fee, "delay_min": 15,
                        **{k: v for k,v in row.items() if k not in ("regime_log_pct", "regime_bars")},
                        **{f"{k}_log_pct": v for k,v in row["regime_log_pct"].items()}
                    })
                stresses[split][str(fee)] = {r.key: candidate[r] for r in definitions
                                             if r.cap == .10 and r.exit == "signal"}
                if fee == BASE_FEE:
                    static[split].update({r.key: candidate[r] for r in definitions
                                          if r.cap == .10 and r.exit == "signal"})
                    if split == "validation":
                        freeze = {family: winning_rule([(r, row) for r,row in candidate.items() if r.family == family])
                                  for family in FAMILIES}
                    if split == "historical_holdout":
                        static[split]["validation_selected"] = {
                            family: {"rule": rule.key, **candidate[rule]} if rule else None
                            for family,rule in freeze.items()}
                        static[split]["zero_delay_frozen_daily"] = evaluate(a, b, frozen, fee, 0)
    tmp.replace(report_path)
    print("Completed chronological parameter grid", flush=True)

    folds = []
    for year in (2024, 2025, 2026):
        for month in (1, 7):
            start = f"{year}-{month:02d}-01"
            train = f"{year-1}-{month:02d}-01"
            until = f"{year if month == 1 else year+1}-{7 if month == 1 else 1:02d}-01"
            x, a, b = boundary(train), boundary(start), min(boundary(until), last_full_day)
            if b-a < 32*30:
                continue
            outcomes = {}
            for family in FAMILIES:
                members = [r for r in definitions if r.family == family]
                trials = [(r, evaluate(x, a, r)) for r in members]
                chosen = winning_rule(trials)
                outcomes[family] = {"rule": chosen.key, "test": evaluate(a, b, chosen)} if chosen else {
                    "rule": None, "test": {"net_pct": 0., "drawdown_pct": 0., "orders": 0}}
            folds.append({"train_from": stamp(clock[x]), "test_from": stamp(clock[a]), "test_to": stamp(clock[b]),
                          "finished_six_months": clock[b] == day_start(until),
                          "families": outcomes, "daily_comparator": evaluate(a, b, frozen),
                          "hold100": evaluate(a, b, h100), "hold10": evaluate(a, b, h10)})
    rollup = {}
    for name in (*FAMILIES, "daily_comparator", "hold100", "hold10"):
        rows = [fold["families"][name]["test"] if name in FAMILIES else fold[name] for fold in folds]
        rollup[name] = {"compounded_pct": round(100*(math.prod(1+r["net_pct"]/100 for r in rows)-1), 5),
                        "positive_folds": sum(r["net_pct"] > 0 for r in rows),
                        "worst_fold_drawdown_pct": max(r["drawdown_pct"] for r in rows),
                        "orders": sum(r["orders"] for r in rows)}
    return cut, static, stresses, folds, rollup, evaluate


def shadow(points, through, paper=False):
    if os.getenv("GITHUB_REF_NAME") != "main":
        return None
    now = int(datetime.now(TZ).timestamp())
    point = points[-1]
    t = point[0]
    if not 0 <= now-t < M45 or t > through:
        return None
    folder = OUT / "multiframe_shadow"
    if paper:
        folder = folder / "paper_signals"
    path = folder / (datetime.fromtimestamp(t, TZ).strftime("%Y-%m-%dT%H%M")+".json")
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    rules = [Rule(f, bear) for f in FAMILIES for bear in ("ban", "bounce")
             if bear == "ban" or f not in ("daily", "3h_45m")]
    report = {"signal_close_bjt": stamp(t), "captured_at_bjt": stamp(now),
              "execution": "Research target only. Earliest hypothetical fill follows actual publication. No account writes.",
              "daily_close_bjt": stamp(point[11]), "h3_close_bjt": stamp(point[12]),
              "daily_regime": point[6], "etf_total_musd": point[9], "derivatives_multiplier": point[10],
              "run_id": os.getenv("GITHUB_RUN_ID"), "source_sha256": sha256(Path(__file__).read_bytes()).hexdigest(),
              "targets": {r.key: raw_target(r, point) for r in rules}}
    if paper:
        report["targets"].update({"daily:ban:0.20:signal": raw_target(Rule("daily", cap=.20), point),
                                  "buy_hold_10": .10, "cash": 0.})
    write_json(path, report)
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shadow-only", action="store_true")
    args = parser.parse_args()
    rows, audit = load_prices()
    through = rows[-1][0]+STEP
    if not args.shadow_only and os.getenv("GITHUB_REF_NAME") == "main":
        now = int(datetime.now(TZ).timestamp())
        observed = capture_derivatives(now)
        if observed:
            DATA_DIR.mkdir(exist_ok=True)
            path = DATA_DIR / (str(now)+"-"+str(os.getenv("GITHUB_RUN_ID"))+".json")
            if not path.exists():
                write_json(path, observed)
    points = features(rows)
    if args.shadow_only:
        print("Shadow observation:", shadow(points, through), flush=True)
        print("Paper observation:", shadow(points, through, True), flush=True)
        return
    # Prefix reruns must yield byte-equivalent feature state before each cutoff.
    for length in (len(rows)//2, len(rows)-160):
        prefix = features(rows[:length])
        if prefix != points[:len(prefix)]:
            raise AssertionError("Noncausal cross-timeframe feature")
    if any(not (0 <= r[0]-r[11] < DAY and 0 <= r[0]-r[12] < H3)
           for r in points if r[11] and r[12]):
        raise AssertionError("Cross-timeframe clock violation")
    opens = {r[0]: float(r[1]) for r in rows}
    folder = OUT / "multiframe"
    folder.mkdir(exist_ok=True)
    cuts, comparisons, stresses, folds, rollup, evaluate = sweep(points, opens, folder / "sweep.csv")
    day = list(aggregate(rows, "day_08"))
    da = next(i for i,r in enumerate(day) if r[0] >= day_start("2025-01-01"))
    target = signals(day, "1d_08")[0]["regime10"]
    old = simulate(day, target, da, len(day), BASE_FEE, "regime10")
    replicated = comparisons["historical_holdout"]["zero_delay_frozen_daily"]["net_pct"]
    if abs(old["net_return_pct"]-replicated) > .00011:
        raise AssertionError(f"Frozen baseline parity failure: {old['net_return_pct']} != {replicated}")
    coverage = {
        "observed_etf_files": len(list(etf_observations())),
        "observed_derivatives_files": len(list(derivative_observations())),
        "etf_first_eligible_in_price_clock": next((stamp(p[0]) for p in points if p[9] is not None), None),
        "derivatives_first_eligible_in_price_clock": next((stamp(p[0]) for p in points if p[10] is not None), None),
        "splits": {name: {"etf_bars": sum(p[9] is not None for p in points[a:b]),
                           "etf_filter_bars": sum(p[9] is not None and p[9] < -100 for p in points[a:b]),
                           "derivatives_bars": sum(p[10] is not None for p in points[a:b]),
                           "derivatives_filter_bars": sum(p[10] == .5 for p in points[a:b])}
                   for name,(a,b) in cuts.items()}
    }
    oos_a, oos_b = cuts["historical_holdout"]
    paired = {}
    for family in FAMILIES:
        if family in ("daily", "3h_45m"):
            continue
        results = []
        for cap, fee, exit in product(CAPS, COSTS, EXITS):
            ban = evaluate(oos_a, oos_b, Rule(family, "ban", cap, exit), fee)
            bounce = evaluate(oos_a, oos_b, Rule(family, "bounce", cap, exit), fee)
            results.append(round(bounce["net_pct"]-ban["net_pct"], 5))
        paired[family] = {"bounce_wins": sum(d > 0 for d in results), "ban_wins": sum(d < 0 for d in results),
                          "ties": sum(d == 0 for d in results), "min_delta_pp": min(results),
                          "max_delta_pp": max(results), "mean_delta_pp": round(mean(results), 5)}
    write_json(folder / "summary.json", {
        "run_id": os.getenv("GITHUB_RUN_ID"), "price_audit": audit, "frozen_legacy": old,
        "holdout_from": stamp(points[oos_a][0]), "holdout_through": stamp(points[oos_b][0]),
        "holdout_anchor": comparisons["historical_holdout"],
        "oos_0_30pct": stresses["historical_holdout"]["0.003"],
        "oos_0_60pct": stresses["historical_holdout"]["0.006"],
        "bear_pair_sensitivity": paired, "rollup": rollup, "external_coverage": coverage,
        "status": "Research and prospective shadow targets only"
    })
    write_json(folder / "validation.json", {
        "periods": {n: {"from": stamp(points[a][0]), "through": stamp(points[b][0]),
                        "base_cost": comparisons[n], "anchor_cost_stress": stresses[n]} for n,(a,b) in cuts.items()},
        "walk_forward": folds, "walk_forward_compounded": rollup
    })
    print("Published grid, rolling validation and legacy benchmark parity", flush=True)


if __name__ == "__main__":
    main()
