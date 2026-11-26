"""Event discovery, execution experiments, and transaction attribution."""
import csv
import itertools
import math
import random
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from itertools import groupby
from pathlib import Path
from statistics import mean, median, stdev
from urllib.request import urlopen

from multiframe_btc import (DAY, FAMILIES, M45, OUT, STEP, Position, Rule, closed_bars,
                            day_start, features, load_prices, raw_target, run, stamp, write_json)

ROOT = OUT / "edge"
FEES = (0.0015, 0.0045, 0.006)
FEE = FEES[1]
HORIZONS = (1, 4, 8, 16, 32, 96, 224, 448, 960)
TREASURY = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DTB3"


def csv_write(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def trace(points, opens, a, b, rule, fee):
    journal = []
    current = None
    targets = [raw_target(rule, p) for p in points]

    def observe(t, price, p, prior, prior_cash, fees, prior_orders, reason, regime):
        nonlocal current
        if prior_orders == p.orders:
            return
        if current is None:
            current = {"at": t, "price": price, "regime": regime, "cashflow": 0., "cost": 0.,
                       "orders": 0, "fills": []}
        current["cashflow"] += p.cash-prior_cash
        current["cost"] += p.fees-fees
        current["orders"] += 1
        current["fills"].append((t, price, p.qty-prior, p.fees-fees, reason))
        if p.qty == 0:
            journal.append((current, t, price, reason))
            current = None

    metric = run(points, opens, a, b, rule, fee, STEP, targets, observer=observe)
    if abs(sum(r[0]["cashflow"] for r in journal)*100-metric["net_pct"]) > .00001:
        raise AssertionError("transaction cashflows do not reconcile")
    return metric, journal


def attribution(points, rows, opens, a, b):
    campaigns = []
    fills = []
    report = {}
    first = rows[0][0]
    for family in FAMILIES:
        for bear in ("ban", "bounce") if family not in ("daily","3h_45m") else ("ban",):
            rule = Rule(family, bear)
            zero, free = trace(points, opens, a, b, rule, 0.)
            refs = [(q["at"], t) for q,t,_,_ in free]
            direct, journal = trace(points, opens, a, b, rule, FEE)
            fee_legacy, _ = trace(points, opens, a, b, rule, FEES[0])
            if refs != [(q["at"],t) for q,t,_,_ in journal]:
                raise AssertionError("zero/paid fills diverged")
            net = []
            durations = []
            for k,(r,t,price,reason) in enumerate(journal, 1):
                start = (r["at"]-first)//STEP
                stop = (t-first)//STEP
                interval = rows[start:stop]  # exclude market movement after the exit
                low = min([r["price"], price, *(float(x[3]) for x in interval)])
                high = max([r["price"], price, *(float(x[2]) for x in interval)])
                gain = r["cashflow"]*100
                net.append(gain)
                durations.append((t-r["at"])/3600)
                gross = gain + r["cost"]*100
                campaigns.append(dict(rule=rule.key, id=k, entry=stamp(r["at"]), exit=stamp(t),
                    entry_regime=r["regime"], entry_price=r["price"], exit_price=price,
                    exit_reason=reason, holding_hours=durations[-1], orders=r["orders"],
                    gross_pct=gross, net_pct=gain, cost_pct=r["cost"]*100,
                    btc_mfe_pct=100*(high/r["price"]-1), btc_mae_pct=100*(low/r["price"]-1),
                    losing_cause=("negative_gross_edge" if gross < 0 else
                                  "cost_flip" if gain < 0 else "")))
                for at, fill_price, qty, fee, label in r["fills"]:
                    fills.append(dict(rule=rule.key, id=k, at=stamp(at), price=fill_price,
                                      signed_btc_per_initial_dollar=qty, fee_fraction=fee, reason=label))
            gains = sorted((x for x in net if x>0), reverse=True)
            subset = [x for x in campaigns if x["rule"] == rule.key]
            report[rule.key] = dict(zero_cost=zero, prior_fee=fee_legacy, costed=direct,
                gross_same_paid_fills_pct=round(sum(x["gross_pct"] for x in subset),6),
                median_hold_hours=median(durations) if durations else None,
                median_btc_mfe_pct=median(x["btc_mfe_pct"] for x in subset) if subset else None,
                median_btc_mae_pct=median(x["btc_mae_pct"] for x in subset) if subset else None,
                top5_share_positive_pnl_pct=100*sum(gains[:5])/sum(gains) if gains else None,
                loss_reasons=dict(Counter(x["losing_cause"] for x in subset if x["losing_cause"])))
    csv_write(ROOT/"campaigns.csv", campaigns)
    csv_write(ROOT/"orders.csv", fills)
    return report


def discover(rows, points):
    candles = list(closed_bars(rows, M45))
    assert [r[0]+M45 for r in candles] == [p[0] for p in points]
    volumes = []
    for _, group in groupby(rows,key=lambda r:r[0]//M45):
        batch=list(group)
        if len(batch)==M45//STEP:
            volumes.append(sum(float(x[5]) for x in batch))
    assert len(volumes)==len(candles)
    first = rows[0][0]
    simple = [r[4]/candles[i-1][4]-1 if i else 0. for i,r in enumerate(candles)]
    events = defaultdict(list)
    streak = direction = state_age = 0
    old_state = None
    break_level = None
    atr_pct = [None] * len(candles)
    for i, (t,o,h,l,c) in enumerate(candles):
        sign = 1 if simple[i]>0 else -1 if simple[i]<0 else 0
        streak = streak+1 if sign and sign == direction else (1 if sign else 0)
        direction = sign
        old_state, state_age = points[i][6], state_age+1 if points[i][6]==old_state else 1
        if i < 70:
            continue
        past = candles[i-32:i]
        prev_high = max(x[2] for x in past)
        prev_low = min(x[3] for x in past)
        vol = stdev(simple[i-32:i])
        base_vol = stdev(simple[i-64:i-32])
        prior_volume = median(volumes[i-32:i])
        actual = points[i]
        atr_pct[i] = actual[4]/c
        breakout = c > prev_high
        pullback = bool(break_level and 0 < i-break_level[0] <= 8 and l <= break_level[1] <= c)
        if pullback:
            break_level = None
        if breakout:
            break_level = (i,prev_high)
        for label, active in (
                ("unconditional", True), ("surge_45m", simple[i]>=.015), ("drop_45m", simple[i]<=-.015),
                ("three_up", sign>0 and streak==3), ("three_down", sign<0 and streak==3),
                ("breakout_24h", breakout), ("breakdown_24h", c < prev_low),
                ("squeeze_breakout", breakout and vol < .75*base_vol),
                ("squeeze_breakdown", c < prev_low and vol < .75*base_vol),
                ("volume_expansion_up", prior_volume>0 and volumes[i]>2*prior_volume and sign>0),
                ("volume_expansion_down", prior_volume>0 and volumes[i]>2*prior_volume and sign<0),
                ("low_volume", prior_volume>0 and volumes[i]<.5*prior_volume),
                ("sharp_drop_bounce", simple[i-1]<-.015 and simple[i]>.004),
                ("breakout_retest", pullback), ("vol_expansion", vol>1.5*base_vol),
                ("vol_contraction", vol<.67*base_vol),
                ("bull_alignment", actual[6]=="bull" and actual[8]==1 and actual[2]==1),
                ("bear_countertrend", actual[6]=="bear" and actual[8]==1 and actual[2]==1),
                ("young_bull", actual[6]=="bull" and state_age<=32),
                ("persistent_bull", actual[6]=="bull" and state_age>=7*32),
                ("persistent_bear", actual[6]=="bear" and state_age>=7*32)):
            if active:
                events[label].append(i)
    # Market-state durations retain censored boundary runs.
    duration = defaultdict(list)
    state_rows = []
    for n,(regime,group) in enumerate(groupby(points,key=lambda x:x[6])):
        g = list(group)
        state_rows.append((regime,g[0][0],g[-1][0]+M45,len(g)))
    for reg,a,b,n in state_rows[1:-1]:
        duration[reg].append(n/32)
    durations = {k: {"runs":len(v),"median_days":median(v),"max_days":max(v)} for k,v in duration.items()}
    return candles, events, durations, atr_pct, first


def event_table(rows, points, candles, events, splits, first):
    prices = [(float(r[1]),float(r[2]),float(r[3]),float(r[4])) for r in rows]
    out = []
    # Monotonic queues compute exact interval extrema in time linear in the source history.
    from collections import deque
    rng = random.Random(3684)
    for hold in HORIZONS:
        width = hold*3
        lows = [None]*len(prices)
        q = deque()
        for ix, (_,_,low,_) in enumerate(prices):
            if q and q[0] <= ix-width:
                q.popleft()
            while q and prices[q[-1]][2] >= low:
                q.pop()
            q.append(ix)
            if ix+1>=width:
                lows[ix+1-width] = prices[q[0]][2]
        for split, (lo, hi) in splits.items():
            for label, indexes in sorted(events.items()):
                chosen = []
                prev_end = lo
                for i in indexes:
                    entry = points[i][0]+STEP
                    finish = entry+hold*M45
                    if entry < lo or finish > hi or entry < prev_end:
                        continue
                    k=(entry-first)//STEP
                    last=k+width-1
                    if last>=len(prices):
                        continue
                    cost = (1-FEE)/(1+FEE)
                    o=prices[k][0]
                    gross = prices[last][3]/o-1
                    chosen.append((datetime.fromisoformat(stamp(entry)).strftime("%Y-%m"),
                                   gross, (1+gross)*cost-1, lows[k]/o-1))
                    prev_end=finish
                if not chosen:
                    continue
                gross = [x[1] for x in chosen]
                net = [x[2] for x in chosen]
                mae = sorted(x[3] for x in chosen)
                ordered = sorted(net)
                wins = [x for x in net if x>0]
                loss = [-x for x in net if x<0]
                month = defaultdict(float)
                for m,_,n,_ in chosen:
                    month[m] += n
                vals = list(month.values())
                p = None
                # Fixed-seed month-cluster sign flip. Tests positive absolute net expectancy, not BTC alpha.
                if len(vals)>=12 and len(net)>=30:
                    observed = sum(vals)
                    p = (1+sum(sum(x if rng.getrandbits(1) else -x for x in vals)>=observed
                               for _ in range(4095)))/4096
                out.append(dict(split=split,signal=label,hours=hold*.75,n=len(chosen),
                    cluster_months=len(vals),gross_mean_pct=100*mean(gross),net_mean_pct=100*mean(net),
                    p10_pct=100*ordered[int(.1*(len(ordered)-1))],p50_pct=100*median(net),
                    p90_pct=100*ordered[int(.9*(len(ordered)-1))],win_pct=100*len(wins)/len(net),
                    payoff=mean(wins)/mean(loss) if wins and loss else None,
                    mae_p10_pct=100*mae[int(.1*(len(mae)-1))],worst_mae_pct=100*min(mae),p_value=p,holm_p=None,
                    excess_vs_unconditional_pp=None))
    base = {(r["split"],r["hours"]):r["net_mean_pct"] for r in out if r["signal"]=="unconditional"}
    for r in out:
        r["excess_vs_unconditional_pp"] = r["net_mean_pct"]-base[r["split"],r["hours"]]
    for split in splits:
        tests = sorted((r for r in out if r["split"]==split and r["p_value"] is not None),
                       key=lambda r:r["p_value"])
        adjusted = 0.
        for n,r in enumerate(tests):
            adjusted = max(adjusted,min(1.,(len(tests)-n)*r["p_value"]))
            r["holm_p"]=adjusted
    csv_write(ROOT/"events.csv",out)
    return out

def treasury_rates():
    try:
        with urlopen(TREASURY, timeout=12) as stream:
            lines = stream.read().decode().splitlines()
        rates = [(day_start(r["observation_date"])+2*DAY,float(r["DTB3"])/100)
                 for r in csv.DictReader(lines) if r["DTB3"] not in ("",".")]
        return rates, None
    except (OSError, ValueError, KeyError) as err:
        return [], str(err)


def metrics(nav, exposure, fees, orders, clock, periods):
    peak = 1.
    dd = 0.
    for value in nav:
        peak = max(peak, value)
        dd = max(dd, 1-value/peak)
    daily = [v for t,v in zip(clock,nav) if t % DAY==0]
    dr = [b/a-1 for a,b in zip(daily,daily[1:])]
    annual_vol = stdev(dr)*math.sqrt(365.2425) if len(dr)>1 else 0.
    monthly = {}
    last = nav[0]
    for month, group in groupby(zip(clock[1:],nav[1:]),key=lambda kv: datetime.fromisoformat(stamp(kv[0])).strftime("%Y-%m")):
        end = list(group)[-1][1]
        monthly[month] = 100*(end/last-1)
        last = end
    years = periods/(365.2425*DAY)
    return dict(net_pct=round(100*(nav[-1]-1),5),cagr_pct=round(100*(nav[-1]**(1/years)-1),5),
                drawdown_pct=round(100*dd,5),annual_vol_pct=round(100*annual_vol,5),
                avg_exposure_pct=round(100*mean(exposure),5),fees_pct=round(100*fees,5),orders=orders,
                months_positive=sum(r>0 for r in monthly.values()),months=len(monthly),monthly_pct=monthly)


def schedules(points, first, setup_events=None):
    daily_vol = 0.
    previous = None
    direction = 0.
    consecutive = 0
    hline = []
    legacy = []
    volatility = []
    regime = []
    for p in points:
        t, price = p[:2]
        if t==p[11]:
            if previous:
                daily_vol = .94*daily_vol+.06*(price/previous-1)**2
            previous = price
        if t==p[12]:
            consecutive = consecutive+1 if p[8]>0 else 0
            direction = p[8]
        daily_ceiling = 0. if p[6] in ("warmup","bear") else 1. if p[6]=="bull" else .4
        hline.append((daily_ceiling if direction>0 else 0., daily_ceiling if consecutive>=2 else 0.))
        legacy.append(p[7]/.1)
        volatility.append(max(.008,math.sqrt(daily_vol)))
        regime.append(daily_ceiling)
    results = {"legacy":legacy,"lowturn":[x[0] for x in hline],"lowturn_confirm":[x[1] for x in hline]}
    if setup_events:
        active_through = -1
        out = []
        indexes, horizon = setup_events
        chosen = set(indexes)
        for i in range(len(points)):
            if i in chosen and i>=active_through and regime[i]>0:
                active_through = i+horizon
            out.append(regime[i] if i<active_through else 0.)
        results["event_candidate"] = out
    return results, volatility


def execution(points, opens, a, b, schedule, vol, scale, mode, sizing, fee, rates=None):
    p = Position()
    nav = [1.]
    exposure = []
    clock = [points[a][0]]
    wanted = None
    time = 0
    for i in range(a,b):
        t,close = points[i][:2]
        if schedule is None:
            cash_rate = rates[i] if rates else 0.
            p.cash *= (1+cash_rate)**(M45/(365.2425*DAY))
        elif schedule == "hold":
            if i==a:
                p.trade(scale,opens[t+STEP],fee)
        else:
            raw = schedule[i]
            factor = 1.
            if sizing=="vol":
                factor=min(1.75,.025/vol[i])
            elif sizing=="risk":
                factor=min(1.5,.025/vol[i])*(1. if points[i][6]=="bull" else .5)
            daily_mode = sizing!="fixed"
            target = min(1.,scale*raw*factor)
            if raw == 0. or raw!=wanted or (daily_mode and t==points[i][11]):
                wanted=raw
                if target!=p.target:
                    weight=p.qty*close/(p.cash+p.qty*close)
                    execute = mode!="threshold" or target==0. or abs(target-weight)>=.025
                    if execute:
                        price=opens[t+STEP]
                        # A one-bar bounded wait uses only executable sampled opens in chronological order.
                        if mode=="wait":
                            buy=target>weight
                            touches=price<=close*.999 if buy else price>=close*1.001
                            if not touches:
                                price=opens[t+2*STEP]
                        p.trade(target,price,fee)
        close_next=points[i+1][1]
        mark=p.cash+p.qty*close_next
        exposure.append(p.qty*close_next/mark)
        time=points[i+1][0]
        if i+1==b:
            p.trade(0.,close_next,fee)
            mark=p.cash
        nav.append(mark)
        clock.append(time)
    return metrics(nav,exposure,p.fees,p.orders,clock,time-clock[0])


def fixed_choice(event_rows):
    # The only candidate may come from development, subject to one fixed validation check.
    permitted = {"breakout_24h","squeeze_breakout","breakout_retest","sharp_drop_bounce","three_down",
                 "volume_expansion_up","vol_contraction"}
    eligible = [x for x in event_rows if x["split"]=="development" and x["signal"] in permitted
                and 24<=x["hours"]<=168 and x["n"]>=60 and x["holm_p"] is not None
                and x["holm_p"]<=.05 and x["excess_vs_unconditional_pp"]>.20]
    if not eligible:
        return None
    candidate=max(eligible,key=lambda x:(x["excess_vs_unconditional_pp"],x["n"]))
    later=next((x for x in event_rows if x["split"]=="validation" and x["signal"]==candidate["signal"]
                and x["hours"]==candidate["hours"]),None)
    passed = bool(later and later["n"]>=20 and later["net_mean_pct"]>0 and later["excess_vs_unconditional_pp"]>0)
    return {"development":candidate,"validation": later,"passed_validation":passed}


def experiment(points, opens, events, event_rows, splits):
    clock=[p[0] for p in points]
    idx={name:(bisect_left(clock,a),bisect_left(clock,b)) for name,(a,b) in splits.items()}
    chosen=fixed_choice(event_rows)
    setup=None
    if chosen and chosen["passed_validation"]:
        c=chosen["development"]
        setup=(events[c["signal"]],int(c["hours"]/.75))
    signals,vol=schedules(points,clock[0],setup)
    rates,err=treasury_rates()
    timings=[r[0] for r in rates]
    cash_rates=[rates[n][1] if (n:=bisect_right(timings,t)-1)>=0 else 0. for t in clock]
    variants={"legacy":("legacy","direct","fixed"),"lowturn_direct":("lowturn","direct","fixed"),
              "lowturn_wait":("lowturn","wait","fixed"),"lowturn_threshold":("lowturn","threshold","fixed"),
              "lowturn_confirm":("lowturn_confirm","direct","fixed"),
              "lowturn_vol":("lowturn","direct","vol"),"lowturn_risk":("lowturn","direct","risk"),
              "hold_same_risk":("hold","direct","fixed")}
    if setup:
        variants["event_candidate"]=("event_candidate","direct","fixed")
    x,y=idx["development"]
    risk_target = execution(points,opens,x,y,signals["legacy"],vol,.1,"direct","fixed",FEE)["annual_vol_pct"]
    scales={}
    for name,(base,mode,sizing) in variants.items():
        schedule=signals.get(base,base)
        scale=.1
        # Fixed two passes with development-only volatility, never the historical comparison period.
        for _ in range(2):
            observed=execution(points,opens,x,y,schedule,vol,scale,mode,sizing,FEE)["annual_vol_pct"]
            if observed>0 and name!="legacy":
                scale=max(.001,min(.5,scale*risk_target/observed))
        scales[name]=scale
    tests={}
    for split,(a,b) in idx.items():
        tests[split]={}
        for fee in FEES:
            result={name:execution(points,opens,a,b,signals.get(base,base),vol,scales[name],mode,sizing,fee)
                    for name,(base,mode,sizing) in variants.items()}
            result["cash"]=execution(points,opens,a,b,None,vol,0.,"direct","fixed",fee)
            rates_cover=timings and timings[0]<=clock[a] and clock[b]-timings[-1]<=10*DAY
            result["treasury_3m_proxy"]=(execution(points,opens,a,b,None,vol,0.,"direct","fixed",fee,cash_rates)
                                        if rates_cover else None)
            tests[split][str(fee)]=result
    rolling={}
    for year in (2024,2025,2026):
        for month in (1,7):
            start=f"{year}-{month:02d}-01"
            end=f"{year+(month==7)}-{7 if month==1 else 1:02d}-01"
            a,b=bisect_left(clock,day_start(start)),min(bisect_left(clock,day_start(end)),idx["historical_reused"][1])
            if b-a<32*30:
                continue
            rolling[start]={name:{k:v for k,v in execution(points,opens,a,b,signals.get(base,base),vol,scales[name],
                    mode,sizing,FEE).items() if k!="monthly_pct"} for name,(base,mode,sizing) in variants.items()}
    return {"baseline_development_vol_pct":risk_target,"scale":scales,"tests":tests,"rolling_frozen_rules":rolling,
            "event_candidate":chosen,"treasury_source":TREASURY,"treasury_error":err,
            "treasury_spans":[stamp(timings[0]),stamp(timings[-1])] if timings else None}


def main():
    ROOT.mkdir(exist_ok=True)
    rows,audit=load_prices()
    points=features(rows)
    opens={r[0]:float(r[1]) for r in rows}
    clock=[p[0] for p in points]
    last=max(t for t in clock if t % DAY==0)
    splits={"development":(day_start("2022-02-01"),day_start("2024-01-01")),
            "validation":(day_start("2024-01-01"),day_start("2025-01-01")),
            "historical_reused":(day_start("2025-01-01"),last)}
    candles,events,durations,_,first=discover(rows,points)
    table=event_table(rows,points,candles,events,splits,first)
    print(f"events: {len(table)}",flush=True)
    start=bisect_left(clock,splits["historical_reused"][0])
    stop=bisect_left(clock,last)
    trade=attribution(points,rows,opens,start,stop)
    print("attribution reconciled",flush=True)
    models=experiment(points,opens,events,table,splits)
    sig={split:[r for r in table if r["split"]==split and r["signal"]!="unconditional" and r["holm_p"] is not None and r["holm_p"]<=.05] for split in splits}
    write_json(ROOT/"summary.json",dict(audit=audit,fees_per_side=FEES,comparison_to=stamp(last),
               attribution=trade,market_regime_duration=durations,significant_positive_net=sig,execution=models))
    print("research complete",flush=True)


if __name__=="__main__":
    main()
