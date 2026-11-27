"""Frozen, receipt-gated forward forecast comparison. Nothing fits until data matures."""
import hashlib
import json
import math
import random
from collections import defaultdict
from pathlib import Path
from statistics import mean

from btc_live_sources import epoch
from research_prices import stamp

P=Path("research/ops/protocol.json")
DAY=86400


def get_live(rec,key):
    x=rec["live"].get(key,{})
    return x["value"] if x.get("status")=="valid" else None


def feature_vector(r, history, spec):
    ret=r.get("last_completed_3h")
    if ret is None:
        return None
    spot=get_live(r,"binance_spot_cvd")
    perp=get_live(r,"okx_btc_contract_taker_delta")
    oi=get_live(r,"okx_oi")
    if not spot or not perp or perp["imbalance_3h"] is None or not oi or not oi.get("btc"):
        return None
    rows=spot["rows"][-12:]
    tt=[epoch(x[0]) for x in rows]
    if len(tt)!=12 or any(b-a!=900 for a,b in zip(tt,tt[1:])):
        return None
    volume=sum(x[2] for x in rows)
    if not volume:
        return None
    # Separate venues, with a bounded age disparity for the two closed 3h windows.
    if abs(tt[-1]-epoch(r["live"]["okx_btc_contract_taker_delta"]["event_at"]))>1200:
        return None
    t=epoch(r["finished_at"])
    previous=None
    for x in reversed(history):
        age=t-epoch(x["finished_at"])
        if age>12600:
            break
        old=get_live(x,"okx_oi")
        if age>=10800 and old and old.get("btc"):
            previous=x
            break
    if previous is None:
        return None
    return (ret,sum(x[1] for x in rows)/volume,perp["imbalance_3h"],oi["btc"]/get_live(previous,"okx_oi")["btc"]-1)


def solve(a,b):
    """Solve the small positive-definite normal equations with pivoting."""
    m=[list(row)+[y] for row,y in zip(a,b)]
    for i in range(len(m)):
        pivot=max(range(i,len(m)),key=lambda r:abs(m[r][i]))
        m[i],m[pivot]=m[pivot],m[i]
        factor=m[i][i]
        m[i]=[x/factor for x in m[i]]
        for j,row in enumerate(m):
            if j!=i:
                v=row[i]
                m[j]=[x-v*y for x,y in zip(row,m[i])]
    return [row[-1] for row in m]


def train(rows,n,penalty):
    columns=list(zip(*(r[1][:n] for r in rows)))
    mu=[mean(c) for c in columns]
    sd=[max(1e-9,math.sqrt(mean((x-v)**2 for x in c))) for c,v in zip(columns,mu)]
    y=mean(r[2] for r in rows)
    xx=[[(x-v)/s for x,v,s in zip(r[1][:n],mu,sd)] for r in rows]
    a=[[sum(x[i]*x[j] for x in xx)+(penalty if i==j else 0) for j in range(n)] for i in range(n)]
    b=[sum(x[i]*(r[2]-y) for x,r in zip(xx,rows)) for i in range(n)]
    beta=solve(a,b)
    return lambda x:y+sum(b*(v-m)/s for b,v,m,s in zip(beta,x,mu,sd))


def evaluate(rows,predict,spec):
    nav=peak=1.
    dd=0.
    orders=0
    accuracy=[]; continuation=[]; reversal=[]; errors=[]
    for t,x,y,net in rows:
        p=predict(x)
        accuracy.append((p>0)==(y>0))
        errors.append((p-y)**2)
        if abs(x[0])>=spec["continue_or_reverse_threshold"]:
            predicted_continue=p*x[0]>0
            actual_continue=y*x[0]>0
            if actual_continue:
                continuation.append(predicted_continue)
            elif y:
                reversal.append(not predicted_continue)
        if p>2*(spec["fee_per_side"]+spec["slippage_per_side"]):
            nav*=1+net
            orders+=1
            peak=max(peak,nav)
            dd=max(dd,1-nav/peak)
    return {"n":len(rows),"direction_accuracy":mean(accuracy),"mse":mean(errors),
            "continuation_recall":mean(continuation) if continuation else None,
            "reversal_recall":mean(reversal) if reversal else None,
            "round_trips":orders,"net_pct":100*(nav-1),"trade_close_max_drawdown_pct":100*dd},errors


def p_value(rows,errors):
    weeks=defaultdict(float)
    for row,value in zip(rows,errors):
        weeks[int(row[0])//(7*DAY)]+=value
    if len(weeks)<8:
        return None
    groups=list(weeks.values())
    rng=random.Random(20261127)
    target=sum(groups)
    return (1+sum(sum(v if rng.getrandbits(1) else -v for v in groups)>=target for _ in range(4095)))/4096


def progress(history):
    raw=P.read_bytes()
    spec=json.loads(raw)
    sig=[]
    observed=[]
    for r in history:
        v=feature_vector(r,observed,spec)
        if v is not None:
            sig.append((r,v))
        observed.append(r)
    result={"protocol":spec["protocol"],"protocol_sha256":hashlib.sha256(raw).hexdigest(),
            "receipts":len(history),"eligible_feature_sets":len(sig),"comparisons":{},
            "status":"accumulating_no_strategy_promotion","minimum_per_phase":spec["minimum_independent_samples_per_phase"]}
    if not sig:
        return result
    begin=epoch(sig[0][0]["finished_at"])+spec["decision_delay_seconds"]
    dev=begin+spec["development_calendar_days"]*DAY
    val=dev+spec["validation_calendar_days"]*DAY
    result["calendar"]={"development_start":stamp(begin),"validation_start":stamp(dev),"forward_start":stamp(val)}
    marks=[]
    for record in history:
        quote=get_live(record,"bitstamp_price")
        if quote:
            entry=record["live"]["bitstamp_price"]
            marks.append((epoch(entry["event_at"]),epoch(entry["acquired_at"]),quote,set(record["seen_committed_receipts"])))
    from bisect import bisect_left, bisect_right
    marks.sort(key=lambda m:m[0])
    clocks=[m[0] for m in marks]
    correction=[]
    for h in spec["outcome_horizons_seconds"]:
        last_exit=0.
        bins=defaultdict(list)
        for original,x in sig:
            eligible=epoch(original["finished_at"])+spec["decision_delay_seconds"]
            if eligible<last_exit:
                continue
            cursor=bisect_left(clocks,eligible)
            limit=bisect_right(clocks,eligible+spec["max_entry_wait_seconds"])
            entry=next((m for m in marks[cursor:limit] if original["id"] in m[3]),None)
            if not entry:
                continue
            cursor=bisect_left(clocks,entry[0]+h)
            if cursor>=len(marks) or marks[cursor][0]>entry[0]+h+spec["max_exit_wait_seconds"]:
                continue
            end=marks[cursor]
            a,b=entry[0],end[0]
            phase="development" if b<dev else "validation" if a>=dev and b<val else "forward" if a>=val else None
            if not phase:
                continue
            buy,sell=entry[2]["ask"],end[2]["bid"]
            fee=spec["fee_per_side"]
            slip=spec["slippage_per_side"]
            mid_a=mean([entry[2]["ask"],entry[2]["bid"]])
            mid_b=mean([end[2]["ask"],end[2]["bid"]])
            net=sell*(1-fee)*(1-slip)/(buy*(1+fee)*(1+slip))-1
            bins[phase].append((a,x,mid_b/mid_a-1,net))
            last_exit=b
        cell={"completed_independent_samples":{k:len(bins[k]) for k in ("development","validation","forward")}}
        result["comparisons"][f"{h//3600}h"]=cell
        minimum=spec["minimum_independent_samples_per_phase"]
        now=epoch(history[-1]["finished_at"])
        if now<val or min(len(bins["development"]),len(bins["validation"]))<minimum:
            cell["status"]="waiting_for_development_and_validation"
            continue
        a=train(bins["development"],1,spec["ridge_penalty"])
        b=train(bins["development"],4,spec["ridge_penalty"])
        for phase in ("validation","forward"):
            batch=bins[phase]
            if len(batch)<minimum:
                continue
            base,loss1=evaluate(batch,a,spec)
            funds,loss2=evaluate(batch,b,spec)
            p=p_value(batch,[x-y for x,y in zip(loss1,loss2)])
            r={"price":base,"price_and_live_flow":funds,"incremental_mse_one_sided_cluster_p":p}
            cell[phase]=r
            if p is not None:
                correction.append((p,r))
        cell["status"]="review" if "forward" in cell else "waiting_for_independent_forward"
    adjusted=0.
    for i,(p,r) in enumerate(sorted(correction,key=lambda x:x[0])):
        adjusted=max(adjusted,min(1.,p*(len(correction)-i)))
        r["holm_p"]=adjusted
    return result
