"""One prospective heartbeat, paper reconciliation and the shared daily interface."""
import argparse
import csv
import json
import math
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from urllib.error import HTTPError

from btc_live_sources import collect, epoch, read
from research_paper_edge import RULES, ROOT as PAPER
from research_prices import local, stamp, write_json

OPS=Path("research/ops")
SNAPS=OPS/"receipts"
SIGNALS=Path("research/multiframe_shadow/paper_signals")
DAYS=OPS/"daily"
SLOT=2700
DAY=86400
EVENT=os.getenv("GITHUB_EVENT_NAME","local")
REPO=os.getenv("GITHUB_REPOSITORY","lxiaomng-lgtm/btc-kline-data")


def jsonfile(path, default=None):
    if path.exists():
        return json.loads(path.read_text())
    return default


def slot_path(t):
    return local(t//SLOT*SLOT).strftime("%Y-%m-%dT%H%M")+".json"


def output(name,value):
    p=os.getenv("GITHUB_OUTPUT")
    if p:
        with open(p,"a") as f:
            f.write(f"{name}={value}\n")


def poll_gate():
    t=time.time()
    name=slot_path(t)
    output("make_signal",str(not (PAPER/name).exists() and not (SIGNALS/name).exists()).lower())
    output("slot",name)
    print("Current allowed slot:",name)


def wallet():
    records={}
    fees=Counter()
    fills=Counter()
    identity_issues=[]
    bad_slots=set()
    delays=[]
    previous={}
    latest=None
    expected_keys=set(RULES)
    for path in sorted(PAPER.glob("*.json")):
        p=jsonfile(path)
        t=int(epoch(p["signal_at"]))
        if t in records:
            identity_issues.append([path.name,"duplicate_slot"])
            bad_slots.add(t)
        records[t]=p
        if set(p["accounts"])!=expected_keys:
            identity_issues.append([path.name,"account_schema_drift"])
            bad_slots.add(t)
        q=p.get("quote")
        pub=epoch(p["confirmed_published_at"])
        delays.append(pub-t)
        if q:
            e=epoch(q["exchange_at"])
            received=epoch(q["received_at"])
            if not t<=pub<e<=received or received-e>20 or "signal_run_id" in p and received>=t+SLOT:
                identity_issues.append([path.name,"invalid_publication_quote_clock"])
                bad_slots.add(t)
            mid=(q["bid"]+q["ask"])/2
        for k,a in p["accounts"].items():
            start=p["initial_usd_each"]
            past=previous.get(k,{"cash_usd":start,"position_btc":0.,"peak_usd":start})
            qty=a["signed_quantity_btc"]
            cost=a["fee_usd"]
            pricing=a["fill_price_usd"]
            if abs(past["cash_usd"]-qty*(pricing or 0)-cost-a["cash_usd"])>1e-6 or abs(past["position_btc"]+qty-a["position_btc"])>1e-9:
                identity_issues.append([path.name,k,"cash_or_btc_mismatch"])
                bad_slots.add(t)
            if qty and (not q or not pricing or abs(abs(qty)*pricing*p["assumed_taker_fee"]-cost)>1e-6):
                identity_issues.append([path.name,k,"fill_or_fee_mismatch"])
                bad_slots.add(t)
            if q and abs(a["cash_usd"]+a["position_btc"]*mid-a["nav_usd"])>1e-6:
                identity_issues.append([path.name,k,"nav_mismatch"])
                bad_slots.add(t)
            peak=max(past["peak_usd"],a["nav_usd"])
            if abs(a["peak_usd"]-peak)>1e-6 or abs(a["drawdown"]-(1-a["nav_usd"]/peak))>1e-8:
                identity_issues.append([path.name,k,"peak_or_drawdown_mismatch"])
                bad_slots.add(t)
            fees[k]+=cost
            fills[k]+=qty!=0
        previous=p["accounts"]
        latest=p
    stats={}
    if latest:
        start=latest["initial_usd_each"]
        buy=100*(latest["accounts"]["buy_hold_10"]["nav_usd"]/start-1)
        cash=100*(latest["accounts"]["cash"]["nav_usd"]/start-1)
        for k,a in latest["accounts"].items():
            ret=100*(a["nav_usd"]/start-1)
            stats[k]={"initial_usd":start,"btc":a["position_btc"],"cash_usd":a["cash_usd"],
                "nav_usd":a["nav_usd"],"return_pct":ret,"max_drawdown_pct":100*a["maximum_drawdown"],
                "excess_vs_buy_hold_pp":ret-buy,"excess_vs_cash_pp":ret-cash,
                "cumulative_simulated_fee_usd":fees[k],"fill_count":fills[k],
                "last_fee_usd":a["fee_usd"],"last_signed_btc":a["signed_quantity_btc"],
                "last_fill_price_usd":a["fill_price_usd"],"target_weight":a["target_weight"]}
    return records,bad_slots,identity_issues,stats,delays,latest


def candle_state(record):
    if not record or "value" not in record:
        return {}
    rows=record["value"]["rows"]
    clocks={r[0]:r for r in rows}
    now=epoch(record["acquired_at"])
    out={}
    for name,step in (("15m",900),("45m",2700),("3h",10800),("6h",21600),("1d_08",86400)):
        due=int(now)//step*step
        data=None
        end=None
        for close in (due,due-step):
            block=[clocks.get(t) for t in range(close-step,close,900)]
            if all(block):
                end=close
                data={"open":block[0][1],"high":max(r[2] for r in block),"low":min(r[3] for r in block),
                      "close":block[-1][4],"volume_btc":sum(r[5] for r in block)}
                break
        out[name]={"expected_closed_at":stamp(due),"actual_closed_at":stamp(end) if end is not None else None,
                   "status":"valid" if end==due else "stale" if data else "missing",
                   "forming_since":stamp(due),"ohlcv":data,"alignment":"Asia/Shanghai 08:00 anchor"}
    return out


def latest_etf():
    p=jsonfile(Path("research/etf.json"))
    if not p or not p.get("latest_double_verified"):
        return {"status":"unavailable"}
    designation=p["latest_double_verified"]
    with Path("research/etf_history.csv").open() as f:
        row=next((x for x in csv.DictReader(f) if x["ny_session"]==designation["date"] and x["status"]=="FINAL"),None)
    if not row:
        return {"status":"unavailable","reason":"latest verified ETF row absent"}
    observed=p["observed_at"]
    age=max(0,time.time()-epoch(observed))
    return {"status":"FINAL","us_session":row["ny_session"],"observed_at":observed,"observation_age_seconds":age,
            "total_usd_m":float(row["total_usd_m"]),"source_last_session_listed":p["through"],
            "freshness":"review_etf_release_calendar" if age>36*3600 else "recent_observation",
            "funds_usd_m":{k:float(v) for k,v in row.items() if v and k.isupper()},"sources":p["sources"]}


def runner_history(now):
    token=os.getenv("GH_TOKEN")
    now_local=local(now)
    a=now_local.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
    url=f"https://api.github.com/repos/{REPO}/actions/workflows/multiframe-shadow.yml/runs"
    utc_a=datetime.fromtimestamp(a,timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
    utc_b=datetime.fromtimestamp(now,timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
    q={"branch":"main","created":f"{utc_a}..{utc_b}","per_page":100}
    try:
        found=[]
        for page in range(1,5):
            raw=read(url,{**q,"page":page},timeout=10,token=token)
            found+=raw["workflow_runs"]
            if len(found)>=raw["total_count"] or not raw["workflow_runs"]:
                break
        return {"events":dict(Counter(r["event"] for r in found)),
                "run_status":dict(Counter(r["conclusion"] or r["status"] for r in found)),
                "queue_wait_seconds":[max(0,epoch(r["run_started_at"])-epoch(r["created_at"]))
                    for r in found if r.get("run_started_at")],"api_total":raw["total_count"],
                "truncated":len(found)<raw["total_count"]}
    except (OSError,ValueError,KeyError) as err:
        return {"error":str(err)}


def quality(now,receipts,rec,bad,issues):
    cur=local(now).replace(hour=0,minute=0,second=0,microsecond=0)
    a=int(cur.timestamp())
    since=jsonfile(OPS/"protocol.json")["quality_start_bjt"]
    start=epoch(since)
    daily={}
    for i in range(31):
        day=a-i*DAY
        first=math.ceil(max(day,start)/SLOT)*SLOT
        stop=min(day+DAY,now-SLOT+1)
        tt=list(range(first,int(stop),SLOT))
        available=[t for t in tt if t in rec and rec[t].get("quote") and t not in bad]
        daily[stamp(day)[:10]]={"expected":len(tt),"valid":len(available),
            "missing_at":[stamp(t) for t in tt if t not in available],
            "coverage_pct":100*len(available)/len(tt) if tt else None}
    completed=list(daily.values())[1:31]
    ready=len(completed)==30 and all(x["expected"]==32 for x in completed)
    whole={"eligible_30_complete_days":ready,"expected":sum(x["expected"] for x in completed),
           "valid":sum(x["valid"] for x in completed)}
    whole["coverage_pct"]=100*whole["valid"]/whole["expected"] if whole["expected"] else None
    whole["accepted"]=bool(ready and whole["coverage_pct"]>=99 and not issues)
    current=[x for x in receipts if epoch(x["finished_at"])>=a]
    src=defaultdict(list)
    for x in current:
        for k,v in x["live"].items():
            src[k].append(v)
    data={}
    for k,obs in src.items():
        valid=[v for v in obs if v["status"]=="valid"]
        waits=[v["age_seconds"] for v in valid if v.get("age_seconds") is not None]
        clocks={v["event_at"] for v in valid}
        field="rows"
        tics={int(epoch(t[0])) for v in valid for t in v["value"].get(field,[])
              if t and isinstance(t[0],str) and a<epoch(t[0])<=now}
        if k=="bitstamp_15m":
            tics={int(epoch(t)) for v in valid for t in v["value"].get("close_times",[]) if a<epoch(t)<=now}
        width=900 if k=="bitstamp_15m" or "binance" in k and "cvd" in k else 300 if "taker_delta" in k else None
        expected=sum(t<=now for t in range(a+width,a+DAY+1,width)) if width else None
        data[k]={"expected_elapsed_bars":expected,"completeness_pct":100*len(tics)/expected if expected else None,
                 "live_checks":len(obs),"valid":len(valid),"failures":sum(x["status"]=="unavailable" for x in obs),
                 "stale":sum(x["status"]=="stale" for x in obs),"distinct_event_times":len(clocks),
                 "repeated_reads_same_event":len(valid)-len(clocks),
                 "archive_rows_seen_today":len(tics),"median_live_age_seconds":median(waits) if waits else None}
    today=daily[stamp(a)[:10]]
    pub=[epoch(p["confirmed_published_at"])-t for t,p in rec.items() if t>=a]
    return {"date_bjt":stamp(a)[:10],"recorded_at":stamp(now),
            "scheduler":{"expected_by_clock":sum(t<=now for t in range(a+120,a+DAY,600) if t>=start),
                         "receipts":len(current),"event_receipts":dict(Counter(x["event"] for x in current)),
                         "github_run_api":runner_history(now)},
            "paper_today":today,"paper_30d":whole,"paper_days":daily,
            "lifetime_paper_records":len(rec),"paper_reconciliation_issues":issues,"source_quality":data,
            "publish_delay_seconds":{"samples":len(pub),"median":median(pub) if pub else None,
                                     "maximum":max(pub,default=None)}}


def run_tick():
    from btc_forward import progress
    OPS.mkdir(exist_ok=True)
    SNAPS.mkdir(exist_ok=True)
    DAYS.mkdir(exist_ok=True)
    started=time.time()
    run=os.getenv("GITHUB_RUN_ID","local")
    attempt=os.getenv("GITHUB_RUN_ATTEMPT","1")
    history=[jsonfile(path) for path in sorted(SNAPS.glob("*.json"))]
    tokenized={x["id"] for x in history}
    number=f"{run}-{attempt}"
    if number in tokenized and run!="local":
        print("This run attempt already has a receipt",number)
        return
    live=collect()
    heartbeat=jsonfile(OPS/"external-heartbeat.json",{})
    for name,v in heartbeat.get("secondary_live",{}).items():
        if name not in ("binance_perpetual_cvd","binance_oi") or not v.get("event_at"):
            continue
        age=time.time()-epoch(v["event_at"])
        max_age=1100 if "cvd" in name else 1500
        if age>=-2 and epoch(v["acquired_at"])<=time.time()+2:
            live["secondary_cloud_"+name]={**v,"age_seconds":age,"max_age_seconds":max_age,
                                           "status":"valid" if age<=max_age and v["status"]=="valid" else "stale"}
    now=time.time()
    # Stable fixed JSON rows omit 192 OHLCV entries while capture retains auditable timestamps.
    rec,bad,issues,accounts,delays,paper=wallet()
    state=candle_state(live["bitstamp_15m"])
    parents=[x["id"] for x in history[-12:]]
    last3=state.get("3h",{}).get("ohlcv") if state.get("3h",{}).get("status")=="valid" else None
    br=live["bitstamp_15m"].get("value")
    if br:
        br["close_times"]=[stamp(t[0]+900) for t in br["rows"]]
        del br["rows"]
    item={"id":number,"run_id":run,"event":EVENT,"started_at":stamp(started),"finished_at":stamp(now),
          "paper_step_outcomes":{k.lower():os.getenv(k) for k in ("SIGNAL_OUTCOME","PUBLISH_SIGNAL_OUTCOME","FILL_OUTCOME","COMMIT_FILL_OUTCOME")},
          "paper_attempt":jsonfile(Path("paper_attempt.json")),"live":live,"seen_committed_receipts":parents,
          "last_completed_3h":last3["close"]/last3["open"]-1 if last3 else None}
    dest=SNAPS/(local(now).strftime("%Y-%m-%dT%H%M%S-")+number+".json")
    # All receipt inputs were available at the observed capture clock.
    if not dest.exists():
        write_json(dest,item)
    hygiene=quality(now,[*history,item],rec,bad,issues)
    today=hygiene["date_bjt"]
    write_json(DAYS/(today+".json"),hygiene)
    forward=progress([*history,item])
    write_json(OPS/"forward_progress.json",forward)
    market={}
    for k,v in live.items():
        m={**v}
        if m.get("event_at") and m.get("max_age_seconds"):
            m["valid_until_bjt"]=stamp(epoch(m["event_at"])+m["max_age_seconds"])
        if m.get("value"):
            m["value"]={a:b for a,b in m["value"].items() if a not in ("rows","close_times")}
        market[k]=m
    price=market.get("bitstamp_price")
    fallback={"source":"Bitstamp closed 15m","value":state.get("15m",{}).get("ohlcv"),
              "event_at":state.get("15m",{}).get("actual_closed_at") or None}
    if price and price.get("status")!="valid":
        fallback["status"]="closed_bar_only"
    else:
        fallback=None
    write_json(Path("btc_daily.json"),{"schema":"btc_daily_v1","generated_at_bjt":stamp(now),"source_run_id":run,
        "price":price,"price_fallback":fallback,"candles":state,"etf":latest_etf(),
        "live_market":market,"settled_flows_reference":"research/flow/coverage.json",
        "shadow":{"accounts":accounts,"latest_slot":paper["signal_at"] if paper else None,
                  "confirmed_published_at":paper["confirmed_published_at"] if paper else None,
                  "quote":paper.get("quote") if paper else None,"research_cost":{"fee_per_side":.004,"slippage_per_side":.0005,"actual_account_fee":"unverified"}},
        "quality":{"file":str(DAYS/(today+".json")),"today":hygiene["paper_today"],"last30d":hygiene["paper_30d"],
                   "reconciliation_errors":len(issues),"scheduler":hygiene["scheduler"],
                   "kline_15m":market["bitstamp_15m"],"funding_live_validity":{k:v["status"] for k,v in market.items() if k!="bitstamp_15m"}},
        "external_heartbeat":{k:v for k,v in heartbeat.items() if k!="secondary_live"},
        "forward_research":forward,"formal_account_policy":"A/B/C frozen; no executions from this dataset"})
    print("Published candidate receipt",dest,"live status:",{k:v["status"] for k,v in live.items()},flush=True)


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--gate",action="store_true")
    args=p.parse_args()
    if args.gate:
        poll_gate()
    else:
        run_tick()


if __name__=="__main__":
    main()
