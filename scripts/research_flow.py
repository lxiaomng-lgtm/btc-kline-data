"""Acquire, normalize, and evaluate venue-specific BTC capital-flow observations."""
import argparse
import csv
import gzip
import io
import json
import math
import os
import random
import re
import time
from bisect import bisect_right
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median, correlation
from threading import Lock
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zipfile import ZipFile

from research_prices import load_prices, stamp, write_json

UTC = timezone.utc
STEP = 900
DAY = 86400
VENUES = {"spot": "spot", "perp": "futures/um"}
VISION = "https://data.binance.vision/data"
OUT = Path("research/flow")
CACHE = Path(os.getenv("BTC_FLOW_CACHE", ".flow-cache"))
EXPORT = Path("flow-export")
PERIODS = {"15m": 900, "45m": 2700, "3h": 10800, "6h": 21600, "1d": 86400}
HORIZONS = (2700, 10800, 21600, 43200, DAY, 3*DAY, 7*DAY)
FEE = .0045
START = date(2021, 11, 1)
SPLITS = {"development": ("2022-01-01","2024-01-01"), "validation": ("2024-01-01","2025-01-01"),
          "reused_historical": ("2025-01-01","2027-01-01")}
COINBASE = "https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD/candles"
FEATURES = ("spot_imbalance", "perp_imbalance", "oi_change", "funding", "premium_bps", "liquidation_usd", "etf_musd")


def utc(t):
    return datetime.fromtimestamp(t, UTC).isoformat()


def epoch(s):
    return int(datetime.fromisoformat(s).timestamp())


def window(d):
    return int(datetime.combine(d, datetime.min.time(), UTC).timestamp())


def get(url, params=None, method="GET", limit=24):
    if params:
        url += "?" + urlencode(params)
    req = Request(url, headers={"User-Agent":"BTC flow research","Cache-Control":"no-cache"}, method=method)
    with urlopen(req, timeout=limit) as r:
        return r.read() if method=="GET" else {"status":r.status,"bytes":r.headers.get("Content-Length")}


def file_rows(url):
    name = url.rsplit("/",1)[1]
    directory = CACHE / ("spot" if "/spot/" in url else "um")
    directory.mkdir(parents=True,exist_ok=True)
    path=directory/name
    if path.exists():
        payload=path.read_bytes()
    else:
        for attempt in range(2):
            try:
                payload=get(url)
                break
            except (TimeoutError,HTTPError) as e:
                if attempt or isinstance(e,HTTPError) and e.code not in (429,500,502,503,504):
                    raise
                time.sleep(1)
    with ZipFile(io.BytesIO(payload)) as zip_file:
        item, = zip_file.namelist()
        records = list(csv.reader(io.StringIO(zip_file.read(item).decode("utf-8-sig"))))
    if not path.exists():
        tmp=path.with_suffix(".tmp")
        tmp.write_bytes(payload)
        tmp.replace(path)
    return records


def decode_kline(row):
    raw=int(row[0]); scale=1000000 if raw>=10**15 else 1000
    t=raw//scale
    o,h,l,c,v,buy=map(float,(*row[1:6],row[9]))
    if (raw%(STEP*scale) or (int(row[6])+1)//scale != t+STEP or not all(map(math.isfinite,(o,h,l,c,v,buy)))
        or min(o,h,l,c)<=0 or not l<=min(o,c)<=max(o,c)<=h or not 0<=buy<=v):
        raise ValueError(f"Invalid 15m source candle {t}")
    return t,(c,v,2*buy-v)


def month_keys(today):
    d=START
    while d<today.replace(day=1):
        yield d.strftime("%Y-%m")
        d=d.replace(year=d.year+(d.month==12),month=d.month%12+1)


def acquire(today, monitor=False):
    jobs={}
    this_month=today.replace(day=1)
    for kind,prefix in VENUES.items():
        for month in (() if monitor else month_keys(today)):
            jobs[kind,month]=f"{VISION}/{prefix}/monthly/klines/BTCUSDT/15m/BTCUSDT-15m-{month}.zip"
        first_day=today-timedelta(days=3) if monitor else this_month
        for n in range((today-first_day).days):
            d=first_day+timedelta(days=n)
            jobs[kind,str(d)]=f"{VISION}/{prefix}/daily/klines/BTCUSDT/15m/BTCUSDT-15m-{d}.zip"
    for month in (() if monitor else month_keys(today)):
        jobs["funding",month]=f"{VISION}/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-{month}.zip"
    for n in range(1,4 if monitor else (today-START).days+1):
        d=today-timedelta(days=n)
        jobs["oi",str(d)]=f"{VISION}/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-{d}.zip"

    def one(item):
        (kind,tag),url=item
        out={}
        rejects=[]
        try:
            for row in file_rows(url):
                if kind=="oi":
                    if row[0]=="create_time" or not row[2]:
                        continue
                    t=int(datetime.fromisoformat(row[0]).replace(tzinfo=UTC).timestamp())
                    if t%STEP:
                        continue
                    try:
                        value=float(row[2])
                        if not math.isfinite(value) or value<=0:
                            raise ValueError("non-positive or nonfinite OI")
                    except ValueError:
                        rejects.append(f"Invalid OI at {utc(t)}")
                        continue
                elif kind=="funding":
                    if not row[0].isdigit():
                        continue
                    t=int(row[0])//1000
                    value=float(row[-1])
                    if not math.isfinite(value) or abs(value)>1:
                        raise ValueError("Invalid funding")
                else:
                    if not row[0].isdigit():
                        continue
                    try:
                        t,value=decode_kline(row)
                    except ValueError as err:
                        rejects.append(str(err))
                        continue
                if t in out:
                    raise ValueError(f"Duplicate timestamp {utc(t)}")
                out[t]=value
            return (kind,tag),out,("; ".join(rejects) if rejects else None)
        except (OSError,ValueError,KeyError,IndexError) as e:
            return (kind,tag),{},str(e)

    data=defaultdict(dict)
    failures={}
    overlap=Counter()
    with ThreadPoolExecutor(max_workers=20) as pool:
        for (kind,tag),records,err in pool.map(one,jobs.items()):
            if err:
                failures[f"{kind}/{tag}"]=err
            for t,value in records.items():
                if t in data[kind]:
                    overlap[kind]+=1
                    if data[kind][t]!=value:
                        raise ValueError(f"Conflicting {kind} {utc(t)}")
                data[kind][t]=value
    return data,failures,dict(overlap),len(jobs)


def rows_csv(path):
    if path.exists():
        with path.open() as handle:
            yield from csv.DictReader(handle)


def historical_context(data):
    data["premium"]={epoch(r["open_utc"])+3600:float(r["coinbase_vs_bitstamp_bps"])
        for r in rows_csv(Path("research/market_hourly.csv")) if r.get("coinbase_vs_bitstamp_bps")}
    data["okx_funding"]={epoch(r["open_utc"]):float(r["funding_rate"])
        for r in rows_csv(Path("research/funding_okx.csv"))}
    data["etf"]={epoch(r["ny_session"]+"T00:00:00+00:00"):float(r["total_usd_m"])
        for r in rows_csv(Path("research/etf_history.csv")) if r["status"] in ("FINAL","HISTORICAL_ONLY")}
    liq=Path("research/liquidation_sample.json")
    data["liquidation"]={epoch(r["through_exclusive"]):r["liquidated_trade_usd"]
        for r in json.loads(liq.read_text()).values()} if liq.exists() else {}


def backfill_premium(data, today, prices):
    size=3*DAY
    periods=range(window(START),window(today),size)
    cache=CACHE/"coinbase"
    cache.mkdir(parents=True,exist_ok=True)
    lock=Lock()
    next_request=0.

    def paced_request(a,b):
        nonlocal next_request
        for attempt in range(5):
            with lock:
                now=time.monotonic()
                wait=max(0.,next_request-now)
                next_request=max(now,next_request)+.34
            time.sleep(wait)
            try:
                return get(COINBASE,{"start":a,"end":b-1,"granularity":"FIFTEEN_MINUTE"},limit=16)
            except (TimeoutError,HTTPError) as err:
                status=getattr(err,"code",None)
                if attempt==4 or status not in (None,429,502,503,504):
                    raise
                retry=(getattr(err,"headers",{}) or {}).get("Retry-After","0")
                pause=max(2**attempt,min(25,int(retry)) if str(retry).isdigit() else 0)
                if status==429:
                    with lock:
                        next_request=max(next_request,time.monotonic()+pause)
                time.sleep(pause)
        raise AssertionError("Request retry exhausted")

    def one(a):
        b=min(a+size,window(today))
        key=cache/f"{a}-{b}.json"
        try:
            raw=key.read_bytes() if key.exists() else paced_request(a,b)
            rows=json.loads(raw)["candles"]
            found={}
            for r in rows:
                t=int(r["start"])
                if not a<=t<b:
                    continue
                c=float(r["close"])
                if t%STEP or not math.isfinite(c) or c<=0 or t in found:
                    raise ValueError(f"Bad Coinbase candle {utc(t)}")
                found[t]=c
            if not found:
                raise ValueError("No eligible Coinbase candles")
            if not key.exists():
                key.write_bytes(raw)
            return a,found,None
        except (OSError,ValueError,KeyError) as e:
            if isinstance(e,HTTPError) and e.code in (429,502,503):
                time.sleep(2)
            return a,{},str(e)

    issues={}
    with ThreadPoolExecutor(max_workers=6) as pool:
        for a,rows,error in pool.map(one,periods):
            if error:
                issues["coinbase/"+utc(a)[:10]]=error
            for t,c in rows.items():
                if t in prices:
                    v=(c/prices[t][3]-1)*1e4
                    data["premium_15m"][t+STEP]=v
                    if (t+STEP)%3600==0:
                        data["premium"][t+STEP]=v
    return issues


def inventory(data):
    out={}
    for kind,values in data.items():
        tt=sorted(values)
        frequency=STEP if kind in ("spot","perp","oi","premium_15m") else 3600 if kind=="premium" else None
        missing=(tt[-1]-tt[0])//frequency+1-len(tt) if frequency and tt else None
        out[kind]={"rows":len(tt),"first":utc(tt[0]) if tt else None,"last":utc(tt[-1]) if tt else None,
                   "frequency_seconds":frequency,"missing_within_span":missing,
                   "timestamp":"open" if kind in VENUES else "session_date_utc_label" if kind=="etf" else "event_or_window_close",
                   "first_historical_availability_verified":False,"cost":"public unauthenticated"}
    return out


def save_exports(data, first, end):
    EXPORT.mkdir(exist_ok=True)
    schema=["interval_open_utc","interval_close_utc","spot_venue","perp_venue","spot_cvd_delta_btc","spot_volume_btc",
            "perp_cvd_delta_btc","perp_volume_btc","open_interest_binance_btc","binance_funding_settled_sum",
            "binance_funding_observations","okx_funding_settled_sum","okx_funding_observations",
            "coinbase_bitstamp_premium_at_close_bps","deribit_sampled_liquidation_usd",
            "deribit_sample_windows","availability","built_at_utc"]
    for tf,step in PERIODS.items():
        with gzip.open(EXPORT/f"{tf}.csv.gz","wt",newline="") as stream:
            w=csv.writer(stream);w.writerow(schema)
            start=(first+step-1)//step*step
            funding_sets=[sorted(data[k]) for k in ("funding","okx_funding","liquidation")]
            built=utc(time.time())
            for t in range(start,end-step+1,step):
                close=t+step
                vals=[]
                for k in ("spot","perp"):
                    block=[data[k].get(i) for i in range(t,close,STEP)]
                    vals.extend((sum(r[2] for r in block),sum(r[1] for r in block)) if all(block) else (None,None))
                w.writerow([utc(t),utc(close),"Binance BTCUSDT spot","Binance USD-M BTCUSDT perpetual",*vals,
                           data["oi"].get(close),
                           *[v for name,ts in zip(("funding","okx_funding"),funding_sets[:2])
                             for v in total_interval(data[name],ts,t,close)],
                           (data["premium_15m"].get(close) if step<3600 else data["premium"].get(close)),*total_interval(data["liquidation"],funding_sets[2],t,close),
                           "historical_event_clock_only",built])


def total_interval(data, ts, start, end):
    ints=ts[bisect_right(ts,start):bisect_right(ts,end)]
    return (sum(data[t] for t in ints) if ints else None,len(ints))


def pearson(x,y):
    if len(x)<3 or min(max(x)-min(x),max(y)-min(y))<=1e-12:
        return None
    return correlation(x,y)


def clustered_p(values, dates, two_sided=False):
    groups=defaultdict(float)
    for v,d in zip(values, dates):
        groups[d]+=v
    if len(groups)<12:
        return None
    values=list(groups.values())
    actual=sum(values)
    if two_sided:
        actual=abs(actual)
    randomizer=random.Random(417)
    hits=0
    for _ in range(2047):
        score=sum(x if randomizer.getrandbits(1) else -x for x in values)
        hits+=(abs(score) if two_sided else score)>=actual
    return (1+hits)/2048


def holm(rows):
    for _,bucket in group_rows(rows, "split"):
        tested=sorted((r for r in bucket if r["p"] is not None),key=lambda r:r["p"])
        adj=0.
        for i,row in enumerate(tested):
            adj=max(adj,min(1.,row["p"]*(len(tested)-i)))
            row["holm_p"]=adj


def group_rows(rows, field):
    out=defaultdict(list)
    for r in rows:
        out[r[field]].append(r)
    return out.items()


def flow_rows(data, price):
    fairs={k:sorted(data[k]) for k in ("funding","okx_funding","liquidation","etf")}
    rows=[]
    for t in sorted(price):
        if t%10800 or t-86400 not in price:
            continue
        if t+1800 not in price:
            continue
        block=[]
        for kind in ("spot","perp"):
            xs=[data[kind].get(s) for s in range(t-10800,t,STEP)]
            block.append(sum(x[2] for x in xs)/sum(x[1] for x in xs)
                         if all(xs) and sum(x[1] for x in xs)>0 else None)
        oi1=data["oi"].get(t);oi0=data["oi"].get(t-10800)
        oi=oi1/oi0-1 if oi1 and oi0 else None
        i=bisect_right(fairs["funding"],t)-1
        ftime=fairs["funding"][i] if i>=0 else 0
        funding=data["funding"].get(ftime) if 0<=t-ftime<=16*3600 else None
        p=price[t-STEP][3]
        r3=p/price[t-10800][0]-1
        r24=p/price[t-DAY][0]-1
        vols=[math.log(price[s][3]/price[s][0]) for s in range(t-10800,t,STEP)]
        sigma=math.sqrt(sum(v*v for v in vols))
        liq,n=total_interval(data["liquidation"],fairs["liquidation"],t-10800,t)
        # For descriptive joins only, add two days to the US session label to avoid same-day joins.
        ei=bisect_right(fairs["etf"],t-2*DAY)-1
        flow=fairs["etf"][ei] if ei>=0 else 0
        etf=data["etf"].get(flow) if t-flow<=5*DAY else None
        rows.append((t,(r3,r24,sigma),(*block,oi,funding,data["premium_15m"].get(t,data["premium"].get(t)),liq if n else None,etf)))
    return rows


def diagnose(price_feature, flow):
    ret=price_feature[0]
    s,f,oi,funding,premium,liq,_=flow
    if s is None or f is None:
        return None
    if ret>.006 and s>.06 and s>f+.025:
        return "spot_bid_dominant_pattern"
    if ret>.006 and f>.06 and f>s+.025:
        if oi is not None and oi<-.003:
            return "short_cover_consistent_pattern"
        if oi is not None and oi>.003 and (funding is None or funding>=0):
            return "perp_buy_rising_oi_pattern"
        return "perp_buy_dominant_oi_unresolved"
    if ret<-.006 and f<-.05 and oi is not None and oi<-.003:
        return ("falling_oi_sampled_liquidations_positive" if liq
                else "long_deleveraging_consistent_pattern")
    if ret<-.004 and s>.06 and f<-.03:
        return "possible_spot_absorption_pattern"
    if ret<-.006 and s<-.06 and f<-.06:
        return "aligned_selling_pattern"
    return "other"


def split_for(t):
    label=utc(t)[:10]
    return next((k for k,(a,b) in SPLITS.items() if a<=label<b),None)


def fwd(price,t,h):
    entry=t+STEP
    end=entry+h
    if entry not in price or end-STEP not in price:
        return None
    p=price[entry][0]
    o=price[end-STEP][3]
    return o/p-1


def write_csv(path,rows):
    if not rows:
        return
    with path.open("w",newline="") as f:
        wr=csv.DictWriter(f,fieldnames=list(rows[0]))
        wr.writeheader(); wr.writerows(rows)


def event_study(price, features):
    groups=defaultdict(list)
    runs=defaultdict(list)
    state=None
    begin=last=0
    for t,p,f in features:
        name=diagnose(p,f)
        if name!=state or t-last!=10800:
            if state and last:
                runs[state].append((last-begin+10800)/3600)
            state,begin=name,t
        last=t
        split=split_for(t)
        if split and name:
            groups[split,name].append(t)
            groups[split,"covered_unconditional"].append(t)
    out=[]
    for (split,name),ts in groups.items():
        for h in HORIZONS:
            net=[];gross=[]; ups=[];downs=[]; months=[]; free=0
            for t in ts:
                e=t+STEP
                last=e+h-STEP
                if t<free or e not in price or last not in price or split_for(last)!=split:
                    continue
                gross.append(price[last][3]/price[e][0]-1)
                net.append((1+gross[-1])*(1-FEE)/(1+FEE)-1)
                candle=[price[i] for i in range(e,last+1,STEP)]
                ups.append(max(r[1] for r in candle)/price[e][0]-1)
                downs.append(min(r[2] for r in candle)/price[e][0]-1)
                months.append(utc(t)[:7]); free=e+h
            if not net:
                continue
            q=sorted(net);offset=len(q)-1
            wins=[y for y in net if y>0]; losses=[-y for y in net if y<0]
            out.append(dict(split=split,state=name,hours=h/3600,n=len(q),months=len(set(months)),
                net_mean_pct=100*mean(q),gross_mean_pct=100*mean(gross),p10_pct=100*q[int(.1*offset)],
                median_pct=100*median(q),p90_pct=100*q[int(.9*offset)],win_pct=100*len(wins)/len(q),
                payoff=mean(wins)/mean(losses) if wins and losses else None,
                mfe_median_pct=100*median(ups),mae_median_pct=100*median(downs),
                consecutive_state_median_hours=median(runs[name]) if runs[name] else None,
                p=clustered_p(net,months) if len(net)>=60 else None,holm_p=None))
    holm(out)
    write_csv(OUT/"state_returns.csv",out)
    return {"cells":len(out),"testable":sum(r["p"] is not None for r in out),
            "adjusted_positive_cells":sum(r["holm_p"] is not None and r["holm_p"]<=.05 for r in out)}


def lead_lag(price, features):
    out=[]
    for split in SPLITS:
        for j,name in enumerate(FEATURES):
            for lag in (-24,-12,-6,-3,0,3,6,12,24):
                pairs=[]
                for t,_,f in features:
                    if split_for(t)!=split or f[j] is None:
                        continue
                    close=t+lag*3600
                    if close-STEP not in price or close-10800 not in price or split_for(close-STEP)!=split:
                        continue
                    p=price[close-STEP][3]/price[close-10800][0]-1
                    pairs.append((f[j],p,utc(t)[:7]))
                if len(pairs)<20:
                    continue
                x,y,m=zip(*pairs)
                r=pearson(x,y)
                xm,ym=mean(x),mean(y)
                cov=[(a-xm)*(b-ym) for a,b in zip(x,y)]
                out.append(dict(split=split,feature=name,price_window_end_relative_hours=lag,n=len(x),
                                pearson=r,p=clustered_p(cov,m,True) if r is not None else None,holm_p=None))
    holm(out); write_csv(OUT/"lead_lag.csv",out)
    return {"cells":len(out),"adjusted_cells":sum(r["holm_p"] is not None and r["holm_p"]<=.05 for r in out)}


def models(price, features, resolve=split_for, true_observation=False):
    import numpy as np
    report={}
    for subset,indices in (("spot_perp",(0,1)),("with_oi_funding",(0,1,2,3)),
                           ("with_oi_funding_premium",(0,1,2,3,4)),
                           ("with_oi_funding_premium_etf",(0,1,2,3,4,6)),
                           ("all_including_sampled_liquidation",tuple(range(7)))):
        for h in (10800,DAY):
            batches=defaultdict(list)
            occupied=0
            for t,p,f in features:
                name=resolve(t)
                if not name or t<occupied or resolve(t+STEP+h-STEP)!=name or any(f[j] is None for j in indices):
                    continue
                y=fwd(price,t,h)
                if y is not None:
                    batches[name].append((t,p,tuple(f[j] for j in indices),y));occupied=t+STEP+h
            coverage={k:len(batches[k]) for k in SPLITS}
            result={"matched_samples":coverage}
            report[f"{subset}/{h//3600}h"]=result
            if min(coverage.values())<100:
                result["status"]="insufficient_common_chronological_history"
                continue
            train=batches["development"]
            result["status"]="observed_forward_replay" if true_observation else "retrospective_event_clock_simulation"
            predicted={}
            for kind in ("price","plus_flow"):
                def design(batch):
                    return np.asarray([p if kind=="price" else (*p,*f) for _,p,f,_ in batch],dtype=float)
                x=design(train);y=np.asarray([r[-1] for r in train]);mu=x.mean(0);sd=x.std(0);sd[sd==0]=1
                x=(x-mu)/sd
                beta=np.linalg.solve(x.T@x+20*np.eye(x.shape[1]),x.T@(y-y.mean()))
                predicted[kind]={}
                for split in ("validation","reused_historical"):
                    test=batches[split]
                    actual=np.asarray([r[-1] for r in test])
                    pred=((design(test)-mu)/sd)@beta+y.mean()
                    buy=pred>(2*FEE)
                    trade=(1+actual[buy])*(1-FEE)/(1+FEE)-1
                    nav=np.cumprod(np.r_[1.,1+trade]); draw=1-(nav/np.maximum.accumulate(nav)).min()
                    predicted[kind][split]={"mse":float(np.mean((actual-pred)**2)),
                        "direction_accuracy":float(np.mean((pred>0)==(actual>0))),
                        "trades":int(np.sum(buy)),"net_pct":100*float(nav[-1]-1),
                        "trade_close_drawdown_pct":100*float(draw),
                        "errors":(actual-pred)**2}
            for split in ("validation","reused_historical"):
                a=predicted["price"][split];b=predicted["plus_flow"][split]
                diff=a.pop("errors")-b.pop("errors")
                result[split]={"price":a,"plus_flow":b,"net_difference_pp":b["net_pct"]-a["net_pct"],
                    "paired_loss_improvement_p":clustered_p(diff,[utc(q[0])[:7] for q in batches[split]])}
    tests=[{"split":k,"p":v[k]["paired_loss_improvement_p"],"holm_p":None,"cell":v[k]}
           for v in report.values() for k in ("validation","reused_historical") if k in v]
    holm(tests)
    for row in tests: row["cell"]["paired_loss_improvement_holm_p"]=row["holm_p"]
    return report


def capture(now, data, prices):
    folder=OUT/"observations"
    folder.mkdir(exist_ok=True)
    run=os.getenv("GITHUB_RUN_ID","local") + "-" + os.getenv("GITHUB_RUN_ATTEMPT","1")
    path=folder/(utc(now)[:19].replace(":","").replace("-","")+"-"+run+".json")
    if path.exists():
        raise ValueError("Capture filename already exists")
    failures={}
    live=defaultdict(dict)
    end=now//STEP*STEP
    endpoints={
        "spot":("https://data-api.binance.vision/api/v3/klines",{"symbol":"BTCUSDT","interval":"15m","limit":96}),
        "perp":("https://fapi.binance.com/fapi/v1/klines",{"symbol":"BTCUSDT","interval":"15m","limit":96}),
        "coinbase":(COINBASE,
                    {"start":end-DAY,"end":end-1,"granularity":"FIFTEEN_MINUTE"}),
        "binance_oi":("https://fapi.binance.com/futures/data/openInterestHist",{"symbol":"BTCUSDT","period":"15m","limit":96}),
        "okx_oi":("https://www.okx.com/api/v5/public/open-interest",{"instType":"SWAP","instId":"BTC-USDT-SWAP"}),
        "bybit_oi":("https://api.bybit.com/v5/market/open-interest",{"category":"linear","symbol":"BTCUSDT","intervalTime":"15min","limit":4})}

    def source(task):
        k,(url,params)=task
        try:
            raw=json.loads(get(url,params,limit=12))
            if k in VENUES:
                parsed=dict(decode_kline(r) for r in raw if int(r[0])//1000+STEP<=end)
            elif k=="coinbase":
                parsed={int(r["start"]):float(r["close"]) for r in raw["candles"] if int(r["start"])+STEP<=end}
            elif k=="binance_oi":
                parsed={int(r["timestamp"])//1000:float(r["sumOpenInterest"]) for r in raw if int(r["timestamp"])//1000<=end}
            else:
                if (k=="okx_oi" and raw.get("code")!="0") or (k=="bybit_oi" and raw.get("retCode")!=0):
                    raise ValueError(str(raw.get("msg",raw.get("retMsg","non-success code"))))
                # Preserve venue units and event timestamp verbatim for these source checks.
                return k,{"request_url":url,"received_at_utc":utc(time.time()),"raw":raw},None
            if not parsed or (k not in VENUES and any(x<=0 or not math.isfinite(x) for x in parsed.values())):
                raise ValueError("empty or invalid public feed")
            return k,{"request_url":url,"received_at_utc":utc(time.time()),"rows":sorted(parsed.items())},None
        except (OSError,ValueError,KeyError,TypeError,IndexError) as err:
            return k,None,str(err)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for k,result,err in pool.map(source,endpoints.items()):
            if err:
                failures[k]=err
            else:
                live[k]=result
    cb=live.get("coinbase",{}).get("rows",[])
    prem=[(t+STEP,1e4*(v/prices[t][3]-1)) for t,v in cb if t in prices]
    live["coinbase_bitstamp_15m_premium"]={"rows":prem,"received_at_utc":utc(time.time())}

    # Persist sampled raw source values. Downloaded historical bars are available only after this capture.
    tail={}
    for k in ("spot","perp","oi","funding","okx_funding","liquidation"):
        values=sorted((t,v) for t,v in data[k].items() if t>=end-3*DAY)
        if k in ("spot","perp","oi"):
            by_day=defaultdict(list)
            for t,v in values:
                by_day[t//DAY].append((t,v))
            tail[k]=[row for block in by_day.values() for row in block[-16:]]
        else:
            tail[k]=values
    source_coverage=[(k,len(d["rows"]),d["rows"]) for k,d in live.items() if d.get("rows")]
    for k,d in live.items():
        if d.get("rows"):
            d["rows"]=d["rows"][-16:]
            unit="15m" if k=="binance_oi" or k in VENUES or k=="coinbase" else "source"
            d["row_period"]=unit
    source_files=sorted(Path("research/edge/source_observations").glob("*.json"))
    etf_files=sorted(Path("research/etf_observations").glob("*.json"))
    observed=int(time.time())
    doc={"capture_started_at_utc":utc(now),"observed_at_utc":utc(observed),
         "strict_eligible_after_utc":utc(observed+STEP),"run_id":run,"live":live,
         "archive_tail_event_rows":tail,"errors":failures,
         "live_coverage":{k:{"rows_in_request":n,"first_event_utc":utc(source[0][0]) if source else None,
                             "last_event_utc":utc(source[-1][0]) if source else None}
                          for k,n,source in source_coverage},
         "immutable_etf_reference":str(etf_files[-1]) if etf_files else None,
         "immutable_derivatives_reference":str(source_files[-1]) if source_files else None,
         "definition":"Observation time is not event time. Older archive rows cannot be used before this receipt."}
    write_json(path,doc)
    return str(path), failures


def consecutive_missing(times, start, end, frequency):
    return [t for t in range(start,end,frequency) if t not in times]


def observe_quality(data, now, failures, repo_health):
    # Time window uses the last elapsed settlement boundary, with today's live buckets listed as provisional.
    end=now//STEP*STEP
    result={}
    for name,interval,anchor in (("spot",STEP,end),("perp",STEP,end),("oi",STEP,end),("premium",3600,end//3600*3600)):
        vals=data[name]; ts=set(vals); reference=end//DAY*DAY if name!="premium" else end//3600*3600
        gaps=consecutive_missing(ts, reference-DAY,reference,interval)
        latest=max(vals) if vals else None
        result[name]={"trailing_settled_day_expected":DAY//interval, "missing":len(gaps),"first_12_missing_utc":[utc(t) for t in gaps[:12]],
                      "last_event_utc":utc(latest) if latest else None,"age_seconds":now-latest if latest else None,
                      "status":"gap" if gaps else "available"}
    ledger_paths=list(Path("research/paper_edge").glob("*.json"))
    target_paths=list(Path("research/multiframe_shadow/paper_signals").glob("*.json"))
    recent={};notices=[];fills=0; busted=[]
    for file in sorted(ledger_paths):
        day=json.loads(file.read_text())
        signal=epoch(day["signal_at"])
        if signal in recent:
            notices.append("duplicate_signal "+day["signal_at"])
        quote=day.get("quote")
        if quote and not datetime.fromisoformat(day["confirmed_published_at"])<datetime.fromisoformat(quote["exchange_at"])<=datetime.fromisoformat(quote["received_at"]):
            busted.append(str(file))
        if quote:
            mid=(quote["bid"]+quote["ask"])/2
            if any(abs(a["cash_usd"]+a["position_btc"]*mid-a["nav_usd"])>1e-7 for a in day["accounts"].values()):
                busted.append(str(file))
        fills+=sum(a["status"]=="paper_fill" for a in day["accounts"].values())
        recent[signal]=day
    for name, jobs in repo_health.items():
        if isinstance(jobs,list) and any(r["conclusion"] in ("failure","timed_out") and
              now-epoch(r["created_at"])<DAY for r in jobs):
            notices.append("workflow failure in last 24h: "+name)
    if any(q["missing"] for q in result.values()):
        notices.append("funding-input continuity gap; see per-series counts")
    if busted:
        notices.append("invalid_quote_clock_or_nav "+",".join(busted))
    start=max(min(recent,default=end),end-DAY)
    slots=consecutive_missing(recent,((start+2699)//2700)*2700,(end//2700)*2700,2700)
    if slots:
        notices.append(f"{len(slots)} missed shadow reporting slots since first ledger or last 24h; no replay")
    if set(p.stem for p in target_paths)-set(p.stem for p in ledger_paths):
        notices.append("published paper signals without execution receipt")
    latest=recent[max(recent)] if recent else None
    return {"generated_utc":utc(now),"signals_expected_in_monitored_window":max(0,((end//2700)*2700-((start+2699)//2700)*2700)//2700),
        "slots_not_recorded_utc":[utc(t) for t in slots],"status": "review" if notices or failures else "ok",
        "paper_records":len(recent),"simulated_fills":fills,"paper_invalid_records":busted,"paper_warnings":notices,
        "paper_last":({k:latest[k] for k in ("signal_at","confirmed_published_at","quote")} if latest else None),
        "paper_accounts":({k:{"nav_usd":a["nav_usd"],"return_pct":100*(a["nav_usd"]/latest["initial_usd_each"]-1),
                "max_drawdown_pct":100*a["maximum_drawdown"]} for k,a in latest["accounts"].items()} if latest else {}),
        "collector_errors":failures,"input_quality":result,"workflows":repo_health}


def available_samples(price):
    files=sorted((OUT/"observations").glob("*.json"))
    known=[]
    oldest_lag_hours=[]
    used=set()
    for path in files:
        snap=json.loads(path.read_text())
        live=snap["live"]
        t=epoch(snap["strict_eligible_after_utc"])
        t=(t+STEP-1)//STEP*STEP
        if t in used or any(k not in price for k in (t-STEP,t-DAY,t-10800)):
            continue
        sources=[]
        for name in VENUES:
            history=dict(snap["archive_tail_event_rows"][name])
            history.update(live.get(name,{}).get("rows",[]))
            sources.append(history)
        imbalances=None
        for a in sorted(sources[0].keys() & sources[1].keys(),reverse=True):
            if not STEP<=t-a<=36*3600:
                continue
            window=range(a+STEP-10800,a+STEP,STEP)
            if all(k in src for src in sources for k in window):
                vols=[sum(src[k][1] for k in window) for src in sources]
                if min(vols)>0:
                    imbalances=[sum(src[k][2] for k in window)/v for src,v in zip(sources,vols)]
                    break
        if imbalances is None:
            continue
        r3=price[t-STEP][3]/price[t-10800][0]-1
        r24=price[t-STEP][3]/price[t-DAY][0]-1
        vol=math.sqrt(sum(math.log(price[a][3]/price[a][0])**2 for a in range(t-10800,t,STEP)))
        used.add(t)
        # A reported older venue window remains an older signal. No event timestamp is promoted to availability.
        oldest_lag_hours.append((t-a-STEP)/3600)
        tail=snap["archive_tail_event_rows"]
        oi_rows=dict(tail["oi"])
        oi_rows.update(live.get("binance_oi",{}).get("rows",[]))
        valid_oi=[q for q in oi_rows if 0<=t-q<=36*3600 and q-10800 in oi_rows]
        q=max(valid_oi,default=0)
        oi=oi_rows[q]/oi_rows[q-10800]-1 if q and oi_rows[q-10800]>0 else None
        funded=[(q,v) for q,v in tail["funding"] if 0<=t-q<=16*3600]
        funding=max(funded)[1] if funded else None
        premiums=[(q,v) for q,v in live.get("coinbase_bitstamp_15m_premium",{}).get("rows",[]) if 0<=t-q<=3600]
        premium=max(premiums)[1] if premiums else None
        liquidations=[v for q,v in tail["liquidation"] if 0<=t-q<=10800]
        etf=None
        reference=snap.get("immutable_etf_reference")
        if reference:
            doc=json.loads(Path(reference).read_text())
            if epoch(doc["observed_at_utc"])<=epoch(snap["observed_at_utc"]):
                for item in sorted(doc["observations"],key=lambda x:x["date"],reverse=True):
                    if item["status"]=="FINAL" and item["recent"] and item["recent"]==item["history"]:
                        etf=float(item["recent"][-1])
                        break
        known.append((t,(r3,r24,vol),(*imbalances,oi,funding,premium,sum(liquidations) if liquidations else None,etf)))
    known.sort()
    covered={utc(r[0])[:10] for r in known}
    out={"receipts":len(files),"days_with_same_receipt_spot_and_perp":len(covered),
         "usable_signal_observations":len(known),
         "median_venue_alignment_age_hours":median(oldest_lag_hours) if known else None,
         "per_feature_signal_coverage":{name:sum(r[2][i] is not None for r in known) for i,name in enumerate(FEATURES)},
         "first_complete_eligible_utc":utc(known[0][0]) if known else None,
         "status":"insufficient_forward_samples_no_promotion",
         "future_gate":"First 60 calendar days development, next 30 validation, following days forward; at least 100 non-overlapping matched outcomes each; no promotion."}
    if len(covered)>=120:
        first=known[0][0]//DAY*DAY
        def resolve(t):
            days=(t-first)//DAY
            if days<0:
                return None
            return "development" if days<60 else "validation" if days<90 else "reused_historical"
        evaluation=models(price,known,resolve,True)
        for v in evaluation.values():
            if "reused_historical" in v:
                v["forward"]=v.pop("reused_historical")
        out["status"]="forward_comparisons_available_review_required"
        out["comparisons"]=evaluation
    return out


def workflow_health():
    repo=os.getenv("GITHUB_REPOSITORY","lxiaomng-lgtm/btc-kline-data")
    try:
        raw=json.loads(get(f"https://api.github.com/repos/{repo}/actions/runs",
                           {"branch":"main","per_page":55},limit=12))["workflow_runs"]
        names={"Archive BTC history","Update BTC candles","BTC multiframe shadow observations",
               "BTC historical strategy research","BTC profitability edge research","BTC flow research and quality"}
        by_name=defaultdict(list)
        for row in raw:
            if row["name"] in names:
                by_name[row["name"]].append({k:row[k] for k in ("id","event","status","conclusion","created_at")})
        return dict(by_name)
    except (OSError,ValueError,KeyError) as e:
        return {"error":str(e)}


def vendor_probes(today):
    yesterday=today-timedelta(days=2)
    checks={
      "Bitstamp recent trade tape":("https://www.bitstamp.net/api/v2/transactions/btcusd/?time=hour","GET"),
      "Coinbase public candle probe":("https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD/candles?"+
           urlencode({"start":window(yesterday),"end":window(yesterday)+3600,"granularity":"FIFTEEN_MINUTE"}),"GET"),
      "Binance old daily OI probe":(f"{VISION}/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2022-01-02.zip","HEAD"),
      "Binance recent liquidation-archive naming probe":(f"{VISION}/futures/um/daily/liquidationSnapshot/BTCUSDT/BTCUSDT-liquidationSnapshot-{yesterday}.zip","HEAD"),
      "Bybit perpetual archived trades probe":(f"https://public.bybit.com/trading/BTCUSDT/BTCUSDT{yesterday}.csv.gz","HEAD"),
      "Bybit spot archived trades probe":(f"https://public.bybit.com/spot/BTCUSDT/BTCUSDT_{yesterday}.csv.gz","HEAD"),
      "OKX historic perpetual candle probe":("https://www.okx.com/api/v5/market/history-candles?instId=BTC-USDT-SWAP&bar=15m&limit=2","GET"),
      "OKX archive catalogue":("https://www.okx.com/historical-data","HEAD")}
    def probe(task):
        k,(url,method)=task
        try:
            r=get(url,method=method,limit=12)
            return k,{"url":url,**(r if isinstance(r,dict) else {"status":200,"bytes":len(r)})}
        except OSError as e:
            return k,{"url":url,"error":str(e),"status":getattr(e,"code",None)}
    with ThreadPoolExecutor(max_workers=8) as ex:
        return dict(ex.map(probe,checks.items()))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--no-capture",action="store_true")
    parser.add_argument("--monitor-only",action="store_true")
    args=parser.parse_args()
    OUT.mkdir(parents=True,exist_ok=True)
    now=int(time.time())
    today=datetime.fromtimestamp(now,UTC).date()
    rows,audit=load_prices()
    price={t:tuple(map(float,(o,h,l,c))) for t,o,h,l,c,_ in rows}
    data,errors,duplicates,jobs=acquire(today,args.monitor_only)
    historical_context(data)
    if not args.monitor_only:
        errors.update(backfill_premium(data,today,price))
    if not args.monitor_only and (not data["spot"] or not data["perp"]):
        raise ValueError(f"Missing core five-year sources: {list(errors.items())[:12]}")
    print("Archive inventory", {k:len(v) for k,v in data.items()}, "failed",len(errors),flush=True)
    if not args.no_capture:
        receipt, live_errors=capture(now,data,price)
    else:
        receipt,live_errors=None,{}
    health=observe_quality(data,int(time.time()),{**live_errors,**errors},workflow_health())
    health["price_audit"]=audit
    if receipt:
        health["live_capture_coverage"]=json.loads(Path(receipt).read_text())["live_coverage"]
    snapshot_age=int(time.time())-epoch(audit["snapshot_close"])
    health["price_snapshot_age_seconds"]=snapshot_age
    if snapshot_age>3600:
        health["paper_warnings"].append("published 15m snapshot older than one hour")
        health["status"]="review"
    write_json(OUT/"quality_latest.json",health)
    daily=OUT/"daily_quality"
    daily.mkdir(exist_ok=True)
    write_json(daily/(stamp(now)[:10]+".json"),health)
    if args.monitor_only:
        print("Hourly capture and quality complete",health["status"],flush=True)
        return
    write_json(OUT/"coverage.json",{"run_id":os.getenv("GITHUB_RUN_ID"),"generated_utc":utc(time.time()),
        "price_audit":audit,"inventory":inventory(data),"archive_file_requests":jobs,
        "archive_failures":errors,"exact_duplicates_across_files":duplicates,"live_receipt":receipt,
        "live_errors":live_errors,"free_vendor_endpoint_probes":vendor_probes(today),
        "limitations":["Historical archive publication history unverified; no strict point-in-time claims",
                       "Liquidation windows cover sampled Deribit only; missing window is not zero",
                       "CVD is signed taker-volume increment in BTC, not external capital transferred",
                       "ETF session dates differ from observation time; historical use descriptive only",
                       "OI unit is Binance contracts represented in BTC; stablecoin versus USD feeds differ"]})
    save_exports(data,rows[0][0],rows[-1][0]+STEP)
    features=flow_rows(data,price)
    s=event_study(price,features); l=lead_lag(price,features);m=models(price,features)
    write_json(OUT/"research.json",{"run_id":os.getenv("GITHUB_RUN_ID"),"per_side_cost":FEE,
        "state_outcomes":s,"lead_lag":l,"models":m,"strict_point_in_time":available_samples(price),
        "notes":["Binance archive timestamps are event clocks with an assumed 15m latency; results retrospective only.",
                 "States use price and venue taker imbalance, OI and funding; inferred interpretations are hypotheses.",
                 "Negative/zero/positive lag compares past/same/future 3h price windows, not causal direction.",
                 "Train 2022–23; validate 2024; previously seen 2025+ comparison. Same matched rows for paired regressions.",
                 "The true future records are separate; do not promote models on retrospective scores."]})
    print("Research and quality complete",s,l,health["paper_records"],flush=True)


if __name__=="__main__":
    main()
