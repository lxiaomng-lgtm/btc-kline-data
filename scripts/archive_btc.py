#!/usr/bin/env python3
"""Bitstamp-only monthly archive, UTC epoch opens, Beijing display time."""
import csv, datetime as dt, json, time, urllib.request
from pathlib import Path
from decimal import Decimal
TZ=dt.timezone(dt.timedelta(hours=8))
STEP=900
ROOT=Path("history/15m")
FIELDS=["timestamp","beijing_time","open","high","low","close","volume"]
def fetch(a,b):
    url=f"https://www.bitstamp.net/api/v2/ohlc/btcusd/?step=900&limit=1000&start={a}&end={b-1}&exclude_current_candle=true"
    for i in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"btc-history-archive"}),timeout=30) as r:
                rows=json.load(r)["data"]["ohlc"]
            out={}
            for x in rows:
                t=int(x["timestamp"])
                if not a<=t<b: continue
                o,h,l,c,v=[Decimal(x[k]) for k in FIELDS[2:]]
                if t%STEP or not all(z.is_finite() for z in (o,h,l,c,v)) or min(o,h,l,c)<=0 or v<0 or h<max(o,l,c) or l>min(o,h,c) or t in out: raise ValueError("invalid candle")
                out[t]={"timestamp":t,"beijing_time":dt.datetime.fromtimestamp(t,TZ).isoformat(),**{k:x[k] for k in FIELDS[2:]}}
            if set(out)!=set(range(a,b,STEP)): raise ValueError(f"incomplete range {a} {b}: {len(out)}")
            return out
        except Exception:
            if i==4: raise
            time.sleep(2**i)
def main():
    ROOT.mkdir(parents=True,exist_ok=True)
    now=dt.datetime.now(TZ)
    end=int(now.timestamp())//STEP*STEP
    start=dt.datetime(now.year-5,now.month,1,tzinfo=TZ)
    archived = sorted(ROOT.glob("????-??.csv"))
    if archived:
        earliest = dt.datetime.strptime(archived[0].stem, "%Y-%m").replace(tzinfo=TZ)
        start = min(start, earliest)
    month=start
    manifest=[]
    while int(month.timestamp())<end:
        nxt=dt.datetime(month.year+(month.month==12),month.month%12+1,1,tzinfo=TZ)
        a=int(month.timestamp()); b=min(end,int(nxt.timestamp()))
        p=ROOT/(month.strftime("%Y-%m")+".csv")
        old={}
        if p.exists():
            with p.open() as f:
                for x in csv.DictReader(f):
                    t=int(x["timestamp"])
                    if t in old or not a<=t<b or t%STEP: raise ValueError(f"invalid archive {p}")
                    old[t]=x
        missing=[t for t in range(a,b,STEP) if t not in old]
        i=0
        while i<len(missing):
            j=i+1
            while j<len(missing) and j-i<999 and missing[j]==missing[j-1]+STEP: j+=1
            old.update(fetch(missing[i],missing[j-1]+STEP))
            i=j
            time.sleep(.25)
        if missing:
            tmp=p.with_suffix(".tmp")
            with tmp.open("w",newline="") as f:
                w=csv.DictWriter(f,fieldnames=FIELDS);w.writeheader();w.writerows(old[t] for t in sorted(old))
            tmp.replace(p)
        manifest.append({"path":str(p),"count":len(old),"first_open":a,"last_close":b})
        month=nxt
    Path("history/index.json").write_text(json.dumps({"source":"Bitstamp","pair":"BTC/USD","step":STEP,"timezone":"Asia/Shanghai","months":manifest,"count":sum(x["count"] for x in manifest),"latest_close":end,"validated_at":now.isoformat()},indent=2)+"\n")
if __name__=="__main__":main()
