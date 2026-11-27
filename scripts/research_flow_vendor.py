"""Reconstruct one cross-venue Bybit comparison day from tick archives."""
import csv
import gzip
import hashlib
from collections import defaultdict
from datetime import date
from urllib.request import Request, urlopen

from research_flow import CACHE, EXPORT, OUT, PERIODS, STEP, utc, window, write_csv, write_json


CONTROL_DAY = date(2026, 11, 25)


def ticks(url, start, end):
    cache = CACHE/"vendor"
    cache.mkdir(parents=True,exist_ok=True)
    path=cache/url.split("/")[-1]
    if not path.exists():
        tmp=path.with_suffix(".tmp")
        size=0
        with urlopen(Request(url,headers={"User-Agent":"BTC funding flow validation"}),timeout=35) as r, tmp.open("wb") as f:
            while chunk:=r.read(1<<20):
                size+=len(chunk)
                if size>190_000_000:
                    raise ValueError("Bybit comparison file exceeded bounded download size")
                f.write(chunk)
        tmp.replace(path)
    with path.open("rb") as raw:
        sha=hashlib.file_digest(raw,"sha256").hexdigest()
    data=defaultdict(lambda:[0.,0.,0])
    ids=set()
    missing=bad=outside=duplicates=0
    with gzip.open(path,"rt",newline="",encoding="utf-8-sig") as f:
        reader=csv.DictReader(f)
        cols={k.lower().replace("_",""): k for k in (reader.fieldnames or [])}
        def get(row,*keys):
            for k in keys:
                if k in cols:
                    return row[cols[k]]
            return None
        for row in reader:
            try:
                raw=float(get(row,"timestamp","tradetime","time","t"))
                scale=1e6 if raw>1e15 else 1000 if raw>1e12 else 1
                stamp=int(raw/scale)
                if not start<=stamp<end:
                    outside+=1
                    continue
                key=get(row,"trdmatchid","tradeid","id","execid")
                if key and key in ids:
                    duplicates+=1
                    continue
                if key:
                    ids.add(key)
                else:
                    missing+=1
                size=float(get(row,"size","tradeqty","qty","volume","quantity"))
                side=(get(row,"side","takerside","tradeside") or "").lower()
                if side not in ("buy","sell"):
                    maker=(get(row,"isbuyermaker","buyermaker") or "").lower()
                    if maker not in ("true","false","0","1"):
                        raise ValueError("No recognized aggressor side")
                    side="sell" if maker in ("true","1") else "buy"
                if not 0<size<1e5:
                    raise ValueError("Trade quantity not finite or plausible")
                a=data[stamp//STEP*STEP]
                a[0]+=size if side=="buy" else -size
                a[1]+=size
                a[2]+=1
            except (TypeError,ValueError):
                bad+=1
    summary={"archive":url,"bytes":path.stat().st_size,"sha256":sha,"fields":reader.fieldnames,
             "ticks":sum(a[2] for a in data.values()),"duplicates_skipped":duplicates,"unkeyed":missing,
             "outside_day":outside,"invalid":bad,"coverage":len(data),"missing_15m":96-len(data)}
    if not data:
        raise ValueError(f"No usable tick data: {summary}")
    return summary,data


def main():
    start=window(CONTROL_DAY)
    sources={"bybit_spot":f"https://public.bybit.com/spot/BTCUSDT/BTCUSDT_{CONTROL_DAY}.csv.gz",
             "bybit_perpetual":f"https://public.bybit.com/trading/BTCUSDT/BTCUSDT{CONTROL_DAY}.csv.gz"}
    report={"control_date_utc":str(CONTROL_DAY),"sources":{}}
    for kind,url in sources.items():
        try:
            stats,data=ticks(url,start,start+86400)
            report["sources"][kind]=stats
            write_csv(OUT/(kind+"_sample.csv"),[{"open_utc":utc(t),"cvd_delta_btc":v[0],"volume_btc":v[1],
                       "ticks":v[2],"venue":kind,"availability":"event_clock_only"}
                       for t,v in sorted(data.items())])
            for tf,interval in PERIODS.items():
                with gzip.open(EXPORT/(kind+"_"+tf+".csv.gz"),"wt",newline="") as stream:
                    writer=csv.writer(stream)
                    writer.writerow(("open_utc","close_utc","cvd_delta_btc","volume_btc","ticks","venue","availability"))
                    for t in range(start,start+86400,interval):
                        block=[data.get(i) for i in range(t,t+interval,STEP)]
                        if all(block):
                            writer.writerow((utc(t),utc(t+interval),*(sum(r[i] for r in block) for i in range(3)),
                                             kind,"event_clock_only"))
        except (OSError,ValueError,KeyError) as err:
            report["sources"][kind]={"archive":url,"error":str(err)}
    write_json(OUT/"bybit_sample.json",report)
    print(report,flush=True)


if __name__=="__main__":
    main()
