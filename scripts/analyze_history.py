import csv,json,datetime as dt
from pathlib import Path
from decimal import Decimal as D
TZ=dt.timezone(dt.timedelta(hours=8))
root=Path("history")
idx=json.loads((root/"index.json").read_text())
rows=[]
for m in idx["months"]:
    with open(m["path"]) as f:
        batch=list(csv.DictReader(f))
    assert len(batch)==m["count"]
    assert [int(x["timestamp"]) for x in batch]==list(range(m["first_open"],m["last_close"],900))
    for x in batch:
        t=int(x["timestamp"])
        o,h,l,c,v=[D(x[k]) for k in ("open","high","low","close","volume")]
        assert all(z.is_finite() for z in (o,h,l,c,v)) and min(o,h,l,c)>0 and v>=0 and h>=max(o,l,c) and l<=min(o,h,c)
        assert int(dt.datetime.fromisoformat(x["beijing_time"]).timestamp())==t
        rows.append((t,o,h,l,c,v))
assert len(rows)==idx["count"] and all(b[0]-a[0]==900 for a,b in zip(rows,rows[1:]))
assert rows[-1][0]+900==idx["latest_close"]
out=root/"derived";out.mkdir(exist_ok=True)
def key(t,period):
    d=dt.datetime.fromtimestamp(t,TZ)
    if period=="1w": return int((d.replace(hour=0,minute=0,second=0)-dt.timedelta(days=d.weekday())).timestamp())
    if period=="1M": return int(d.replace(day=1,hour=0,minute=0,second=0).timestamp())
    n=int(period);return (t+28800)//n*n-28800
def end(t,p):
    if p=="1w":return t+604800
    if p=="1M":
        d=dt.datetime.fromtimestamp(t,TZ)
        return int(d.replace(year=d.year+(d.month==12),month=d.month%12+1).timestamp())
    return t+int(p)
summary={}
for name,p in [("45m","2700"),("3h","10800"),("6h","21600"),("1d","86400"),("1w","1w"),("1M","1M")]:
    groups={}
    for r in rows:groups.setdefault(key(r[0],p),[]).append(r)
    result=[]
    for t,g in groups.items():
        e=end(t,p)
        if g[0][0]!=t or g[-1][0]+900!=e or len(g)!=(e-t)//900:continue
        result.append([t,dt.datetime.fromtimestamp(t,TZ).isoformat(),g[0][1],max(x[2] for x in g),min(x[3] for x in g),g[-1][4],sum(x[5] for x in g)])
    with (out/(name+".csv")).open("w",newline="") as f:
        w=csv.writer(f);w.writerow(["timestamp","beijing_time","open","high","low","close","volume"]);w.writerows(result)
    summary[name]={"count":len(result),"first_open":result[0][0],"last_close":end(result[-1][0],p)}
(out/"audit.json").write_text(json.dumps({"source":"Bitstamp","raw_count":len(rows),"first_open":rows[0][0],"latest_close":rows[-1][0]+900,"duplicates":0,"missing":0,"invalid_ohlcv":0,"periods":summary,"audited_at":dt.datetime.now(TZ).isoformat()},indent=2)+"\n")
