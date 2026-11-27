"""Timestamped, venue-separated live market source adapters."""
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from statistics import mean
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from research_prices import stamp

BITSTAMP = "https://www.bitstamp.net/api/v2"
COINBASE = "https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD"
OKX = "https://www.okx.com/api/v5"
BINANCE_SPOT = "https://data-api.binance.vision/api/v3"
BINANCE_FUTURES = "https://fapi.binance.com/fapi/v1"
BYBIT = "https://api.bybit.com/v5"
STEP = 900


def read(url, params=None, timeout=10, token=None):
    headers={"User-Agent":"btc-ops/1.0","Cache-Control":"no-cache","Accept":"application/json"}
    if token:
        headers["Authorization"]="Bearer "+token
    if params:
        url+="?"+urlencode(params)
    with urlopen(Request(url,headers=headers),timeout=timeout) as resp:
        return json.load(resp)


def finite(v):
    value=float(v)
    if not math.isfinite(value):
        raise ValueError("non-finite data")
    return value


def epoch(s):
    from datetime import datetime
    return datetime.fromisoformat(s).timestamp()


def wrapper(name,url,params,parser,max_age):
    sent=time.time()
    try:
        doc=read(url,params)
        got=time.time()
        event,values=parser(doc,got)
        age=got-event
        if age < -2:
            raise ValueError("future market event")
        return name,{"event_at":stamp(event),"acquired_at":stamp(got),"requested_at":stamp(sent),
                     "source_published_at":None,"age_seconds":round(age,3),"max_age_seconds":max_age,
                     "status":"valid" if age<=max_age else "stale",
                     "endpoint":url,"parameters":params,"value":values}
    except (OSError,ValueError,KeyError,IndexError,TypeError) as err:
        return name,{"status":"unavailable","event_at":None,"acquired_at":stamp(time.time()),
                     "requested_at":stamp(sent),"source_published_at":None,
                     "endpoint":url,"parameters":params,
                     "http_status":getattr(err,"code",None),"error":str(err)}


def kline_delta(doc,now):
    rows=[]
    seen=set()
    for x in doc:
        t=int(x[0])//1000
        end=(int(x[6])+1)//1000
        v,b=finite(x[5]),finite(x[9])
        if end>now:
            continue
        if t%STEP or end-t!=STEP or t in seen or not 0<=b<=v:
            raise ValueError("invalid signed-volume input")
        rows.append((end,2*b-v,v))
        seen.add(t)
    rows.sort()
    return rows[-1][0],{"period_seconds":STEP,"unit":"BTC","increment":rows[-1][1],
                         "rows":[[stamp(t),d,v] for t,d,v in rows]}


def okay(doc):
    if str(doc["code"])!="0":
        raise ValueError(str(doc.get("msg",doc["code"])))
    return doc["data"]


def okx_taker(doc,now):
    rows={}
    for t,s,b,*_ in okay(doc):
        point=int(t)/1000
        sell,buy=finite(s),finite(b)
        if point+300>now:
            continue
        if point%300 or point in rows or min(sell,buy)<0:
            raise ValueError("invalid taker-volume bucket")
        rows[point]=(buy-sell,buy+sell)
    latest=max(rows)
    # A full extra interval is required before use, irrespective of the source's bucket convention.
    tail=list(range(int(latest)-35*300,int(latest)+1,300))
    unit="BTC as reported by OKX rubik/stat/taker-volume"
    total=sum(rows[x][1] for x in tail) if all(x in rows for x in tail) else 0
    return latest,{"timestamp_qualification":"OKX reported bucket timestamp; conservatively wait one bucket",
                   "market_scope":"BTC all OKX futures/swaps, not Binance BTCUSDT or exact BTC-USDT-SWAP",
                   "unit":unit,"period_seconds":300,"latest_increment":rows[latest][0],
                   "imbalance_3h":sum(rows[x][0] for x in tail)/total if total else None,
                   "rows":[[stamp(t),*rows[t]] for t in sorted(rows)[-52:]]}


def okx_oi(doc,_):
    d=okay(doc)[0]
    vals={k:finite(d[k]) for k in ("oi","oiCcy","oiUsd") if d.get(k)}
    if not vals or min(vals.values())<=0:
        raise ValueError("invalid open interest")
    return int(d["ts"])/1000,{"instrument":d["instId"],"contracts":vals.get("oi"),
                              "btc":vals.get("oiCcy"),"usd":vals.get("oiUsd")}


def okx_funding(doc,_):
    d=okay(doc)[0]
    return int(d["ts"])/1000,{"instrument":d["instId"],"rate":finite(d["fundingRate"]),
                              "funding_event_at":stamp(int(d["fundingTime"])/1000),
                              "next_funding_at":stamp(int(d["nextFundingTime"])/1000) if d.get("nextFundingTime") else None,
                              "kind":"live indicated rate; separate from settled funding"}


def binance_oi(doc,_):
    x=doc[-1]
    v=finite(x["sumOpenInterest"])
    if v<=0:
        raise ValueError("nonpositive OI")
    return int(x["timestamp"])/1000,{"btc":v,"usd":finite(x["sumOpenInterestValue"])}


def bybit_oi(doc,_):
    if doc.get("retCode")!=0:
        raise ValueError(str(doc.get("retMsg")))
    x=doc["result"]["list"][0]
    return int(x["timestamp"])/1000,{"btc":finite(x["openInterest"]),"instrument":"BTCUSDT linear"}


def bitstamp_price(doc,_):
    t=int(doc["timestamp"])
    vals={k:finite(doc[k]) for k in ("last","bid","ask")}
    if not 0<vals["bid"]<vals["ask"] or vals["last"]<=0:
        raise ValueError("bad Bitstamp quote")
    return t,vals


def coinbase_price(doc,_):
    trade=doc["trades"][0]
    vals={"last":finite(trade["price"]),"bid":finite(doc["best_bid"]),"ask":finite(doc["best_ask"])}
    if not 0<vals["bid"]<vals["ask"] or vals["last"]<=0:
        raise ValueError("bad Coinbase quote")
    return epoch(trade["time"]),vals


def bars(doc,now):
    rows={}
    for d in doc["data"]["ohlc"]:
        t=int(d["timestamp"])
        o,h,l,c,v=(finite(d[k]) for k in ("open","high","low","close","volume"))
        if t+STEP>now:
            continue
        if t%STEP or t in rows or not 0<l<=min(o,c)<=max(o,c)<=h or v<0:
            raise ValueError("invalid Bitstamp candle")
        rows[t]=(o,h,l,c,v)
    times=sorted(rows)
    missing=(times[-1]-times[0])//STEP+1-len(times)
    return times[-1]+STEP,{"count":len(times),"missing":missing,"duplicates":0,
                           "rows":[[t,*rows[t]] for t in times]}


def collect():
    tasks=(
      ("bitstamp_price",BITSTAMP+"/ticker/btcusd/",None,bitstamp_price,30),
      ("coinbase_price",COINBASE+"/ticker",{"limit":1},coinbase_price,30),
      ("bitstamp_15m",BITSTAMP+"/ohlc/btcusd/",{"step":900,"limit":192,"exclude_current_candle":"true"},bars,960),
      ("binance_spot_cvd",BINANCE_SPOT+"/klines",{"symbol":"BTCUSDT","interval":"15m","limit":16},kline_delta,1100),
      ("binance_perpetual_cvd",BINANCE_FUTURES+"/klines",{"symbol":"BTCUSDT","interval":"15m","limit":16},kline_delta,1100),
      ("binance_oi","https://fapi.binance.com/futures/data/openInterestHist",{"symbol":"BTCUSDT","period":"15m","limit":2},binance_oi,1500),
      ("bybit_oi",BYBIT+"/market/open-interest",{"symbol":"BTCUSDT","category":"linear","intervalTime":"15min","limit":2},bybit_oi,1500),
      ("okx_oi",OKX+"/public/open-interest",{"instType":"SWAP","instId":"BTC-USDT-SWAP"},okx_oi,120),
      ("okx_funding",OKX+"/public/funding-rate",{"instId":"BTC-USDT-SWAP"},okx_funding,300),
      ("okx_btc_contract_taker_delta",OKX+"/rubik/stat/taker-volume",{"ccy":"BTC","instType":"CONTRACTS","period":"5m"},okx_taker,1200))
    with ThreadPoolExecutor(max_workers=10) as pool:
        result=dict(pool.map(lambda a:wrapper(*a),tasks))
    b,c=result["bitstamp_price"],result["coinbase_price"]
    if b["status"]==c["status"]=="valid" and abs(epoch(b["event_at"])-epoch(c["event_at"]))<=15:
        mids=[mean([x["value"]["bid"],x["value"]["ask"]]) for x in (b,c)]
        result["coinbase_bitstamp_live_premium"]={"status":"valid","event_at":min(b["event_at"],c["event_at"]),
            "acquired_at":max(b["acquired_at"],c["acquired_at"]),"source_published_at":None,
            "value":{"basis_points":1e4*(mids[1]/mids[0]-1),"maximum_event_separation_seconds":15}}
    return result
