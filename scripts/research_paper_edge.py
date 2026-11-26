"""Journal prospective simulated fills from an exchange order-book snapshot."""
import json
import math
import os
import re
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from urllib.request import Request, urlopen

from research_prices import OUT, stamp, write_json

BOOK = "https://www.bitstamp.net/api/v2/order_book/btcusd/"
TAKER = .004
SLIPPAGE = .0005
START = 10_000.
RULES = ("daily:ban:0.10:signal", "daily:ban:0.20:signal", "three:ban:0.10:signal",
         "three_etf:ban:0.10:signal", "three_etf_derivatives:ban:0.10:signal")
ROOT = OUT/"paper_edge"


def get(url, token=None):
    headers = {"User-Agent": "btc-paper-research", "Cache-Control": "no-cache"}
    if token:
        headers["Authorization"] = "Bearer "+token
    with urlopen(Request(url, headers=headers),timeout=8) as stream:
        return json.load(stream)


def ts(value):
    return datetime.fromisoformat(value).timestamp()


def next_book(after, seconds=45):
    errors = []
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:
            record=get(BOOK)
            receive=time.time()
            if "microtimestamp" in record:
                exchange=int(record["microtimestamp"])/1_000_000
            else:
                exchange=int(record["timestamp"])
            bid,bid_size=map(float,record["bids"][0][:2])
            ask,ask_size=map(float,record["asks"][0][:2])
            if (after < exchange <= receive+1 and receive-exchange <= 20
                    and all(map(math.isfinite,(bid,ask,bid_size,ask_size)))
                    and 0 < bid < ask and min(bid_size,ask_size)>0):
                return dict(exchange_at=stamp(exchange),received_at=stamp(receive),
                            bid=bid,ask=ask,bid_btc=bid_size,ask_btc=ask_size,source=BOOK), errors
            errors.append("quote not newer than publication readback, crossed, or stale")
        except (OSError, ValueError, KeyError, IndexError) as err:
            errors.append(str(err))
        time.sleep(2)
    return None, list(dict.fromkeys(errors))


def account_state(prior, target, q):
    cash,balance,peak,prior_dd = (prior[k] for k in ("cash_usd","position_btc","peak_usd","maximum_drawdown"))
    result={"status":"awaiting_valid_quote","fill_price_usd":None,"signed_quantity_btc":0.,
            "fee_usd":0.,"cash_usd":cash,"position_btc":balance,"nav_usd":prior["nav_usd"],
            "peak_usd":peak,"drawdown":prior["drawdown"],"maximum_drawdown":prior_dd,
            "target_weight":target}
    if not q:
        return result
    mid=(q["bid"]+q["ask"])/2
    marked=cash+balance*mid
    delta=target*marked-balance*mid
    result["status"]="marked_no_order"
    if abs(delta)>=5 or (target==0 and balance>0):
        buying=delta>0
        price=q["ask"]*(1+SLIPPAGE) if buying else q["bid"]*(1-SLIPPAGE)
        qty=min(q["ask_btc"],cash/(price*(1+TAKER)),delta/price) if buying else -min(q["bid_btc"],balance,-delta/price)
        if qty:
            fee=abs(qty)*price*TAKER
            cash-=qty*price+fee
            balance+=qty
            result.update(status="paper_fill",fill_price_usd=price,signed_quantity_btc=qty,fee_usd=fee)
    marked=cash+balance*mid
    peak=max(peak,marked)
    dd=1-marked/peak
    result.update(cash_usd=cash,position_btc=balance,nav_usd=marked,peak_usd=peak,
                  drawdown=dd,maximum_drawdown=max(prior_dd,dd))
    return result


def main():
    token=os.environ["GH_TOKEN"]
    repo=os.environ["GITHUB_REPOSITORY"]
    run=os.environ["GITHUB_RUN_ID"]
    # The workflow already pushed the immutable signal and fast-forwarded the checkout.
    matches=[]
    for path in sorted((OUT/"multiframe_shadow").glob("*.json"),reverse=True)[:3]:
        doc=json.loads(path.read_text())
        if str(doc["run_id"])==run:
            matches.append((path,doc))
    if not matches:
        print("No fresh signal published by this run; no historical order may be replayed.")
        return
    path, doc=matches[0]
    ROOT.mkdir(exist_ok=True)
    journal=ROOT/(path.stem+".json")
    if journal.exists():
        return
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{4}",path.stem):
        raise ValueError("Unexpected signal path")
    gh=f"https://api.github.com/repos/{repo}"
    observed=get(f"{gh}/contents/{path}?ref=main",token)
    verified=time.time()
    import base64
    if json.loads(base64.b64decode(observed["content"]))!=doc:
        raise ValueError("Published signal contents differ from local signal")
    commits=get(f"{gh}/commits?path={path}&sha=main&per_page=1",token)
    commit=commits[0]
    committed_at=commit["commit"]["committer"]["date"]
    latest=sorted(ROOT.glob("*.json"))
    zero=dict(cash_usd=START,position_btc=0.,nav_usd=START,peak_usd=START,drawdown=0.,maximum_drawdown=0.)
    prior=json.loads(latest[-1].read_text())["accounts"] if latest else {}
    if latest and latest[-1].stem>=path.stem:
        raise ValueError("Shadow journal is no longer chronological")
    # Conservative recorded publication upper bound from actual readback. Never use an older quote.
    if not 0 <= verified-ts(doc["signal_close_bjt"]) <= 3600:
        print("Signal too old for a paper execution")
        return
    quote, errors=next_book(verified)
    targets={name:doc["targets"].get(name,2*doc["targets"][RULES[0]]) for name in RULES}
    accounts={name:account_state(prior.get(name,zero),value,quote) for name,value in targets.items()}
    write_json(journal,{"run_id":run,"signal_file":str(path),"signal_at":doc["signal_close_bjt"],
              "git_publication_commit_at":committed_at,"confirmed_published_at":stamp(verified),
              "publication_commit":commit["sha"],"signal_blob":observed["sha"],
              "paper_only":True,"initial_usd_each":START,"assumed_taker_fee":TAKER,
              "assumed_extra_slippage":SLIPPAGE,"quote":quote,"quote_errors":errors,"accounts":accounts})
    print("Published paper journal",journal,"quote available:",quote is not None,flush=True)


if __name__=="__main__":
    main()
