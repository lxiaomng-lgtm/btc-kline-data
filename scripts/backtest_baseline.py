import csv,json,math,statistics,datetime as dt
from pathlib import Path
rows=list(csv.DictReader(open("history/derived/1d.csv")))
o=[float(r["open"]) for r in rows]; c=[float(r["close"]) for r in rows]
dates=[r["beijing_time"][:10] for r in rows]
fee=.0015
def signal(name,i,holding):
    if name=="hold":return 1
    if name=="fixed10":return .1
    if name=="sma200":return float(c[i]>statistics.mean(c[i-199:i+1]))
    if name=="sma50_200":return float(statistics.mean(c[i-49:i+1])>statistics.mean(c[i-199:i+1]))
    if name=="breakout20_10":
        if c[i]>max(c[i-20:i]):return 1
        if c[i]<min(c[i-10:i]):return 0
        return holding
    raise ValueError(name)
def run(name,a,b):
    cash=100000.; qty=0.; peak=cash; dd=0.; trades=0; target=0.; costs=0.; prev=0.
    for i in range(a,b):
        target=signal(name,i-1,target)
        # Fixed allocation rebalanced daily; other strategies only on signal changes.
        if i==a or target!=prev or name=="fixed10":
            nav=cash+qty*o[i]; delta=target*nav-qty*o[i]
            if delta>0:
                spend=min(delta,cash/(1+fee)); qty+=spend/o[i]; cash-=spend*(1+fee); costs+=spend*fee
                if spend>1e-8:trades+=1
            elif delta<0:
                sell=min(-delta,qty*o[i]);qty-=sell/o[i];cash+=sell*(1-fee);costs+=sell*fee
                if sell>1e-8:trades+=1
        prev=target
        nav=cash+qty*c[i];peak=max(peak,nav);dd=max(dd,1-nav/peak)
    liquidation=qty*c[b-1]*fee
    nav=cash+qty*c[b-1]-liquidation
    dd=max(dd,1-nav/peak)
    return {"start":dates[a],"end":dates[b-1],"days":b-a,"final_usd":round(nav,2),"net_return_pct":round((nav/100000-1)*100,3),"annualized_pct":round(((nav/100000)**(365.2425/(b-a))-1)*100,3),"max_daily_close_drawdown_pct":round(dd*100,3),"orders":trades,"cost_usd_including_terminal_liquidation":round(costs+liquidation,2)}
names=["hold","fixed10","sma200","sma50_200","breakout20_10"]
splits={"development":(200,next(i for i,d in enumerate(dates) if d>="2024-01-01")),"validation":(next(i for i,d in enumerate(dates) if d>="2024-01-01"),next(i for i,d in enumerate(dates) if d>="2025-01-01")),"historical_test":(next(i for i,d in enumerate(dates) if d>="2025-01-01"),len(rows))}
result={"source":"Bitstamp daily Beijing boundaries","initial_usd":100000,"one_way_cost":fee,"execution":"prior closed daily signal, next daily open; terminal liquidation cost; no leverage or cash interest","limitations":"Historical test is not prospective; daily-close drawdown only; fixed10 is a new benchmark, not existing B ledger; no ETF, macro or Chan rules; no parameter optimization.","results":{k:{n:run(n,a,b) for n in names} for k,(a,b) in splits.items()}}
p=Path("history/backtests");p.mkdir(exist_ok=True);(p/"baseline.json").write_text(json.dumps(result,indent=2)+"\n")
