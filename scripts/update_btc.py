#!/usr/bin/env python3
"""Validate Bitstamp candles before publishing main/latest.json. Standard library only."""
import argparse, base64, copy, datetime as dt, json, math, os, time, urllib.request, urllib.error
from decimal import Decimal
from pathlib import Path
SOURCE = 'https://www.bitstamp.net/api/v2/ohlc/btcusd/?step=900&limit=192&exclude_current_candle=true'
TZ = dt.timezone(dt.timedelta(hours=8))
def bj(t): return dt.datetime.fromtimestamp(t,TZ).isoformat(timespec='milliseconds')
def validate(rows, now):
    if len(rows)!=192: raise ValueError(f'Expected 192 candles, got {len(rows)}')
    times=[]
    for x in rows:
        t=int(x['timestamp']); times.append(t)
        if str(t)!=str(x['timestamp']) or t%900: raise ValueError('Invalid timestamp')
        vals=[Decimal(str(x[k])) for k in ('open','high','low','close','volume')]
        if not all(v.is_finite() for v in vals): raise ValueError('Nonfinite OHLCV')
        o,h,l,c,v=vals
        if min(o,h,l,c)<=0 or v<0 or h<max(o,c,l) or l>min(o,c,h): raise ValueError('Invalid OHLCV')
        if t+900>now: raise ValueError('Unclosed candle')
    if len(set(times))!=192: raise ValueError('Duplicate candle')
    if any(b-a!=900 for a,b in zip(times,times[1:])): raise ValueError('Missing or unordered candle')
    expected=int(now)//900*900
    if times[-1]+900!=expected: raise ValueError(f'Stale: latest close {times[-1]+900}, expected {expected}')
    return times

def request_json(url, method='GET', payload=None, token=None):
    headers={'Accept':'application/vnd.github+json' if token else 'application/json','User-Agent':'btc-kline-data-updater','Cache-Control':'no-cache'}
    if token: headers.update({'Authorization':'Bearer '+token,'X-GitHub-Api-Version':'2022-11-28'})
    data=None if payload is None else json.dumps(payload).encode()
    req=urllib.request.Request(url,data=data,headers=headers,method=method)
    try:
        with urllib.request.urlopen(req,timeout=15) as res: return json.load(res)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f'HTTP {e.code} for {url.split("?")[0]}') from None

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--dry-run',action='store_true'); args=ap.parse_args()
    started=time.time(); report={'run_started_at':bj(started),'event':os.getenv('GITHUB_EVENT_NAME','local'),'attempts':[],'status':'failed'}
    Path('run-report.json').write_text(json.dumps(report,indent=2))
    try:
        token=os.getenv('GH_TOKEN'); repo=os.getenv('GITHUB_REPOSITORY','lxiaomng-lgtm/btc-kline-data')
        if not args.dry_run and (not token or os.getenv('GITHUB_ACTIONS')!='true'): raise RuntimeError('Publishing requires authorized GitHub Actions environment')
        endpoint=f'https://api.github.com/repos/{repo}/contents/latest.json'
        previous_meta=request_json(endpoint+'?ref=main',token=token)
        previous=json.loads(base64.b64decode(previous_meta['content']))
        # Start runners four minutes early to absorb some scheduler/queue delay.
        if os.getenv('GITHUB_EVENT_NAME')=='schedule':
            target=int((time.time()+240)//900)*900
            report['target_close_at']=bj(target)
            while time.time()<target+1:
                time.sleep(min(15,target+1-time.time()))
        acquisition_started=time.time()
        for attempt in range(1,7):
            astart=time.time()
            try:
                data=request_json(SOURCE); received=time.time(); rows=data['data']['ohlc']; times=validate(rows,received)
                report['attempts'].append({'request_started_at':bj(astart),'response_completed_at':bj(received),'status':'valid','latest_close':bj(times[-1]+900)})
                break
            except Exception as e:
                report['attempts'].append({'request_started_at':bj(astart),'response_completed_at':bj(time.time()),'status':'failed','error':str(e)})
                if attempt==6 or time.time()-acquisition_started>65: raise
                time.sleep(3)
        result=copy.deepcopy(previous)
        result.update({'mode':'automatic_snapshot','symbol':'BITSTAMP:BTCUSD','interval_seconds':900,'timezone':'Asia/Shanghai','count':192,'snapshot_received_at':bj(received),'automatic_updates':not args.dry_run,'candles':[{'beijing_time':dt.datetime.fromtimestamp(t,TZ).strftime('%Y-%m-%d %H:%M:%S+08:00'),'timestamp':t,**{k:str(x[k]) for k in ('open','high','low','close','volume')}} for x,t in zip(rows,times)],'source_url':SOURCE,'request_started_at':bj(astart),'collected_at':bj(received),'latest_candle_close_at':bj(times[-1]+900),'update_status':'validated','collection_delay_seconds':round(received-(times[-1]+900),3),'validation':{'count':192,'duplicates':0,'missing':0,'abnormal':0},'workflow_run_url':f'https://github.com/{repo}/actions/runs/{os.getenv("GITHUB_RUN_ID", "local")}'})
        report.update({'latest_candle_close_at':result['latest_candle_close_at'],'collected_at':bj(received),'collection_delay_seconds':result['collection_delay_seconds'],'previous_latest_open':previous['candles'][-1]['timestamp'],'new_latest_open':times[-1]})
        if times[-1]<int(previous['candles'][-1]['timestamp']): raise ValueError('Refuse to regress data')
        if args.dry_run:
            report.update({'status':'dry_run_validated','automatic_publication':False}); Path('candidate.json').write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n')
        else:
            content=json.dumps(result,indent=2,ensure_ascii=False)+'\n'
            put=request_json(endpoint,method='PUT',token=token,payload={'message':'Update BTCUSD 15m through '+result['latest_candle_close_at'],'content':base64.b64encode(content.encode()).decode(),'sha':previous_meta['sha'],'branch':'main'})
            completed=time.time()
            verify=request_json(endpoint+'?ref=main',token=token)
            if verify['sha']!=put['content']['sha']: raise RuntimeError('Published blob readback mismatch')
            report.update({'status':'published_and_read_back','publication_completed_at':bj(completed),'readback_completed_at':bj(time.time()),'publication_delay_seconds':round(completed-(times[-1]+900),3),'within_60_seconds':completed-(times[-1]+900)<=60,'commit_sha':put['commit']['sha'],'content_sha':put['content']['sha']})
        print(json.dumps(report,ensure_ascii=False))
    except Exception as e:
        report['error']=str(e); raise
    finally:
        Path('run-report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
        summary=os.getenv('GITHUB_STEP_SUMMARY')
        if summary:
            with open(summary,'a') as f:f.write('## BTC update receipt\n```json\n'+json.dumps(report,indent=2,ensure_ascii=False)+'\n```\n')
if __name__=='__main__': main()
