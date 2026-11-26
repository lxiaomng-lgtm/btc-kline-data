"""Freeze publicly collected source values as seen at one actual observation time."""
import csv
import json
import os
from datetime import datetime, timedelta, timezone
from research_derivatives import complete_deribit_window

from research_prices import OUT, write_json

NOW=datetime.now(timezone.utc)


def recent_csv(path, field, since, until):
    with path.open() as f:
        return [r for r in csv.DictReader(f) if since <= datetime.fromisoformat(r[field]) <= until]


def main():
    folder=OUT/"edge"/"source_observations"
    folder.mkdir(parents=True,exist_ok=True)
    path=folder/(NOW.strftime("%Y%m%dT%H%M%S")+"-"+os.environ["GITHUB_RUN_ID"]+".json")
    if path.exists():
        return
    derivatives=json.loads((OUT/"market.json").read_text())
    observed=datetime.fromisoformat(derivatives["observed_at_utc"])
    etf_files=sorted((OUT/"etf_observations").glob("*.json"))
    etf=json.loads(etf_files[-1].read_text()) if etf_files else None
    since=observed-timedelta(days=3)
    snapshots={"captured_at_utc":NOW.isoformat(),"run_id":os.environ["GITHUB_RUN_ID"],
        "market_report_observed_at_utc":observed.isoformat(),"source_errors":derivatives["source_error_counts"],
        "availability":"Historical event time, collector observation and capture time are separate. The hourly rows can be backfilled. This immutable capture may only inform later decisions.",
        "source_definitions":derivatives["definitions"],"etf":etf,
        "hourly_oi_cvd_premium":recent_csv(OUT/"market_hourly.csv","open_utc",since,observed-timedelta(hours=1)),
        "funding_binance":recent_csv(OUT/"funding.csv","open_utc",since,observed),
        "funding_okx":recent_csv(OUT/"funding_okx.csv","open_utc",since,observed),
        "deribit_liquidation_samples":{k:r for k,r in json.loads((OUT/"liquidation_sample.json").read_text()).items()
            if since<=datetime.fromisoformat(k)<=observed}}
    # Preserve the trade records behind a sampled window if a liquidation flag was observed.
    samples=snapshots["deribit_liquidation_samples"]
    flagged=[(k,r) for k,r in samples.items() if r["flags"]]
    if flagged:
        begin,record=max(flagged)
        start=int(datetime.fromisoformat(begin).timestamp()*1000)
        end=int(datetime.fromisoformat(record["through_exclusive"]).timestamp()*1000)
        try:
            trades=[r for r in complete_deribit_window(start,end) if r.get("liquidation")]
            snapshots["deribit_flagged_trade_records"]={"window_start":begin,
                "window_end":record["through_exclusive"],"capture_time_utc":datetime.now(timezone.utc).isoformat(),
                "matching_trade_ids":len({r["trade_id"] for r in trades}),"trades":trades}
        except (OSError,ValueError,KeyError) as err:
            snapshots["deribit_flagged_trade_error"]=str(err)
    write_json(path,snapshots)
    print("Captured source records",path,flush=True)


if __name__=="__main__":
    main()
