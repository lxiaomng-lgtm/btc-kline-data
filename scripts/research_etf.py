"""Persist Farside's source values and study historical flow/price divergence."""

import csv
import hashlib
from collections import Counter
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from html.parser import HTMLParser
from pathlib import Path
from statistics import correlation, mean
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from research_prices import OUT, stamp, write_json

URLS = {"recent": "https://farside.co.uk/btc/",
        "history": "https://farside.co.uk/bitcoin-etf-flow-all-data/"}
FUNDS = ("IBIT", "FBTC", "BITB", "ARKB", "BTCO", "EZBC", "BRRR", "HODL",
         "BTCW", "MSBT", "GBTC", "BTC")
NOT_LISTED_UNTIL = {"BTC": date(2024, 7, 31), "MSBT": date(2026, 4, 8)}
NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


class TableRows(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.row = []
        elif tag in ("td", "th") and self.row is not None:
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self.cell is not None:
            self.row.append("".join(self.cell).strip())
            self.cell = None
        elif tag == "tr" and self.row is not None:
            self.rows.append(self.row)
            self.row = None
            self.cell = None


def flow_number(cell):
    if cell in ("", "-", "–", "—"):
        return None
    return Decimal(cell.replace(",", "").replace("(", "-").replace(")", "").replace("−", "-"))


def parse(html):
    tables = TableRows()
    tables.feed(html)
    rows = {}
    seen_header = False
    for row in tables.rows:
        if len(row) != len(FUNDS) + 2:
            continue
        if tuple(row[1:-1]) == FUNDS:
            seen_header = True
        try:
            day = datetime.strptime(row[0], "%d %b %Y").date()
        except ValueError:
            continue
        if not seen_header:
            raise ValueError("Farside columns changed")
        values = tuple(map(flow_number, row[1:]))
        if day in rows or any(x is not None and not x.is_finite() for x in values):
            raise ValueError(f"Invalid Farside row {day}")
        rows[day] = values
    if not rows:
        raise ValueError("Farside returned no dated rows")
    return rows


def download(url):
    with urlopen(Request(url, headers={"User-Agent": "BTC historical research/1.0"}), timeout=30) as response:
        return response.read().decode("utf-8")


def classification(day, historical, recent):
    vals = historical if historical is not None else recent
    values, total = vals[:-1], vals[-1]
    if all(x is None for x in values) and total == 0:
        return "NO_FLOW_ROW"
    for name, val in zip(FUNDS, values):
        if val is None and day >= NOT_LISTED_UNTIL.get(name, date.min):
            return "PENDING"
    if total is None:
        return "PENDING"
    known = [x for x in values if x is not None]
    residual = abs(sum(known) - total)
    if recent is not None:
        if recent != historical:
            return "PENDING"
        return "FINAL" if residual == 0 else "TOTAL_MISMATCH"
    if residual > Decimal("0.05") * (len(known) + 1):
        return "TOTAL_MISMATCH"
    return "HISTORICAL_ONLY"


def relationship(pairs):
    if len(pairs) < 3:
        return {"n": len(pairs)}
    x, y = zip(*pairs)
    return {"n": len(pairs), "pearson_r": round(correlation(x, y), 5) if len(set(x)) > 1 and len(set(y)) > 1 else None,
            "mean_return_pct": round(100 * mean(y), 5), "up_pct": round(100 * sum(v > 0 for v in y) / len(y), 3)}


def lookup_prices():
    result = {}
    paths = sorted(Path("history/15m").glob("*.csv"))
    tail = OUT / "bitstamp_tail_15m.csv"
    if tail.exists():
        paths.append(tail)
    for f in paths:
        with f.open() as stream:
            for row in csv.DictReader(stream):
                t = int(row["timestamp"])
                value = float(row["open"]), float(row["close"])
                if t in result and result[t] != value:
                    raise ValueError(f"Conflicting Bitstamp price at {stamp(t)}")
                result[t] = value
    return result


def main():
    OUT.mkdir(exist_ok=True)
    payloads = {name: download(url) for name, url in URLS.items()}
    recent, history = (parse(payloads[key]) for key in ("recent", "history"))
    observed = datetime.now(UTC)
    prices = lookup_prices()
    sessions = []
    counts = Counter()
    samples = {"same_session": [], "late_entry_24h": [],
               "divergence_inflow_btc_down": [], "divergence_outflow_btc_up": [],
               "agreement_inflow_btc_up": [], "agreement_outflow_btc_down": []}
    splits = {k: {name: [] for name in samples} for k in ("2024", "2025", "2026")}
    for day in sorted(set(history) | set(recent)):
        older, newer = history.get(day), recent.get(day)
        vals = older if older is not None else newer
        status = classification(day, older, newer)
        counts[status] += 1
        total = vals[-1]
        sessions.append([str(day), status, *vals, newer[-1] if newer else None])
        if status not in ("FINAL", "HISTORICAL_ONLY") or not total:
            continue
        open_ny = int(datetime.combine(day, time(9, 30), NY).timestamp())
        close_ny = int(datetime.combine(day, time(16), NY).timestamp())
        late = int(datetime.combine(day + timedelta(days=2), time(), UTC).timestamp())
        if open_ny not in prices or close_ny - 900 not in prices:
            continue
        same = prices[close_ny - 900][1] / prices[open_ny][0] - 1
        sample = (float(total), same)
        samples["same_session"].append(sample)
        splits[str(day.year)]["same_session"].append(sample)
        if late not in prices or late + 86400 - 900 not in prices:
            continue
        forward = prices[late + 86400 - 900][1] / prices[late][0] - 1
        pair = (float(total), forward)
        samples["late_entry_24h"].append(pair)
        splits[str(day.year)]["late_entry_24h"].append(pair)
        group = None
        if same < 0:
            group = "divergence_inflow_btc_down" if total > 0 else "agreement_outflow_btc_down"
        elif same > 0:
            group = "agreement_inflow_btc_up" if total > 0 else "divergence_outflow_btc_up"
        if group:
            samples[group].append(pair)
            splits[str(day.year)][group].append(pair)
    with (OUT / "etf_history.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ny_session", "status", *FUNDS, "total_usd_m", "recent_total_usd_m"])
        writer.writerows(sessions)
    finals = [row for row in sessions if row[1] == "FINAL"]
    if not finals:
        raise ValueError("No matching ETF sessions across the two current Farside pages")
    write_json(OUT / "etf.json", {
        "sources": URLS, "observed_at": observed.isoformat(), "from": sessions[0][0],
        "through": sessions[-1][0], "counts": dict(counts),
        "latest_double_verified": {"date": finals[-1][0], "total_usd_m": float(finals[-1][-2])},
        "rounding": "FINAL requires exact displayed sums and two matching pages. The historical-only tolerance is 0.05 USDm per numeric fund plus 0.05 USDm for Total. Dash remains blank; only the pre-listing exceptions can qualify as complete.",
        "same_window": "09:30 to 16:00 fixed New York clock window, including early-close days. Descriptive, no assumed availability.",
        "forward_window": "Entry at 00:00 UTC session date +2 calendar days, hold 24h. Using revised history without original publish timestamps. Descriptive only; gross price moves without trades.",
        "all_years": {k: relationship(v) for k, v in samples.items()},
        "by_calendar_year": {year: {k: relationship(v) for k, v in groups.items()} for year,groups in splits.items()},
        "page_sha256": {k: hashlib.sha256(v.encode()).hexdigest() for k, v in payloads.items()},
    })
    snapshot = OUT / "etf_observations"
    snapshot.mkdir(exist_ok=True)
    receipt = snapshot / f"{observed.date()}.json"
    if not receipt.exists():
        days = sorted(set(recent) | set(history))[-7:]
        write_json(receipt, {"observed_at_utc": observed.isoformat(),
                             "columns": [*FUNDS, "Total"],
                             "observations": [{"date": str(d), "recent": list(map(str, recent[d])) if d in recent else None,
                                               "history": list(map(str, history[d])) if d in history else None,
                                               "status": classification(d, history.get(d), recent.get(d))} for d in days]})
    print(f"ETF {dict(counts)}, last dual verified {finals[-1][0]}")


if __name__ == "__main__":
    main()
