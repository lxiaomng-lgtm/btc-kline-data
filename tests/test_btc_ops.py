"""Regression tests for an interrupted paper signal and its recovery."""
import base64
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import research_paper_edge as paper
from research_prices import local, stamp


class PaperRecovery(unittest.TestCase):
    slot=int(datetime(2026,11,27,7,30,tzinfo=timezone.utc).timestamp())
    name=local(slot).strftime("%Y-%m-%dT%H%M")+".json"

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.previous=os.getcwd()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(os.chdir,self.previous)
        os.chdir(self.tmp.name)
        root=Path("research/multiframe_shadow/paper_signals")
        root.mkdir(parents=True)
        self.signal=dict(signal_close_bjt=stamp(self.slot),run_id="older",
                         targets={k:.1 if k=="buy_hold_10" else 0. for k in paper.RULES})
        (root/self.name).write_text(json.dumps(self.signal))
        self.journal=Path("research/paper_edge")/self.name
        def get(url,token):
            return {"content":base64.b64encode(json.dumps(self.signal).encode()),"sha":"blob"} if "/contents/" in url else [
                {"sha":"commit","commit":{"committer":{"date":stamp(self.slot+10)}}}]
        p=patch.object(paper,"get",side_effect=get)
        self.api=p.start();self.addCleanup(p.stop)
        e=patch.dict(os.environ,{"GH_TOKEN":"test","GITHUB_REPOSITORY":"a/b","GITHUB_RUN_ID":"new"})
        e.start(); self.addCleanup(e.stop)

    def test_current_slot_recovers_once(self):
        now=self.slot+30
        quote=dict(bid=60000.,ask=60001.,bid_btc=1.,ask_btc=1.,exchange_at=stamp(now+1),received_at=stamp(now+2))
        with patch.object(paper.time,"time",return_value=now),patch.object(paper,"next_book",return_value=(quote,[])) as fetch:
            paper.main()
            previous=self.journal.read_bytes()
            paper.main()
        self.assertEqual(fetch.call_count,1)
        self.assertEqual(previous,self.journal.read_bytes())
        self.assertTrue(json.loads(previous)["recovered_live_slot"])

    def test_closed_slot_has_no_fetch_or_fill(self):
        with patch.object(paper.time,"time",return_value=self.slot+2700),patch.object(paper,"next_book") as fetch:
            paper.main()
        self.assertFalse(self.journal.exists())
        fetch.assert_not_called()

    def test_unavailable_quote_preserves_recovery(self):
        with patch.object(paper.time,"time",return_value=self.slot+30),patch.object(paper,"next_book",return_value=(None,["offline"])):
            paper.main()
        self.assertFalse(self.journal.exists())
        self.assertTrue(Path("paper_attempt.json").exists())


if __name__=="__main__":
    unittest.main()
