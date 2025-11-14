import types
import json
from datetime import datetime

import pytest


# Import the module under test
import Prefect_literatureSearch as pls


class DummyResp:
    def __init__(self, json_data=None, text_data="", status=200, headers=None):
        self._json = json_data
        self.text = text_data
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code and int(self.status_code) >= 400:
            raise Exception(f"HTTP {self.status_code}")

    def json(self):
        return self._json


class DummySession:
    def __init__(self, get_map=None, post_map=None, patch_map=None):
        self.get_map = get_map or {}
        self.post_map = post_map or {}
        self.patch_map = patch_map or {}

    def get(self, url, params=None, timeout=None):
        h = self.get_map.get(url)
        if callable(h):
            return h(params)
        return h or DummyResp(status=404)

    def post(self, url, headers=None, json=None, timeout=None):
        h = self.post_map.get(url)
        if callable(h):
            return h(json)
        return h or DummyResp(status=404)

    def patch(self, url, headers=None, json=None, timeout=None):
        h = self.patch_map.get(url)
        if callable(h):
            return h(json)
        return h or DummyResp(status=404)


def test_get_config_defaults(monkeypatch):
    monkeypatch.setenv("NCBI_EMAIL", "you@example.com")
    monkeypatch.setenv("NCBI_API_KEY", "api-key")
    cfg = pls.get_config.fn()  # call the underlying function of Prefect task
    assert cfg["QUERY_TERM"]
    assert cfg["RETMAX"] == 200
    assert cfg["RELDATE_DAYS"] == 365
    assert cfg["NCBI_API_KEY"] == "api-key"
    assert cfg["EMAIL"] == "you@example.com"


def test_validate_results_ok():
    esearch_out = {"count": 50, "ids": ["1", "2", "3"]}
    cfg = {"HISTORICAL_MEDIAN": 100, "GOLD_SET": []}
    out = pls.validate_results.fn(esearch_out, cfg)
    assert out["validation"]["status"] == "OK"


def test_validate_results_alert_on_jump():
    esearch_out = {"count": 1000, "ids": []}
    cfg = {"HISTORICAL_MEDIAN": 100, "GOLD_SET": []}
    out = pls.validate_results.fn(esearch_out, cfg)
    assert out["validation"]["status"] == "ALERT"


def test_normalize_records_merge_esummary_and_abstracts():
    esummary_json = {
        "result": {
            "uids": ["12345"],
            "12345": {
                "title": "Test Title",
                "fulljournalname": "Test Journal",
                "pubdate": "2024/01/15",
                "articleids": [{"idtype": "doi", "value": "10.1000/testdoi"}],
            }
        }
    }
    abstracts = {"12345": "Abstract text here."}
    recs = pls.normalize_records.fn(esummary_json, abstracts)
    assert len(recs) == 1
    r = recs[0]
    assert r["PMID"] == "12345"
    assert r["DOI"] == "10.1000/testdoi"
    assert r["Abstract"] == "Abstract text here."
    assert r["DedupeKey"] == "10.1000/testdoi"
    assert r["PubDateParsed"].year in (2024,)


def test_validate_goldset_detects_missing():
    records = [
        {"PMID": "111", "DOI": "10.1/a"},
        {"PMID": "222", "DOI": None},
    ]
    cfg = {"GOLD_SET": ["333", "10.1/a"]}
    out = pls.validate_goldset.fn(records, cfg)
    assert out["goldMissing"] is True
    assert "333" in out["missing"]


def test_classify_records_split():
    records = [
        {"PMID": "1", "DedupeKey": "A"},
        {"PMID": "2", "DedupeKey": "B"},
    ]
    index = {"A": "page-1"}
    to_create, to_update = pls.classify_records.fn(records, index)
    assert len(to_update) == 1 and to_update[0]["page_id"] == "page-1"
    assert len(to_create) == 1 and to_create[0]["DedupeKey"] == "B"


def test_notion_properties_mapping():
    rec = {
        "Title": "Some Paper",
        "DOI": "10.10/xyz",
        "PMID": "999",
        "URL": "https://pubmed.ncbi.nlm.nih.gov/999/",
        "Journal": "J Test",
        "PubDateParsed": datetime(2023, 5, 1).date(),
        "DedupeKey": "10.10/xyz",
    }
    props = pls._notion_page_properties_from_record(rec)
    assert props["Title"]["title"][0]["text"]["content"] == "Some Paper"
    assert props["URL"]["url"].startswith("https://")
    assert props["Dedupe Key"]["rich_text"][0]["text"]["content"] == "10.10/xyz"
    assert props["PubDate"]["date"]["start"].startswith("2023-05-01")


def test_pubmed_esearch_builds_params(monkeypatch):
    # Arrange a dummy session to capture params
    captured = {}

    def get_handler(params):
        captured.update(params)
        # minimal esearch JSON
        data = {
            "esearchresult": {
                "count": "2",
                "idlist": ["100", "101"],
                "webenv": "we",
                "querykey": "qk",
            }
        }
        return DummyResp(json_data=data, status=200)

    dummy = DummySession(get_map={
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi": get_handler
    })

    monkeypatch.setenv("NCBI_EMAIL", "you@example.com")
    monkeypatch.setenv("NCBI_API_KEY", "api-key")

    monkeypatch.setattr(pls, "_make_session", lambda: dummy)
    cfg = pls.get_config.fn()
    out = pls.pubmed_esearch.fn(cfg)
    assert out["count"] == 2
    # check some key params were passed through
    assert captured["email"] == "you@example.com"
    assert captured["api_key"] == "api-key"
    assert captured["usehistory"] == "y"


def test_pubmed_esummary_history_paginates(monkeypatch):
    # Create two batches totaling 3 records
    calls = {"count": 0}

    def esummary_handler(params):
        calls["count"] += 1
        start = int(params.get("retstart", 0))
        # emit one uid per call
        uid = str(200 + start)
        js = {"result": {uid: {"title": f"Paper {uid}", "articleids": []}}}
        return DummyResp(json_data=js, status=200)

    dummy = DummySession(get_map={
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi": esummary_handler
    })
    monkeypatch.setattr(pls, "_make_session", lambda: dummy)

    cfg = pls.get_config.fn(retmax=1)
    esearch_out = {"webenv": "we", "query_key": "qk"}
    merged = pls.pubmed_esummary_history.fn(cfg, esearch_out, total=2)
    uids = [k for k in merged.get("result", {}).keys() if k.isdigit()]
    assert len(uids) == 2
    assert calls["count"] >= 2
