import sys
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import modules.enrichment as enrichment  # noqa: E402


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(enrichment.time, "sleep", lambda *args, **kwargs: None)


def _base_record(pmid: str = "1") -> dict:
    return {
        "PMID": pmid,
        "Title": "Test Title",
        "Authors": "Doe J",
        "Journal": "Test Journal",
        "URL": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
    }


def _empty_cfg():
    return {"AI_PROVIDER": "openai"}


def test_openai_escalates_on_ambiguous_score(monkeypatch):
    calls = []

    def fake_openai(user_prompt, logger, model_name="gpt-5-nano"):
        calls.append(model_name)
        if model_name == "gpt-5-nano":
            return (
                {
                    "RelevanceScore": 75,
                    "WhyRelevant": "Ambiguous",
                    "StudySummary": "",
                    "Methods": "",
                    "KeyFindings": "",
                    "DataTypes": "",
                    "Group": "",
                },
                5,
            )
        return (
            {
                "RelevanceScore": 90,
                "WhyRelevant": "Clear",
                "StudySummary": "",
                "Methods": "",
                "KeyFindings": "",
                "DataTypes": "",
                "Group": "",
            },
            5,
        )

    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.setattr(enrichment, "_call_openai_api", fake_openai)

    result = enrichment.ai_enrich_records(
        records=[_base_record()],
        efetch_data={"1": {"Abstract": "example abstract"}},
        pmc_fulltext_map={},
        cfg=_empty_cfg(),
    )

    assert calls == ["gpt-5-nano", "gpt-5-mini"]
    assert result[0]["RelevanceScore"] == 90
    assert result[0]["PipelineConfidence"] == "Medium"


def test_openai_falls_back_when_primary_raises(monkeypatch):
    calls = []

    def flaky_openai(user_prompt, logger, model_name="gpt-5-nano"):
        calls.append(model_name)
        if model_name == "gpt-5-nano":
            raise ValueError("simulated failure")
        return (
            {
                "RelevanceScore": 80,
                "WhyRelevant": "Recovered",
                "StudySummary": "",
                "Methods": "",
                "KeyFindings": "spatial findings",
                "DataTypes": "Visium, scRNA-seq",
                "Group": "Doe Lab",
            },
            5,
        )

    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.setattr(enrichment, "_call_openai_api", flaky_openai)

    result = enrichment.ai_enrich_records(
        records=[_base_record()],
        efetch_data={"1": {"Abstract": "example abstract"}},
        pmc_fulltext_map={},
        cfg=_empty_cfg(),
    )

    assert calls == ["gpt-5-nano", "gpt-5-mini"]
    rec = result[0]
    assert rec["RelevanceScore"] == 80
    assert rec["PipelineConfidence"] == "Medium-Ambiguous"
    assert rec["DataTypes"] == "visium, scrna-seq"
