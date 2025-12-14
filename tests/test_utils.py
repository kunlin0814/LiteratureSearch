import sys
import pathlib
import xml.etree.ElementTree as ET
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.data_extraction_utils import (  # noqa: E402
    extract_geo_sra_from_pubmed_xml,
    extract_mesh_from_pubmed_xml,
)
from modules.normalization import normalize_records  # noqa: E402
from modules.pmc_utils import extract_pmc_sections  # noqa: E402
from modules.notion_utils import build_notion_page_properties  # noqa: E402


def test_extract_geo_sra_from_pubmed_xml_combines_databank_and_references():
    xml = """
    <PubmedArticle>
      <MedlineCitation>
        <Article>
          <DataBankList>
            <DataBank>
              <DataBankName>GEO</DataBankName>
              <AccessionNumberList>
                <AccessionNumber>GSE12345</AccessionNumber>
              </AccessionNumberList>
            </DataBank>
            <DataBank>
              <DataBankName>SRA</DataBankName>
              <AccessionNumberList>
                <AccessionNumber>SRP999999</AccessionNumber>
              </AccessionNumberList>
            </DataBank>
          </DataBankList>
        </Article>
      </MedlineCitation>
      <PubmedData>
        <ReferenceList>
          <Reference>
            <Citation>Data deposited in GSE54321 and PRJNA123456</Citation>
          </Reference>
        </ReferenceList>
      </PubmedData>
    </PubmedArticle>
    """
    elem = ET.fromstring(xml)
    geo, sra = extract_geo_sra_from_pubmed_xml(elem)
    assert geo == "GSE12345, GSE54321"
    assert sra == "PRJNA123456, SRP999999"


def test_extract_mesh_from_pubmed_xml_handles_major_topics_and_qualifiers():
    xml = """
    <PubmedArticle>
      <MedlineCitation>
        <MeshHeadingList>
          <MeshHeading>
            <DescriptorName MajorTopicYN="Y">Prostatic Neoplasms</DescriptorName>
            <QualifierName MajorTopicYN="N">genetics</QualifierName>
          </MeshHeading>
          <MeshHeading>
            <DescriptorName MajorTopicYN="N">Spatial Transcriptomics</DescriptorName>
          </MeshHeading>
        </MeshHeadingList>
      </MedlineCitation>
    </PubmedArticle>
    """
    elem = ET.fromstring(xml)
    heading_list, mesh_terms, major_mesh = extract_mesh_from_pubmed_xml(elem)
    assert "Prostatic Neoplasms (genetics)" in heading_list
    assert "Spatial Transcriptomics" in heading_list
    assert mesh_terms == "Prostatic Neoplasms; Spatial Transcriptomics"
    assert major_mesh == "Prostatic Neoplasms"


def test_normalize_records_merges_esummary_and_efetch_data():
    esummary_json = {
        "result": {
            "uids": ["12345"],
            "12345": {
                "title": "Test Title",
                "fulljournalname": "Journal of Tests",
                "pubdate": "2024 Jan 01",
                "articleids": [{"idtype": "doi", "value": "10.1234/test"}],
                "authors": [{"name": "Doe J"}, {"lastname": "Smith", "firstname": "Alice"}],
                "pubtype": ["Journal Article"],
            },
        }
    }
    efetch_data = {
        "12345": {
            "Abstract": "An example abstract.",
            "GEO_List": "GSE00001",
            "SRA_Project": "SRP00001",
            "MeshHeadingList": "Prostate",
            "MeSH_Terms": "Prostate; Spatial Transcriptomics",
            "Major_MeSH": "Prostate",
        }
    }

    records = normalize_records(esummary_json, efetch_data)
    assert len(records) == 1
    rec = records[0]
    assert rec["PMID"] == "12345"
    assert rec["DOI"] == "10.1234/test"
    assert rec["DedupeKey"] == "10.1234/test"
    assert rec["Title"] == "Test Title"
    assert rec["Journal"] == "Journal of Tests"
    assert rec["Authors"] == "Doe J, Smith Alice"
    assert rec["Abstract"] == "An example abstract."
    assert rec["GEO_List"] == "GSE00001"
    assert rec["SRA_Project"] == "SRP00001"
    assert rec["MeshHeadingList"] == "Prostate"
    assert rec["MeSH_Terms"] == "Prostate; Spatial Transcriptomics"
    assert rec["Major_MeSH"] == "Prostate"
    assert isinstance(rec["PubDateParsed"], date)


def test_extract_pmc_sections_collects_key_sections():
    pmc_xml = """
    <article>
      <abstract><p>Study abstract.</p></abstract>
      <body>
        <sec>
          <title>Methods</title>
          <p>Visium workflow described.</p>
        </sec>
        <sec>
          <title>Results</title>
          <p>Spatial patterns identified.</p>
        </sec>
      </body>
      <back>
        <sec>
          <title>Data Availability</title>
          <p>Data in GSE12345.</p>
        </sec>
        <sec>
          <title>Code availability</title>
          <p>Repository linked.</p>
        </sec>
      </back>
    </article>
    """
    extracted = extract_pmc_sections(pmc_xml)
    assert "ABSTRACT:" in extracted
    assert "METHODS:" in extracted
    assert "RESULTS:" in extracted
    assert "DATA AVAILABILITY:" in extracted
    assert "CODE AVAILABILITY:" in extracted


def test_build_notion_page_properties_maps_core_fields_and_dedupes():
    record = {
        "Title": "Example Paper",
        "PMID": "12345",
        "DOI": "10.1234/example",
        "URL": "https://pubmed.ncbi.nlm.nih.gov/12345/",
        "Journal": "Test Journal",
        "Abstract": "Abstract text",
        "Authors": "Doe J; Smith A",
        "MeshHeadingList": "Prostate",
        "MeSH_Terms": "Prostate; Spatial Transcriptomics",
        "Major_MeSH": "Prostate",
        "PublicationTypes": "Journal Article",
        "DedupeKey": "10.1234/example",
        "PubDateParsed": date(2024, 1, 1),
        "RelevanceScore": 90,
        "PipelineConfidence": "High",
        "FullTextUsed": True,
        "StudySummary": "Summary text",
        "WhyRelevant": "Because spatial prostate study.",
        "Methods": "Visium; scRNA-seq",
        "KeyFindings": "Finding A; Finding B",
        "DataTypes": "scRNA-seq; Visium",
        "GEO_List": "GSE00001",
        "SRA_Project": "SRP00001",
        "Group": "Doe Lab",
    }

    props = build_notion_page_properties(record)

    assert props["Title"]["title"][0]["text"]["content"] == "Example Paper"
    assert props["PMID"]["rich_text"][0]["text"]["content"] == "12345"
    assert props["DOI"]["rich_text"][0]["text"]["content"] == "10.1234/example"
    assert props["DedupeKey"]["rich_text"][0]["text"]["content"] == "10.1234/example"
    assert props["PubDate"]["date"]["start"].startswith("2024-01-01")

    data_types = {entry["name"] for entry in props["DataTypes"]["multi_select"]}
    assert data_types == {"scRNA-seq", "Visium"}

    assert props["PipelineConfidence"]["multi_select"][0]["name"] == "High"
    assert props["FullTextUsed"]["checkbox"] is True
    assert props["StudySummary"]["rich_text"][0]["text"]["content"] == "Summary text"
    assert props["Group"]["rich_text"][0]["text"]["content"] == "Doe Lab"
