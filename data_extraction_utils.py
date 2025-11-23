"""
Data extraction utilities for PubMed XML.

This module handles extraction of:
- GEO accessions (from DataBankList and ReferenceList)
- SRA/BioProject accessions (from DataBankList and ReferenceList)
- MeSH terms (major and all)
- MeSH headings with qualifiers
"""

import re
import xml.etree.ElementTree as ET
from typing import Tuple, Set


def extract_geo_sra_from_pubmed_xml(article_element: ET.Element) -> Tuple[str, str]:
    """
    Extract GEO and SRA accessions from PubMed article XML.
    
    Searches both DataBankList (structured metadata) and ReferenceList (citations)
    to capture data accessions. Many newer papers cite data repositories as
    references rather than using structured metadata.
    
    Args:
        article_element: PubmedArticle XML element
        
    Returns:
        Tuple of (geo_list, sra_project) as comma-separated strings
    """
    geo_accessions = set()
    sra_accessions = set()
    
    # Method 1: DataBankList (structured metadata)
    data_bank_list_elem = article_element.find(".//DataBankList")
    if data_bank_list_elem is not None:
        for databank in data_bank_list_elem.findall(".//DataBank"):
            db_name_elem = databank.find("DataBankName")
            db_name = db_name_elem.text.strip() if db_name_elem is not None and db_name_elem.text else ""
            acc_list_elem = databank.find("AccessionNumberList")
            if acc_list_elem is not None:
                accs = [acc.text.strip() for acc in acc_list_elem.findall("AccessionNumber") if acc.text]
                if db_name.upper() == "GEO":
                    geo_accessions.update(accs)
                elif db_name.upper() == "SRA":
                    sra_accessions.update(accs)
    
    # Method 2: ReferenceList (fallback for papers that cite data as references)
    # ReferenceList is under PubmedData, so we parse from the serialized article
    article_str = ET.tostring(article_element, encoding='unicode')
    if '<ReferenceList>' in article_str:
        temp_root = ET.fromstring(f'<temp>{article_str}</temp>')
        ref_list_elem = temp_root.find('.//ReferenceList')
        
        if ref_list_elem is not None:
            for ref in ref_list_elem.findall('.//Reference'):
                citation_elem = ref.find('Citation')
                if citation_elem is not None:
                    # Citation may have child elements like <i>, so use itertext()
                    citation_text = ''.join(citation_elem.itertext())
                    # Extract GEO accessions (GSE followed by digits)
                    geo_matches = re.findall(r'GSE\d+', citation_text)
                    geo_accessions.update(geo_matches)
                    # Extract SRA/BioProject accessions
                    sra_matches = re.findall(r'(?:PRJNA|SRP|SRR|SRX|SRS)\d+', citation_text)
                    sra_accessions.update(sra_matches)
    
    geo_list = ", ".join(sorted(geo_accessions)) if geo_accessions else ""
    sra_project = ", ".join(sorted(sra_accessions)) if sra_accessions else ""
    
    return geo_list, sra_project


def extract_mesh_from_pubmed_xml(article_element: ET.Element) -> Tuple[str, str, str]:
    """
    Extract MeSH terms and headings from PubMed article XML.
    
    Args:
        article_element: PubmedArticle XML element
        
    Returns:
        Tuple of (mesh_heading_list, mesh_terms, major_mesh) as semicolon-separated strings
    """
    mesh_terms_set: Set[str] = set()
    major_mesh_set: Set[str] = set()
    mesh_heading_entries = []
    
    for mh in article_element.findall(".//MeshHeading"):
        desc = mh.find("DescriptorName")
        if desc is None:
            continue
        desc_text = desc.text
        major_topic_yn = desc.attrib.get("MajorTopicYN", "N")
        
        qualifiers = []
        for q in mh.findall("QualifierName"):
            if q.text:
                qualifiers.append(q.text)
                if q.attrib.get("MajorTopicYN", "N") == "Y":
                    major_topic_yn = "Y"
        
        if qualifiers:
            entry = f"{desc_text} ({', '.join(qualifiers)})"
        else:
            entry = desc_text
        mesh_heading_entries.append(entry)
        mesh_terms_set.add(desc_text)
        if major_topic_yn == "Y":
            major_mesh_set.add(desc_text)
    
    mesh_heading_list = "; ".join(mesh_heading_entries) if mesh_heading_entries else ""
    mesh_terms = "; ".join(sorted(mesh_terms_set)) if mesh_terms_set else ""
    major_mesh = "; ".join(sorted(major_mesh_set)) if major_mesh_set else ""
    
    return mesh_heading_list, mesh_terms, major_mesh
