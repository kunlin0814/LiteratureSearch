"""
PMC (PubMed Central) XML parsing utilities.

This module handles extraction of specific sections from PMC full-text XML:
- Abstract
- Methods
- Results
- Data Availability
- Code Availability
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any


def extract_pmc_sections(pmc_xml: str) -> str:
    """
    Extract relevant sections from PMC full-text XML.
    
    Parses the PMC XML and extracts only the sections needed for AI analysis:
    - Abstract
    - Methods
    - Results
    - Data Availability
    - Code Availability
    
    Args:
        pmc_xml: Raw PMC XML string from efetch
        
    Returns:
        Concatenated text from extracted sections
    """
    try:
        root = ET.fromstring(pmc_xml)
        
        sections = []
        
        # Abstract - usually in <abstract>
        abstract = root.find(".//abstract")
        if abstract is not None:
            abstract_text = " ".join(abstract.itertext()).strip()
            if abstract_text:
                sections.append(f"ABSTRACT:\n{abstract_text}")
        
        # Methods and Results - search in <body> sections
        body = root.find(".//body")
        if body is not None:
            for sec in body.findall(".//sec"):
                title_elem = sec.find("title")
                if title_elem is None:
                    continue
                    
                title = (title_elem.text or "").strip().lower()
                
                # Methods section
                if any(keyword in title for keyword in ["method", "material", "experimental"]):
                    methods_text = " ".join(sec.itertext()).strip()
                    sections.append(f"METHODS:\n{methods_text}")
                
                # Results section
                elif any(keyword in title for keyword in ["result", "finding"]):
                    results_text = " ".join(sec.itertext()).strip()
                    sections.append(f"RESULTS:\n{results_text}")
        
        # Data Availability - in <back> matter
        back = root.find(".//back")
        if back is not None:
            for sec in back.findall(".//sec"):
                title_elem = sec.find("title")
                if title_elem is None:
                    continue
                    
                title = (title_elem.text or "").strip().lower()
                
                # Data Availability
                if "data availability" in title or "data access" in title:
                    data_text = " ".join(sec.itertext()).strip()
                    sections.append(f"DATA AVAILABILITY:\n{data_text}")
                
                # Code Availability
                if "code availability" in title or "software availability" in title:
                    code_text = " ".join(sec.itertext()).strip()
                    sections.append(f"CODE AVAILABILITY:\n{code_text}")
        
        return "\n\n".join(sections) if sections else ""
        
    except Exception as e:
        return f"Error parsing PMC XML: {str(e)}"
