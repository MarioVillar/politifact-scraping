"""
politifact_scraping — Scrape content from the PolitiFact website.
"""

from politifact_scraping.scraping import PolitifactScraper
from politifact_scraping.mongodb import PolitiFactDB

__all__ = ["PolitifactScraper", "PolitiFactDB"]
