# PolitiFact Scraping

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19062297.svg)](https://doi.org/10.5281/zenodo.19062297)
[![PyPI version](https://img.shields.io/pypi/v/politifact-scraping)](https://pypi.org/project/politifact-scraping/)
[![Python versions](https://img.shields.io/pypi/pyversions/politifact-scraping)](https://pypi.org/project/politifact-scraping/)
[![License: CC BY 4.0](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)

A Python library to scrape fact-check articles, speakers, reviewers and issues from the [PolitiFact](https://www.politifact.com/) website. Optionally store everything in MongoDB.

## Installation

```bash
pip install politifact-scraping
```

## Quick start

### Scrape data into Python objects

```python
from politifact_scraping import PolitifactScraper

# Optionally restrict the date range (defaults to all available data since 2007)
scraper = PolitifactScraper(init_date="2025-01-01", end_date="2025-12-31")

# Scrape all articles, speakers, reviewers and issues
articles  = scraper.scrape_all_articles()
speakers  = scraper.scrape_all_speakers()
reviewers = scraper.scrape_all_reviewers()
issues    = scraper.scrape_all_issues()
```

Each method returns a list of dictionaries. For example, an article dictionary contains keys such as `article_url`, `title`, `subtitle`, `article_text`, `label`, `speaker_date`, `publish_date`, `image_url`, `sources`, and more.

### Scrape a single article by title

```python
article = scraper.scrape_article_from_title("The claim you want to search for")
```

### Scrape and store in MongoDB

```python
from politifact_scraping import PolitiFactDB

db = PolitiFactDB()

# Scrape everything and persist to MongoDB in one call
db.scrape_and_store(init_date="2025-01-01", end_date="2025-12-31")

# Or query previously stored articles
results = db.find_articles(
    filter={"label": "false"},
    populate_speaker=True,
    num_docs=10,
)
```

## Data collected

The scraper extracts four entity types from PolitiFact:

| Entity       | Key fields                                                                                                                      |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| **Article**  | `article_url`, `title`, `subtitle`, `article_text`, `label`, `speaker_date`, `publish_date`, `image_url`, `sources`, `language` |
| **Speaker**  | `speaker_id`, `name`, `description`, `image_url`, `personal_website_url`, truth-o-meter counts                                  |
| **Reviewer** | `reviewer_id`, `name`, `job_position`, `description`, `image_url`, `twitter_url`, `phone_number`                                |
| **Issue**    | `issue_id`, `name`, `description`, `image_url`, truth-o-meter counts                                                            |

Truth-o-meter labels: `true`, `mostly_true`, `half_true`, `mostly_false`, `false`, `pants_on_fire`.

## Environment variables

MongoDB storage requires the following environment variables (e.g. in a `.env` file):

| Variable           | Description                              |
| ------------------ | ---------------------------------------- |
| `MONGODB_HOST`     | Connection string to the MongoDB cluster |
| `MONGODB_USER`     | User name with access permissions        |
| `MONGODB_PASSWORD` | Password for the user                    |

The data is stored in a database named `politifact` with collections `articles`, `speakers`, `reviewers` and `issues`.

## Requirements

- Python ≥ 3.10
- beautifulsoup4, requests, fuzzywuzzy, python-Levenshtein, pymongo, python-dotenv, numpy, pydantic-core

## License

This project is licensed under the [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) license.

## Citation

If you use this package in your research, please cite:

```bibtex
@dataset{mario_villar_sanz_2026_19062297,
  author       = {Mario Villar Sanz and
                  Zylowski, Thorsten and
                  Wölfel, Matthias and
                  Rico, Noelia and
                  Díaz, Irene},
  title        = {PolitiFact scraping dataset},
  year         = 2026,
  publisher    = {Zenodo},
  doi          = {10.5281/zenodo.19062297},
  url          = {https://doi.org/10.5281/zenodo.19062297},
}
```
