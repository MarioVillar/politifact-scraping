if __name__ == "__main__":
    from politifact_scraping.scraping import PolitifactScraper

    # Example with init and end dates, if not provided, it will scrape all the data available
    scraper = PolitifactScraper(init_date="2025-12-01", end_date="2026-03-31")

    # Scrape the data
    articles = scraper.scrape_all_articles()

    print(articles[0])

    speakers = scraper.scrape_all_speakers()

    print(speakers[0])

    reviewers = scraper.scrape_all_reviewers()

    print(reviewers[0])

    issues = scraper.scrape_all_issues()

    print(issues[0])
