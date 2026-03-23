"""
This module provides functionalities to download the text of the articles from Politifact
"""

from bs4 import BeautifulSoup
import requests
from fuzzywuzzy import fuzz
from datetime import datetime
import concurrent.futures
import unicodedata
import re

from src.utils import extract_date


class PolitifactScraper:
    """
    Scrape information from Politifact website.

    Extracts information about articles, speakers and reviewers from the Politifact website.

    :cvar str politifact_url: the base url of the Politifact website.
    :cvar str search_base_url: the base url for searching articles in the Politifact website.
    :cvar str url_list_articles: the url template for listing articles in the Politifact website from an specific year and page (articles are distributed in different pages for the same year).
    :cvar str issues_url: the base url for the article issues in the Politifact website.
    :cvar str national_issue_url: the url of the page with the National issue.
    :cvar str reviewers_url: the url of the page with the staff members (reviewers).
    :ivar int end_date: the date until which data will be scraped. Defaults to the current date.
    :ivar int init_date: the date from which data will be scraped. Defaults to 2007-01-01, the year in which Politifact started publishing.
    """

    _headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    }
    politifact_url = "https://www.politifact.com"
    search_base_url = f"{politifact_url}/search/?q="
    url_list_articles = f"{politifact_url}/factchecks/list/?page=<NUM_PAG>&pubdate=<YEAR>"

    issues_url = f"{politifact_url}/issues/"
    national_issue_url = f"{politifact_url}/truth-o-meter/"

    reviewers_url = f"{politifact_url}/staff/"  # Same as reviewers

    def __init__(self, init_date: datetime | str = None, end_date: datetime | str = None) -> None:
        """
        Constructor of the class PolitifactScraper.

        Saves the current year as the year until which data will be scraped.

        :param init_date: The date from which the data will be scraped. If passed as string, it should be on the format "%Y-%m-%d". Defaults to 2007-01-01.
        :type init_date: datetime | str, optional
        :param end_date: The date until which the data will be scraped. If passed as string, it should be on the format "%Y-%m-%d". Defaults to the current date.
        :type end_date: datetime | str, optional

        :raises ValueError: If the date is not a datetime object or a string in the format "%Y-%m-%d".
        :raises ValueError: If the date is not a datetime object or a string in the format "%Y-%m-%d".
        """
        if init_date is not None:
            if isinstance(init_date, datetime):
                self.init_date = init_date
            else:
                try:
                    self.init_date = datetime.strptime(init_date, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(
                        "The init date should be either a datetime object or a string in the format YYYY-MM-DD."
                    )
        else:
            self.init_date = datetime(2007, 1, 1)

        if end_date is not None:
            if isinstance(end_date, datetime):
                self.end_date = end_date
            else:
                try:
                    self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(
                        "The end date should be either a datetime object or a string in the format YYYY-MM-DD."
                    )
        else:
            self.end_date = datetime.now()

    def _extract_score_card_items(self, soup: BeautifulSoup) -> dict[str, str]:
        """
        Extract the score card items from the soup object.

        The score card items are the items that contain the number of checks for each label in the truth-o-meter.
        The labels are: "True", "Mostly true", "Half true", "Mostly false", "False" and "Pants on fire".

        :param soup: The soup object containing the HTML of the page.
        :type soup: BeautifulSoup

        :return: A dictionary with the following keys and values:
        - *"true_counts": str* - The number of articles labeled as "True" in the truth-o-meter
        - *"mostly_true_counts": str* - The number of articles labeled as "Moslty true" in the truth-o-meter
        - *"half_true_counts": str* - The number of articles labeled as "Half true" in the truth-o-meter
        - *"mostly_false_counts": str* - The number of articles labeled as "Mostly false" in the truth-o-meter
        - *"false_counts": str* - The number of articles labeled as "False" in the truth-o-meter
        - *"pants_on_fire_counts": str* - The number of articles labeled as "Pants on fire" in the truth-o-meter
        :rtype: dict[str, str]
        """
        # Initialize the dictionary with None values in case their are not found
        return_dict = {
            "true_counts": None,
            "mostly_true_counts": None,
            "half_true_counts": None,
            "mostly_false_counts": None,
            "false_counts": None,
            "pants_on_fire_counts": None,
        }

        # Find the truth-o-meter list items with the history checks
        tru_o_meter_items = soup.find_all("div", class_="m-scorecard__item")

        # For each truth-o-meter item, extract the number of checks
        for item in tru_o_meter_items:
            # Extract the label of the count of checks
            label = self.normalize_text(item.find("h4", class_="m-scorecard__title").find(text=True, recursive=False))

            if label is not None:
                label = label.lower().replace(" ", "_").replace("-", "_")

                # Extract the number of checks of this label
                checks_text = self.normalize_text(
                    item.find("div", class_="m-scorecard__body").find("p", class_="m-scorecard__checks").get_text()
                )

                # Extract just the number of checks using a regexp
                match = re.search(r"\d+", checks_text)
                return_dict[f"{label}_counts"] = int(match.group()) if match else None

        return return_dict

    def normalize_text(self, text: str) -> str:
        """
        Normalize the text to remove any special characters and accents.

        :param text: The text to normalize.
        :type text: str
        :return: The normalized text.
        :rtype: str
        """
        return unicodedata.normalize("NFKD", text).strip() if text is not None else None

    def scrape_all_issues_urls(self) -> set[str]:
        """
        Scrape all the issues urls from the Politifact website.

        :return: A set with the urls of the issues, just in case there were duplicates.
        :rtype: set[str]
        """
        # Extract all the issues from the issues page
        response = requests.get(self.issues_url, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        issues = {self.politifact_url + cat["href"] for cat in soup.select("div.c-chyron__value a[href]")}
        issues.add(self.national_issue_url)
        return issues

    def scrape_all_reviewer_urls(self) -> set[str]:
        """
        Scrape all the reviewers urls (staff members) from the Politifact website.

        :return: A set with the urls of the reviewers, just in case there were duplicates.
        :rtype: set[str]
        """
        # Extract all the reviewers from the reviewers page
        response = requests.get(self.reviewers_url, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        reviewers = {self.politifact_url + rev["href"] for rev in soup.select("li.m-list__item a[href]")}

        return reviewers

    def scrape_all_speaker_articles_ulrs(self) -> tuple[set[str], set[str]]:
        """
        Scrape all the urls of the articles and the speakers from Politifact.

        It scrapes the articles from the first year until the current year.

        A set is used for both the speaker urls and article urls to avoid duplicates. Clearly, the same speaker can appear in different articles. Just in case, to prevent any duplicates, a set is used for the article urls too.

        :return: A tuple with two elements:
        - *set[str]* - A set with the urls of the speakers.
        - *set[str]* - A set with the urls of the articles.
        """

        def get_ulrs_from_page(year: int, page_num: int) -> tuple[set[str], set[str]]:
            """
            Get the urls of the speakers and the articles from a single page.

            If the page does not have any data, it returns empty sets.

            A set is used for both the speaker urls and article urls to avoid duplicates. Clearly, the same speaker can appear in different articles. Just in case, to prevent any duplicates, a set is used for the article urls too.

            :param year: The year of the articles.
            :type year: int
            :param page_num: The page number of the corresponding year.
            :type page_num: int

            :return: A tuple with two elements:
            - *set[str]* - A set with the urls of the speakers.
            - *set[str]* - A set with the urls of the articles.
            """
            page_url = self.url_list_articles.replace("<NUM_PAG>", str(page_num)).replace("<YEAR>", str(year))

            response = requests.get(page_url, headers=self._headers)
            soup = BeautifulSoup(response.text, "html.parser")

            # Find the list items in the page that contain the articles
            list_items = soup.select("ul.o-listicle__list li.o-listicle__item")

            url_speakers = set()
            url_articles = set()

            # For each list item, extract the urls of the speakers and the articles
            for item in list_items:
                footer = item.select_one("footer")
                item_date = extract_date(text=footer.text) if footer else None

                if footer is None or self.init_date <= item_date <= self.end_date:
                    # Extract the URL of the speaker (inside m-statement__author)
                    speaker_div = item.select_one("div.m-statement__author a[href]")
                    if speaker_div:
                        url_speakers.add(self.politifact_url + speaker_div["href"])

                    # Extract the URL of the article (inside m-statement__content)
                    article_div = item.select_one("div.m-statement__content a[href]")
                    if article_div:
                        url_articles.add(self.politifact_url + article_div["href"])

            return url_speakers, url_articles

        def get_urls_from_year(year: int) -> tuple[set[str], set[str]]:
            """
            Get the urls of the speakers and the articles from a whole year.

            A set is used for both the speaker urls and article urls to avoid duplicates. Clearly, the same speaker can appear in different articles. Just in case, to prevent any duplicates, a set is used for the article urls too.

            :param year: The year of the articles.
            :type year: int

            :return: A tuple with two elements:
            - *set[str]* - A set with the urls of the speakers.
            - *set[str]* - A set with the urls of the articles.
            """
            page_num = 1
            year_speaker_urls = set()
            year_article_urls = set()

            page_num = 1

            # Emulate a do-while loop to get all the pages of the year
            while True:
                # Get the urls from the page
                urls_speakers, urls_articles = get_ulrs_from_page(year=year, page_num=page_num)

                # If there are no urls, break the loop
                if not urls_speakers and not urls_articles:
                    break

                # Add the urls to the lists
                year_speaker_urls.update(urls_speakers)
                year_article_urls.update(urls_articles)

                page_num += 1

            return year_speaker_urls, year_article_urls

        speaker_urls = set()
        article_urls = set()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Create tasks for each year
            future_to_year = {
                executor.submit(get_urls_from_year, year): year
                for year in range(self.init_date.year, self.end_date.year + 1)
            }

            # Collect the results as they complete
            for future in concurrent.futures.as_completed(future_to_year):
                urls_speakers, urls_articles = future.result()
                speaker_urls.update(urls_speakers)
                article_urls.update(urls_articles)

        return speaker_urls, article_urls

    def scrape_all_ulrs(self) -> tuple[set[str], set[str], set[str], set[str]]:
        """
        Scrape all the urls of the articles, speakers, issues and reviewers from Politifact.

        It scrapes the articles from the first year until the current year.

        A set is used for the urls to avoid duplicates. Clearly, the same speaker can appear in different articles. Just in case, to prevent any duplicates, a set is used for the article, issues and reviewers urls too.

        :return: A tuple with four elements:
        - *set[str]* - A set with the urls of the speakers.
        - *set[str]* - A set with the urls of the articles.
        - *set[str]* - A set with the urls of the issues.
        - *set[str]* - A set with the urls of the reviewers.
        """
        speaker_urls, article_urls = self.scrape_all_speaker_articles_ulrs()

        return speaker_urls, article_urls, self.scrape_all_issues_urls(), self.scrape_all_reviewer_urls()

    def scrape_article_from_url(self, url: str) -> dict[str, str | datetime]:
        """
        Scrape the information of the article from Politifact from its url.

        :param url: The url of the article.
        :type url: str

        :return: A dictionary with the following keys and values:

        - *"article_url": str* - The url of the article in Politifact
        - *"language": str* - The language of the article. It can be either "english" or "spanish"
        - *"title": str* - The title of the article
        - *"subtitle": str* - The sub title of the article
        - *"article_text": str* - The text of the article
        - *"context": str* - The context of the statement (where the statement was made)
        - *"speaker_date": datetime* - The date at which the speaker made the statement
        - *"publish_date": datetime* - The date at which Politifact published the review
        - *"image_url": str* - The url of the image of the article
        - *"issues_ids": list[str]* - The ids of the issues of the article. It can be empty if there are no issues.
        - *"reviewers_ids": list[str]* - The ids of the reviewers of the article. It can be empty if there are no reviewers.
        - *"speaker_id": str* - The id of the speaker of the article. It can be empty if there are no speakers.
        - *"label": str* - The label of the review of the article. It can be either "true", "mostly_true", "half_true", "mostly_false", "false" or "pants_on_fire".

        :rtype: dict[str, str | datetime]
        """
        return_dict = {}

        # Fetch the news article page
        response = requests.get(url, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        return_dict["article_url"] = url

        # Extract the speaker of the article
        speaker = soup.select_one("a.m-statement__name")
        return_dict["speaker_id"] = speaker["href"].split("/")[-2] if speaker else None

        # Extract the language of the article
        language = soup.select_one("div.lang-sub-nav.m-togglist__list strong")
        language = self.normalize_text(text=language.text) if language else None

        # For some reason (some special characters) the scraped "Español" is not exactly the same as "Español" in the string
        if fuzz.ratio("Español", language) > 70:
            language = "spanish"
        elif fuzz.ratio("English", language) > 70:
            language = "english"

        return_dict["language"] = language

        # Extract the title
        title = soup.find("div", class_="m-statement__quote")
        return_dict["title"] = self.normalize_text(text=title.text).strip('"') if title else None

        # Extract the subtitle
        subtitle = soup.find("h1", class_="c-title c-title--subline")
        return_dict["subtitle"] = self.normalize_text(subtitle.text) if subtitle else None

        # Extract the article text
        article_paragraphs = soup.find("article", class_="m-textblock").find_all("p")
        return_dict["article_text"] = (
            self.normalize_text("".join([p.text for p in article_paragraphs]).replace("\t", ""))
            if article_paragraphs
            else None
        )

        # Extract the statement description
        statement_description = soup.find("div", class_="m-statement__desc")
        statement_description = self.normalize_text(statement_description.text) if statement_description else None

        # Date in which the speaker made the statement
        return_dict["speaker_date"] = (
            extract_date(text=statement_description, language=language) if statement_description else None
        )

        # Extract the context of the statement
        return_dict["context"] = (
            self.normalize_text(text=statement_description.split("in")[1].split(":")[0]).lower()
            if statement_description
            else None
        )

        # Extract the date at which Politifact published the review
        publish_date = soup.find("span", class_="m-author__date")
        return_dict["publish_date"] = (
            extract_date(text=self.normalize_text(publish_date.text), language=language) if publish_date else None
        )

        # Extract the image url
        image_url = soup.find("img", class_="c-image__original lozad")
        return_dict["image_url"] = image_url["data-src"].strip() if image_url else None

        # Extract the reviewers of the article (some of them have more than one)
        reviewers = soup.select("div.m-author__content.copy-xs.u-color--chateau a[href]")
        return_dict["reviewers_ids"] = [rev["href"].split("/")[-2] for rev in reviewers] if reviewers else []

        # Extract the issues of the article
        issues_urls = soup.select("li.m-list__item a[href]")
        return_dict["issues_ids"] = (
            [cat["href"].split("/")[-2] for cat in issues_urls if not "personalities" in cat["href"]]
            if issues_urls
            else []
        )

        # Extract the sources used by the reviewers from the article
        sources_list = soup.select("section#sources.m-superbox p")

        return_dict["sources"] = []

        for source in sources_list:
            text = self.normalize_text(text=source.text)

            if text is not None and len(text) > 0:
                links = [a["href"] for a in source.find_all("a", href=True)]

                return_dict["sources"].append(
                    {
                        "text": text,
                        "links": links,
                    }
                )

        # Extract the label of the article
        label = soup.select_one("div.m-statement__meter div.c-image img.c-image__original")

        if label is not None:
            label = self.normalize_text(label["alt"])

            # Correct the label to match the truth-o-meter labels
            match label:
                case "barely-true":
                    label = "mostly_false"
                case "pants-fire":
                    label = "pants_on_fire"

            label = label.replace("-", "_").replace(" ", "_")

        return_dict["label"] = label

        return return_dict

    def scrape_all_articles(self, batch_size: int = 100) -> list[dict]:
        """
        Scrape all articles from the Politifact website using multiple threads.

        First scrapes all article URLs, then scrapes each article in batches using separate threads.

        :param batch_size: The number of articles to scrape in each batch, defaults to 100.
        :type batch_size: int, optional

        :return: A list of dictionaries, each containing the scraped information of an article.
        :rtype: list[dict]
        """
        _, article_urls = self.scrape_all_speaker_articles_ulrs()
        article_urls = list(article_urls)

        def scrape_articles_batch(batch_urls: list[str]) -> list[dict]:
            """
            Scrape a batch of articles from the Politifact website.

            :param batch_urls: The list of article URLs to scrape.
            :type batch_urls: list[str]
            :return: A list of dictionaries with the scraped article information.
            :rtype: list[dict]
            """
            articles_info = []

            for url in batch_urls:
                try:
                    art_info = self.scrape_article_from_url(url=url)
                    articles_info.append(art_info)
                except Exception:
                    continue

            return articles_info

        all_articles = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for i in range(0, len(article_urls), batch_size):
                batch_urls = article_urls[i : i + batch_size]
                futures.append(executor.submit(scrape_articles_batch, batch_urls))

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_articles.extend(future.result())
                except Exception:
                    continue

        return all_articles

    def scrape_all_speakers(self, batch_size: int = 100) -> list[dict]:
        """
        Scrape all speakers from the Politifact website using multiple threads.

        First scrapes all speaker URLs, then scrapes each speaker in batches using separate threads.

        :param batch_size: The number of speakers to scrape in each batch, defaults to 100.
        :type batch_size: int, optional

        :return: A list of dictionaries, each containing the scraped information of a speaker.
        :rtype: list[dict]
        """
        speaker_urls, _ = self.scrape_all_speaker_articles_ulrs()
        speaker_urls = list(speaker_urls)

        def scrape_speakers_batch(batch_urls: list[str]) -> list[dict]:
            """
            Scrape a batch of speakers from the Politifact website.

            :param batch_urls: The list of speaker URLs to scrape.
            :type batch_urls: list[str]
            :return: A list of dictionaries with the scraped speaker information.
            :rtype: list[dict]
            """
            speakers_info = []

            for url in batch_urls:
                try:
                    speak_info = self.scrape_speaker_from_url(url=url)
                    speakers_info.append(speak_info)
                except Exception:
                    continue

            return speakers_info

        all_speakers = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for i in range(0, len(speaker_urls), batch_size):
                batch_urls = speaker_urls[i : i + batch_size]
                futures.append(executor.submit(scrape_speakers_batch, batch_urls))

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_speakers.extend(future.result())
                except Exception:
                    continue

        return all_speakers

    def scrape_speaker_from_url(self, url: str) -> dict[str, str]:
        """
        Scrape the information of the speaker from Politifact from its url.

        :param url: The url of the speaker page.
        :type url: str

        :return: A dictionary with the following keys and values:

        - *"speaker_id": str* - The id of the speaker in Politifact (extracted from the url)
        - *"speaker_url": str* - The url of the speaker page in Politifact
        - *"name": str* - The name of the article
        - *"description": str* - The description of the speaker
        - *"image_url": str* - The url of the image of the speaker
        - *"personal_website_url": str* - The url of the personal website of the speaker
        - *"true_counts": str* - The number of articles of the speaker labeled as "True" in the truth-o-meter
        - *"mostly_true_counts": str* - The number of articles of the speaker labeled as "Moslty true" in the truth-o-meter
        - *"half_true_counts": str* - The number of articles of the speaker labeled as "Half true" in the truth-o-meter
        - *"mostly_false_counts": str* - The number of articles of the speaker labeled as "Mostly false" in the truth-o-meter
        - *"false_counts": str* - The number of articles of the speaker labeled as "False" in the truth-o-meter
        - *"pants_on_fire_counts": str* - The number of articles of the speaker labeled as "Pants on fire" in the truth-o-meter

        :rtype: dict[str, str | datetime]
        """
        return_dict = {}

        # Fetch the speaker page
        response = requests.get(url, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        return_dict["speaker_url"] = url

        return_dict["speaker_id"] = url.split("/")[-2]

        # Extract the name
        name = soup.find("h1", class_="m-pageheader__title")
        return_dict["name"] = self.normalize_text(name.text) if name else None

        # Extract the description
        description = soup.find("div", class_="m-pageheader__body")
        return_dict["description"] = self.normalize_text(description.text) if description else None

        # Extract the image url
        image_url = soup.find("img", class_="c-image__original")
        return_dict["image_url"] = image_url["src"].strip() if image_url else None

        # Extract the personal website url
        footer_link = soup.select_one("footer.m-pageheader__footer a[href]")
        return_dict["personal_website_url"] = footer_link["href"] if footer_link else None

        # Find the truth-o-meter list items with the history checks
        return_dict.update(self._extract_score_card_items(soup=soup))

        return return_dict

    def scrape_all_issues(self, batch_size: int = 5) -> list[dict]:
        """
        Scrape all issues from the Politifact website using multiple threads.

        First scrapes all issue URLs, then scrapes each issue in batches using separate threads.

        :param batch_size: The number of issues to scrape in each batch, defaults to 5.
        :type batch_size: int, optional

        :return: A list of dictionaries, each containing the scraped information of an issue.
        :rtype: list[dict]
        """
        issue_urls = list(self.scrape_all_issues_urls())

        def scrape_issues_batch(batch_urls: list[str]) -> list[dict]:
            """
            Scrape a batch of issues from the Politifact website.

            :param batch_urls: The list of issue URLs to scrape.
            :type batch_urls: list[str]
            :return: A list of dictionaries with the scraped issue information.
            :rtype: list[dict]
            """
            issues_info = []

            for url in batch_urls:
                try:
                    iss_info = self.scrape_issue_from_url(url=url)
                    issues_info.append(iss_info)
                except Exception:
                    continue

            return issues_info

        all_issues = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for i in range(0, len(issue_urls), batch_size):
                batch_urls = issue_urls[i : i + batch_size]
                futures.append(executor.submit(scrape_issues_batch, batch_urls))

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_issues.extend(future.result())
                except Exception:
                    continue

        return all_issues

    def scrape_issue_from_url(self, url: str) -> dict[str, str]:
        """
        Scrape the information of the issue from Politifact from its url.

        :param url: The url of the issue page.
        :type url: str

        :return: A dictionary with the following keys and values:

        - *"issue_id": str* - The id of the issue in Politifact (extracted from the url)
        - *"issue_url": str* - The url of the issue page in Politifact
        - *"name": str* - The name of the issue
        - *"description": str* - The description of the issue
        - *"image_url": str* - The url of the image of the issue
        - *"true_counts": str* - The number of articles of the issue labeled as "True" in the truth-o-meter
        - *"mostly_true_counts": str* - The number of articles of the issue labeled as "Moslty true" in the truth-o-meter
        - *"half_true_counts": str* - The number of articles of the issue labeled as "Half true" in the truth-o-meter
        - *"mostly_false_counts": str* - The number of articles of the issue labeled as "Mostly false" in the truth-o-meter
        - *"false_counts": str* - The number of articles of the issue labeled as "False" in the truth-o-meter
        - *"pants_on_fire_counts": str* - The number of articles of the issue labeled as "Pants on fire" in the truth-o-meter

        :rtype: dict[str, str]
        """
        return_dict = {}

        # Fetch the issues page
        response = requests.get(url, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        return_dict["issue_url"] = url

        return_dict["issue_id"] = url.split("/")[-2]

        # Extract the name
        name = soup.find("h1", class_="m-pageheader__title")
        return_dict["name"] = self.normalize_text(name.text) if name else None

        if return_dict["name"] is None:
            name = soup.find("h1", class_="m-issue__title")
            return_dict["name"] = self.normalize_text(name.text) if name else None

        # Extract the description
        description = soup.select_one("div.m-issue__body p")
        return_dict["description"] = self.normalize_text(text=description.text) if description else None

        # Extract the image url
        image_url = soup.select_one("div.m-issue__bg img.c-image__original")
        return_dict["image_url"] = image_url["src"].strip() if image_url else None

        # Find the truth-o-meter list items with the history checks
        return_dict.update(self._extract_score_card_items(soup=soup))

        return return_dict

    def scrape_all_reviewers(self, batch_size: int = 1) -> list[dict]:
        """
        Scrape all reviewers from the Politifact website using multiple threads.

        First scrapes all reviewer URLs, then scrapes each reviewer in batches using separate threads.

        :param batch_size: The number of reviewers to scrape in each batch, defaults to 1 (because there are few reviewers).
        :type batch_size: int, optional

        :return: A list of dictionaries, each containing the scraped information of a reviewer.
        :rtype: list[dict]
        """
        reviewer_urls = list(self.scrape_all_reviewer_urls())

        def scrape_reviewers_batch(batch_urls: list[str]) -> list[dict]:
            """
            Scrape a batch of reviewers from the Politifact website.

            :param batch_urls: The list of reviewer URLs to scrape.
            :type batch_urls: list[str]
            :return: A list of dictionaries with the scraped reviewer information.
            :rtype: list[dict]
            """
            reviewers_info = []

            for url in batch_urls:
                try:
                    rev_info = self.scrape_reviewer_from_url(url=url)
                    reviewers_info.append(rev_info)
                except Exception:
                    continue

            return reviewers_info

        all_reviewers = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for i in range(0, len(reviewer_urls), batch_size):
                batch_urls = reviewer_urls[i : i + batch_size]
                futures.append(executor.submit(scrape_reviewers_batch, batch_urls))

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_reviewers.extend(future.result())
                except Exception:
                    continue

        return all_reviewers

    def scrape_reviewer_from_url(self, url: str) -> dict[str, str]:
        """
        Scrape the information of the reviewer from Politifact from its url.

        Reviwers are the staff members that review the articles.

        :param url: The url of the reviewer page.
        :type url: str

        :return: A dictionary with the following keys and values:

        - *"reviewer_id": str* - The id of the reviewer in Politifact (extracted from the url)
        - *"reviewer_url": str* - The url of the reviewer page in Politifact
        - *"name": str* - The name of the reviewer
        - *"description": str* - The description of the reviewer
        - *"job_position": str* - The job position of the reviewer
        - *"twitter_url": str* - The url of the twitter account of the reviewer
        - *"phone_number": str* - The phone number of the reviewer
        - *"image_url": str* - The url of the image of the reviewer

        :rtype: dict[str, str]
        """
        return_dict = {}

        # Fetch the reviewer page
        response = requests.get(url, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        return_dict["reviewer_url"] = url

        return_dict["reviewer_id"] = url.split("/")[-2]

        # Extract the name
        name = soup.find("h1", class_="m-pageheader__title")
        return_dict["name"] = self.normalize_text(name.text) if name else None

        # Extract the image url
        image_url = soup.find("img", class_="c-image__original")
        return_dict["image_url"] = image_url["src"].strip() if image_url else None

        # Extract the job position
        job_position = soup.select_one("div.m-pageheader__body h3")
        return_dict["job_position"] = self.normalize_text(text=job_position.text) if job_position else None

        # Extract the description
        description = soup.select_one("div.m-pageheader__body p")
        return_dict["description"] = self.normalize_text(text=description.text) if description else None

        # Extract the contact info
        contact_info = soup.select("footer.m-pageheader__footer a[href]")

        if contact_info is not None:
            for info in contact_info:
                return_dict["twitter_url"] = (
                    self.normalize_text(text=info["href"]) if "twitter" in info["href"] else None
                )

                return_dict["phone_number"] = (
                    self.normalize_text(text=info["href"].replace("tel:", "")) if "tel" in info["href"] else None
                )

                # The email cannot be extracted because it is protected
                # return_dict["email"] = self.normalize_text(text=info.text.replace("mailto:", "")) if "email" in info["href"] else None

        return return_dict

    def scrape_article_from_title(self, target_text: str) -> dict[str, str | datetime]:
        """
        Scrape the information of the article from Politifact knowing the title of the article.

        Search the article in the Politifact website using the title of the article. Then, it scrapes the article info from the article's url.

        :param target_text: The text to search in the articles.
        :type target_text: str

        :raises ValueError: If the news article link is not found.

        :return: A dictionary with the following keys and values:

        - *"article_url": str* - The url of the article in Politifact
        - *"title": str* - The title of the article
        - *"subtitle": str* - The sub title of the article
        - *"article_text": str* - The text of the article
        - *"speaker_date": datetime* - The date at which the speaker made the statement
        - *"publish_date": datetime* - The date at which Politifact published the review
        - *"image_url": str* - The url of the image of the article

        :rtype: dict[str, str | datetime]
        """
        search_url = f"{self.search_base_url}{target_text.replace(' ', '+')}"

        # Fetch search results page after search query
        response = requests.get(search_url, headers=self._headers)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the link to the news article with the target text
        link_tag = soup.find(lambda tag: tag.name == "a" and fuzz.ratio(target_text, tag.text.strip()) > 85)

        if not link_tag:
            raise ValueError("News article link not found.")

        link = f"{self.politifact_url}{link_tag['href']}"

        return self.scrape_article_from_url(url=link)
