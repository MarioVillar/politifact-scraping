"""
This module provides functionalities to connect to the MongoDB databases
"""

import traceback
from datetime import datetime
import time
from typing import Any
from bson import ObjectId
from pymongo import MongoClient
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo.errors import DuplicateKeyError, BulkWriteError, WriteError
from pydantic_core import core_schema
from typing import Any
from bson import ObjectId
from datetime import datetime

from politifact_scraping.scraping import PolitifactScraper
from politifact_scraping.logging_config import LOG_POLITIFACT_SCRAPING, LOG_DELETED_MONGODB_DOCS, LOG_MONGODB
from politifact_scraping.utils import load_env_var


class PyObjectId(str):
    """
    Custom ObjectId class for Pydantic validation.

    This class is used to validate ObjectId fields in Pydantic models.

    :param ObjectId: The ObjectId class from the bson module.
    :type ObjectId: class
    :raises ValueError: If the ObjectId is not valid.
    :return: The ObjectId instance.
    :rtype: ObjectId
    :yield: The ObjectId instance.
    :rtype: ObjectId
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any) -> core_schema.CoreSchema:
        return core_schema.json_or_python_schema(
            json_schema=core_schema.str_schema(),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(ObjectId),
                    core_schema.chain_schema(
                        [
                            core_schema.str_schema(),
                            core_schema.no_info_plain_validator_function(cls.validate),
                        ]
                    ),
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(lambda x: str(x)),
        )

    @classmethod
    def validate(cls, value) -> ObjectId:
        if not ObjectId.is_valid(value):
            raise ValueError("Invalid ObjectId")

        return ObjectId(value)


class MongoDBConnection:
    """
    Create and close connections to MongoDB databases.

    :ivar str client_str: MongoDB connection string
    :ivar pymongo.MongoClient client: MongoDB client object
    :ivar pymongo.database.Database db: The database connection object
    """

    def __init__(self, host_env_name: str, db_name: str) -> None:
        """
        Constructor for MongoDBConnection class.

        :param host_env_name: Environment variable name for the MongoDB host
        :type host_env_name: str
        """
        self._host_env_name = host_env_name
        self._client = None
        self._db = None
        self._db_name = db_name

        self.connect()

    def _client_str(self) -> str:
        """
        Get the MongoDB connection string.

        :return: MongoDB connection string
        :rtype: str
        """
        return f"mongodb+srv://{load_env_var('MONGODB_USER')}:{load_env_var('MONGODB_PASSWORD')}@{load_env_var(self._host_env_name)}/"

    def __del__(self) -> None:
        """
        Destructor for MongoDBConnection class: disconnects from the MongoDB database.
        """
        self.disconnect()

    def connect(self) -> None:
        """
        Connect to the MongoDB database.
        """
        if self._db_name is not None:
            self._client = MongoClient(self._client_str())
            self._db = self._client[self._db_name]

    def disconnect(self) -> None:
        """
        Disconnect from the MongoDB database.
        """
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None

    def is_connected(self) -> bool:
        """
        Check if the connection is established.

        :return: True if the connection is established, False otherwise
        :rtype: bool
        """
        return self._client is not None

    def manage_write_error(self, error: BulkWriteError | DuplicateKeyError | WriteError) -> None:
        """
        Manage write errors by logging them and returning a list of exceptions.

        :param error: The object captured from the exception, containing the details of the error.
        :type error: BulkWriteError | DuplicateKeyError | WriteError
        """
        if isinstance(error, DuplicateKeyError):
            LOG_MONGODB.error(f"Duplicate key error: {error}")
        elif isinstance(error, WriteError):
            LOG_MONGODB.error(f"Write error: {error}")
        elif isinstance(error, BulkWriteError):
            errors = error.details.get("writeErrors", [])

            for err in errors:
                code = err.get("code")
                message = err.get("errmsg")
                detail = {"index": err.get("index"), "op": err.get("op"), "code": code, "errmsg": message}

                if code == 11000:
                    exception = DuplicateKeyError(message, details=detail)
                    LOG_MONGODB.error(f"Duplicate key error: {exception}")
                else:
                    exception = WriteError(message, code=code, details=detail)
                    LOG_MONGODB.error(f"Write error: {exception}")
        else:
            LOG_MONGODB.error(f"Unknown MongoDB error: {error}")

    def _insert_one(self, collection: str, doc: dict, log_into_file: bool = True, **kwargs: Any) -> ObjectId:
        """
        Insert a single document into a collection.

        Same as using directly `insert_one` from pymongo, but adds the createdAt and updatedAt fields to the document.

        :param collection: The name of the collection to insert the document into
        :type collection: str
        :param doc: The document to insert
        :type doc: dict
        :param log_into_file: If True, the error is logged into a file. If False, the error is raised.
        :type log_into_file: bool, optional
        :param kwargs: Additional arguments to pass to the insert_one method
        :type kwargs: Any
        """
        if not self.is_connected():
            self.connect()

        doc["createdAt"] = datetime.now()
        doc["updatedAt"] = datetime.now()

        # Insert the document into the collection
        try:
            return self._db[collection].insert_one(doc, **kwargs)
        except (BulkWriteError, DuplicateKeyError, WindowsError) as e:
            if log_into_file:
                self.manage_write_error(error=e)
            else:
                raise e

    def _insert_many(
        self, collection: str, docs: list[dict], log_into_file: bool = True, **kwargs: Any
    ) -> list[ObjectId]:
        """
        Insert multiple documents into a collection.

        Same as using directly `insert_many` from pymongo, but adds the createdAt and updatedAt fields to each document.

        :param collection: The name of the collection to insert the documents into
        :type collection: str
        :param docs: The documents to insert
        :type docs: list[dict]
        :param log_into_file: If True, the error is logged into a file. If False, the error is raised.
        :type log_into_file: bool, optional
        :param kwargs: Additional arguments to pass to the insert_many method
        :type kwargs: Any
        """
        if not self.is_connected():
            self.connect()

        for doc in docs:
            doc["createdAt"] = datetime.now()
            doc["updatedAt"] = datetime.now()

        # Insert the documents into the collection
        try:
            return self._db[collection].insert_many(docs, **kwargs).inserted_ids
        except (BulkWriteError, DuplicateKeyError, WindowsError) as e:
            if log_into_file:
                self.manage_write_error(error=e)
            else:
                raise e

    def _update_one(
        self, collection: str, filter: dict, update: dict, log_into_file: bool = True, **kwargs: Any
    ) -> None:
        """
        Update a single document in a collection.

        Same as using directly `update_one` from pymongo, but adds the updatedAt field to the document.

        :param collection: The name of the collection to update the document in
        :type collection: str
        :param filter: The filter to find the document to update
        :type filter: dict
        :param update: The update to apply to the document
        :type update: dict
        :param log_into_file: If True, the error is logged into a file. If False, the error is raised.
        :type log_into_file: bool, optional
        :param kwargs: Additional arguments to pass to the update_one method
        :type kwargs: Any
        """
        if not self.is_connected():
            self.connect()

        update["updatedAt"] = datetime.now()

        # Update the document in the collection
        try:
            self._db[collection].update_one(filter, update, **kwargs)
        except (BulkWriteError, DuplicateKeyError, WindowsError) as e:
            if log_into_file:
                self.manage_write_error(error=e)
            else:
                raise e

    def _update_many(
        self, collection: str, filter: dict, update: dict, log_into_file: bool = True, **kwargs: Any
    ) -> None:
        """
        Update multiple documents in a collection.

        Same as using directly `update_many` from pymongo, but adds the updatedAt field to the documents.

        :param collection: The name of the collection to update the documents in.
        :type collection: str
        :param filter: The filter to find the documents to update.
        :type filter: dict
        :param update: The update to apply to the documents.
        :type update: dict
        :param log_into_file: If True, the error is logged into a file. If False, the error is raised.
        :type log_into_file: bool, optional
        :param kwargs: Additional arguments to pass to the update_many method.
        :type kwargs: Any
        """
        if not self.is_connected():
            self.connect()

        update["updatedAt"] = datetime.now()

        # Update the document in the collection
        try:
            self._db[collection].update_many(filter, update, **kwargs)
        except (BulkWriteError, DuplicateKeyError, WindowsError) as e:
            if log_into_file:
                self.manage_write_error(error=e)
            else:
                raise e

    def _delete_null_coll(self, coll_name: str, delete_null_imgs: bool = True) -> None:
        """
        Delete documents with null or NaN values from a specific collection.

        :param coll_name: The name of the collection to delete the documents from
        :type coll_name: str
        :param delete_null_imgs: If True, documents with null values in the image_url field are also deleted. Defaults to True.
        :type delete_null_imgs: bool, optional
        """

        def delete_value(value: Any) -> None:
            """
            Delete documents with a specific value from the collection.

            :param value: The value to search for documents with it and delete them from the collection
            :type value: Any
            """
            query = [{key: value} for key in self._db[coll_name].find_one().keys() if key != "image_url"]

            if delete_null_imgs:
                query.append({"image_url": value})

            # Get documents with that value
            documents_to_delete = list(self._db[coll_name].find({"$or": query}, {"statement": 1}))

            # Delete the documents with that value
            try:
                self._db[coll_name].delete_many({"$or": query})
            except (BulkWriteError, DuplicateKeyError, WindowsError) as e:
                self.manage_write_error(error=e)

            # Log each document deleted
            for doc in documents_to_delete:
                LOG_DELETED_MONGODB_DOCS.info(
                    f"Deleted from collection {coll_name} the document with statement: {doc['statement']}"
                )

        # Delete documents with null and NaN values from the collection coll_name
        delete_value(value=None)
        delete_value(value=np.nan)


class PolitiFactDB(MongoDBConnection):
    """
    Create, manage and close connections to the PolitiFact cluster in the MongoDB database.
    """

    def __init__(self) -> None:
        """
        Initialises the connection string for the PolitiFact cluster.

        The cluster host should be stored in an environment variable named MONGODB_HOST.
        """
        super().__init__(host_env_name="MONGODB_HOST", db_name="politifact")

        self._article_coll = "articles"
        self._speakers_coll = "speakers"
        self._reviewers_coll = "reviewers"
        self._issues_coll = "issues"

        self._participants_coll = "participants"
        self._exp_rounds_coll = "experiment_rounds"
        self._metrics_coll = "metrics"

        self._questions_coll = "survey_questions"
        self._survey_rounds_coll = "survey_rounds"
        self._final_questions_coll = "final_survey_questions"
        self._final_survey_rounds_coll = "final_survey_rounds"

        # Create indexes
        self._db[self._article_coll].create_index("article_url", unique=True)

        self._db[self._speakers_coll].create_index("speaker_id", unique=True)

        self._db[self._reviewers_coll].create_index("reviewer_id", unique=True)

        self._db[self._issues_coll].create_index("issue_id", unique=True)

        self._db[self._exp_rounds_coll].create_index("round_name", unique=True)

        self._db[self._metrics_coll].create_index("python_varname", unique=True)
        self._db[self._metrics_coll].create_index("name", unique=True)

        self._db[self._survey_rounds_coll].create_index("round_number", unique=True)
        self._db[self._final_survey_rounds_coll].create_index("round_number", unique=True)

    def scrape_and_store_articles(self, article_urls: list[str], batch_size: int = 100) -> None:
        """
        Scrape a batch of articles from the Politifact website and store them in the database in the same operation.

        Each batch is scraped and stored in the MongoDB database in separate threads.

        :param article_urls: The list of article URLs to scrape.
        :type article_urls: list[str]
        :param batch_size: The number of articles to scrape in each batch, defaults to 100.
        :type batch_size: int, optional

        :raises TypeError: If the article_urls parameter cannot be converted to a list.
        """
        try:
            article_urls = list(article_urls)
        except TypeError as e:
            LOG_POLITIFACT_SCRAPING.error(
                f"Error in method PolitiFactDB.scrape_and_store_articles: the value of article_urls cannot be converted to a list -> {e}"
            )
            return

        scraper = PolitifactScraper()

        def scrape_and_store_articles_batch(batch_urls: list[str]) -> None:
            """
            Scrape a batch of articles from the Politifact website and store them in the database in the same operation.

            :param batch_urls: The list of article URLs to scrape.
            :type batch_urls: list[str]
            """
            articles_info = []

            for url in batch_urls:
                try:
                    art_info = scraper.scrape_article_from_url(url=url)
                    articles_info.append(art_info)
                except:
                    continue

            self._insert_many(collection=self._article_coll, docs=articles_info)

        futures = []

        start_time = time.time()

        # Launch the scraping process in multiple threads
        # Scrape the articles in batches of URLs, each batch in a different thread
        with ThreadPoolExecutor() as executor:
            for i in range(0, len(article_urls), batch_size):
                batch_urls = article_urls[i : i + batch_size]
                futures.append(executor.submit(scrape_and_store_articles_batch, batch_urls))

            # Wait for all the threads to finish and handle exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    LOG_POLITIFACT_SCRAPING.error(f"Error in a thread while scraping and storing articles: {e}")
                    LOG_POLITIFACT_SCRAPING.error(traceback.format_exc())

        end_time = time.time()

        LOG_POLITIFACT_SCRAPING.info(
            f"Scraping and storing the info of the articles of PolitiFact took {(end_time - start_time):.2f} seconds."
        )

    def scrape_and_store_speakers(self, speaker_urls: list[str], batch_size: int = 100) -> None:
        """
        Scrape a batch of speakers from the Politifact website and store them in the database in the same operation.

        Each batch is scraped and stored in the MongoDB database in separate threads.

        :param speaker_urls: The list of speaker URLs to scrape.
        :type speaker_urls: list[str]
        :param batch_size: The number of speakers to scrape in each batch, defaults to 100.
        :type batch_size: int, optional

        :raises TypeError: If the speaker_urls parameter cannot be converted to a list.
        """
        try:
            speaker_urls = list(speaker_urls)
        except TypeError as e:
            LOG_POLITIFACT_SCRAPING.error(
                f"Error in method PolitiFactDB.scrape_and_store_speakers: the value of speaker_urls cannot be converted to a list -> {e}"
            )
            return

        scraper = PolitifactScraper()

        def scrape_and_store_speakers_batch(batch_urls: list[str]) -> None:
            """
            Scrape a batch of speakers from the Politifact website and store them in the database in the same operation.

            :param batch_urls: The list of speaker URLs to scrape.
            :type batch_urls: list[str]
            """
            speakers_info = []

            for url in batch_urls:
                speak_info = scraper.scrape_speaker_from_url(url=url)
                speakers_info.append(speak_info)

            self._insert_many(collection=self._speakers_coll, docs=speakers_info)

        futures = []

        start_time = time.time()

        # Launch the scraping process in multiple threads
        # Scrape the speakers in batches of URLs, each batch in a different thread
        with ThreadPoolExecutor() as executor:
            for i in range(0, len(speaker_urls), batch_size):
                batch_urls = speaker_urls[i : i + batch_size]
                futures.append(executor.submit(scrape_and_store_speakers_batch, batch_urls))

            # Wait for all the threads to finish and handle exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    LOG_POLITIFACT_SCRAPING.error(f"Error in a thread while scraping and storing speakers: {e}")
                    LOG_POLITIFACT_SCRAPING.error(traceback.format_exc())

        end_time = time.time()

        LOG_POLITIFACT_SCRAPING.info(
            f"Scraping and storing the info of the speakers of PolitiFact took {(end_time - start_time):.2f} seconds."
        )

    def scrape_and_store_issues(self, issue_urls: list[str], batch_size: int = 5) -> None:
        """
        Scrape a batch of issues from the Politifact website and store them in the database in the same operation.

        Each batch is scraped and stored in the MongoDB database in separate threads.

        :param issue_urls: The list of issue URLs to scrape.
        :type issue_urls: list[str]
        :param batch_size: The number of issues to scrape in each batch, defaults to 5.
        :type batch_size: int, optional

        :raises TypeError: If the issue_urls parameter cannot be converted to a list.
        """
        try:
            issue_urls = list(issue_urls)
        except TypeError as e:
            LOG_POLITIFACT_SCRAPING.error(
                f"Error in method PolitiFactDB.scrape_and_store_issues: the value of issue_urls cannot be converted to a list -> {e}"
            )
            return

        scraper = PolitifactScraper()

        def scrape_and_store_issues_batch(batch_urls: list[str]) -> None:
            """
            Scrape a batch of issues from the Politifact website and store them in the database in the same operation.

            :param batch_urls: The list of issue URLs to scrape.
            :type batch_urls: list[str]
            """
            issues_info = []

            for url in batch_urls:
                iss_info = scraper.scrape_issue_from_url(url=url)
                issues_info.append(iss_info)

            self._insert_many(collection=self._issues_coll, docs=issues_info)

        futures = []

        start_time = time.time()

        # Launch the scraping process in multiple threads
        # Scrape the speakers in batches of URLs, each batch in a different thread
        with ThreadPoolExecutor() as executor:
            for i in range(0, len(issue_urls), batch_size):
                batch_urls = issue_urls[i : i + batch_size]
                futures.append(executor.submit(scrape_and_store_issues_batch, batch_urls))

            # Wait for all the threads to finish and handle exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    LOG_POLITIFACT_SCRAPING.error(f"Error in a thread while scraping and storing issues: {e}")
                    LOG_POLITIFACT_SCRAPING.error(traceback.format_exc())

        end_time = time.time()

        LOG_POLITIFACT_SCRAPING.info(
            f"Scraping and storing the info of the issues of PolitiFact took {(end_time - start_time):.2f} seconds."
        )

    def scrape_and_store_reviewers(self, reviewer_urls: list[str], batch_size: int = 1) -> None:
        """
        Scrape a batch of reviewers from the Politifact website and store them in the database in the same operation.

        Each batch is scraped and stored in the MongoDB database in separate threads.

        :param reviewer_urls: The list of reviewer URLs to scrape.
        :type reviewer_urls: list[str]
        :param batch_size: The number of reviewers to scrape in each batch, defaults to 1 (because there are few reviewers).
        :type batch_size: int, optional

        :raises TypeError: If the reviewer_urls parameter cannot be converted to a list.
        """
        try:
            reviewer_urls = list(reviewer_urls)
        except TypeError as e:
            LOG_POLITIFACT_SCRAPING.error(
                f"Error in method PolitiFactDB.scrape_and_store_reviewers: the value of reviewer_urls cannot be converted to a list -> {e}"
            )
            return

        scraper = PolitifactScraper()

        def scrape_and_store_reviewers_batch(batch_urls: list[str]) -> None:
            """
            Scrape a batch of reviewers from the Politifact website and store them in the database in the same operation.

            :param batch_urls: The list of reviewer URLs to scrape.
            :type batch_urls: list[str]
            """
            reviewers_info = []

            for url in batch_urls:
                rev_info = scraper.scrape_reviewer_from_url(url=url)
                reviewers_info.append(rev_info)

            self._insert_many(collection=self._reviewers_coll, docs=reviewers_info)

        futures = []

        start_time = time.time()

        # Launch the scraping process in multiple threads
        # Scrape the speakers in batches of URLs, each batch in a different thread
        with ThreadPoolExecutor() as executor:
            for i in range(0, len(reviewer_urls), batch_size):
                batch_urls = reviewer_urls[i : i + batch_size]
                futures.append(executor.submit(scrape_and_store_reviewers_batch, batch_urls))

            # Wait for all the threads to finish and handle exceptions
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    LOG_POLITIFACT_SCRAPING.error(f"Error in a thread while scraping and storing reviewers: {e}")
                    LOG_POLITIFACT_SCRAPING.error(traceback.format_exc())

        end_time = time.time()

        LOG_POLITIFACT_SCRAPING.info(
            f"Scraping and storing the info of the reviewers of PolitiFact took {(end_time - start_time):.2f} seconds."
        )

    def scrape_and_store(
        self,
        init_date: datetime | str = None,
        end_date: datetime | str = None,
        scrape_articles: bool = True,
        scrape_speakers: bool = True,
        scrape_issues: bool = True,
        scrape_reviewers: bool = True,
        batch_size_articles: int = 100,
        batch_size_speakers: int = 100,
        batch_size_issues: int = 5,
        batch_size_reviewers: int = 1,
    ) -> None:
        """
        Scrape the articles, speakers, issues and reviewers from the Politifact website and store them in the database.

        This method uses the :py:class:`src.data.scraping.PolitifactScraper` class to scrape the articles, speakers, issues and reviewers from the Politifact website.

        The scraped data is stored in the database in the corresponding collections.

        :param init_date: The date from which the data will be scraped. If passed as string, it should be on the format "%Y-%m-%d". Defaults to 2007-01-01.
        :type init_date: datetime | str, optional
        :param end_date: The date until which the data will be scraped. If passed as string, it should be on the format "%Y-%m-%d". Defaults to the current
        :type end_date: datetime | str, optional
        :param scrape_articles: Whether to scrape the articles or not. Defaults to True.
        :type scrape_articles: bool, optional
        :param scrape_speakers: Whether to scrape the speakers or not. Defaults to True.
        :type scrape_speakers: bool, optional
        :param scrape_issues: Whether to scrape the issues
        or not. Defaults to True.
        :type scrape_issues: bool, optional
        :param scrape_reviewers: Whether to scrape the reviewers or not. Defaults to True.
        :type scrape_reviewers: bool, optional
        :param batch_size_articles: The batch size for the articles scraping. Each batch is scraped and stored in the MongoDB database in separate threads. Defaults to 100.
        :type batch_size_articles: int, optional
        :param batch_size_speakers: The batch size for the speakers scraping. Each batch is scraped and stored in the MongoDB database in separate threads. Defaults to 100.
        :type batch_size_speakers: int, optional
        :param batch_size_issues: The batch size for the issues scraping. Each batch is scraped and stored in the MongoDB database in separate threads. Defaults to 5.
        :type batch_size_issues: int, optional
        :param batch_size_reviewers: The batch size for the reviewers scraping. Each batch is scraped and stored in the MongoDB database in separate threads. Defaults to 1 (because there are few reviewers).
        :type batch_size_reviewers: int, optional
        """
        scraper = PolitifactScraper(init_date=init_date, end_date=end_date)

        ##################################################################################
        # Scrape the URLs
        start_time = time.time()

        speaker_urls, article_urls, issue_urls, reviewers_urls = scraper.scrape_all_ulrs()

        end_time = time.time()

        LOG_POLITIFACT_SCRAPING.info(
            f"Scraping of all the ULRs of PolitiFact tool {(end_time - start_time):.2f} seconds."
        )

        ##################################################################################
        # Scrape the articles in batches of URLs, each batch in a different thread

        if scrape_articles:
            self.scrape_and_store_articles(article_urls=article_urls, batch_size=batch_size_articles)

        ##################################################################################
        # Scrape the speakers in batches of URLs, each batch in a different thread

        if scrape_speakers:
            self.scrape_and_store_speakers(speaker_urls=speaker_urls, batch_size=batch_size_speakers)

        ##################################################################################
        # Scrape the issues in batches of URLs, each batch in a different thread
        if scrape_issues:
            self.scrape_and_store_issues(issue_urls=issue_urls, batch_size=batch_size_issues)

        ##################################################################################
        # Scrape the reviewers (batch size of 1 becuase there are just a few), each URL in a different thread
        if scrape_reviewers:
            self.scrape_and_store_reviewers(reviewer_urls=reviewers_urls, batch_size=batch_size_reviewers)

        ##################################################################################

    def find_articles(
        self,
        filter: dict = None,
        projection: dict = None,
        sort: list[tuple[str, int]] = None,
        num_docs: int = None,
        populate_speaker: bool = False,
        speaker_projection: dict = None,
        populate_reviewers: bool = False,
        reviewers_projection: dict = None,
        populate_issues: bool = False,
        issues_projection: dict = None,
        **kwargs: Any,
    ) -> list[dict]:
        """
        Find articles in the database that match the given filter and projection (if any is provided). The information of the speakers, reviewers and issues can be populated (added) if needed.

        :param filter: The filter to apply to the query. If None, all the documents are returned, defaults to None
        :type filter: dict, optional
        :param projection: The projection to apply to the query. If None, all the fields are returned, defaults to None
        :type projection: dict, optional
        :param sort: The sort to apply to the query. If None, no sorting is applied, defaults to None
        :type sort: list[tuple[str, int]], optional
        :param num_docs: The maximum number of documents to return. If None, all the documents are returned, defaults to None
        :type num_docs: int, optional
        :param populate_speaker: Whether to populate the information of the speaker or not. Defaults to False.
        :type populate_speaker: bool, optional
        :param speaker_projection: The projection to apply to the speaker information, in case it is populated. If None, all the fields are returned, defaults to None
        :type speaker_projection: dict, optional
        :param populate_reviewers: Whether to populate the information of the reviewers or not., defaults to False
        :type populate_reviewers: bool, optional
        :param reviewers_projection: The projection to apply to the reviewers information, in case it is populated. If None, all the fields are returned, defaults to None
        :type reviewers_projection: dict, optional
        :param populate_issues: Whether to populate the information of the issues or not., defaults to False
        :type populate_issues: bool, optional
        :param issues_projection: The projection to apply to the issues information, in case it is populated. If None, all the fields are returned, defaults to None
        :type issues_projection: dict, optional

        :return: articles retrieved from the database.
        :rtype: list[dict]
        """
        cursor = self._db[self._article_coll].find(filter=filter, projection=projection, **kwargs)

        if sort is not None:
            cursor = cursor.sort(sort)

        if num_docs is not None:
            cursor = cursor.limit(num_docs)

        articles = list(cursor)

        # Populate the speaker information
        if populate_speaker:
            for article in articles:
                speaker_id = article["speaker_id"]

                article["speaker"] = self._db[self._speakers_coll].find_one(
                    filter={"speaker_id": speaker_id}, projection=speaker_projection
                )

        # Populate the reviewers information
        if populate_reviewers:
            for article in articles:
                reviewer_ids = article["reviewer_ids"]

                article["reviewers"] = list(
                    self._db[self._reviewers_coll].find(
                        filter={"reviewer_id": {"$in": reviewer_ids}}, projection=reviewers_projection
                    )
                )

        # Populate the issues information
        if populate_issues:
            for article in articles:
                issue_ids = article["issue_ids"]

                article["issues"] = list(
                    self._db[self._issues_coll].find(
                        filter={"issue_id": {"$in": issue_ids}}, projection=issues_projection
                    )
                )

        return articles

    def get_issues(self, filter: dict = None, projection: dict = None) -> list[dict]:
        """
        Get the issues in the database that match the given filter and projection (if any is provided).

        :param filter: The filter to apply to the query. If None, all the documents are returned, defaults to None
        :type filter: dict, optional
        :param projection: The projection to apply to the query. If None, all the fields are returned, defaults to None
        :type projection: dict, optional

        :return: issues retrieved from the database.
        :rtype: list[dict]
        """
        cursor = self._db[self._issues_coll].find(filter=filter, projection=projection)

        issues = list(cursor)

        return issues

    def get_reviewers(self, filter: dict = None, projection: dict = None) -> list[dict]:
        """
        Get the reviewers in the database that match the given filter and projection (if any is provided).

        :param filter: The filter to apply to the query. If None, all the documents are returned, defaults to None
        :type filter: dict, optional
        :param projection: The projection to apply to the query. If None, all the fields are returned, defaults to None
        :type projection: dict, optional

        :return: reviewers retrieved from the database.
        :rtype: list[dict]
        """
        cursor = self._db[self._reviewers_coll].find(filter=filter, projection=projection)

        reviewers = list(cursor)

        return reviewers

    def get_all_survey_questions(self) -> list[dict]:
        """
        Get all survey questions.

        :return: A list with all survey question documents.
        :rtype: list[dict]
        """
        return list(self._db[self._questions_coll].find())

    def get_all_final_survey_questions(self) -> list[dict]:
        """
        Get all final survey questions.

        :return: A list with all final survey question documents.
        :rtype: list[dict]
        """
        return list(self._db[self._final_questions_coll].find())

    def get_survey_question_data(self, question_text: str) -> dict:
        """
        Get the data of a survey question given its text.

        :param question_text: The text of the question to retrieve.
        :type question_text: str

        :return: A dictionary with the data of the question. If no question with the given text is found, None is returned.
        :rtype: dict | None
        """
        question_data = self._db[self._questions_coll].find_one(filter={"question_text": question_text})

        return question_data

    def get_final_survey_question_data(self, question_text: str) -> dict:
        """
        Get the data of a question from the final survey, given its text.

        :param question_text: The text of the question to retrieve.
        :type question_text: str

        :return: A dictionary with the data of the question. If no question with the given text is found, None is returned.
        :rtype: dict | None
        """
        question_data = self._db[self._final_questions_coll].find_one(filter={"question_text": question_text})

        return question_data

    def get_explanations(self) -> tuple[list[str], list[str]]:
        """
        Get the explanations generated by the LLM for each experiment round.

        :return: A tuple with the following elements:
        - *list[str]* - A list with the overall explanations generated by the LLM for each experiment round.
        - *list[str]* - A list with the explanations generated by the LLM for each individual element of the news for each experiment round.
        :rtype: tuple[list[str], list[str]]
        """
        cursor = self._db[self._exp_rounds_coll].find(projection={"llm_overall_expl": 1, "llm_ind_ele_expl": 1})

        rounds = list(cursor)

        global_expl = []
        ind_ele_expl = []

        for exp_round in rounds:
            global_expl.append(exp_round["llm_overall_expl"])

            ind_ele_expl.extend(exp_round["llm_ind_ele_expl"])

        return global_expl, ind_ele_expl

    def get_participant_data(self, participant_projection: dict = None, article_projection: dict = None) -> list[dict]:
        """
        Get the article selections made by the participants in the surveys, with article data populated.

        :param participant_projection: The projection for the participant documents.
        :type participant_projection: dict
        :param article_projection: The projection for the article documents.
        :type article_projection: dict

        :return: A list with the article selections of the participants. Each article ID is replaced with the full article document.
        :rtype: list[dict]
        """
        cursor = self._db[self._participants_coll].find(projection=participant_projection)

        participants = list(cursor)

        # Fetch experiment rounds to get consensus rankings keyed by round_name
        exp_rounds_cursor = self._db[self._exp_rounds_coll].find(projection={"round_name": 1, "consensus_rk": 1})
        # Map round_name -> {article_id_str: position}
        consensus_rk_map = {}
        for exp_round in exp_rounds_cursor:
            rk_positions = {str(oid): idx + 1 for idx, oid in enumerate(exp_round["consensus_rk"])}
            consensus_rk_map[exp_round["round_name"]] = rk_positions

        # Collect all article IDs across all participants and round types
        all_article_ids = set()
        for p in participants:
            for round_type, article_ids in p.get("article_sel", {}).items():
                all_article_ids.update(article_ids)

        # Fetch all articles in a single query
        articles_cursor = self._db[self._article_coll].find(
            filter={"_id": {"$in": [ObjectId(aid) for aid in all_article_ids]}}, projection=article_projection
        )
        articles_map = {str(art["_id"]): art for art in articles_cursor}

        # Replace each article ID with its full document and add consensus rank position
        for p in participants:
            for round_type, article_ids in p.get("article_sel", {}).items():
                populated = []
                rk_positions = consensus_rk_map.get(round_type, {})
                for aid in article_ids:
                    article = articles_map.get(aid)
                    if article is not None:
                        article_copy = dict(article)
                        article_copy["consensus_rank"] = rk_positions.get(aid)
                        populated.append(article_copy)
                    else:
                        populated.append(None)
                p["article_sel"][round_type] = populated

        return participants

    def get_consensus_rks(self, round_projection: dict = None, article_projection: dict = None) -> list[dict]:
        """
        Get the consensus rankings of the experiment rounds, with article data populated.

        :param round_projection: The projection for the experiment round documents.
        :type round_projection: dict, optional
        :param article_projection: The projection for the article documents.
        :type article_projection: dict, optional
        :return: A list of dictionaries containing the consensus rankings and associated article data.
        :rtype: list[dict]
        """
        if round_projection is None:
            round_projection = {"round_name": 1, "consensus_rk": 1}
        else:
            round_projection = {"round_name": 1, "consensus_rk": 1, **round_projection}

        cursor = self._db[self._exp_rounds_coll].find(projection=round_projection)

        exp_rounds = list(cursor)

        # Collect all article IDs across all rounds
        all_article_ids = set()
        for exp_round in exp_rounds:
            for oid in exp_round.get("consensus_rk", []):
                all_article_ids.add(oid)

        # Fetch all articles in a single query
        articles_cursor = self._db[self._article_coll].find(
            filter={"_id": {"$in": list(all_article_ids)}}, projection=article_projection
        )
        articles_map = {art["_id"]: art for art in articles_cursor}

        # Replace each article ID in consensus_rk with the full article document
        for exp_round in exp_rounds:
            exp_round["consensus_rk"] = [articles_map.get(oid) for oid in exp_round.get("consensus_rk", [])]

        return exp_rounds
