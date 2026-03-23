"""
Utility functions for the project.
"""

import os
import re
from datetime import datetime
import toml
import locale
from contextlib import contextmanager
from typing import Any

import dotenv

dotenv.load_dotenv(override=True)


@contextmanager
def temporary_locale(language: str):
    """
    Temporarily set the locale for date extraction.

    This is useful for extracting dates in different languages.

    :param language: The language to set the locale to. Can be either "english" or "spanish".
    :type language: str
    :raises ValueError: If the language is not "english" or "spanish".

    :yield: Executes the code inside the 'with' block with the specified locale.
    """
    if language not in ["english", "spanish"]:
        raise ValueError("Language must be either 'english' or 'spanish'.")

    available_locales = {
        "english": "en_US.UTF-8",
        "spanish": "es_ES.UTF-8",
    }

    # Save the current locale
    current_locale = locale.getlocale(locale.LC_TIME)
    try:
        # Set the new locale
        locale.setlocale(locale.LC_TIME, available_locales[language])
        yield  # Execute the code inside the 'with' block
    finally:
        # Restore the original locale after the block
        locale.setlocale(locale.LC_TIME, current_locale)


def extract_date(text: str, language: str = "english", date_format: str = "%B %d, %Y") -> datetime | None:
    """
    Extracts a date from text, in the format "<month by letters> <day>, <year>".

    The format codes can be checked in `date str formats <https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes>`_

    :param text: The text to extract the date from.
    :type text: str
    :param language: The language of the date. Can be either "english" or "spanish".
    :type language: str
    :param date_format: The format of the date to extract. Defaults to "%B %d, %Y".
    :type date_format: str

    :raises ValueError: If the language is not "english" or "spanish".
    :raises ValueError: If the date format does not match the extracted date.

    :return: The date extracted from the text. None if no date is found.
    :rtype: datetime | None
    """
    if language not in ["english", "spanish"]:
        raise ValueError("Language must be either 'english' or 'spanish'.")

    # RegExp with exact month names
    months_regex = {
        "english": r"(January|February|March|April|May|June|July|August|September|October|November|December)",
        "spanish": r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)",
    }
    date_exact_pattern = rf"({months_regex[language]})\s(\d{{1,2}}),\s(\d{{4}})"

    # RegExp without looking for exact month names
    date_generic_pattern = r"([A-Za-z]+ \d{1,2}, \d{4})"

    match = re.search(date_exact_pattern, text, re.IGNORECASE)

    if not match:
        match = re.search(date_generic_pattern, text)

    date = None

    # If found, convert to datetime
    if match:
        date_str = match.group()
        with temporary_locale(language):
            try:
                date = datetime.strptime(date_str, date_format)
            except ValueError:
                raise ValueError(f"Date format '{date_format}' does not match the extracted date '{date_str}'.")

    return date


def load_env_var(var_name: str, default_value: Any = None, toml_section: str = None, is_bool: bool = False) -> Any:
    """
    Load an environment variable or a value from the .streamlit/config.toml file.

    :param var_name: The name of the environment variable to load.
    :type var_name: str
    :param default_value: The default value to return if the environment variable is not found. Defaults to None.
    :type default_value: Any, optional
    :param toml_section: The section in the .streamlit/config.toml file to look for the variable. If None, it will look in the root section. Defaults to None
    :type toml_section: str, optional

    :raises ValueError: If the environment variable is not found in either the environment variables or the .streamlit/config.toml file and no default value is provided.

    :return: The value of the environment variable or the value from the .streamlit/config.toml file.
    :rtype: Any
    """
    value = os.getenv(var_name)

    if value is None:
        if default_value is not None:
            return default_value
        else:
            raise ValueError(f"Environment variable '{var_name}' not found in  environment variables.")
    elif is_bool:
        value = value.lower() == "true" if isinstance(value, str) else bool(value)

    return value
