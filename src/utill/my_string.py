import random
import re
import string
import urllib


def replace_nonnumeric(string: str, replace: str) -> str:
    return re.sub('[^0-9a-zA-Z]+', replace, string)


def generate_random_string(length: int = 16, is_alphanum: bool = True):
    letters = string.ascii_letters
    if not is_alphanum:
        letters += r'1234567890!@#$%^&*()-=_+[]{};\':",./<>?'
    return ''.join(random.choice(letters) for i in range(length))


def decode_url(url):
    return urllib.parse.unquote(url)
