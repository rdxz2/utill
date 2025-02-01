import random
import re
import string


def generate_random_string(length: int = 4, alphanum: bool = False): return ''.join(random.choice(string.ascii_letters + string.digits + (r'!@#$%^&*()-=_+[]{};\':",./<>?' if alphanum else '')) for _ in range(length))


def replace_nonnumeric(string: str, replace: str) -> str: return re.sub('[^0-9a-zA-Z]+', replace, string)
