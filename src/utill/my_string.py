import hashlib
import random
import re
import string


def generate_random_string(length: int = 4, alphanum: bool = False): return ''.join(random.choice(string.ascii_letters + string.digits + (r'!@#$%^&*()-=_+[]{};\':",./<>?' if not alphanum else '')) for _ in range(length))


def replace_nonnumeric(string: str, replace: str) -> str: return re.sub('[^0-9a-zA-Z]+', replace, string)


def mask(string: str, mask_length_min: int = 5, mask_length_max: int = 50, display_length: int = 5) -> str:
    if not string:
        mask_length = mask_length_min
    else:
        hash_value = int(hashlib.sha256(string.encode()).hexdigest(), 16)
        mask_length = mask_length_min + (hash_value % (mask_length_max - mask_length_min + 1))

    return ('*' * mask_length) + (string[(-display_length if len(string) > display_length else -1):])
