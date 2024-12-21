import random
import string


def generate_random_string(length: int = 4): return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))
