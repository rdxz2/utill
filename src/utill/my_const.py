from enum import Enum


class ByteSize:
    KB = 1_024
    MB = 1_048_576
    GB = 1_073_741_824
    TB = 1_099_511_627_776


class HttpMethod(Enum):
    GET = 1
    POST = 2
    PUT = 3
    DELETE = 4

    def __str__(self):
        return self.name
