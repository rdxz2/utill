from enum import StrEnum


class ByteSize:
    KB = 1_024
    MB = 1_048_576
    GB = 1_073_741_824
    TB = 1_099_511_627_776


from enum import StrEnum


class HttpMethod(StrEnum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"
