# Using this library

Installation

```sh
pip install utill
```

Usage syntax

```py
from utill.__MODULE__ import __OBJECT__
```

Example

```py
# Using the string module
from utill.my_string import generate_random_string

print(generate_random_string(16))
```

## Initial set up

This package contains CLI command

```sh
utill conf init
```

# Additional extensions

Syntax

```sh
pip install utill[__EXTENSION_NAME__]
```

Extension list:

- google-cloud
- postgresql
- pdf
