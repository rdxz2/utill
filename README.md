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

# Per module usages

## my_bq

Executing a query

```py
from utill.my_bq import BQ

# Initialize BigQuery client
bq = BQ()

# Execute a query, returns iterable QueryJob
job = bq.execute_query('...')

# Convert into list for quick data conversion
results = list(job)

# Iterate the results
for row in job:
    # Do anything with the row
```

Uploading CSV file into BigQuery table

```py
from utill.my_bq import BQ, Dtype, LoadStrategy

# Initialize BigQuery client
bq = BQ()

# Load the data
filename = '/path/to/file.csv'  # Your local CSV file location
bq_table_fqn = 'project.dataset.table'  # An FQN (fully qualified name) of a BigQuery table to export
columns = {
    'col1': Dtype.INT64,
    'col2': Dtype.STRING,
    'col3': Dtype.DATE,
    ...
}
partition_col = 'col3'  # Optional, for performance and cost optimization
cluster_cols = ['col1']  # Optional, for performance and cost optimization
load_strategy = LoadStrategy.APPEND  # Optional, default to APPEND
bq.upload_csv(filename, bq_table_fqn, columns, partition_col, cluster_cols, load_strategy)
```

Exporting query into CSV

```py
from utill.my_bq import BQ

# Initialize BigQuery client
bq = BQ()

query = 'SELECT * FROM `project.dataset.table`'  # The query to export
filename = '/path/to/file.csv'  # Destination CSV file location
bq.download_csv(query, filename)
```

Exporting table into XLSX

```py
from utill.my_bq import BQ

# Initialize BigQuery client
bq = BQ()

bq_table_fqn = 'project.dataset.table'  # An FQN (fully qualified name) of a BigQuery table to export
filename = '/path/to/file.xlsx'  # Destination XLSX file location
bq.download_xlsx(src_table_fqn, dst_filename)
```
