Title: Improve PySpark json reads with explicit schema
Date: 2022-07-31
Category: Data Engineering
Tags: blog, pyspark, databricks, json, schema, tinsel
Slug: pyspark-json-schema
Authors: Dan
Summary: Reduce PySpark performance overhead when reading json and csv datasets
Status: published

# Improve your PySpark ETL's performance by providing explicit schema

Have you ever stumbled upon a Spark ETL and you were left wondering how a simple loading of a
dataset can take hours, even though the filtered dataset you are specifying is relatively small?
While there can be multiple reasons for the ETL being slow, from the cloud provider to wrong cluster Spark
configuration of executors, we will focus in this blogpost on optimizing dataframe reads for json and csv datasets.


Going through the [Spark Scala source code](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/streaming/DataStreamReader.scala#L213)
we can already understand one of the reasons of our query being slow. When reading json datasets Spark will go
through all the json files once to infer the schema before loading our datasets. In case of a heavily distributed
dataset across multiple json files, this can be a source of considerable time spent by Spark Executors. We can
reference the [Spark Documentation](https://spark.apache.org/docs/latest/sql-data-sources-json.html) in order
to better understand what kind of properties are set by default when reading json datasets. A quick fix to speed
our query is to set a smaller sampling ratio for Spark schema inference, by default this value is set to `1.0`.
A better long term fix for your ETL code, especially if the data infrastructure provides some schema guarantees,
is to provide Spark Schema when reading your datasets.

## How to provide a Spark schema when querying json files
### #1 Provide a typed PySpark Schema

As an example we will use an order event that we might receive via our event-bus. This event will have some custom
metadata as well as the order itself with custom order items. In this case our PySpark schema would look like this:

```python
from pyspark.sql.types import *

schema = StructType([
                StructField("metadata",StringType(),False),
                StructField("order",
                            StructType([
                                StructField("order_id",StringType(),False),
                                StructField("created_at",TimestampType(),True),
                                StructField("updated_at",TimestampType(),True),
                                StructField("customer_id",StringType(),False),
                                StructField("order_items",
                                            ArrayType(
                                                StructType([
                                                    StructField("item_id",StringType(),False),
                                                    StructField("item_value",DoubleType(),False)]),False),False)]),False)])

```

We would read the dataframe with the following PySpark command:

```python
df = spark.read.json("our/path", schema=schema)
```

With this configuration spark will read the dataset directly without trying to infer the schema. This setup
can feel quite cumbersome as the data engineer needs to work with Spark types directly, and the definition
of the struct can be quite verbose.

### #2 Provide PySpark schema as Python dataclasses

We can improve the example above using the library `tinsel` that we have leveraged in previous blogposts.
To install the library in your notebook simply run:
```%pip install tinsel```
Using tinsel transformer we can write our schema as dataclasses as in the following example:
```python
from typing import List, NamedTuple, Optional
from tinsel import struct, transform

@struct
class OrderItem(NamedTuple):
  item_id: str
  item_value: float

@struct
class Order(NamedTuple):
  order_id: str
  created_at: Optional[datetime]
  updated_at: Optional[datetime]
  customer_id: str
  order_items: List[OrderItem]

@struct
class Event(NamedTuple):
    metadata: str
    order: Order

schema = transform(Event)
df = spark.read.json("our/path", schema=schema)

```


### #3 Save and load json schema for PySpark dataframes

Writing our schema as python dataclasses is already an excellent step forward, however this might not always
be the right solution. Maintaining schema and schema migration can be quite challenging, and the software
developers might opt on using version control to specify the schemas as yaml or json. With PySpark we can
load the schema specified as json as a static resource, for example from S3. Using the example above we
can generate the json schema:
```python
df.schema.json()
```
Which would print our schema:
```python
json_schema =
"""
    {
  "fields": [
    {
      "metadata": {},
      "name": "metadata",
      "nullable": false,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "order",
      "nullable": false,
      "type": {
        "fields": [
          {
            "metadata": {},
            "name": "order_id",
            "nullable": false,
            "type": "string"
          },
          {
            "metadata": {},
            "name": "created_at",
            "nullable": true,
            "type": "timestamp"
          },
          {
            "metadata": {},
            "name": "updated_at",
            "nullable": true,
            "type": "timestamp"
          },
          {
            "metadata": {},
            "name": "customer_id",
            "nullable": false,
            "type": "string"
          },
          {
            "metadata": {},
            "name": "order_items",
            "nullable": false,
            "type": {
              "containsNull": false,
              "elementType": {
                "fields": [
                  {
                    "metadata": {},
                    "name": "item_id",
                    "nullable": false,
                    "type": "string"
                  },
                  {
                    "metadata": {},
                    "name": "item_value",
                    "nullable": false,
                    "type": "double"
                  }
                ],
                "type": "struct"
              },
              "type": "array"
            }
          }
        ],
        "type": "struct"
      }
    }
  ],
  "type": "struct"
}"""
```

The schema can now be loaded using the following command:
```python
import json

new_schema = StructType.fromJson(json.loads(json_schema))
df = spark.read.json("our/path", schema=new_schema)
```


