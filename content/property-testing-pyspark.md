Title: PySpark Property-based testing
Date: 2022-07-15
Category: Data Engineering
Tags: blog, pyspark, databricks, hypothesis, property-based testing
Slug: property-testing-pyspark
Authors: Dan
Summary: Property-based testing for Python, PySpark and Databricks
Status: published

# Automate all your PySpark Unit Test with Hypothesis!

Unit testing is often regarded as a main pillar of testing your software applications, and it usually involves
testing a single/unit component and ensuring that it covers all the edge cases the software developer can think of.
The Functional Programming world, specifically Haskell with QuickCheck and Scala with ScalaCheck, have introduced the
idea of testing based on property specifications and automatic test data generation in order to complement
the traditional unit testing approach. The idea is that we define a property that specifies the program behaviour and
the data is automatically generated for us to overcome all the test scenarios, using shrinking in order to simplify the
verification of the test case.

## PySpark property-based testing

Testing your PySpark notebooks can be quite challenging by itself, the data infrastructure itself has often quite
limited support for pytest and adding such tests to your existing data pipelines adds quite some complexity.
Additionally, testing your ETL transformation function can be quite cumbersome as it involves generating data,
maintaining it, or simply skipping the unit tests in favor of simple integrity tests. In this blog post we will see
how we can easily generate data from dataclasses and think of unit testing in terms of properties rather than data
integrity. We will start with Hypothesis, an excellent Python library built for property-based testing.

We can begin by specifying some simple dataclasses we will be using in order to showcase the potential of the library:

```python
from datetime import datetime, timedelta
from typing import List, NamedTuple
from tinsel import struct, transform

@struct
class OrderItem(NamedTuple):
  item_id: str
  item_value: float

@struct
class Order(NamedTuple):
  order_id: str
  created_at: datetime
  updated_at: datetime
  customer_id: str
  order_items: List[OrderItem]
```

Our transaction dataset will be composed by orders with a set of order items for each order. The dataclasses are defined
as NamedTuples because PySpark has limited support for dataclasses. We will be working with a secondary python library
called Tinsel, which allows us to easily convert our dataclass schema to a Spark DataFrame schema.

At this point we can leverage the Hypothesis library to specify arbitrary generators we will be using for our dataclasses:

```python
import hypothesis.strategies as st
from hypothesis import given, settings, HealthCheck

week_sunday = (datetime.now() - timedelta(days = datetime.now().weekday() + 1))
time_window = (datetime.now() - timedelta(days = datetime.now().weekday() + 7))

def order_strategy():
    return st.lists(st.builds(Order,
                     order_id=st.text(min_size=5),
                     created_at=st.datetimes(min_value=time_window, max_value=week_sunday),
                     updated_at=st.datetimes(min_value=time_window, max_value=week_sunday),
                     customer_id=st.text(min_size=5),
                     order_items=st.lists(st.builds(OrderItem,
                                           item_id=st.text(min_size=5),
                                           item_value=st.floats(min_value=0, max_value=100)), min_size=1)), min_size=5)



```

In the above code snippet I have specified some upper and down boundaries for the datetimes I will be using in my test
dataset. Likewise for the strings, floats and lists I have used different specifiers. For additional custom generator
strategy consult the excellent Hypothesis documentation.

Now that we have our custom generators we can specify our example transformation function we would like to test:

```python
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def test_dataframe_transformations(df: DataFrame) -> DataFrame:
    temp_df = df.select('*', F.explode_outer('order_items'))\
        .select(F.col('created_at'), F.col('col.item_value').alias('item_value'))\
        .withColumn('day', F.to_date('created_at'))\
        .withColumn('week', F.weekofyear('created_at'))

    weekly_revenue_df = temp_df.groupBy('week').agg(
        F.sum('item_value').alias('weekly_revenue')
    )

    return temp_df.groupBy('week', 'day').agg(
        F.sum('item_value').alias('daily_revenue')
    ).join(weekly_revenue_df, on='week', how='inner')
```
The property I am interested to test in the above code snippet is the revenue itself. Logically the daily revenue should
always be smaller or equal than the weekly revenue, unless I have an undetected bug in my code.

Now, in order to be able to use pyspark in your unit testing you need to setup a global SparkSession:

```python
import unittest

from pyspark.sql import SparkSession

class PySparkTestCase(unittest.TestCase):
    """Set-up of global test SparkSession"""

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[1]")
                     .appName("PySpark unit test")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

```

We can setup different profiles for our Hypothesis generators, for example the amount of examples or the deadline:

```python
settings.register_profile(
    "my_profile",
    max_examples=50,
    deadline=60 * 1000,  # Allow 1 min per example (deadline is specified in milliseconds)
    suppress_health_check=(HealthCheck.too_slow, HealthCheck.data_too_large),
)
```

Using our generators, settings and SparkSession we can now write our first unit test:

```python
class SimpleTestCase(PySparkTestCase):

    def test_spark(self):
        @given(data=order_strategy())
        @settings(settings.load_profile("my_profile"))
        def test_samples(data):
            df = self.spark.createDataFrame(data=data, schema=schema, verifySchema=False)
            collected_list = test_dataframe_transformations(df).collect()

            for row in collected_list:
                assert row['weekly_revenue'] >= row['daily_revenue']


        test_samples()

```

This code snippet will feed all the generated datasets to the transformation function as a Spark DataFrame, collect
the results and check the property we were after all along.

In the next blog post we will tackle the orchestration of unit tests and the addition to your Databricks CI/CD pipeline
using [Microsoft's Nutter](https://github.com/microsoft/nutter)! Stay Tuned!

#### The code is available on [github](https://github.com/xdanny/property-testing-pyspark)

### References
[Hypothesis Docs](https://hypothesis.readthedocs.io/en/latest/)
[Tinsel PySpark schema converter](https://github.com/benchsci/tinsel)
[Property Based Testing with QuickCheck](https://typeable.io/blog/2021-08-09-pbt)
