Title: Integration Testing for your Databricks CI/CD data pipelines
Date: 2022-07-19
Category: Data Engineering
Tags: blog, nutter, e2e testing, integration testing, pyspark, databricks, hypothesis
Slug: integration-testing-nutter
Authors: Dan
Summary: Integration Testing for your Databricks CI/CD Data Pipelines
Status: published

# Integration Testing for your Databricks CI/CD Data Pipelines with Microsoft Nutter

In this blogpost we will continue our journey of testing our Data Pipelines. If you haven't checked out
the first post, [make sure you do](https://unpackingdata.com/property-testing-pyspark). As assessed in the first post, writing unit tests in pyspark can be challenging.
Especially when we go to the world of notebooks, setting up your testing infrastructure can be challenging.
Most often the data scientists/data engineers in your organization will prefer working on and developing their
Data Pipelines in a dedicated environment like Databricks or JupyterLab. In order to enable cross-team
collaboration, peer-review and enforce best testing practices, the notebooks will have to be version controlled.
In Databricks, for example, the preferred way to work on notebooks is to use
[Databricks Repos](https://docs.databricks.com/repos/index.html). However, because Databricks doesn't have
support for pytest, you cannot write a separate notebook for tests. The data engineer will be writing
pytest's using an IDE as part of the notebook pull request, the execution part being left to the CI/CD pipeline
in a mocked Spark environment. This ensures that our code is determinstic and our functions are correct, but
it does not necessarily translate to correct functioning in a production-like environment.

## Microsoft Nutter - Testing Framework for Databricks

To overcome the limits described above, a popular framework open-sourced by Microsoft is
[Nutter](https://github.com/microsoft/nutter). The library has two main components: the runner and the CLI.
We will be exploring in this blogpost only the runner, leaving the CLI as an exercise for the readers. The
runner runtime is a module that can be used once you install the library in your notebook. In Databricks
we can use the following command, that will install nutter as well as hypothesis and tinsel libraries used to
generate our datasets:

```python
%pip install -U nutter hypothesis tinsel
```

As an example, we will be using the same function as in the first series blogpost to illustrate the example
in a separate notebook:

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

Now, in our testing notebook, we can import the needed functions with the following Databricks command:

```python
%run /{notebook_to_test_path}
```

In order to setup our test datasets, we will be using the same setup as in the previous blogpost:

```python
import hypothesis.strategies as st
from hypothesis import given, settings, HealthCheck

settings.register_profile(
"my_profile",
max_examples=50,
deadline=60 * 1000,  # Allow 1 min per example (deadline is specified in milliseconds)
suppress_health_check=(HealthCheck.too_slow, HealthCheck.data_too_large),
)
```

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

We are now ready to create our first tests with Microsoft Nutter. The test framework follows the following
conventions for the test fixtures:

```
before_(testname) - (optional) - if provided, is run prior to the 'run_' method. This method can be used to setup any test pre-conditions

run_(testname) - (optional) - if provider, is run after 'before_' if before was provided, otherwise run first. This method is typically used to run the notebook under test

assertion_(testname) (required) - run after 'run_', if run was provided. This method typically contains the test assertions

after_(testname) (optional) - if provided, run after 'assertion_'. This method typically is used to clean up any test data used by the test
```

In the case where we want to test full notebooks, we can setup for example our static dataframe from a file
in the before function, run the ETL transformations, assert that the created delta table corresponds to our
expectations and use the after in order to clean up all the test resources we might have spinned up. In this
blogpost, however, we will only be using the assertion as we are generating the data on the fly. Our test
fixture then would look like this:

```python
from runtime.nutterfixture import NutterFixture, tag

default_timeout = 600

class Test1Fixture(NutterFixture):
def init(self):
NutterFixture.init(self)

@given(data=order_strategy())
@settings(settings.load_profile("my_profile"))
def test_samples(self, data):
  df = spark.createDataFrame(data=data, schema=transform(Order), verifySchema=False)
  collected_list = test_dataframe_transformations(df).collect()
  for row in collected_list:
    assert row['weekly_revenue'] >= row['daily_revenue']

def assertion_test_transform_data(self):
    self.test_samples()

def after_test_transform_data(self):
    print('done')

```

We can now run our test notebook, and the results will look like this:

```
Notebook exited:
Notebook: N/A - Lifecycle State: N/A, Result: N/A
Run Page URL: N/A

PASSING TESTS

test_transform_data (31.414987 seconds)
```

This notebook can be added as part of your CI/CD pipeline, and we will explore in the next blogpost how.
We can write and run unit/integration/end2end tests using Nutter and export the results via CSV or JUnit.
Happy Coding!

### References

[Microsoft Nutter](https://github.com/microsoft/nutter)
