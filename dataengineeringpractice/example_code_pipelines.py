
import os
from datetime import date, datetime
import json
import singer
import pprint
import requests
from pyspark.sql.functions import col, avg, stddev_samp, max as sfmax
from pyspark.sql import Row
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import (
    SparkSubmitOperator)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

"""
Note: code is for reference only (taken from an online course)
"""


if __name__ == '__main__':
    # Snapshots in a data lake:
    # note: below is an example of the directory structure of a data lake; it
    # has directories for 1) 'landing', 2) 'clean', and 3) 'business', to hold
    # raw data, cleaned data (cleaned to a basic, generic standard by a Data
    # Engineer), and processed data (processed by a Data Scientist for
    # specific business needs/products) respectively

    # repl:~$ cd ~/workspace/mnt/data_lake
    # repl:~/workspace/mnt/data_lake$ ls
    # business  clean  landing
    # repl:~/workspace/mnt/data_lake$ ls landing/
    # marketing_api  purchased.csv  ratings_with_incomplete_rows.csv
    # prices.csv     ratings.csv    ratings_with_invalid_rows.csv
    # repl:~/workspace/mnt/data_lake$ ls clean/
    # product_ratings
    # repl:~/workspace/mnt/data_lake$ ls business/
    # customer_viewing_behavior

    ######################################################################
    # The data catalog:

    # Here’s the string representation of the `catalog`, a Python dictionary
    # made available to you:
    # {'diaper_reviews': <__main__.FileSystemDataLink object at 0x7f315e5bebe0>,
    #  'prices': <__main__.DatabaseDataLink object at 0x7f315e5bec50>}
    #
    # In [1]: type(catalog['diaper_reviews'].read())
    # Out[1]: pyspark.sql.dataframe.DataFrame

    ######################################################################
    # Working with JSON:

    database_address = {
        "host": "10.0.0.5",
        "port": 8456
    }

    # Open the configuration file in writable mode
    with open("database_config.json", "w") as fh:
        # Serialize the object in this file handle
        json.dump(obj=database_address, fp=fh)

    ######################################################################
    # Specifying the schema of the data:

    # {'items': [{'brand': 'Huggies',
    #             'model': 'newborn',
    #             'price': 6.8,
    #             'currency': 'EUR',
    #             'quantity': 40,
    #             'date': '2019-02-01',
    #             'countrycode': 'DE'
    #             },
    #            {…}]

    # Complete the JSON schema
    schema = {'properties': {
        'brand': {'type': 'string'},
        'model': {'type': 'string'},
        'price': {'type': 'number'},
        'currency': {'type': 'string'},
        'quantity': {'type': 'number', 'minimum': 1},
        'date': {'type': 'string', 'format': 'date'},
        'countrycode': {'type': 'string', 'pattern': "^[A-Z]{2}$"},
        'store_name': {'type': 'string'}}}

    # Write the schema
    singer.write_schema(stream_name='products', schema=schema,
                        key_properties=[])

    ######################################################################
    # Communicating with an API:

    endpoint = "http://localhost:5000"

    # Fill in the correct API key
    api_key = "scientist007"

    # Create the web API’s URL
    authenticated_endpoint = "{}/{}".format(endpoint, api_key)

    # Get the web API’s reply to the endpoint
    api_response = requests.get(authenticated_endpoint).json()
    pprint.pprint(api_response)

    # Create the API’s endpoint for the shops
    shops_endpoint = "{}/{}/{}/{}".format(endpoint, api_key, "diaper/api/v1.0",
                                          "shops")
    shops = requests.get(shops_endpoint).json()
    print(shops)

    # Create the API’s endpoint for items of the shop starting with a "D"
    items_of_specific_shop_URL = "{}/{}/{}/{}/{}".format(
        endpoint, api_key, "diaper/api/v1.0", "items", "DM")
    products_of_shop = requests.get(items_of_specific_shop_URL).json()
    pprint.pprint(products_of_shop)

    ######################################################################
    # Streaming records:

    # Use the convenience function to query the API
    tesco_items = retrieve_products("Tesco")

    singer.write_schema(stream_name="products", schema=schema,
                        key_properties=[])

    # Write a single record to the stream, that adheres to the schema
    singer.write_record(stream_name="products",
                        record={**tesco_items[0], "store_name": "Tesco"})

    for shop in requests.get(SHOPS_URL).json()["shops"]:
        # Write all of the records that you retrieve from the API
        singer.write_records(
            stream_name="products",
            # Use the same stream name that you used in the schema
            records=({**item, "store_name": "Tesco"}
                     for item in retrieve_products(shop))
        )

    ######################################################################
    # Chain taps and targets:
    # note: below is an example of adding a file to a data lake, using a
    # Singer tap (with an already existing Singer target, 'target-csv', and
    # a Singer tap, 'tap-marketing-api')

    # repl:~/workspace$ cd /home/repl/workspace/mnt/data_lake
    # repl:~/workspace/mnt/data_lake$ ls
    # landing
    # repl:~/workspace/mnt/data_lake$ cd landing/
    # repl:~/workspace/mnt/data_lake/landing$ ls
    # marketing_api
    # repl:~/workspace/mnt/data_lake/landing$ tap-marketing-api | target-csv \
    #   --config ~/workspace/ingest/data_lake.conf

    ######################################################################
    # Reading a CSV file:

    # Read a csv file and set the headers
    df = (spark.read
          .options(header=True)
          .csv("/home/repl/workspace/mnt/data_lake/landing/ratings.csv"))

    df.show()

    ######################################################################
    # Defining a schema:

    # Define the schema
    schema = StructType([
        StructField("brand", StringType(), nullable=False),
        StructField("model", StringType(), nullable=False),
        StructField("absorption_rate", ByteType(), nullable=True),
        StructField("comfort", ByteType(), nullable=True)
    ])

    better_df = (spark
                 .read
                 .options(header="true")
                 # Pass the predefined schema to the Reader
                 .schema(schema)
                 .csv("/home/repl/workspace/mnt/data_lake/landing/"
                      "ratings.csv"))
    pprint(better_df.dtypes)

    ######################################################################
    # Removing invalid rows:

    # Specify the option to drop invalid rows
    ratings = (spark
               .read
               .options(header=True, mode='DROPMALFORMED')
               .csv("/home/repl/workspace/mnt/data_lake/landing/"
                    "ratings_with_invalid_rows.csv"))
    ratings.show()

    ######################################################################
    # Filling unknown data:

    print("BEFORE")
    ratings.show()

    print("AFTER")
    # Replace nulls with arbitrary value on column subset
    ratings = ratings.fillna(4, subset=["comfort"])
    ratings.show()

    ######################################################################
    # Conditionally replacing values:

    # Add/relabel the column
    categorized_ratings = ratings.withColumn(
        "comfort",
        # Express the condition in terms of column operations
        when(col("comfort") > 3, "sufficient").otherwise("insufficient"))

    categorized_ratings.show()

    ######################################################################
    # Selecting and renaming columns:

    # Select the columns and rename the "absorption_rate" column
    result = ratings.select([col("brand"),
                             col("model"),
                             col("absorption_rate").alias("absorbency")])

    # Show only unique values
    result.distinct().show()

    ######################################################################
    # Grouping and aggregating data:

    aggregated = (purchased
        # Group rows by 'Country'
        .groupBy(col('Country'))
        .agg(
        # Calculate the average salary per group and rename
        avg('Salary').alias('average_salary'),
        # Calculate the standard deviation per group
        stddev_samp('Salary'),
        # Retain the highest salary per group and rename
        sfmax('Salary').alias('highest_salary')
    )
    )

    aggregated.show()

    ######################################################################
    # Creating a deployable artifact:
    # note: below is an example of zipping some code, which can then be run as
    # a PySpark program (the zipping/packaging step becomes more important
    # when your code consists of many modules)

    # repl:~/workspace$ cd /home/repl/workspace/spark_pipelines/
    # repl:~/workspace/spark_pipelines$ ls
    # __init__.py  Pipfile  pydiaper  script
    # repl:~/workspace/spark_pipelines$ zip --recurse-paths pydiaper.zip pydiaper
    #   adding: pydiaper/ (stored 0%)
    #   adding: pydiaper/__init__.py (stored 0%)
    #   adding: pydiaper/cleaning/ (stored 0%)
    #   adding: pydiaper/cleaning/clean_prices.py (deflated 58%)
    #   adding: pydiaper/cleaning/clean_ratings.py (deflated 53%)
    #   adding: pydiaper/cleaning/__init__.py (stored 0%)
    #   adding: pydiaper/master/ (stored 0%)
    #   adding: pydiaper/master/__init__.py (stored 0%)
    #   adding: pydiaper/master/summarize.py (deflated 55%)
    #   adding: pydiaper/data_catalog/ (stored 0%)
    #   adding: pydiaper/data_catalog/__init__.py (stored 0%)
    #   adding: pydiaper/data_catalog/catalog.py (deflated 47%)
    #   adding: pydiaper/config.py (deflated 28%)
    #   adding: pydiaper/resources/ (stored 0%)
    #   adding: pydiaper/resources/landing/ (stored 0%)
    #   adding: pydiaper/resources/landing/prices.csv (deflated 45%)
    #   adding: pydiaper/resources/landing/ratings.csv (deflated 15%)
    # repl:~/workspace/spark_pipelines$ ls
    # __init__.py  Pipfile  pydiaper  pydiaper.zip  script  zip_file.zip

    ######################################################################
    # Submitting your Spark job:
    # note: below is an example of submitting a Spark job (the path of the
    # zipped archive is 'spark_pipelines/pydiaper/pydiaper.zip', whereas the
    # path to your application entry point is
    # 'spark_pipelines/pydiaper/pydiaper/cleaning/clean_ratings.py')

    # note: submitting a Spark job to a cluster is simple if the dependencies
    # of a job are distributed across a cluster's nodes

    # repl:~/workspace$ ls
    # mnt  spark_pipelines
    # repl:~/workspace$ spark-submit --py-files \
    #   spark_pipelines/pydiaper/pydiaper.zip \
    #   spark_pipelines/pydiaper/pydiaper/cleaning/clean_ratings.py
    # Picked up _JAVA_OPTIONS: -Xmx512m
    # Picked up _JAVA_OPTIONS: -Xmx512m
    # 20/01/06 19:15:13 WARN NativeCodeLoader: Unable to load native-hadoop
    # library for your platform... using builtin-java classes where applicable

    ######################################################################
    # Verifying your pipeline’s output:
    # note: below is an example of checking that the expected files have been
    # created and in the right location, after successfully executing a
    # PySpark job using spark-submit

    # repl:~$ ls /home/repl/workspace/mnt/data_lake/clean/product_ratings
    # part-00000-3dcd8643-f643-4c69-a2b8-9b72487e7cb3-c000.snappy.parquet
    # part-00001-3dcd8643-f643-4c69-a2b8-9b72487e7cb3-c000.snappy.parquet
    # _SUCCESS

    ######################################################################
    # Creating in-memory DataFrames:
    # note: for unit tests, it is often appropriate to create Spark DataFrames
    # in memory (which will usually be very small, i.e. just a test case)

    Record = Row("country", "utm_campaign", "airtime_in_minutes", "start_date",
                 "end_date")

    # Create a tuple of records
    data = (
        Record("USA", "DiapersFirst", 28, date(2017, 1, 20),
               date(2017, 1, 27)),
        Record("Germany", "WindelKind", 31, date(2017, 1, 25), None),
        Record("India", "CloseToCloth", 32, date(2017, 1, 25),
               date(2017, 2, 2))
    )

    # Create a DataFrame from these records
    frame = spark.createDataFrame(data)
    frame.show()

    ######################################################################
    # Making a function more widely reusable:

    # pipenv run pytest .
    # note: running this in the command terminal uses pytest (must be pip
    # installed) to run the tests in the directory

    ######################################################################
    # Improving style guide compliancy:
    # note: below is an example of using Circleci to automate 1) the use of
    # pytest to run unit tests, and 2) the use of flake8 to check compliancy
    # with PEP8

    ##### ~/workspace/spark_pipelines/pydiaper/Pipfile ######
    # [[source]]
    # name = "pypi"
    # url = "https://pypi.org/simple"
    # verify_ssl = true
    #
    # [dev-packages]
    # pyspark-stubs = ">=2.4.0"
    # pytest = "*"
    # flake8 = "*"
    #
    # [packages]
    # pyspark = ">=2.4.0"
    #
    # [requires]
    # python_version = "3.6"
    ##### #####

    ##### ~/workspace/spark_pipelines/pydiaper/.circleci/config.yml #####
    # version: 2
    # jobs:
    #   build:
    #     working_directory: ~/data_scientists/optimal_diapers/
    #     docker:
    #       - image: gcr.io/my-companys-container-registry-on-google-cloud-123456/python:3.6.4
    #     steps:
    #       - checkout
    #       - run:
    #           command: |
    #             sudo pip install pipenv
    #             pipenv install
    #       - run:
    #           command: |
    #             pipenv run flake8 .
    #       - run:
    #           command: |
    #             pipenv run pytest .
    #       - store_test_results:
    #           path: test-results
    #       - store_artifacts:
    #           path: test-results
    #           destination: tr1
    ##### #####

    # repl:~/workspace$ ls
    # spark_pipelines
    # repl:~/workspace$ cd spark_pipelines/
    # repl:~/workspace/spark_pipelines$ ls
    # pydiaper
    # repl:~/workspace/spark_pipelines$ cd pydiaper/
    # repl:~/workspace/spark_pipelines/pydiaper$ ls
    # __init__.py  Pipfile  Pipfile.lock  pydiaper
    # repl:~/workspace/spark_pipelines/pydiaper$ ls -a
    # .   .circleci   __init__.py  Pipfile.lock
    # ..  .gitignore  Pipfile      pydiaper
    # repl:~/workspace/spark_pipelines/pydiaper$ cd .circleci/
    # repl:~/workspace/spark_pipelines/pydiaper/.circleci$ ls
    # config.yml

    ######################################################################
    # Specifying the DAG schedule:

    reporting_dag = DAG(
        dag_id="publish_EMEA_sales_report",
        # Insert the cron expression
        schedule_interval="0 7 * * 1",
        start_date=datetime(2019, 11, 24),
        default_args={"owner": "sales"}
    )

    ######################################################################
    # Specifying operator dependencies:

    # Specify direction using verbose method
    prepare_crust.set_downstream(apply_tomato_sauce)

    tasks_with_tomato_sauce_parent = [add_cheese, add_ham, add_olives,
                                      add_mushroom]
    for task in tasks_with_tomato_sauce_parent:
        # Specify direction using verbose method on relevant task
        apply_tomato_sauce.set_downstream(task)

    # Specify direction using bitshift operator
    tasks_with_tomato_sauce_parent >> bake_pizza

    # Specify direction using verbose method
    bake_pizza.set_upstream(prepare_oven)

    ######################################################################
    # Preparing a DAG for daily pipelines:

    # Create a DAG object
    dag = DAG(
        dag_id='optimize_diaper_purchases',
        default_args={
            # Don't email on failure
            'email_on_failure': False,
            # Specify when tasks should have started earliest
            'start_date': datetime(2019, 6, 25)
        },
        # Run the DAG daily
        schedule_interval='@daily')

    ######################################################################
    # Scheduling bash scripts with Airflow:

    config = os.path.join(os.environ["AIRFLOW_HOME"],
                          "scripts",
                          "configs",
                          "data_lake.conf")

    ingest = BashOperator(
        # Assign a descriptive id
        task_id="ingest_data",
        # Complete the ingestion pipeline
        bash_command="tap-marketing-api | target-csv --config %s" % config,
        dag=dag)

    ######################################################################
    # Scheduling Spark jobs with Airflow:

    # Set the path for our files.
    entry_point = os.path.join(os.environ["AIRFLOW_HOME"], "scripts",
                               "clean_ratings.py")
    dependency_path = os.path.join(os.environ["AIRFLOW_HOME"], "dependencies",
                                   "pydiaper.zip")

    with DAG('data_pipeline', start_date=datetime(2019, 6, 25),
             schedule_interval='@daily') as dag:
        # Define task clean, running a cleaning job.
        clean_data = SparkSubmitOperator(
            application=entry_point,
            py_files=dependency_path,
            task_id='clean_data',
            conn_id='spark_default')

    ######################################################################
    # Scheduling the full data pipeline with Airflow:

    spark_args = {"py_files": dependency_path,
                  "conn_id": "spark_default"}
    # Define ingest, clean and transform job.
    with dag:
        ingest = BashOperator(task_id='Ingest_data',
                              bash_command='tap-marketing-api | target-csv --config %s' % config)
        clean = SparkSubmitOperator(application=clean_path,
                                    task_id='clean_data', **spark_args)
        insight = SparkSubmitOperator(application=transform_path,
                                      task_id='show_report', **spark_args)

        # set triggering sequence
        ingest >> clean >> insight

    ######################################################################
    # Airflow’s executors:
    # note: below is an example of Airflow's config. Of particular interest is
    # 'executor = CeleryExecutor', which controls the way that Airflow
    # executes the jobs (e.g. SequentialExecutor would not use parallel
    # processing)

    # repl:~$ vim /home/repl/workspace/airflow/airflow.cfg
    #
    # # The home folder for airflow, default is ~/airflow
    # airflow_home = /home/repl/workspace/airflow
    #
    # # The folder where your airflow pipelines live, most likely a
    # # subfolder in a code repository
    #
    # # The folder where airflow should store its log files
    # # This path must be absolute
    # # location. If remote_logging is set to true, see UPDATING.md for additional
    # # configuration requirements.
    #
    # # Logging level
    # logging_level = INFO
    # # Logging class
    # # Specify the class that will specify the logging configuration# This class has to be on the python classpath
    # simple_log_format = %%(asctime)s %%(levelname)s - %%(message)slog_processor_filename_template = {{ filename }}.log
    # # Hostname by providing a path to a callable, which will resolve the hostname
    # hostname_callable = socket:getfqdn
    #
    # # Default timezone in case supplied date times are naive
    # default_timezone = utc
    #
    # # The executor class that airflow should use. Choices include
    # # SequentialExecutor, LocalExecutor, CeleryExecutor, DaskExecutor, KubernetesExecutor
    # executor = CeleryExecutor
    #
    # # The SqlAlchemy connection string to the metadata database.
    # # SqlAlchemy supports many different database engine, more information
    # # their website
    # ......

    ######################################################################
