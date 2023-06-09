# Databricks notebook source
# MAGIC %md
# MAGIC ###Downloading libraries

# COMMAND ----------

!pip install kaggle

# COMMAND ----------

# MAGIC %md
# MAGIC ###Downloading the NYT dataset

# COMMAND ----------

import os

dbutils.fs.cp('/FileStore/kaggle.json', 'file:/kaggle/kaggle.json')
os.environ['KAGGLE_CONFIG_DIR'] = '/kaggle'

!kaggle datasets download -d aryansingh0909/nyt-articles-21m-2000-present --force
!unzip nyt-articles-21m-2000-present.zip

dbutils.fs.mv("file:/databricks/driver/nyt-metadata.csv", "/FileStore/nyt-metadata.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading .csv file to DataFrame

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, DateType, FloatType, ArrayType, MapType

nyt_schema = StructType([
    StructField('abstract', StringType(), True), 
    StructField('web_url', StringType(), False), 
    StructField('snippet', StringType(), True), 
    StructField('lead_paragraph', StringType(), True), 
    StructField('print_section', StringType(), True), 
    StructField('print_page', FloatType(), False), 
    StructField('source', StringType(), False), 
    #StructField('multimedia', ArrayType(MapType(StringType(), StringType())), True), 
    StructField('multimedia', StringType(), True), 
    #StructField('headline', MapType(StringType(), StringType()), True), 
    StructField('headline', StringType()), 
    #StructField('keywords',  ArrayType(MapType(StringType(), StringType())), True), 
    StructField('keywords', StringType(), True),
    StructField('pub_date', DateType(), False), 
    StructField('document_type', StringType(), True), 
    StructField('news_desk', StringType(), True), 
    StructField('section_name', StringType(), True), 
    #StructField('byline', MapType(StringType(), StringType()), True), 
    StructField('byline', StringType(), True),
    StructField('type_of_material', StringType(), True), 
    StructField('_id', StringType(), True), 
    StructField('word_count', FloatType(), True), 
    StructField('uri', StringType(), False), 
    StructField('subsection_name', StringType(), True),
    StructField('corrupted', StringType(), True)
])

nyt_articles = (spark
    .read
    .option('header', 'true')
    .option('dateFormat', 'yyyy-mm-dd')
    .schema(nyt_schema)
    .csv('/FileStore/nyt-metadata.csv'))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Data processing

# COMMAND ----------

from pyspark.sql.functions import from_json, schema_of_json
nyt_articles.shuffle().take(3)
#(nyt_articles.withColumn("multimedia", to_json("multimedia"))
#             .withColumn("headline", to_json("headline"))
#             .withColumn("keywords", to_json("keywords"))
#             .withColumn("byline", to_json("byline"))
#             .groupBy('corrupted')
#             .count()
#             .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking for errors

# COMMAND ----------

nyt_articles.groupBy('corrupted').count().show()
