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

from pyspark.sql.types import StructField, StructType, StringType, TimestampType, IntegerType, FloatType

nyt_schema = StructType([
    StructField('abstract', StringType(), True), 
    StructField('web_url', StringType(), False), 
    StructField('snippet', StringType(), True), 
    StructField('lead_paragraph', StringType(), True), 
    StructField('print_section', StringType(), True), 
    StructField('print_page', FloatType(), True), 
    StructField('source', StringType(), False), 
    StructField('multimedia', StringType(), True), 
    StructField('headline', StringType()), 
    StructField('keywords', StringType(), True),
    StructField('pub_date', TimestampType(), False), 
    StructField('document_type', StringType(), True), 
    StructField('news_desk', StringType(), True), 
    StructField('section_name', StringType(), True), 
    StructField('byline', StringType(), True),
    StructField('type_of_material', StringType(), True), 
    StructField('_id', StringType(), False), 
    StructField('word_count', FloatType(), True), 
    StructField('uri', StringType(), False), 
    StructField('subsection_name', StringType(), True),
    StructField('corrupted', StringType(), True)
])

nyt_articles = (spark
    .read
    .option('header', 'true')
    .option("quote", "\"")
    .option("escape", "\"")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
    .schema(nyt_schema)
    .csv('/FileStore/nyt-metadata.csv', multiLine=True))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Showing the DataFrame

# COMMAND ----------

from pyspark.sql.functions import rand 
display(nyt_articles.orderBy(rand()).limit(10))
nyt_articles.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Repairing data

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, levenshtein

#there is a row that is split into two with null values 
row1 = nyt_articles.filter(col("web_url") == "https://www.nytimes.com/2022/04/28/sports/football/nfl-draft-hats-2022.html")
row2 = nyt_articles.filter(col("section_name") == "nyt://article/2aec1dad-e9d0-5a3c-931b-cc93890bb60e")

row1 = row1.select(nyt_articles.columns[:3])
row2 = row2.select(nyt_articles.columns[:18])
combined_row = row1.crossJoin(row2)
combined_row = combined_row.toDF(*nyt_articles.columns)
display(combined_row)

# COMMAND ----------

#replace 2 broken rows with repaired one
nyt_articles = nyt_articles.filter(col("web_url") != "https://www.nytimes.com/2022/04/28/sports/football/nfl-draft-hats-2022.html")
nyt_articles = nyt_articles.filter(col("section_name") != "nyt://article/2aec1dad-e9d0-5a3c-931b-cc93890bb60e")
nyt_articles = nyt_articles.union(combined_row)

# COMMAND ----------

#after reading digital values as floats, we should change them to integer because every single one is .0
nyt_articles = nyt_articles.withColumn("print_page", col("print_page").cast(IntegerType()))
nyt_articles = nyt_articles.withColumn("word_count", col("word_count").cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Checking for errors and cleaning data

# COMMAND ----------

def error_checker(df, filtered_df):
    count_errors = filtered_df.count()
    if count_errors > 0:
        print(f"Error: There are {count_errors} corrupted fields")
        display(filtered_df)
        return df.subtract(filtered_df)
    return df

# COMMAND ----------

corrupted = nyt_articles.filter(col("corrupted").isNotNull())

nyt_articles = error_checker(nyt_articles, corrupted)

nyt_articles = nyt_articles.drop("corrupted")

# COMMAND ----------

bad_urls = (nyt_articles.filter(~(col("web_url").rlike("^https://.+$")) |
                               col("web_url").isNull())
           )

nyt_articles = error_checker(nyt_articles, bad_urls)

# COMMAND ----------

bad_nulls = (nyt_articles.filter(col('source').isNull() | 
                                 col('pub_date').isNull() | 
                                 col('_id').isNull() |
                                 col('word_count').isNull() | 
                                 col('uri').isNull())
            )
nyt_articles = error_checker(nyt_articles, bad_nulls)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, current_timestamp

bad_dates = nyt_articles.filter(
    (year('pub_date') < 2000) |
    ("pub_date" > current_timestamp()) |
    (month('pub_date') > 12) |
    (dayofmonth('pub_date') > 31)
)
    
nyt_articles = error_checker(nyt_articles, bad_dates)

# COMMAND ----------

bad_words = nyt_articles.filter(col('word_count').isNull()) 
    
nyt_articles = error_checker(nyt_articles, bad_words)

# COMMAND ----------

bad_ids = nyt_articles.filter(
    (~(col("_id").rlike("nyt://.+$"))) |
    (col("_id") != col("uri"))
)
nyt_articles = error_checker(nyt_articles, bad_ids)

# COMMAND ----------

bad_maps = nyt_articles.filter(
    (~(col("headline").rlike(r"^\{.*\}$"))) |
    (~(col("byline").rlike(r"^\{.*\}$")))
)
nyt_articles = error_checker(nyt_articles, bad_maps)

# COMMAND ----------

bad_arrays = nyt_articles.filter(
    (~(col("keywords").rlike(r"^\[.*\]$"))) |
    (~(col("multimedia").rlike(r"^\[.*\]$")))
)
nyt_articles = error_checker(nyt_articles, bad_maps)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver layer is ready

# COMMAND ----------

nyt_articles = nyt_articles.sort(col("pub_date"))
display(nyt_articles.orderBy(rand()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Saving

# COMMAND ----------

nyt_articles.write.saveAsTable("nyt_silver")
