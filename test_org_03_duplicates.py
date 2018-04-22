import platform
import sys
print('---------------')
print("python version : " + platform.python_version())
print("python location: " + sys.executable)
print('---------------')
# print(sys.path)
# print('---------------')


from pyspark import SparkContext
from pyspark.sql import SQLContext

# needed for StructType
from pyspark.sql.types import *

# needed for regex
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction

from pyspark.sql.session import SparkSession

from pyspark.sql import Row

# -----------------------------------------------------------------------------
# initialize
# -----------------------------------------------------------------------------

debug = True

sc = SparkContext("local", "Simple App")

sqlContext = SQLContext(sc)

# -----------------------------------------------------------------------------
# read file into main_df
# -----------------------------------------------------------------------------
main_df = sqlContext.read  \
    .format('com.databricks.spark.csv') \
    .option('header', 'true') \
    .load('file:///home/steve/pyspark-learning/test_org_03_duplicates_data.csv')
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# l = [('A'),('B'),('B'),('B'),('C'),('C'),('D')]
# rdd = sc.parallelize(l)
# dat = rdd.map(lambda x: Row(item=x[0]))
# main_df = sqlContext.createDataFrame(dat)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# create unique row id.  this will be used to pick the first instance of
# duplicate records
# -----------------------------------------------------------------------------
main_df = main_df.withColumn("row_id", monotonically_increasing_id())

print(main_df.show(truncate=0))
# +----+------+
# |item|row_id|
# +----+------+
# |A   |0     |
# |B   |1     |
# |B   |2     |
# |B   |3     |
# |C   |4     |
# |C   |5     |
# |D   |6     |
# +----+------+

# -----------------------------------------------------------------------------
# create temp dataframe of grouped record counts by item
# -----------------------------------------------------------------------------
temp_df = main_df.groupBy(main_df.item).count().orderBy(main_df.item)
temp_df = temp_df.selectExpr('item as join_item', 'count')

print(temp_df.show())
# +---------+-----+
# |join_item|count|
# +---------+-----+
# |        A|    1|
# |        B|    3|
# |        C|    2|
# |        D|    1|
# +---------+-----+


# -----------------------------------------------------------------------------
# join temp dataframe back to main dataframe
# -----------------------------------------------------------------------------
main_df = main_df.join(temp_df, main_df.item == temp_df.join_item, 'left_outer')
main_df = main_df.drop('join_item')

print(main_df.show())
# +----+------+-----+
# |item|row_id|count|
# +----+------+-----+
# |   A|     0|    1|
# |   B|     1|    3|
# |   B|     2|    3|
# |   B|     3|    3|
# |   C|     4|    2|
# |   C|     5|    2|
# |   D|     6|    1|
# +----+------+-----+

# -----------------------------------------------------------------------------
# flag duplicate items
# -----------------------------------------------------------------------------
udf = UserDefinedFunction(lambda x: 'Y' if x > 1 else 'N')

main_df = main_df.withColumn('dup_flag', udf(col('count')))

print(main_df.show())
# +----+------+-----+--------+
# |item|row_id|count|dup_flag|
# +----+------+-----+--------+
# |   A|     0|    1|       N|
# |   B|     1|    3|       Y|
# |   B|     2|    3|       Y|
# |   B|     3|    3|       Y|
# |   C|     4|    2|       Y|
# |   C|     5|    2|       Y|
# |   D|     6|    1|       N|
# +----+------+-----+--------+

# -----------------------------------------------------------------------------
# create temp dataframe of the minimum row id for each distinct item value
# -----------------------------------------------------------------------------
temp_df = main_df.groupBy(main_df.item).agg(min('row_id').alias('min_row_id')).orderBy(main_df.item)
temp_df = temp_df.selectExpr('item as join_item', 'min_row_id')

print(temp_df.show())
# +---------+----------+
# |join_item|min_row_id|
# +---------+----------+
# |        A|         0|
# |        B|         1|
# |        C|         4|
# |        D|         6|
# +---------+----------+

# -----------------------------------------------------------------------------
# join temp minimum row id dataframe back to main dataframe on item and row_id
# -----------------------------------------------------------------------------
main_df = main_df.join(temp_df,
                       (main_df.item == temp_df.join_item) & (main_df.row_id == temp_df.min_row_id),
                       'left_outer')

main_df = main_df.drop('join_item')

print(main_df.show())
# +----+------+-----+--------+----------+
# |item|row_id|count|dup_flag|min_row_id|
# +----+------+-----+--------+----------+
# |   A|     0|    1|       N|         0|
# |   B|     1|    3|       Y|         1|
# |   B|     2|    3|       Y|      null|
# |   B|     3|    3|       Y|      null|
# |   C|     4|    2|       Y|         4|
# |   C|     5|    2|       Y|      null|
# |   D|     6|    1|       N|         6|
# +----+------+-----+--------+----------+

# -----------------------------------------------------------------------------
# fill null values in min_row_id with value of -1
# -----------------------------------------------------------------------------
main_df = main_df.fillna(-1, subset=['min_row_id'])

print(main_df.show())
# +----+------+-----+--------+----------+
# |item|row_id|count|dup_flag|min_row_id|
# +----+------+-----+--------+----------+
# |   A|     0|    1|       N|         0|
# |   B|     1|    3|       Y|         1|
# |   B|     2|    3|       Y|        -1|
# |   B|     3|    3|       Y|        -1|
# |   C|     4|    2|       Y|         4|
# |   C|     5|    2|       Y|        -1|
# |   D|     6|    1|       N|         6|
# +----+------+-----+--------+----------+

# -----------------------------------------------------------------------------
# flag first record for each item
# -----------------------------------------------------------------------------
udf = UserDefinedFunction(lambda x: 'Y' if x >= 0 else 'N')

main_df = main_df.withColumn('first_item_flag', udf(col('min_row_id')))

print(main_df.show())
# +----+------+-----+--------+----------+---------------+
# |item|row_id|count|dup_flag|min_row_id|first_item_flag|
# +----+------+-----+--------+----------+---------------+
# |   A|     0|    1|       N|         0|              Y|
# |   B|     1|    3|       Y|         1|              Y|
# |   B|     2|    3|       Y|        -1|              N|
# |   B|     3|    3|       Y|        -1|              N|
# |   C|     4|    2|       Y|         4|              Y|
# |   C|     5|    2|       Y|        -1|              N|
# |   D|     6|    1|       N|         6|              Y|
# +----+------+-----+--------+----------+---------------+

# -----------------------------------------------------------------------------
# clean-up and drop temporary 'working' columns
# -----------------------------------------------------------------------------
main_df = main_df.drop('row_id', 'count', 'min_row_id')

print(main_df.show())
# +----+--------+---------------+
# |item|dup_flag|first_item_flag|
# +----+--------+---------------+
# |   A|       N|              Y|
# |   B|       Y|              Y|
# |   B|       Y|              N|
# |   B|       Y|              N|
# |   C|       Y|              Y|
# |   C|       Y|              N|
# |   D|       N|              Y|
# +----+--------+---------------+

# -----------------------------------------------------------------------------
# end of file
# -----------------------------------------------------------------------------
