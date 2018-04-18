import platform
import sys

# print('---------------')
# print("python version : " + platform.python_version())
# print("python location: " + sys.executable)
# print('---------------')
# print(sys.path)
# print('---------------')

sys.path.insert(0, "/opt/spark/python")
sys.path.insert(0, "/opt/spark/python/lib/py4j-0.10.6-src.zip")

# sys.path.insert(0, "/home/steve/hadoop-2.8.0")
# sys.path.insert(0, "/home/steve/hadoop-2.8.0/lib/native")

# print('---------------')
# print(sys.path)
# print('---------------')

from pyspark import SparkContext
from pyspark.sql import SQLContext

# needed for StructType
from pyspark.sql.types import *

# needed for regex
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction

# -----------------------------------------------------------------------------
# initialize
# -----------------------------------------------------------------------------

debug = False

sc = SparkContext("local", "Simple App")

sqlContext = SQLContext(sc)

# -----------------------------------------------------------------------------
# read file into main_df
# -----------------------------------------------------------------------------
main_df = sqlContext.read  \
    .format('com.databricks.spark.csv') \
    .option('header', 'true') \
    .load('file:///home/steve/test_org/test_org_02_ult_survr_data.csv')

print(main_df.show(truncate=0))
# +-----+------+
# |child|parent|
# +-----+------+
# |A    |B     |
# |B    |C     |
# |C    |D     |
# |D    |null  |
# +-----+------+

# -----------------------------------------------------------------------------
# create an empty dataframe to hold ultimate parents
# -----------------------------------------------------------------------------
ult_schema = StructType([
    StructField('child', StringType(), True),
    StructField('ult_parent', StringType(), True)
])

ult_df = sqlContext.createDataFrame([], ult_schema)

# -----------------------------------------------------------------------------
# create two dataframes with aliased fields for iterative child-parent joins
# -----------------------------------------------------------------------------
# wrk_df - contains only records that have a parent to resolve
#          i.e. those records with a null parent are already ultimate survivors
#               for themselves.
#
# ref_df - reference df contains all records.  will be iteratively joined to
#          wrk_df to find the parent of the child's parent.
#
#          +---------+----------+    +---------+----------+
#          |wrk_child|wrk_parent|    |ref_child|ref_parent|
#          +---------+----------+    +---------+----------+
#          |        A|         B|    |        A|         B|
#          |        B|         C|    |        B|         C|
#          |        C|         D|    |        C|         D|
#          +---------+----------+    |        D|      null|
#                                    +---------+----------+
#
# -----------------------------------------------------------------------------

wrk_df = main_df \
    .filter(main_df.parent.isNotNull()) \
    .select(main_df.child.alias('wrk_child'), main_df.parent.alias('wrk_parent'))

ref_df = main_df \
    .select(main_df.child.alias('ref_child'), main_df.parent.alias('ref_parent'))


# -----------------------------------------------------------------------------
# Test for missing parent records
# -----------------------------------------------------------------------------
#   A,B
#   B,C    <-- C does not exist in dataset
# -----------------------------------------------------------------------------
joined_df = wrk_df \
    .join(ref_df, wrk_df.wrk_parent == ref_df.ref_child, 'left_outer') \
    .filter(ref_df.ref_child.isNull())

if joined_df.count() > 0:
    print(joined_df.show())
    raise RuntimeError("error: missing parent records")

# -----------------------------------------------------------------------------
# Test for cross-linked records
# -----------------------------------------------------------------------------
#   A,B   <-- B is the parent of A
#   B,A   <-- A is the parent of B
# -----------------------------------------------------------------------------

joined_df = wrk_df \
    .join(ref_df, wrk_df.wrk_parent == ref_df.ref_child, 'inner') \
    .filter(wrk_df.wrk_child == ref_df.ref_parent)

if joined_df.count() > 0:
    print(joined_df.show())
    raise RuntimeError("error: cross-linked parent-child records exist.")

# -----------------------------------------------------------------------------
# loop until all wrk_df records have found their ultimate parent
# -----------------------------------------------------------------------------
loopCount = 0

while(wrk_df.count() > 0):

    loopCount = loopCount + 1

    # -----------------------------------------------------------------------------
    # join wrk_parent to ref_child to find the parent of wrk_parent
    #
    #  if child.parent.parent is null then the rollup for that child
    #  is complete and its parent is the ultimate parent
    #
    #  if child.parent.parent is not null then the rollup for that child
    #  is *NOT* complete and we need to set the wrk_parent to the
    #  child.parent.parent and repeat the process.
    #
    # -----------------------------------------------------------------------------
    #
    #    child                       child.parent.parent
    #      |                                 |
    #      +------------- RESULT ------------+
    #      |                                 |
    #      |          +--- JOIN ---+         |
    #      |          |            |         |
    # +---------+----------+  +---------+----------+
    # |wrk_child|wrk_parent|  |ref_child|ref_parent|
    # +---------+----------+  +---------+----------+
    # |A        |B         |  |B        |C         | <-- incomplete - child.parent.parent is not null
    # |B        |C         |  |C        |D         | <-- incomplete - child.parent.parent is not null
    # |C        |D         |  |D        |null      | <-- COMPLETE   - child.parent.parent is null
    # +---------+----------+  +---------+----------+
    #
    # -----------------------------------------------------------------------------

    joined_df = wrk_df.join(ref_df, wrk_df.wrk_parent == ref_df.ref_child, 'inner')

    if debug:
        print(joined_df.show(truncate=0))

    # append any completed rollups (records with ref_parent == null) to ult_df
    ult_df = ult_df.union(
        joined_df
        .filter(joined_df.ref_parent.isNull())
        .select(joined_df.wrk_child, joined_df.wrk_parent))

    if debug:
        print(ult_df.show(truncate=0))
    # +-----+----------+
    # |child|ult_parent|
    # +-----+----------+
    # |C    |D         |
    # +-----+----------+

    # rebuild wrk_df with remaining incomplete rollups (records with ref_parent != null)
    wrk_df = joined_df \
        .filter(joined_df.ref_parent.isNotNull()) \
        .select(joined_df.wrk_child, joined_df.ref_parent.alias('wrk_parent'))

# -----------------------------------------------------------------------------
# add ultimate parent column to main_df
# if an ultimate suvivor exists for a child then use the ultimate survivor
# else set (coalesce) the ultimate survivor to the child
# -----------------------------------------------------------------------------
main_df = main_df \
    .join(ult_df, main_df.child == ult_df.child, 'left_outer') \
    .select(main_df['*'], coalesce(ult_df.ult_parent, main_df.child).alias('ult_parent')) \
    .sort(main_df.child)

print("loop count: {0}".format(loopCount))
print(main_df.show(truncate=0))

# +-----+------+----------+
# |child|parent|ult_parent|
# +-----+------+----------+
# |A    |B     |D         | <-- ultimate parent
# |B    |C     |D         | <-- ultimate parent
# |C    |D     |D         | <-- ultimate parent
# |D    |null  |D         | <-- coalesced child
# +-----+------+----------+

# -----------------------------------------------------------------------------
# end
# -----------------------------------------------------------------------------
