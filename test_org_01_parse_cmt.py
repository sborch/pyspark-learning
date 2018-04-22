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

# needed for regex
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction


sc = SparkContext("local", "Simple App")

sqlContext = SQLContext(sc)

data1 = sqlContext.read.format('com.databricks.spark.csv') \
    .option('header', 'true').load('file:///home/steve/pyspark-learning/test_org_01_parse_cmt_data.csv')


# regex pattern to match '999-9999-9'
regexContractNumber = '\d{3}-\d{4}-\d{1}'

# parse strings like ' to ...' from comment_tx into parsed1
data1 = data1.withColumn('parsed1', regexp_extract('comment_tx', ' (?i)to\s+' + regexContractNumber, 0))

# parse 999-9999-9 from parsed1 into parsed2
data1 = data1.withColumn('parsed2', regexp_extract('parsed1', regexContractNumber, 0))

# -----------------------------------------------------------------------------
# strip hyphens from contract number (i.e. '999-9999-9' --> '99999999')
# and place in parsed3
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# option 1 - use udf
# -----------------------------------------------------------------------------
# udf = UserDefinedFunction(lambda x: x.replace('-', ''))  # follow-up on StringType()
# data1 = data1.withColumn('parsed3', udf(col('parsed2')))

# -----------------------------------------------------------------------------
# opton 2 - use regexp_replace
# -----------------------------------------------------------------------------
data1 = data1.withColumn('parsed3', regexp_replace('parsed2', '-', ''))


print(data1.show(truncate=0))


# -----------------------------------------------------------------------------
# end of file
# -----------------------------------------------------------------------------
