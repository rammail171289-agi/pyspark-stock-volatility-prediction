from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("StockVolatilityPrediction").getOrCreate()

# Load Data
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("data/adani_stock_sample.csv")

# Rename Columns
df = df.withColumnRenamed('NO. OF  TRADES', 'NO_OF_TRADES') \
       .withColumnRenamed('52W L', '52W_L') \
       .withColumnRenamed('52W H', '52W_H') \
       .withColumnRenamed('PREV. CLOSE', 'PREV_CLOSE')

# Convert Date
df = df.withColumn('DATE', to_date(col('DATE'), 'dd-MMM-yyyy'))

# Remove commas from numeric columns
labels = ['OPEN','HIGH','LOW','PREV_CLOSE','LTP','CLOSE','VWAP',
          '52W_H','52W_L','VOLUME','VALUE','NO_OF_TRADES']

for c in labels:
    df = df.withColumn(c, regexp_replace(col(c), ',', ''))

# Cast to Double
for c in labels:
    if isinstance(df.schema[c].dataType, StringType):
        df = df.withColumn(c, col(c).cast("double"))

# Calculate Previous Close
window_spec = Window.orderBy(col("DATE"))

df = df.withColumn("previous_close", lag("CLOSE").over(window_spec))

# Daily Return
df_return = df.withColumn(
    "return",
    (col("CLOSE") - col("previous_close")) / col("previous_close")
)

# Average Return
average_return = df_return.select(avg(col("return"))).collect()[0][0]

# Volatility
volatility = df_return.select(stddev(col("return"))).collect()[0][0]

# Last Close Price
latest = df_return.orderBy(col("DATE").desc()).first()

last_close = latest["CLOSE"]
last_date = latest["DATE"]

print("Last Trading Date:", last_date)
print("Last Closing Price:", last_close)

# Expected Price Move
expected_move = last_close * volatility

# Predicted Range
lower_price = last_close - expected_move
upper_price = last_close + expected_move

print("\nPredicted Next Trading Range")
print("Lower Price:", lower_price)
print("Upper Price:", upper_price)
