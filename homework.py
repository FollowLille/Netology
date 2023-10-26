from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


spark = SparkSession.builder.appName('homework').getOrCreate()
df = spark.read.option('header', True).option('inferSchema', True).option('sep', ',').csv('owid-covid-data.csv')

first_result = df.select('iso_code', 'location', (col('total_cases')/col('population')*100).alias('percent'))\
.where(col('date') == '2021-03-31').sort(col('percent').desc()).limit(15)
first_result.toPandas().to_csv('first_work.csv', header=True)


sorted_df = df.select('date', 'location', 'new_cases').where('2021-03-24' < col('date')).where(col('date') <= '2021-03-31')
grouped_df = sorted_df.select('location', 'new_cases').groupBy('location').max('new_cases').sort(col('max(new_cases)').desc())\
.withColumnRenamed('max(new_cases)', 'new_cases').limit(10)
joined_df = grouped_df.join(sorted_df, ['location', 'new_cases']).select('date', 'location', 'new_cases').sort(col('new_cases').desc())
joined_df.withColumn("date", date_format("date", "yyyy-MM-dd")).toPandas().to_csv('second_work.csv', header=True)


w = Window.partitionBy('location').orderBy('date')
sorted_df = df.select('date', 'location', 'new_cases').where(col('date').between('2021-03-24', '2021-03-31'))\
.where(col('location') == 'Russia')
df_with_lags = sorted_df.select('date', lag(col('new_cases')).over(w).alias('previous_case'), 'new_cases').where(col('date')>'2021-03-24')
df_with_delta = df_with_lags.withColumn('delta', col('new_cases')-col('previous_case'))
df_with_delta.withColumn("date", date_format("date", "yyyy-MM-dd")).toPandas().to_csv('third_work.csv', header=True)
