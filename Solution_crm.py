from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.functions import col, avg
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
	# Write code here
  joined_df = customers.join( orders, on ="customer_id", how = "inner").join(products, on ="product_id",how = "inner")
  result_df = joined_df.select("order_id", F.concat_ws(" ", "first_name", "last_name")
                               .alias("customer_name"),
                               col("email").alias("customer_email"),
                               "product_name",
                               col("category").alias("product_category"),
                               "order_date")
  return result_df 
