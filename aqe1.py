# dynamically coalesing the no of shuffle partitions. 
# Doing groupby on orders (7mb) file, out of 200, 194 partitions will be empty at the end. even if it's empty still though our
# task scheduler need to work on this empty partitions. 

# we'll run this code with and without AQE. 

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import getpass, time, os
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .config('spark.ui.port', '0') \
           .appName("optimizations2") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
#   /public/trendytech/datasets/orders.json - 6.7 MB file
    orders_schema = 'order_id long, order_date string, customer_id long, order_status string'
    spark.read.format("json").schema(orders_schema).load("/public/trendytech/datasets/orders.json").createOrReplaceTempView("orders")

    spark.sql("select order_status, count(*) from orders group by 1").write.format('noop').mode("overwrite").save()
    # in the second stage, in this only 8 partitions are having the data, & remaining 192 partition are empty. 
    
    # enabling the AQE
    spark.conf.set("spark.sql.adaptive.enabled", True)
    spark.conf.get("spark.sql.adaptive.enabled")

    orders_schema = 'order_id long, order_date string, customer_id long, order_status string'
    spark.read.format("json").schema(orders_schema).load("/public/trendytech/datasets/orders.json").createOrReplaceTempView("orders")

    spark.sql("select order_status, count(*) from orders group by 1").write.format('noop').mode("overwrite").save()
    # in the first stage, we'll see 2 partitions, because our data is around 7MB
    # in the second stage, again we get only 4 to 5 partitons, (for wide transaformations, mostly we'll get 200 partitions)
