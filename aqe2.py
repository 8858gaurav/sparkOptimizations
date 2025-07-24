# handling skew partitions. 

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
           .appName("optimizationsAQE3") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
# by default AQE is enabled in Pyspark3, here mapping data is less than 10 mb
# it will do a broadcast hash join automatically, in broadcast hash Join, it will not create a new stage to perform the join operations.
    orders_schema = 'order_id long, order_date string, customer_id long, order_status string'
    df_orders_new = spark.read.format("csv").schema(orders_schema).load("/public/trendytech/retail_db/ordersnew")
    df_mapping = spark.read.format("csv").option('delimiter', "|").schema("status string, code int").load("/public/trendytech/datasets/mapping_data")

    df_orders_new.join(df_mapping, df_mapping.status == df_orders_new.order_status, "inner").write.format("csv").mode("overwrite").save("110")
    # this will create a 23 files under this folder hadoop fs -ls 110
    
    # [itv020752@g01 ~]$ hadoop fs -du -h /public/trendytech/retail_db/ordersnew, it's total 1.9GB
    # 0        0        /public/trendytech/retail_db/ordersnew/_SUCCESS
    # 16.8 M   50.4 M   /public/trendytech/retail_db/ordersnew/order_status=CANCELED
    # 89.0 M   267.1 M  /public/trendytech/retail_db/ordersnew/order_status=CLOSED
    # 1.4 G    4.2 G    /public/trendytech/retail_db/ordersnew/order_status=COMPLETE
    # 44.8 M   134.3 M  /public/trendytech/retail_db/ordersnew/order_status=ON_HOLD
    # 8.6 M    25.8 M   /public/trendytech/retail_db/ordersnew/order_status=PAYMENT_REVIEW
    # 89.7 M   269.0 M  /public/trendytech/retail_db/ordersnew/order_status=PENDING
    # 177.1 M  531.2 M  /public/trendytech/retail_db/ordersnew/order_status=PENDING_PAYMENT
    # 97.5 M   292.5 M  /public/trendytech/retail_db/ordersnew/order_status=PROCESSING
    # 18.4 M   55.1 M   /public/trendytech/retail_db/ordersnew/order_status=SUSPECTED_FRAUD

    #[itv020752@g01 ~]$ hadoop fs -ls -h /public/trendytech/datasets/mapping_data
    #-rw-r--r--   3 itv005857 supergroup        118B 2023-07-17 15:38 /public/trendytech/datasets/mapping_data

    # job tab overview
    #  stages:Succeeded/Total            Task(for all stages):Succeeded/Total 
    #    1/1                                         23/23
    #    1/1                                         1/1

    # if we click on 23/23 tasks link, then UI will be like
    # duration       Tasks: Succeeded/Total          input                     output
    # 4 min                23/23                      1.9G(1987.5 MiB)          3.2GB

# disabling the AQE

if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .config('spark.ui.port', '0') \
           .appName("optimizationsAQE4") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .config("spark.sql.adaptive.enabled", False) \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

orders_schema = 'order_id long, order_date string, customer_id long, order_status string'
df_orders_new = spark.read.format("csv").schema(orders_schema).load("/public/trendytech/retail_db/ordersnew")
df_mapping = spark.read.format("csv").option('delimiter', "|").schema("status string, code int").load("/public/trendytech/datasets/mapping_data")

df_orders_new.join(df_mapping, df_mapping.status == df_orders_new.order_status, "inner").write.format("csv").mode("overwrite").save("112")
    
# in the first stage, we'll see 2 partitions, because our data is around 7MB
# in the second stage, again we get around 4 to 5 partitons, (for wide transaformations, mostly we'll get 200 partitions)

