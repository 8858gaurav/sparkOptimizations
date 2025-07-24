# dynamically switching join strategies
# if we used distinct, then our data size should reduced from 12 mb to 10 mb, so that we can do broadcast hash join
# this feature automatically enabled with the pyspark (AQE)

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
           .appName("optimizationsAQE4") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    orders_schema = 'order_id long, order_date string, customer_id long, order_status string'
    df_orders = spark.read.format("csv").schema(orders_schema).load("/public/trendytech/orders/orders_1gb.csv")
    customer_schema = """cust_id long, customer_fname string, customer_lname string, username string, password string,
    address string, city string, state string, pincode long"""
    customer_df = spark.read.format("csv").schema(customer_schema).load("/public/trendytech/retail_db/customersnew")
    
    df_orders.join(customer_df.distinct(), df_orders.customer_id == customer_df.distinct().cust_id, "inner").write.format("csv").mode("overwrite").save("113")
    
    # hadoop fs -ls 113
    # it'll create a 200 files under this path, & the size of this path is 2.9 GB
    
    # After running the code, during the shuffle, we get to know the runtime stats, before that
    # it doesn't know. in case of without AQE, it would show you sort merge join strategies. after enabling the aqe, it will show you
    # a broadcasthash join strategies.

    # by default AQE is enabled in Pyspark3, here customer data is = 12.73 mb
    # distinct will reduces the data size here, it will reduced the data from 12.73 mb to less than 10 mb, so that it can fit on the drivers.
    # it will do a broadcast hash join automatically, in broadcast hash Join, it will not create a new stage to perform the join operations.

    spark.conf.set("spark.sql.adaptive.enabled", True)
    # now run the same thing again, by enabline the AQI
    orders_schema = 'order_id long, order_date string, customer_id long, order_status string'
    df_orders = spark.read.format("csv").schema(orders_schema).load("/public/trendytech/orders/orders_1gb.csv")
    customer_schema = """cust_id long, customer_fname string, customer_lname string, username string, password string,
    address string, city string, state string, pincode long"""
    customer_df = spark.read.format("csv").schema(customer_schema).load("/public/trendytech/retail_db/customersnew")
    df_orders.join(customer_df.distinct(), df_orders.customer_id == customer_df.distinct().cust_id, "inner").write.format("noop").mode("overwrite").save()
    # noop will just do process, and transformations, it'll not create any folder under your local system or on any file system.
    # you will see broadcast hash join + adaptive spark plan under sql tabs