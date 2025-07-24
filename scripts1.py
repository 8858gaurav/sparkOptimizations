# No of partitions initially.

import pyspark, pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import getpass, time, os
username = getpass.getuser()
print(username)


if __name__ == '__main__':
    print("creating spark session")

    spark = SparkSession \
           .builder \
           .config('spark.ui.port', '0') \
           .appName("optimizationsjoin") \
           .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
           .enableHiveSupport() \
           .master("yarn") \
           .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    spark.conf.get("spark.sql.files.maxPartitionBytes") # '134217728b'
    spark.conf.get("spark.sql.shuffle.partitions") # 200

    # whenerver shuffle happends by default 200 partition will get created.
    # formula for partition size
    # partition_size = min of (maxPartitionBytes, file_size/default parallelism)
    # the file size of orders is 1 GB in locally.
    # the file size of orders is  1 GB in hdfs.
    spark.sparkContext.defaultParallelism # default parallelism 2

    orders_schema = 'order_id long, order_date string, customer_id long, order_status string'
    orders_df = spark.read.format("csv").schema(orders_schema).load("/public/trendytech/orders/orders_1gb.csv")

    customer_schema = """customer_id long, customer_fname string, customer_lname string, username string, password string,
    address string, city string, state string, pincode long"""
    customer_df = spark.read.format("csv").schema(customer_schema).load("/public/trendytech/retail_db/customers/")

    orders_df.rdd.getNumPartitions() # 9
    customer_df.rdd.getNumPartitions() # 1

    # by default broadcast join will be active if one of your data < 10 MB
    # [itv020752@g01 ~]$ hadoop fs -ls -h /public/trendytech/retail_db/customers/
    # Found 1 items
    # -rw-r--r--   3 itv005857 supergroup    931.4 K 2023-04-26 16:47 /public/trendytech/retail_db/customers/part-00000
    int(spark.conf.get("spark.sql.autoBroadcastJoinThreshold")[0:-1]) /1024 /1024 # 10MB

    orders_df.join(customer_df, orders_df.customer_id == customer_df.cust_id, "inner").write.format("csv").mode("overwrite").save("output107")
    # this will create only 11 task for us under job section, and 9 files under output folder, since we have a 9 partitions for order file
    # and for each 9 partiton, customer data is also broadcasted in it.
    # you will see broadcast hash join under sql tabs.
    # [itv020752@g01 ~]$ hadoop fs -ls -h output107
    # Found 10 items
    # -rw-r--r--   3 itv020752 supergroup          0 2025-07-23 12:58 output107/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup    351.1 M 2025-07-23 12:58 output107/part-00000-63dc1637-c7ed-49fd-acc1-4c38c38a22a0-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    351.1 M 2025-07-23 12:56 output107/part-00001-63dc1637-c7ed-49fd-acc1-4c38c38a22a0-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    351.1 M 2025-07-23 12:58 output107/part-00002-63dc1637-c7ed-49fd-acc1-4c38c38a22a0-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    351.0 M 2025-07-23 12:58 output107/part-00003-63dc1637-c7ed-49fd-acc1-4c38c38a22a0-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    351.1 M 2025-07-23 12:58 output107/part-00004-63dc1637-c7ed-49fd-acc1-4c38c38a22a0-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    351.1 M 2025-07-23 12:57 output107/part-00005-63dc1637-c7ed-49fd-acc1-4c38c38a22a0-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    351.1 M 2025-07-23 12:58 output107/part-00006-63dc1637-c7ed-49fd-acc1-4c38c38a22a0-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    351.0 M 2025-07-23 12:58 output107/part-00007-63dc1637-c7ed-49fd-acc1-4c38c38a22a0-c000.csv
    # -rw-r--r--   3 itv020752 supergroup    134.0 M 2025-07-23 12:56 output107/part-00008-63dc1637-c7ed-49fd-acc1-4c38c38a22a0-c000.csv

    ################################ do the same thing after disable boradcast Join ###################################

    orders_schema = 'order_id long, order_date string, customer_id long, order_status string'
    spark.read.format("csv").schema(orders_schema).load("/public/trendytech/orders/orders_1gb.csv").createOrReplaceTempView("orders")

    customer_schema = """cust_id long, customer_fname string, customer_lname string, username string, password string,
    address string, city string, state string, pincode long"""
    spark.read.format("csv").schema(customer_schema).load("/public/trendytech/retail_db/customers/").createOrReplaceTempView("customers")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

    spark.sql("select * from customers C inner join orders O on C.cust_id = O.customer_id").write.format("noop").mode("overwrite").save("output109")
    # this will create a 210 task under job section, (200 for simple join, 9 for orders table, & 1 for customer table)

    # same key will go to the same partition to perform join operations. since here we have more than 200 customer_id, all the partition (200) in case of join stages, 
    # will have some values in it. 

    # durations               task             i/p.        shuffle read                   shuffle write
    # 1                      200/200                          438MB
    # 2                        1/1             931.4kb                                        904.6 KB
    # 11                       9/9              ~= 1gB                                        437.1MB

    # why here it's a emplty for 200/200 task, because we are doing noop here, noop means do all the operation, & transafomrations, but
    # don't save the output in local sys or in hdfs. we are not storing the data in this case, that's why we don't need to pass anything under save method. 

    # for 1/1 task, node_local = 1, and for 11/11 task , node_local = 1, for 200/200, node_local = 200, since we are joinning in the same partition. 