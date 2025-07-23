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
           .appName("optimizationsnew2") \
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
    spark.sparkContext.defaultParallelism # default parallelism

    #!hadoop fs -head /public/trendytech/orders/orders_1gb.csv
    maxPartitionBytes = int(spark.conf.get("spark.sql.files.maxPartitionBytes")[0:-1])/1024/1024 # for MB, it's 128
    file_size = 1124979000/1024/1024 # for MB.
    default_parallelism = spark.sparkContext.defaultParallelism

    b = file_size/default_parallelism
    a = maxPartitionBytes

    def minimum(x, y): 
        if x < y:
            return x
        else:
            return y 
    partition_size = minimum(a, b)

    print(a, b)

    print("number of partitions", file_size/partition_size)

    # no of partitions = Max(file_size/partition_size, default level of parallelism)

    print(file_size/partition_size) # 8.38 ~ 9

    def maximum(x, y): 
        if x > y:
            return x
        else:
            return y 
    partition_number = maximum(a, b)


    print("No of partitions ideally", maximum(file_size/partition_size,default_parallelism))

    orders_schema = 'order_id long, order_date string, customer_id long, order_status string'
    orders_df = spark.read.format("csv").schema(orders_schema).load("/public/trendytech/orders/orders_1gb.csv")

    orders_df.rdd.getNumPartitions() # this is equal to the below one
    print("number of partitions", orders_df.rdd.getNumPartitions(), file_size/partition_size, maximum(file_size/partition_size,default_parallelism))
    # number of partitions 9 8.381746709346771 8.381746709346771

    orders_df.groupBy("order_status").count().write.format('orc').mode("overwrite").save("output103")
    # output101 will create a folder under your hdfs home directory (hadoop fs -ls)
    # group by does a local aggregation, at first stage, we have a 9 partitions, on this 9 partition, local aggregation has been 
    # by groupby condition. So, in the first stage, since we have a 1gb data, 9 partitions will be created, each partitions 
    # should have a 128 mb of data. on this 128 mb of data, we are doing a local aggregations. 
    # the output of this local aggregations will be stored on the same partitions. Node_local
    # you will get something like this: 

    # Locality Level   input_size/records      shuffle write size /records
    #    Node_local         128.1 MiB/3081900	         691B/9
    #    Node_local         128.1 MiB/3081899	         691B/9
    #    Node_local         128.1 MiB/3081900	         691B/9
    #    Node_local         128.1 MiB/3081899	         691B/9
    #    Node_local         128.1 MiB/3081900	         691B/9
    #    Node_local         128.1 MiB/3081899	         691B/9
    #    Node_local         128.1 MiB/3081900	         691B/9
    #    Node_local         128.1 MiB/3081899	         691B/9
    #    Node_local         48.9 MiB/3081900	         689B/9

    # Now in the next stage, since we have a count of 9 distinct values for each order status on each partitions (whihc is 9). 
    # this will become the input for your 200 shuffle partitons (wide transformations)
    # so the count of all closed order status will go in 1 partitons (out of 200 partitions in 2nd stage) from the 9 partitions (stage 1)
    # the count of all pending order_status from the 9 partitons (stage1), will go to in another partitions (out of 200 partitions) for stage 2.
    # in this way, we'll have the values only for 9 partitions, and remaining 191 partitons will be empty.

    # in the second stage, we'll get 9 rows as a values (count of each order_status) from the 9 partitions as a input.
    # now this input will be agregated on the same partitions in the second stage. means Node_local

    # Locality Level   output_size/records      shuffle Read size /records
    #    Node_local         19B/1	         691B/9
    #    Node_local         29B/1	         691B/9
    #    Node_local         24B/1	         691B/9
    #    Node_local         23B/1	         691B/9
    #    Node_local         22B/1	         691B/9
    #    Node_local         15B/1	         691B/9
    #    Node_local         16B/1	         691B/9
    #    Node_local         16B/1	         691B/9
    #    Node_local         16B/1	         689B/9
    #    PROCESS_LOCAL                                   # empty rows remaining 191
    #    PROCESS_LOCAL

    #Locality Level Summary: Node local: 9; Process local: 191 (empty rows in this case)

    # [itv020752@g01 ~]$ hadoop fs -ls output103
    # Found 11 items
    # -rw-r--r--   3 itv020752 supergroup          0 2025-07-23 10:53 output103/_SUCCESS
    # -rw-r--r--   3 itv020752 supergroup        139 2025-07-23 10:53 output103/part-00000-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup        458 2025-07-23 10:53 output103/part-00002-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup        426 2025-07-23 10:53 output103/part-00053-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup        420 2025-07-23 10:53 output103/part-00068-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup        446 2025-07-23 10:53 output103/part-00100-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup        435 2025-07-23 10:53 output103/part-00142-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup        414 2025-07-23 10:53 output103/part-00149-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup        449 2025-07-23 10:53 output103/part-00168-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup        420 2025-07-23 10:53 output103/part-00178-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc
    # -rw-r--r--   3 itv020752 supergroup        415 2025-07-23 10:53 output103/part-00185-a5d54f81-708d-40cf-aca7-4ed59cefd652-c000.snappy.orc