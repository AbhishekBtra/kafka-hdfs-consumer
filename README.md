# kafka-hdfs-consumer
This repo targets to read metadata of HDFS files that chnage over a very short period of time. 
Once recived the same can be pushed to BigQuery or local scripts to start offloading the data using distcp, hdfs put or gsutil.
Final data storage layer is BigQuery.
