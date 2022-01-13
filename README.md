# Sparkify Data Lakes

The Sparkify firm has expanded its database into a much bigger product. It now requires
a Data Lake to store the data. It has been decided that the architecture will be hosted
in AWS with an EMR Cluster running Spark to process the data into the lake.

The following scripts allow the raw json data to be processed and stored into parquet files
with Spark.

### etl.py

This script shall be executed within the EMR cluster. Make sure to be able to connect to s3
with the appropriate permissions.
```
python etl.py
````
The script will create a spark instance and will process the songs data logs first.
The songs data logs will help us in creating the songs and artists table. The job will take care of the
instance types and cast them on the tables. When we are done with these tables, they are saved into parquet.
The songs table is partitioned by year and artist. Then we return the reference to the tables and pass it on to
the next part of the script.  
The second part of the script iterates over the log s3 datasets. This creates the time, users, and songplays table.
Finally, we save the tables into parquet, with the time and songplays table partitioned by year and month, and upload them to the desired s3 path.

### dl.cfg
This config file should hold your own user key pair to access AWS.
To run the etl job correctly please fill in the empty rows like this:
```
AWS_ACCESS_KEY_ID=<YOUR AWS ACCESS KEY>
AWS_SECRET_ACCESS_KEY=<YOUR AWS SECRET ACCESS KEY>
s3_output_path=<PATH TO A BUCKET OF YOUR BELONGING WITH WRITE PERMISSIONS>
```
Please do not modify the other parameters since they are the paths to the raw data on the Udacity bucket.  
Note: For better perfomance use `s3a://<URL>` instead of `s3://<URL> for your output path.