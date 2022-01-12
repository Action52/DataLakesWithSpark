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
```

### dl.cfg
This config file should hold your own user key pair to access AWS.
