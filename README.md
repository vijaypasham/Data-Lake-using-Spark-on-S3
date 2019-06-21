
# Data Lake setup using Spark on S3.

   This Project is mainly focused on setting up a data lake for Sparkify startup as their data grown. Source data is on S3 and are JSON log files.
	   
  We are building ETL pipeline to extract data from S3 and process them using Spark and then load back to S3 in the fact and dimension tables.
   
 **etl.py** is used to read data from S3 and process data using spark and also write back to S3.

**ETL Pipeline:**  In order to execute the pipeline we need valid credentials in dl.cfg file.
							   Open the terminal to the root folder.
				Then execute *`python etl.py`* to process data back and fourth using Spark.

    