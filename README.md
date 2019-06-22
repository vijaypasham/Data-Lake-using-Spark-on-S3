
# Data Lake setup using Spark on S3.

   This Project is mainly focused on setting up a data lake for Sparkify startup as their data grown. Source data is on S3 and are JSON log files.
	   

 - **Song_data:** Below is the sample record of Song data.
						`{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0} Log Dataset`
 - **Log_data:** Below are the sample records of log data.
 ![enter image description here](https://s3.amazonaws.com/video.udacity-data.com/topher/2019/February/5c6c3f0a_log-data/log-data.png)

 
 We are building ETL pipeline to extract data from S3 and process them using Spark and then load back to S3 in the fact and dimension tables.

**Fact Table:**   

 - Songplays: This table holds data of user,artist, and song (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

**Dimension Tables:**

 - Users: Holds user data (user_id, first_name, last_name, gender, level)
 - Songs: Holds Songs data(song_id, title, artist_id, year, duration)
 - Artists: Holds artists information (artist_id, name, location, latitude, longitude)
 - Time: Records timestamp for the songplays(start_time, hour, day, week, month, year, weekday)

***How to Run ETL Pipeline::***

This project has  python script **etl.py** which is is used to read data from S3 and process data using spark and also write back to S3.

In order to execute the pipeline we need valid credentials in dl.cfg file.
		-Add AWS key as shown in sample dl.cfg file.
		-Run the etl.py by opening the terminal , Then execute *`python etl.py`* to process data back and fourth using Spark.

    