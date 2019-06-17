[star_schema]:/images/starschema.png "Million Songs DW Schema Design"
[dag]:/images/dag.png "Airflow DAG"
[treeview]:/images/tv.png "DAG Treeview"

# Apache Airflow Pipeline for ETL from S3 to Redshift

This project demonstrates the use of Airflow as a data pipeline tool for running ETL processes. The task in question involves the copy of data from S3 to staging tables in Redshift and consequently loading into dimension and fact tables in the datawarehouse.

## Dependencies

* Apache Airflow

## Running the DAG

* Copy contents of dag folder into your Airflow Dag folder
* Copy contents of plugins folder into your Airflow Plugin folder
* Set up your redshift connection on Apache Airflow and update **redshift_conn_id** variable on *line 23* of **million_songs_etl_dag.py** accordingly
* Set up your AWS connection on Apache Airflow and update **aws_conn_id** variable on *line 24* of **million_songs_etl_dag.py** accordingly
* create the Airflow variable **million_songs_s3_buckets** with the following data `{ "songs":"song_data", "logs":"log_data", "bucket":"udacity-dend", "logs_jsonpaths":"s3://udacity-dend/log_json_path.json" }`
* You should be all set to run the dag now
* Open your Airflow console and run the **million_songs_dag**

## Data Sources

The input data consists of two datasets currently stored on AWS S3:

1. A subset of real song data from the Million Song Dataset: Each file is in JSON format and contains metadata about a song and the artist of that song. The metadata consists of fields such as song ID, artist ID, artist name, song title, artist location, etc. A sample song record is shown below:

    ```javascript
       {"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, 
    "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", 
        "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0} 
    ```

2. Logs of user activity on a purported music streaming application. The data is actually generated using an event generatory. This data captures user activity on the app and stores metadata such as artist name, authentication status, first name,length ,time , ect. A sample of the log data is shown below:

    ```javascript
        {"artist":"N.E.R.D. FEATURING MALICE","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Fox","length":288.9922,"level":"free",
    "location":"New Orleans-Metairie, LA","method":"PUT","page":"NextSong",
        "registration":1541033612796.0,"sessionId":184,"song":"Am I High (Feat. Malice)",
            "status":200,"ts":1541121934796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"101"}
    ```

## Database Schema

The schema design is shown below:

![star_schema]

## The DAG

The DAG is composed of the following functioning tasks

* **initialize_database**: A PostgresOperator which sets up the database and creates the necessary tables
* **stage_events**: A SubdagOperator which optionally creates a staging table, copies in data from S3 and verifies the row count of the operation. In this case it copies in the logs of user activitiy
* **stage_songs**: A custom operator which runs a query on redshift and updates the target table with the results. This instance loads activity data from the staged songs and staged events tables
* **Load_song_dim_table**: A custom operator which runs a query on redshift and updates the target table with the results. This instance loads songs data from the staged songs table
* **Load_user_dim_table**: A custom operator which runs a query on redshift and updates the target table with the results. This instance loads user information from the staged events table
* **Load_time_dim_table**: A custom operator which runs a query on redshift and updates the target table with the results. This instance loads information on times of activity from the staged events table
* **Load_song_dim_table**: A custom operator which runs a query on redshift and updates the target table with the results. This instance loads songs data from the staged songs table
* **Load_song_dim_table**: A custom operator which runs a query on redshift and updates the target table with the results. This instance loads artist data from the staged songs table
* **Run_data_quality_checks**: This custom operator runs a series of quality checks agains the extracted data and fails the dag if the count of any result is less than a preset threshold

![dag]

## The DAG run

We are able to successfully load the data from S3 to Redshift. The below treeview shows sucesses after debugging the code and configuration of the pipleine.

![treeview]
