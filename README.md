#  Project: Data Warehouse With AWS

#### Project Description

In this project, we'll  build an ETL pipeline for a database hosted on Redshift. 

### ETL Pipeline:  

 1.  load data from S3 to staging tables on Redshift  
 2.  execute SQL
    statements that create the analytics tables from these

staging tables.

### Project Datasets

-   Song data:  `s3://udacity-dend/song_data`
-   Log data:  `s3://udacity-dend/log_data`
-   Log data json path:  `s3://udacity-dend/log_json_path.json

- below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

> {"num_songs":  1,  "artist_id":  "ARJIE2Y1187B994AB7", 
> "artist_latitude":  null,  "artist_longitude":  null, 
> "artist_location":  "",  "artist_name":  "Line Renaud",  "song_id": 
> "SOUPIRU12A6D4FA1E1",  "title":  "Der Kleine Dompfaff",  "duration": 
> 152.92036,  "year":  0}

And below is an example of what the data in a log file looks like.

![Log Dataset](https://camo.githubusercontent.com/b1534c91994fb040a3c86c673c7ddc7d04fde62ea541de77def55015e0c5d8ce/68747470733a2f2f766964656f2e756461636974792d646174612e636f6d2f746f706865722f323031392f46656272756172792f35633663313565395f6c6f672d646174612f6c6f672d646174612e706e67)


### Run project Steps:

1- create fact and dimension tables in redshift by run script:

> $ python create_tables.py

2- Run ETL pipeline

> $ python etl.py
