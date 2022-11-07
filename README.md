 ## Sumary
 - A Postgres database with tables designed to optimize queries on song play analysis,
  with ETL pipeline created for the analytics team.

## Purpose of this database
Figure out what songs users are listening to on the music streaming app.

## How to run the Python scripts
```
    cd /path/to/etl.py
    python(3) etl.py

```

## Files in the repository
- data: this folder is holding all the datasets
- test.ipynb:  queries for data validation.
- create_tables.py: setup and teardown schemas, run this file to reset tables before each time run ETL scripts.
- etl.ipynb reads: and processes a single file from song_data and log_data and loads the data into tables.
- etl.py reads and processes files from song_data and log_data and loads them into tables.
- sql_queries.py contains all sql queries, and is imported into the last three files above.

## State and justify the database schema design and ETL pipeline.
- Star schema optimized for queries on song play analysis. This includes the following tables:
    - Fact Table
        - songplays - records in log data associated with song plays i.e. records with page NextSong:
            songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    - Dimension Tables
        - users - users in the app: user_id, first_name, last_name, gender, level
        - songs - songs in music database: song_id, title, artist_id, year, duration
        - artists - artists in music database: artist_id, name, location, latitude, longitude
        - time - timestamps of records in songplays broken down into specific units: start_time, hour,
             day, week, month, year, weekday

## Example queries and results for song play analysis.
```sql
SELECT u.user_id, u.first_name, u.last_name, s.title
FROM songs s
LEFT JOIN songplays sp USING (song_id)
LEFT JOIN users u USING (user_id);
```