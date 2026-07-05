CREATE DATABASE spotify_db;

USE DATABASE spotify_db;

CREATE SCHEMA pipe;
CREATE SCHEMA staging;

CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN =
'arn:aws:iam::926554299321:role/spotify-spark-snowflake-role'
STORAGE_ALLOWED_LOCATIONS =
('s3://spotify-etl-project-duc')
COMMENT = 'Connection to Spotify S3 bucket';

DESC INTEGRATION s3_int;

CREATE OR REPLACE FILE FORMAT parquet_fileformat
TYPE = PARQUET;

CREATE OR REPLACE STAGE spotify_stage
URL = 's3://spotify-etl-project-duc/transformed_data/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = parquet_fileformat;

LIST @spotify_stage;

-- Create staging tables
CREATE OR REPLACE TABLE staging.stg_songs (
    song_id STRING,
    song_name STRING,
    song_duration_ms NUMBER,
    song_url STRING,
    explicit BOOLEAN,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);

CREATE OR REPLACE TABLE staging.stg_artists (
    artist_id STRING,
    artist_name STRING,
    external_url STRING
);

CREATE OR REPLACE TABLE staging.stg_albums (
    album_id STRING,
    album_name STRING,
    release_date DATE,
    total_tracks NUMBER,
    url STRING
);

-- Create final tables
CREATE OR REPLACE TABLE public.tbl_songs (
    song_id STRING,
    song_name STRING,
    song_duration_ms NUMBER,
    song_url STRING,
    explicit BOOLEAN,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);

CREATE OR REPLACE TABLE public.tbl_artists (
    artist_id STRING,
    artist_name STRING,
    external_url STRING
);

CREATE OR REPLACE TABLE public.tbl_albums (
    album_id STRING,
    album_name STRING,
    release_date DATE,
    total_tracks NUMBER,
    url STRING
);

-- Create Snowpipes
CREATE OR REPLACE PIPE pipe.songs_pipe
AUTO_INGEST = TRUE
AS
COPY INTO staging.stg_songs
FROM @spotify_db.public.spotify_stage/songs
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE PIPE pipe.artist_pipe
AUTO_INGEST = TRUE
AS
COPY INTO staging.stg_artists
FROM @spotify_db.public.spotify_stage/artist
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE PIPE pipe.album_pipe
AUTO_INGEST = TRUE
AS
COPY INTO staging.stg_albums
FROM @spotify_db.public.spotify_stage/album
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

DESC PIPE pipe.songs_pipe;


SELECT COUNT(*) FROM staging.stg_songs;
SELECT * from public.tbl_albums;

ALTER PIPE pipe.songs_pipe REFRESH;
ALTER PIPE pipe.album_pipe REFRESH;
ALTER PIPE pipe.artist_pipe REFRESH;


SELECT COUNT(*) FROM staging.stg_songs;
SELECT COUNT(*) FROM staging.stg_albums;
SELECT COUNT(*) FROM staging.stg_artists;

SELECT SYSTEM$PIPE_STATUS('pipe.songs_pipe');

-- Create the streams
CREATE OR REPLACE STREAM staging.stg_songs_stream
ON TABLE staging.stg_songs
SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM staging.stg_artists_stream
ON TABLE staging.stg_artists
SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM staging.stg_albums_stream
ON TABLE staging.stg_albums
SHOW_INITIAL_ROWS = TRUE;

SELECT COUNT(*)
FROM staging.stg_songs_stream;

-- Create Tasks
CREATE OR REPLACE TASK merge_songs_task
WAREHOUSE = COMPUTE_WH
WHEN SYSTEM$STREAM_HAS_DATA('staging.stg_songs_stream')
AS
MERGE INTO public.tbl_songs t
USING (
    SELECT *
    FROM staging.stg_songs_stream
) s
ON t.song_id = s.song_id

WHEN MATCHED THEN
UPDATE SET
    t.song_name = s.song_name,
    t.song_duration_ms = s.song_duration_ms,
    t.song_url = s.song_url,
    t.explicit = s.explicit,
    t.song_added = s.song_added,
    t.album_id = s.album_id,
    t.artist_id = s.artist_id

WHEN NOT MATCHED THEN
INSERT (
    song_id,
    song_name,
    song_duration_ms,
    song_url,
    explicit,
    song_added,
    album_id,
    artist_id
)
VALUES (
    s.song_id,
    s.song_name,
    s.song_duration_ms,
    s.song_url,
    s.explicit,
    s.song_added,
    s.album_id,
    s.artist_id
);

CREATE OR REPLACE TASK merge_artists_task
WAREHOUSE = COMPUTE_WH
WHEN SYSTEM$STREAM_HAS_DATA('staging.stg_artists_stream')
AS
MERGE INTO public.tbl_artists t
USING (
    SELECT *
    FROM staging.stg_artists_stream
) s
ON t.artist_id = s.artist_id

WHEN MATCHED THEN
UPDATE SET
    t.artist_name = s.artist_name,
    t.external_url = s.external_url

WHEN NOT MATCHED THEN
INSERT (
    artist_id,
    artist_name,
    external_url
)
VALUES (
    s.artist_id,
    s.artist_name,
    s.external_url
);

CREATE OR REPLACE TASK merge_albums_task
WAREHOUSE = COMPUTE_WH
WHEN SYSTEM$STREAM_HAS_DATA('staging.stg_albums_stream')
AS
MERGE INTO public.tbl_albums t
USING (
    SELECT *
    FROM staging.stg_albums_stream
) s
ON t.album_id = s.album_id

WHEN MATCHED THEN
UPDATE SET
    t.album_name = s.album_name,
    t.release_date = s.release_date,
    t.total_tracks = s.total_tracks,
    t.url = s.url

WHEN NOT MATCHED THEN
INSERT (
    album_id,
    album_name,
    release_date,
    total_tracks,
    url
)
VALUES (
    s.album_id,
    s.album_name,
    s.release_date,
    s.total_tracks,
    s.url
);

-- Enable the Tasks
ALTER TASK merge_songs_task RESUME;
ALTER TASK merge_artists_task RESUME;
ALTER TASK merge_albums_task RESUME;


SELECT * FROM public.tbl_songs;
SELECT COUNT(*) FROM public.tbl_artists;
SELECT COUNT(*) FROM public.tbl_albums;

SELECT * FROM staging.stg_songs;
SELECT COUNT(*)
FROM staging.stg_songs_stream;

-- TEST
TRUNCATE TABLE staging.stg_songs;
TRUNCATE TABLE staging.stg_artists;
TRUNCATE TABLE staging.stg_albums;

TRUNCATE TABLE public.tbl_songs;
TRUNCATE TABLE public.tbl_artists;
TRUNCATE TABLE public.tbl_albums;

ALTER TASK merge_songs_task SUSPEND;
ALTER TASK merge_artists_task SUSPEND;
ALTER TASK merge_albums_task SUSPEND;

DROP STREAM IF EXISTS staging.stg_songs_stream;
DROP STREAM IF EXISTS staging.stg_artists_stream;
DROP STREAM IF EXISTS staging.stg_albums_stream;