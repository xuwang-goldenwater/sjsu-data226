/*
CREATE DATABASE IF NOT EXISTS dev;

CREATE SCHEMA IF NOT EXISTS dev.raw;
CREATE SCHEMA IF NOT EXISTS dev.analytics;
*/

-- The following SQLs assume you have a database named "dev"
CREATE SCHEMA IF NOT EXISTS adhoc;
CREATE OR REPLACE TABLE adhoc.count_test (
    value int
);

ALTER TABLE adhoc.count_test RENAME COLUMN value to v;

INSERT INTO adhoc.count_test VALUES 
(NULL), (1), (1), (0), (0), (4), (3);

SELECT *
FROM adhoc.count_test;

ALTER TABLE adhoc.count_test RENAME COLUMN v to value;
ALTER TABLE adhoc.count_test RENAME COLUMN value to v;

-- if I want to update NULL value to 100 in every records
UPDATE adhoc.count_test
SET v = 100
WHERE v is NULL;

DELETE FROM adhoc.count_test WHERE v = 0;

-- DROP TABLE
DROP TABLE adhoc.count_test;
SHOW TABLES IN SCHEMA adhoc;

CREATE OR REPLACE TABLE adhoc.count_test (
    value int
);

INSERT INTO adhoc.count_test VALUES 
(NULL), (1), (1), (0), (0), (4), (3);

-- CASE WHEN
SELECT
    value,
    CASE 
        WHEN value > 0 THEN 'positive'
        WHEN value = 0 THEN 'zero'
        WHEN value < 0 THEN 'negative'
        ELSE 'null'
    END sign
FROM adhoc.count_test;

-- COUNT function
SELECT COUNT(1), COUNT(0), COUNT(NULL), COUNT(value), COUNT(DISTINCT value)
FROM adhoc.count_test;

-- NULL
SELECT COUNT(1)
FROM adhoc.count_test
WHERE value = NULL; -- WHERE value != NULL

SELECT COUNT(1)
FROM adhoc.count_test
WHERE value is NULL; -- WHERE value is not NULL

SELECT 0 + NULL, 0 - NULL, 0 * NULL, 0/NULL;

-- GROUP BY PREP
CREATE TABLE IF NOT EXISTS raw.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'  
);

CREATE TABLE IF NOT EXISTS raw.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp  
);

-- for the following query to run, 
-- the S3 bucket should have LIST/READ privileges for everyone
CREATE OR REPLACE STAGE raw.blob_stage
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');

COPY INTO raw.user_session_channel
FROM @raw.blob_stage/user_session_channel.csv;

COPY INTO raw.session_timestamp
FROM @raw.blob_stage/session_timestamp.csv;

SELECT *
FROM raw.session_timestamp
LIMIT 10;

-- GROUP BY 
SELECT channel, COUNT(1) AS cnt
FROM raw.user_session_channel
GROUP BY channel
ORDER BY cnt DESC;

SELECT channel, COUNT(1) AS cnt
FROM raw.user_session_channel
GROUP BY 1
ORDER BY 2 DESC;
