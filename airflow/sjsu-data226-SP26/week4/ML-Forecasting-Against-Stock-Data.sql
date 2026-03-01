CREATE DATABASE USER_DB_RACCOON;  -- CHANGE THIS ACCORDING TO YOUR DATABASE
CREATE SCHEMA raw;
CREATE SCHEMA analytics;

-- Create a table
CREATE TABLE IF NOT EXISTS MARKET_DATA (
    DATE DATE NOT NULL,
    OPEN FLOAT NOT NULL,
    HIGH FLOAT NOT NULL,
    LOW FLOAT NOT NULL,
    CLOSE FLOAT NOT NULL,
    VOLUME BIGINT NOT NULL,
    SYMBOL VARCHAR(10) NOT NULL,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1              -- 헤더 라인 건너뛰기
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    DATE_FORMAT = 'YYYY-MM-DD';

CREATE OR REPLACE STAGE tesla_stage
    FILE_FORMAT = csv_format;

-- Upload the file to the stage using the UI
-- The file is on CANVAS

-- Check the file under the stage
LIST @tesla_stage;

COPY INTO MARKET_DATA (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, SYMBOL)
FROM @tesla_stage
FILE_FORMAT = csv_format;

SELECT 
    MIN(DATE) as START_DATE,
    MAX(DATE) as END_DATE,
    COUNT(*) as RECORD_COUNT
FROM MARKET_DATA 
WHERE SYMBOL = 'TSLA';

-- Inspect the first 10 rows of your training data. This is the data we'll use to create your model.
select * from MARKET_DATA limit 10;

-- Prepare your training data. Timestamp_ntz is a required format. Also, only include select columns.
CREATE VIEW MARKET_DATA_v1 AS SELECT
    to_timestamp_ntz(DATE) as DATE_v1,
    CLOSE,
    SYMBOL
FROM MARKET_DATA;

-- Create your model.
CREATE SNOWFLAKE.ML.FORECAST stock_price(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'MARKET_DATA_v1'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE_v1',
    TARGET_COLNAME => 'CLOSE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Generate predictions and store the results to a table.
BEGIN
    -- This is the step that creates your predictions.
    CALL stock_price!FORECAST(
        FORECASTING_PERIODS => 14,
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE TABLE My_forecasts_2025_09_16 AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View your predictions.
SELECT * FROM My_forecasts_2025_09_16;

-- Union your predictions with your historical data, then view the results in a chart.
SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM MARKET_DATA
UNION ALL
SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM My_forecasts_2025_09_16;
