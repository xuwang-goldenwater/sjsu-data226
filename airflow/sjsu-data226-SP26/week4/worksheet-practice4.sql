-- nullif & coalesce
CREATE OR REPLACE TABLE adhoc.count_test (
    value int
);

INSERT INTO adhoc.count_test VALUES 
(NULL), (1), (1), (0), (0), (4), (3);

SELECT 100/NULL;

SELECT value, 100/value
FROM adhoc.count_test;

SELECT value, 100/NULLIF(value, 0)
FROM adhoc.count_test;

SELECT
     value,
     NULLIF (value, 0),
     COALESCE(value, 0)
FROM adhoc.count_test;

SELECT value, COALESCE(value, 100)
FROM adhoc.count_test;

-- UNION, UNION ALL, EXCEPT, INTERSECT
SELECT 'mark' as first_name, 'zuckerberg' as last_name
UNION
SELECT 'elon', 'musk'
UNION
SELECT 'mark', 'zuckerberg';

SELECT 'mark' as first_name, 'zuckerberg' as last_name
UNION ALL
SELECT 'elon', 'musk'
UNION ALL
SELECT 'mark', 'zuckerberg';

SELECT sessionId FROM raw.user_session_channel
EXCEPT
SELECT sessionId FROM raw.session_timestamp;

SELECT sessionId FROM raw.user_session_channel
INTERSECT
SELECT sessionId FROM raw.session_timestamp;

-- ROW_NUMBER
SELECT usc.userid, usc.channel, st.ts, ROW_NUMBER() OVER (PARTITION BY usc.userid ORDER BY st.ts) nn
FROM raw.user_session_channel usc
JOIN raw.session_timestamp st ON usc.sessionid = st.sessionid
WHERE usc.userid = 27;

-- LAG function
SELECT usc.*, st.ts,
    LAG(channel,1) OVER (PARTITION BY userId ORDER BY ts) prev_channel
FROM raw.user_session_channel usc
JOIN raw.session_timestamp st ON usc.sessionid = st.sessionid
WHERE usc.userid = 27
ORDER BY usc.userid, st.ts;
