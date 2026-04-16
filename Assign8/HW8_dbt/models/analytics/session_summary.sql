WITH usc AS (
    SELECT * FROM {{ ref('user_session_channel') }}
),
st AS (
    SELECT * FROM {{ ref('session_timestamp') }}
)
SELECT 
    usc.userId,
    usc.sessionId,
    usc.channel,
    st.ts
FROM usc
JOIN st ON usc.sessionId = st.sessionId