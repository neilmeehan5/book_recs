DROP TABLE IF EXISTS BUILD_AUTHORS;

CREATE TABLE BUILD_AUTHORS AS
WITH A AS (
    SELECT 
        AUTHOR_ID,
        WORK_ID,
        COUNT(*) AS AUTHOR_COUNT
    FROM AUTHORS
    WHERE ROLE = ''
    GROUP BY AUTHOR_ID, WORK_ID
),
B AS (
    SELECT 
        AUTHOR_ID,
        WORK_ID,
        AUTHOR_COUNT,
        MAX(AUTHOR_COUNT) OVER(PARTITION BY WORK_ID) AS MAX_AUTHOR_COUNT
    FROM A
)
SELECT AUTHOR_ID, WORK_ID
FROM B
WHERE AUTHOR_COUNT = MAX_AUTHOR_COUNT;

ALTER TABLE BUILD_AUTHORS 
ADD PRIMARY KEY (WORK_ID, AUTHOR_ID);
