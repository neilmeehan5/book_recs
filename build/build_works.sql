DROP TABLE IF EXISTS BUILD_WORKS;

CREATE TABLE BUILD_WORKS AS
WITH A AS (
    SELECT
        WORK_ID,
        TITLE,
        RATINGS_COUNT,
        MAX(RATINGS_COUNT) OVER(PARTITION BY WORK_ID) AS MAX_RATINGS
    FROM GOODREADS_BOOKS
),
B AS (
    SELECT DISTINCT
        WORK_ID,
        UPPER(TITLE) AS TITLE
    FROM A
    WHERE RATINGS_COUNT = MAX_RATINGS
),
C AS (
    SELECT
        WORK_ID,
        SUM(RATINGS_COUNT * AVERAGE_RATING) / SUM(RATINGS_COUNT) AS AVERAGE_RATING,
        SUM(RATINGS_COUNT) AS RATINGS_COUNT,
        SUM(TEXT_REVIEWS_COUNT) AS TEXT_REVIEWS_COUNT 
    FROM GOODREADS_BOOKS
    WHERE RATINGS_COUNT > 0
    GROUP BY WORK_ID
)
SELECT *
FROM C
INNER JOIN B
USING(WORK_ID);

ALTER TABLE BUILD_WORKS 
ADD PRIMARY KEY (WORK_ID, TITLE);