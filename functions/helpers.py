import pandas as pd

def search_author(con, text):
    '''Search for an author.'''

    temp = pd.read_sql(
        f"""
        SELECT *
        FROM BUILD_BOOK_AUTHORS
        WHERE AUTHOR_NAME ~ '{text.upper()}'
        ORDER BY RATINGS_COUNT DESC
        """,
        con
    )

    return temp

def search_title(con, text):
    '''Search for a book.'''

    temp = pd.read_sql(
        f"""
        SELECT 
            WORK_ID,
            TITLE,
            AUTHOR_NAME,
            BUILD_WORKS.AVERAGE_RATING AS AVERAGE_RATING,
            BUILD_WORKS.RATINGS_COUNT AS RATINGS_COUNT,
            BUILD_WORKS.TEXT_REVIEWS_COUNT AS TEXT_REVIEWS_COUNT
        FROM BUILD_WORKS
        INNER JOIN BUILD_AUTHORS
        USING(WORK_ID)
        INNER JOIN BUILD_BOOK_AUTHORS
        USING(AUTHOR_ID)
        WHERE TITLE ~ '{text.upper()}'
        ORDER BY RATINGS_COUNT DESC
        """,
        con
    )

    return temp

def get_author_books(con, author_id):
    '''Search for the books by a specific author.'''

    temp = pd.read_sql(
        f"""
        SELECT *
        FROM BUILD_AUTHORS
        INNER JOIN BUILD_WORKS
        USING(WORK_ID)
        WHERE AUTHOR_ID = {author_id}
        ORDER BY RATINGS_COUNT DESC
        """,
        con
    )

    return temp


def get_similar_works(con, work_ids):
    '''Get similar works.'''
    
    temp = pd.read_sql(
        f"""
        -- GET RELEVANT TAGS
        WITH A AS (
            SELECT
                TAG_NAME,
                CAST(TAG_COUNT AS FLOAT) / TAG_TOTAL_COUNT AS PERC1,
                --CAST(TAG_COUNT AS FLOAT) / WORK_TOTAL_COUNT AS PERC2,
                1 / CAST(TAG_NUM_WORK AS FLOAT) AS PERC3 
            FROM BUILD_TAGS
            WHERE WORK_ID IN ({', '.join(str(id) for id in work_ids)})
            ORDER BY CAST(TAG_COUNT AS FLOAT) / TAG_TOTAL_COUNT DESC
        ),
        -- GET TOTAL METRICS
        B AS (
            SELECT 
                TAG_NAME,
                1 AS KEY,
                COUNT(*) AS NUM_MATCH,
                SUM(PERC1) AS PERC1,
                --SUM(PERC2) AS PERC2,
                SUM(PERC3) AS PERC3
            FROM A 
            GROUP BY TAG_NAME
        ),
        -- FIND PERCENTILE
        C AS (
            SELECT
                1 AS KEY,
                PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY NUM_MATCH) AS MATCH_MEDIAN,
                PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY PERC1) AS PERC1_MEDIAN,
                PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY PERC3) AS PERC3_MEDIAN
            FROM B
            GROUP BY KEY
        ),
        -- SUBSET TO TOP HALF
        D AS (
            SELECT * 
            FROM C
            INNER JOIN B 
            USING(KEY)
            WHERE PERC1 > PERC1_MEDIAN
            AND PERC3 > PERC3_MEDIAN
            AND NUM_MATCH >= MATCH_MEDIAN
        ),
        -- GET TOP 100 WORKS THAT HAD THESE TAGS
        E AS (
            SELECT 
                WORK_ID,
                COUNT(*) AS NUM_OVERLAP
            FROM D
            INNER JOIN BUILD_TAGS
            USING(TAG_NAME)
            WHERE WORK_ID NOT IN ({', '.join(str(id) for id in work_ids)})
            GROUP BY WORK_ID
            ORDER BY NUM_OVERLAP DESC
            LIMIT 100
        ),
        -- FOR DEBUGGING
        F AS (
            SELECT 
                WORK_ID,
                TITLE,
                AUTHOR_NAME,
                BUILD_WORKS.AVERAGE_RATING AS AVERAGE_RATING,
                BUILD_WORKS.RATINGS_COUNT AS RATINGS_COUNT,
                BUILD_WORKS.TEXT_REVIEWS_COUNT AS TEXT_REVIEWS_COUNT,
                NUM_OVERLAP
            FROM E
            INNER JOIN BUILD_WORKS
            USING(WORK_ID)
            INNER JOIN BUILD_AUTHORS
            USING(WORK_ID)
            INNER JOIN BUILD_BOOK_AUTHORS
            USING(AUTHOR_ID)
            ORDER BY NUM_OVERLAP DESC
        )
        SELECT * 
        FROM F
        """,
        con
    )
    
    return temp


def get_tags(con, work_ids):
    '''Get relevant tags for a list.'''

    tags = pd.read_sql(
        f"""
        WITH A AS (
            SELECT
                TAG_NAME,
                CAST(TAG_COUNT AS FLOAT) / TAG_TOTAL_COUNT AS PERC1,
                --CAST(TAG_COUNT AS FLOAT) / WORK_TOTAL_COUNT AS PERC2,
                1 / CAST(TAG_NUM_WORK AS FLOAT) AS PERC3 
            FROM BUILD_TAGS
            WHERE WORK_ID IN ({', '.join(str(id) for id in work_ids)})
            ORDER BY CAST(TAG_COUNT AS FLOAT) / TAG_TOTAL_COUNT DESC
        ),
        -- GET TOTAL METRICS
        B AS (
            SELECT 
                TAG_NAME,
                1 AS KEY,
                COUNT(*) AS NUM_MATCH,
                SUM(PERC1) AS PERC1,
                --SUM(PERC2) AS PERC2,
                SUM(PERC3) AS PERC3
            FROM A 
            GROUP BY TAG_NAME
        ),
        -- FIND PERCENTILE
        C AS (
            SELECT
                1 AS KEY,
                PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY NUM_MATCH) AS MATCH_MEDIAN,
                PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY PERC1) AS PERC1_MEDIAN,
                PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY PERC3) AS PERC3_MEDIAN
            FROM B
            GROUP BY KEY
        )
        -- SUBSET TO TOP HALF
        SELECT DISTINCT TAG_NAME, PERC1
        FROM C
        INNER JOIN B 
        USING(KEY)
        WHERE PERC1 > PERC1_MEDIAN
        AND PERC3 > PERC3_MEDIAN
        AND NUM_MATCH >= MATCH_MEDIAN
        ORDER BY PERC1 DESC
        """,
        con
    )

    return tags['tag_name']
