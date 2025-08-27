--DDL for actors table: Create a DDL for an actors table with the following fields:
--films: An array of struct with the following fields:
----film: The name of the film.
----votes: The number of votes the film received.
----rating: The rating of the film.
----filmid: A unique identifier for each film.
--quality_class: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
----star: Average rating > 8.
----good: Average rating > 7 and ≤ 8.
----average: Average rating > 6 and ≤ 7.
----bad: Average rating ≤ 6.
--is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
SELECT * FROM ACTOR_FILMS;

SELECT MIN(YEAR),  MAX(YEAR) FROM actor_films;
--1970 -> 2021



CREATE TYPE FILMS AS (
    FILM TEXT,
    VOTES INTEGER,
    RATING REAL,
    FILMID TEXT
);


DROP TYPE QUALITY_CLASS;
CREATE TYPE QUALITY_CLASS AS ENUM(
    'STAR',
    'GOOD',
    'AVERAGE',
    'BAD'
);

CREATE TABLE ACTORS(
    ACTOR TEXT,
    ACTOR_ID TEXT,
    FILMS FILMS[],
    QUALITY_CLASS QUALITY_CLASS,
    IS_ACTIVE BOOLEAN,
    CURRENT_YEAR INTEGER,
    PRIMARY KEY(ACTOR_ID, CURRENT_YEAR)
);

DROP TABLE ACTORS;









--Cumulative table generation query: Write a query that populates the actors table one year at a time.
WITH PREVIOUS_YEAR AS(
    SELECT * FROM ACTORS
    WHERE CURRENT_YEAR = 1990
),
THIS_YEAR AS(
    SELECT 
        ACTOR,
        ACTORID,
        YEAR,
        ARRAY_AGG(
            ROW(FILM, VOTES, RATING, FILMID)::FILMS
        ) AS FILMS,
        AVG(RATING) AS AVG_RATING
    FROM actor_films
    WHERE YEAR = 1991
    GROUP BY ACTOR, ACTORID, YEAR
)
INSERT INTO ACTORS
SELECT 
    COALESCE(PY.ACTOR, TY.ACTOR) AS ACTOR,
    COALESCE(PY.ACTOR_ID, TY.ACTORID) AS ACTOR_ID,
    CASE 
    WHEN PY.FILMS IS NULL AND TY.YEAR IS NOT NULL
    THEN TY.FILMS
    WHEN PY.FILMS IS NOT NULL AND TY.YEAR IS NOT NULL
    THEN
    PY.FILMS || TY.FILMS
    ELSE PY.FILMS
    END AS FILMS,
    CASE WHEN TY.AVG_RATING IS NOT NULL THEN
        CASE WHEN TY.AVG_RATING > 8 THEN 'STAR'
            WHEN TY.AVG_RATING > 7 THEN 'GOOD'
            WHEN TY.AVG_RATING > 6 THEN 'AVERAGE'
            ELSE 'BAD'
        END::QUALITY_CLASS
    ELSE PY.QUALITY_CLASS 
    END::QUALITY_CLASS AS QUALITY_CLASS,
    CASE 
        WHEN TY.YEAR IS NULL THEN FALSE 
        ELSE TRUE 
    END AS IS_ACTIVE,
    COALESCE(TY.YEAR, PY.CURRENT_YEAR+1) AS CURRENT_YEAR
FROM PREVIOUS_YEAR PY FULL OUTER JOIN THIS_YEAR TY
ON PY.ACTOR_ID = TY.ACTORID

SELECT * FROM ACTORS
WHERE CURRENT_YEAR =1990;







--DDL for actors_history_scd table: Create a DDL for an actors_history_scd table with the following features:
----Implements type 2 dimension modeling (i.e., includes start_date and end_date fields).
----Tracks quality_class and is_active status for each actor in the actors table.
SELECT 
ACTOR_ID,
ACTOR,
QUALITY_CLASS,
IS_ACTIVE,
CURRENT_YEAR
FROM actors
WHERE CURRENT_YEAR = 1990

DROP TABLE actors_history_scd;
CREATE TABLE ACTORS_HISTORY_SCD(
    ACTOR_ID TEXT,
    ACTOR TEXT,
    QUALITY_CLASS QUALITY_CLASS,
    IS_ACTIVE BOOLEAN,
    START_YEAR INTEGER,
    END_YEAR INTEGER,
    CURRENT_YEAR INTEGER,
    PRIMARY KEY(ACTOR_ID, START_YEAR)
)







--Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
WITH PREV_DIM AS (
    SELECT
    CURRENT_YEAR,
    ACTOR_ID,
    ACTOR,
    QUALITY_CLASS,
    LAG(QUALITY_CLASS,1) OVER(PARTITION BY ACTOR_ID ORDER BY CURRENT_YEAR) AS PREV_QUALITY_CLASS,
    IS_ACTIVE,
    LAG(is_active,1) OVER(PARTITION BY ACTOR_ID ORDER BY CURRENT_YEAR) AS PREV_IS_ACTIVE
FROM actors
),
INDICATORS AS(
SELECT 
*,
CASE 
    WHEN PREV_QUALITY_CLASS <> QUALITY_CLASS THEN 1
    WHEN PREV_IS_ACTIVE <> IS_ACTIVE THEN 1  
    ELSE 0
END AS CHANGE_INDICATOR
FROM PREV_DIM
),
STREAKS AS (
SELECT 
*,
SUM(CHANGE_INDICATOR) OVER(PARTITION BY ACTOR_ID ORDER BY CURRENT_YEAR) AS STREAK_INDICATOR
FROM INDICATORS
)
INSERT INTO ACTORS_HISTORY_SCD
SELECT 
ACTOR_ID,
ACTOR,
QUALITY_CLASS,
IS_ACTIVE,
MIN(CURRENT_YEAR) AS START_YEAR,
MAX(CURRENT_YEAR) AS END_YEAR,
1990 AS CURRENT_YEAR
FROM STREAKS
GROUP BY ACTOR_ID, ACTOR, STREAK_INDICATOR, QUALITY_CLASS, IS_ACTIVE
ORDER BY ACTOR_ID;

DELETE FROM actors_history_scd;
SELECT * FROM actors_history_scd;









--Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.
CREATE TYPE ACTOR_SCD_TYPE AS (
    QUALITY_CLASS QUALITY_CLASS,
    IS_ACTIVE BOOLEAN,
    START_YEAR INTEGER,
    END_YEAR INTEGER
);

WITH LAST_YEAR_SCD AS(
    SELECT * FROM actors_history_scd
    WHERE current_year = 1990
    AND END_YEAR = 1990
),
HISTORICAL_SCD AS (
    SELECT
        ACTOR_ID,
        ACTOR,
        QUALITY_CLASS,
        IS_ACTIVE,
        START_YEAR,
        END_YEAR
    FROM actors_history_scd
    WHERE CURRENT_YEAR = 1990
    AND END_YEAR <1990
),
THIS_YEAR_DATA AS(
    SELECT * FROM actors
    WHERE CURRENT_YEAR = 1991
),
UNCHANGED_RECORDS AS (
    SELECT 
        TY.ACTOR_ID,
        TY.ACTOR,
        TY.QUALITY_CLASS,
        TY.IS_ACTIVE,
        LY.START_YEAR,
        TY.CURRENT_YEAR AS END_YEAR
    FROM THIS_YEAR_DATA TY
    JOIN LAST_YEAR_SCD LY
    ON TY.ACTOR_ID = LY.actor_id
    WHERE TY.QUALITY_CLASS = LY.quality_class
    AND TY.IS_ACTIVE = LY.is_active
),
CHANGED_RECORDS AS (
    SELECT
        TY.ACTOR_ID,
        TY.ACTOR,
        UNNEST(
            ARRAY[
                ROW(
                    LY.QUALITY_CLASS,
                    LY.IS_ACTIVE,
                    LY.START_YEAR,
                    LY.END_YEAR
                )::ACTOR_SCD_TYPE,
                ROW(
                    TY.QUALITY_CLASS,
                    TY.IS_ACTIVE,
                    TY.CURRENT_YEAR,
                    TY.CURRENT_YEAR
                )::ACTOR_SCD_TYPE
            ]
        ) AS RECORDS
    FROM THIS_YEAR_DATA TY
    LEFT JOIN LAST_YEAR_SCD LY
    ON TY.ACTOR_ID = LY.actor_id
    WHERE(TY.QUALITY_CLASS <> LY.QUALITY_CLASS
    OR TY.IS_ACTIVE <> LY.IS_ACTIVE)
),
UNNESTED_CHANGED_RECORDS AS (
    SELECT 
        ACTOR_ID,
        ACTOR,
        (RECORDS::ACTOR_SCD_TYPE).QUALITY_CLASS,
        (RECORDS::ACTOR_SCD_TYPE).IS_ACTIVE,
        (RECORDS::ACTOR_SCD_TYPE).START_YEAR,
        (RECORDS::ACTOR_SCD_TYPE).END_YEAR
    FROM CHANGED_RECORDS
),
NEW_RECORDS AS (
    SELECT
        TY.ACTOR_ID,
        TY.ACTOR,
        TY.QUALITY_CLASS,
        TY.IS_ACTIVE,
        TY.CURRENT_YEAR AS START_YEAR,
        TY.CURRENT_YEAR AS END_YEAR
    FROM THIS_YEAR_DATA TY
    LEFT JOIN LAST_YEAR_SCD LY
    ON TY.ACTOR_ID = LY.actor_id
    WHERE LY.ACTOR_ID IS NULL
)
--INSERT INTO actors_history_scd
SELECT 
    *, 
    1991 AS CURRENT_SEASON
FROM(
    --SELECT * FROM HISTORICAL_SCD
    --UNION ALL
    SELECT * FROM UNCHANGED_RECORDS
    UNION ALL
    SELECT * FROM UNNESTED_CHANGED_RECORDS
    UNION ALL
    SELECT * FROM NEW_RECORDS
 ) TT;

