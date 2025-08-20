#Objective: to use data from player_seasons and convert it to SCD2 type data

SELECT * FROM player_seasons;

# Some prep from lab1

#Recreating players table
CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scoring_class scoring_class,
     years_since_last_active INTEGER,
     is_active BOOLEAN,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );

#Pipeline query to insert data
 WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = 2003

), this_season AS (
     SELECT * FROM player_seasons
    WHERE season = 2004
)
INSERT INTO players
SELECT
        COALESCE(ls.player_name, ts.player_name) as player_name,
        COALESCE(ls.height, ts.height) as height,
        COALESCE(ls.college, ts.college) as college,
        COALESCE(ls.country, ts.country) as country,
        COALESCE(ls.draft_year, ts.draft_year) as draft_year,
        COALESCE(ls.draft_round, ts.draft_round) as draft_round,
        COALESCE(ls.draft_number, ts.draft_number)
            as draft_number,
        COALESCE(ls.seasons,
            ARRAY[]::season_stats[]
            ) || CASE WHEN ts.season IS NOT NULL THEN
                ARRAY[ROW(
                ts.season,
                ts.pts,
                ts.ast,
                ts.reb, ts.weight)::season_stats]
                ELSE ARRAY[]::season_stats[] END
            as seasons,
         CASE
             WHEN ts.season IS NOT NULL THEN
                 (CASE WHEN ts.pts > 20 THEN 'star'
                    WHEN ts.pts > 15 THEN 'good'
                    WHEN ts.pts > 10 THEN 'average'
                    ELSE 'bad' END)::scoring_class
             ELSE ls.scoring_class
         END as scoring_class,
         CASE WHEN ts.SEASON IS NOT NULL THEN 0
        ELSE COALESCE(ls.years_since_last_active,0)+1
        END AS years_since_last_season,
        ts.season IS NOT NULL as is_active,
        COALESCE(ts.SEASON, ls.CURRENT_SEASON+1) AS CURRENT_SEASON

    FROM last_season ls
    FULL OUTER JOIN this_season ts
    ON ls.player_name = ts.player_name

DELETE FROM players;
select * from players;


#Creating players_scd
SELECT
player_NAME,
scoring_class,
is_active,
current_season
from players
where current_season = 2002

DROP TABLE players_scd;
CREATE TABLE PLAYERS_SCD(
    PLAYER_NAME TEXT,
    SCORING_CLASS scoring_class,
    IS_ACTIVE BOOLEAN,
    START_SEASON INTEGER,
    END_SEASON INTEGER,
    CURRENT_SEASON INTEGER,
    PRIMARY KEY(PLAYER_NAME, START_SEASON)
)



#insert statment for scd table
WITH PREV_DIM AS(
SELECT
    current_season,
    PLAYER_NAME,
    SCORING_CLASS,
    IS_ACTIVE,
    LAG(scoring_class,1) OVER (PARTITION BY PLAYER_NAME ORDER BY current_season) AS PREV_SCORING_CLASS,
    LAG(IS_ACTIVE,1) OVER (PARTITION BY PLAYER_NAME ORDER BY CURRENT_SEASON) AS PREV_IS_ACTIVE
FROM players
),
INDICATORS AS(
SELECT *,
CASE
WHEN PREV_SCORING_CLASS <> scoring_class THEN 1 
    WHEN PREV_IS_ACTIVE <> is_active THEN 1 
    ELSE 0  
END AS CHANGE_INDICATOR
FROM PREV_DIM
),
STREAKS AS (
SELECT *,
SUM(CHANGE_INDICATOR) OVER (PARTITION BY player_name ORDER BY CURRENT_SEASON) AS STREAK_INDICATOR
FROM INDICATORS
)
--INSERT INTO players_scd
SELECT 
PLAYER_NAME,
scoring_class,
is_active,
MIN(current_season) AS START_SEASON,
MAX(current_season) AS END_SEASON
FROM STREAKS
GROUP BY PLAYER_NAME, STREAK_INDICATOR, scoring_class, is_active
ORDER BY PLAYER_NAME


select * from players_SCD;



# Incremental SCD query for loading in yearly increments
CREATE TYPE scd_type AS (
                    scoring_class scoring_class,
                    is_active boolean,
                    start_season INTEGER,
                    end_season INTEGER
                        )


WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),
     historical_scd AS (
        SELECT
            player_name,
               scoring_class,
               is_active,
               start_season,
               end_season
        FROM players_scd
        WHERE current_season = 2021
        AND end_season < 2021
     ),
     this_season_data AS (
         SELECT * FROM players
         WHERE current_season = 2022
     ),
     unchanged_records AS (
         SELECT
                ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ls.start_season,
                ts.current_season as end_season
        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE ts.scoring_class = ls.scoring_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.player_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season

                        )::scd_type,
                    ROW(
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE (ts.scoring_class <> ls.scoring_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT player_name,
                (records::scd_type).scoring_class,
                (records::scd_type).is_active,
                (records::scd_type).start_season,
                (records::scd_type).end_season
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season AS start_season,
                ts.current_season AS end_season
         FROM this_season_data ts
         LEFT JOIN last_season_scd ls
             ON ts.player_name = ls.player_name
         WHERE ls.player_name IS NULL

     )


SELECT *, 2022 AS current_season FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a



 



