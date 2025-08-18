#Objective: to compress player_seasons table
SELECT * FROM player_seasons;

SELECT COUNT(*) FROM PLAYER_SEASONS;
--12869

SELECT * FROM player_seasons
WHERE player_name = 'Michael Jordan';


#creating new struct data type
DROP TABLE players;
DROP TYPE IF EXISTS SEASON_STATS;
CREATE TYPE SEASON_STATS AS (
    SEASON INTEGER,
    GP REAL,
    PTS REAL,
    AST REAL,
    REB REAL);



#cREATING ENUM TYPE SCORING CLASS
CREATE TYPE SCORING_CLASS AS ENUM(
    'STAR',
    'GOOD',
    'AVERAGE',
    'BAD'
);



#CRreating new table players with the compressed data type
CREATE TABLE PLAYERS(
    PLAYER_NAME TEXT,
    HEIGHT TEXT,
    COLLEGE TEXT,
    COUNTRY TEXT,
    DRAFT_YEAR TEXT,
    DRAFT_ROUND TEXT,
    DRAFT_NUMBER TEXT,
    SEASON_STATS SEASON_STATS[],
    SCORING_CLASS SCORING_CLASS,
    YEARS_SINCE_LAST_SEASON INTEGER,
    CURRENT_SEASON INTEGER,
PRIMARY KEY(PLAYER_NAME, CURRENT_SEASON)
);






# Initial seed query to insert records here
SELECT MIN(SEASON) FROM player_seasons;
--1996

SELECT MAX(SEASON) FROM player_seasons;
--2022

WITH YESTERDAY AS (
    SELECT * FROM players
    WHERE CURRENT_SEASON = 1995
),
TODAY AS (
    SELECT * FROM player_seasons
    WHERE season = 1996
)
INSERT INTO players 
SELECT
    COALESCE(YT.player_name, TT.player_name) AS player_name,
    COALESCE(YT.height, TT.height) AS height,
    COALESCE(YT.college, TT.college) AS college,
    COALESCE(YT.country, TT.country) AS country,
    COALESCE(YT.draft_year, TT.draft_year) AS draft_year,
    COALESCE(YT.draft_round, TT.draft_round) AS draft_round,
    COALESCE(YT.draft_number, TT.draft_number) AS draft_number,
    CASE 
    WHEN YT.SEASON_STATS IS NULL AND TT.SEASON IS NOT NULL 
    THEN 
    ARRAY[
        ROW(TT.SEASON, TT.GP, TT.PTS, TT.REB, TT.AST)::season_stats
    ]
    WHEN YT.SEASON_STATS IS NOT NULL AND TT.SEASON IS NOT NULL THEN
    YT.SEASON_STATS || 
    ARRAY[
        ROW(TT.SEASON, TT.GP, TT.PTS, TT.REB, TT.AST)::season_stats
    ]
    ELSE YT.SEASON_STATS
    END AS SEASON_STATS,
    CASE WHEN TT.SEASON IS NOT NULL THEN
    CASE WHEN TT.PTS > 20 THEN 'star'
         WHEN TT.PTS > 15 THEN 'good'
         WHEN TT.PTS > 10 THEN 'average'
            ELSE 'bad'
        END::SCORING_CLASS
    ELSE YT.SCORING_CLASS
    END AS scoring_class,
    CASE WHEN TT.SEASON IS NOT NULL THEN 0
    ELSE COALESCE(YT.years_since_last_season,0)+1
    END AS years_since_last_season,
    COALESCE(TT.SEASON, YT.CURRENT_SEASON+1) AS CURRENT_SEASON
FROM
YESTERDAY YT FULL OUTER JOIN TODAY TT 
ON YT.PLAYER_NAME = TT.player_name


SELECT * FROM PLAYERS;



#Pipeline query to insert data there on
WITH YESTERDAY AS (
    SELECT * FROM players
    WHERE CURRENT_SEASON = 2002
),
TODAY AS (
    SELECT * FROM player_seasons
    WHERE season = 2003
)
INSERT INTO PLAYERS
SELECT
    COALESCE(YT.player_name, TT.player_name) AS player_name,
    COALESCE(YT.height, TT.height) AS height,
    COALESCE(YT.college, TT.college) AS college,
    COALESCE(YT.country, TT.country) AS country,
    COALESCE(YT.draft_year, TT.draft_year) AS draft_year,
    COALESCE(YT.draft_round, TT.draft_round) AS draft_round,
    COALESCE(YT.draft_number, TT.draft_number) AS draft_number,
    CASE 
    WHEN YT.SEASON_STATS IS NULL AND TT.SEASON IS NOT NULL 
    THEN 
    ARRAY[
        ROW(TT.SEASON, TT.GP, TT.PTS, TT.REB, TT.AST)::season_stats
    ]
    WHEN YT.SEASON_STATS IS NOT NULL AND TT.SEASON IS NOT NULL THEN
    YT.SEASON_STATS || 
    ARRAY[
        ROW(TT.SEASON, TT.GP, TT.PTS, TT.REB, TT.AST)::season_stats
    ]
    ELSE YT.SEASON_STATS
    END AS SEASON_STATS,
    CASE WHEN TT.SEASON IS NOT NULL THEN
    CASE WHEN TT.PTS > 20 THEN 'star'
         WHEN TT.PTS > 15 THEN 'good'
         WHEN TT.PTS > 10 THEN 'average'
            ELSE 'bad'
        END::SCORING_CLASS
    ELSE YT.SCORING_CLASS
    END AS scoring_class,
    CASE WHEN TT.SEASON IS NOT NULL THEN 0
    ELSE COALESCE(YT.years_since_last_season,0)+1
    END AS years_since_last_season,
    COALESCE(TT.SEASON, YT.CURRENT_SEASON+1) AS CURRENT_SEASON
FROM
YESTERDAY YT FULL OUTER JOIN TODAY TT 
ON YT.PLAYER_NAME = TT.player_name;


#Unnest query to flatten array structure
SELECT * FROM PLAYERS, UNNEST(season_stats) AS STATS
WHERE player_name = 'Michael Jordan'
AND CURRENT_SEASON = 2000;






