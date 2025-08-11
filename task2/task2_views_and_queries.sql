-- Views
CREATE OR REPLACE VIEW `ae1-eng.imdb_dw.v_titles_with_ratings` AS
SELECT b.title_id,b.title,b.start_year,b.type,b.genres,r.average_rating,r.num_votes
FROM `ae1-eng.imdb_dw.title_basics` b
JOIN `ae1-eng.imdb_dw.title_ratings` r ON b.title_id=r.title_id;

CREATE OR REPLACE VIEW `ae1-eng.imdb_dw.v_title_cast_ratings` AS
SELECT v.title_id,v.title,v.start_year,v.average_rating,v.num_votes,p.role_category,n.name AS person_name
FROM `ae1-eng.imdb_dw.v_titles_with_ratings` v
JOIN `ae1-eng.imdb_dw.title_principals` p ON v.title_id=p.title_id
JOIN `ae1-eng.imdb_dw.name_basics` n ON p.name_id=n.name_id;

CREATE OR REPLACE VIEW `ae1-eng.imdb_dw.v_nasa_flares_parsed` AS
WITH src AS (
  SELECT flrID,classType,sourceLocation,beginTime,peakTime,endTime
  FROM `ae1-eng.imdb_dw.nasa_solar_flares_2023`
)
SELECT
  flrID,classType,sourceLocation,
  CASE WHEN REGEXP_CONTAINS(beginTime,r":\d{2}:\d{2}Z$") THEN SAFE.PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%Ez",beginTime)
       WHEN REGEXP_CONTAINS(beginTime,r":\d{2}Z$")       THEN SAFE.PARSE_TIMESTAMP("%Y-%m-%dT%H:%M%Ez",beginTime)
       ELSE NULL END AS begin_ts,
  CASE WHEN REGEXP_CONTAINS(peakTime,r":\d{2}:\d{2}Z$") THEN SAFE.PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%Ez",peakTime)
       WHEN REGEXP_CONTAINS(peakTime,r":\d{2}Z$")       THEN SAFE.PARSE_TIMESTAMP("%Y-%m-%dT%H:%M%Ez",peakTime)
       ELSE NULL END AS peak_ts,
  CASE WHEN REGEXP_CONTAINS(endTime,r":\d{2}:\d{2}Z$")  THEN SAFE.PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%S%Ez",endTime)
       WHEN REGEXP_CONTAINS(endTime,r":\d{2}Z$")        THEN SAFE.PARSE_TIMESTAMP("%Y-%m-%dT%H:%M%Ez",endTime)
       ELSE NULL END AS end_ts
FROM src;

-- Queries (screenshots)
SELECT title,start_year,average_rating,num_votes
FROM `ae1-eng.imdb_dw.v_titles_with_ratings`
WHERE num_votes>=1000
ORDER BY average_rating DESC,num_votes DESC
LIMIT 20;

SELECT title,start_year,average_rating,person_name,role_category
FROM `ae1-eng.imdb_dw.v_title_cast_ratings`
WHERE num_votes>=1000
ORDER BY average_rating DESC
LIMIT 30;

SELECT flrID,classType,begin_ts
FROM `ae1-eng.imdb_dw.v_nasa_flares_parsed`
WHERE classType LIKE "X%"
ORDER BY begin_ts DESC
LIMIT 20;
