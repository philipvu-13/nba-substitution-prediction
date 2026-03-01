-- Query to get all of Anthony Edwards' Subs
SELECT
  "actionNumber",
  "period",
  "clock",
  "teamTricode",
  "description",
  CASE
    WHEN "description" ILIKE 'SUB:% FOR%Edwards%' THEN 'OUT'
    WHEN "description" ILIKE 'SUB: Edwards% FOR %' THEN 'IN'
    ELSE 'UNKNOWN'
  END AS edwards_sub_direction
FROM play_by_play_v3_raw
WHERE "gameId" = '0022500806'
  AND (
    "description" ILIKE 'SUB:%FOR%Edwards%'
    OR "description" ILIKE 'SUB:%Edwards%FOR%'
  )
ORDER BY "actionNumber";

-- =========================================================
-- PHASE 2 CHECKPOINT QUERIES (Anthony Edwards, 2025-26)
-- =========================================================

-- 1) Average stint length
SELECT
  AVG(s.stint_length_seconds) AS avg_stint_seconds,
  AVG(s.stint_length_seconds) / 60.0 AS avg_stint_minutes
FROM player_stints s
JOIN games g
  ON g.game_id = s.game_id
LEFT JOIN excluded_games eg
  ON eg.game_id = s.game_id
WHERE eg.game_id IS NULL
  AND (g.season = '2025-26' OR g.season = '22025');


-- 2) Average first stint in Q1 (first stint by sub_in_time per game)
WITH first_q1_stint AS (
  SELECT
    s.game_id,
    s.stint_length_seconds,
    ROW_NUMBER() OVER (
      PARTITION BY s.game_id
      ORDER BY s.sub_in_time ASC
    ) AS rn
  FROM player_stints s
  JOIN games g
    ON g.game_id = s.game_id
  LEFT JOIN excluded_games eg
    ON eg.game_id = s.game_id
  WHERE eg.game_id IS NULL
    AND s.quarter = 1
    AND (g.season = '2025-26' OR g.season = '22025')
)
SELECT
  AVG(stint_length_seconds) AS avg_first_q1_stint_seconds,
  AVG(stint_length_seconds) / 60.0 AS avg_first_q1_stint_minutes
FROM first_q1_stint
WHERE rn = 1;


-- 3) Shortest stint this season
SELECT
  s.game_id,
  s.quarter,
  s.sub_in_time,
  s.sub_out_time,
  s.stint_length_seconds,
  s.score_diff
FROM player_stints s
JOIN games g
  ON g.game_id = s.game_id
LEFT JOIN excluded_games eg
  ON eg.game_id = s.game_id
WHERE eg.game_id IS NULL
  AND (g.season = '2025-26' OR g.season = '22025')
ORDER BY s.stint_length_seconds ASC
LIMIT 1;


-- 4) Does he play longer when Minnesota is losing?
SELECT
  CASE
    WHEN s.score_diff < 0 THEN 'Losing'
    WHEN s.score_diff = 0 THEN 'Tied'
    WHEN s.score_diff > 0 THEN 'Winning'
    ELSE 'Unknown'
  END AS game_state,
  COUNT(*) AS stints,
  AVG(s.stint_length_seconds) AS avg_stint_seconds,
  AVG(s.stint_length_seconds) / 60.0 AS avg_stint_minutes
FROM player_stints s
JOIN games g
  ON g.game_id = s.game_id
LEFT JOIN excluded_games eg
  ON eg.game_id = s.game_id
WHERE eg.game_id IS NULL
  AND (g.season = '2025-26' OR g.season = '22025')
GROUP BY 1
ORDER BY 1;
