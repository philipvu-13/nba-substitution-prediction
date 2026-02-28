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