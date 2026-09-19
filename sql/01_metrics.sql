-- Headline metrics for crashes causing injury or death.
-- Aggregated in SQL so the figures stay correct at full dataset size.
SELECT
  COUNT(*)                                         AS crash_count,
  COALESCE(SUM(number_of_persons_injured), 0)      AS total_injuries,
  COALESCE(SUM(number_of_persons_killed), 0)       AS total_fatalities,
  AVG(number_of_persons_injured)
    FILTER (WHERE number_of_persons_injured > 0)   AS avg_injuries_per_crash
FROM collisions_clean
WHERE (number_of_persons_injured     > 0
    OR number_of_pedestrians_injured > 0
    OR number_of_persons_killed      > 0)
  AND crash_datetime >= $start_date
  AND crash_datetime <  $end_date
  AND list_contains($boroughs, COALESCE(borough, 'UNKNOWN'));
