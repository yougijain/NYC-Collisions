-- Total and average injuries by borough, labelling NULL boroughs 'UNKNOWN'.
SELECT
  COALESCE(borough_resolved, 'UNKNOWN')    AS borough,
  COUNT(*)                        AS crash_count,
  SUM(number_of_persons_injured)  AS total_injuries,
  AVG(number_of_persons_injured)  AS avg_injuries_per_crash
FROM collisions_clean
WHERE crash_datetime >= $start_date
  AND crash_datetime <  $end_date
  AND list_contains($boroughs, COALESCE(borough_resolved, 'UNKNOWN'))
GROUP BY borough_resolved
ORDER BY total_injuries DESC;
