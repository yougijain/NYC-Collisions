-- Monthly trends for crashes causing injury or death.
SELECT
  strftime(crash_datetime, '%Y-%m')  AS month,
  COUNT(*)                           AS crash_count,
  SUM(number_of_persons_injured)     AS total_injuries,
  SUM(number_of_persons_killed)      AS total_fatalities,
  AVG(number_of_persons_injured)     AS avg_injuries_per_crash
FROM collisions_clean
WHERE (number_of_persons_injured > 0
    OR number_of_persons_killed  > 0)
  AND crash_datetime >= $start_date
  AND crash_datetime <  $end_date
  AND list_contains($boroughs, COALESCE(borough_resolved, 'UNKNOWN'))
GROUP BY month
ORDER BY month;
