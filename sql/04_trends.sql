-- Monthly totals over every crash, with the harmful ones counted alongside.
--
-- This used to select only injury and fatal crashes, which made its
-- crash_count a different quantity from the one in 01_metrics.sql and left
-- no way to show what share of crashes cause harm -- the series that
-- actually moved over this period.
SELECT
  strftime(crash_datetime, '%Y-%m')  AS month,
  COUNT(*)                           AS crash_count,
  COUNT(*) FILTER (
    WHERE number_of_persons_injured > 0
       OR number_of_persons_killed  > 0)  AS harmful_crash_count,
  SUM(number_of_persons_injured)     AS total_injuries,
  SUM(number_of_persons_killed)      AS total_fatalities
FROM collisions_clean
WHERE crash_datetime >= $start_date
  AND crash_datetime <  $end_date
  AND list_contains($boroughs, COALESCE(borough_resolved, 'UNKNOWN'))
GROUP BY month
ORDER BY month;
