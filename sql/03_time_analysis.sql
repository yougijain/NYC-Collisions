-- Crash counts by hour of day.
WITH hourly AS (
  SELECT
    EXTRACT(hour FROM crash_datetime)::INTEGER AS hour_24,
    COUNT(*)                                   AS crash_count,
    COUNT(*) FILTER (
      WHERE number_of_persons_injured > 0
         OR number_of_persons_killed  > 0)     AS harmful_crash_count
  FROM collisions_clean
  WHERE crash_datetime >= $start_date
    AND crash_datetime <  $end_date
    AND list_contains($boroughs, COALESCE(borough_resolved, 'UNKNOWN'))
  GROUP BY hour_24
)
SELECT
  CASE
    WHEN hour_24 = 0  THEN '12 AM'
    WHEN hour_24 BETWEEN 1 AND 11 THEN printf('%d AM', hour_24)
    WHEN hour_24 = 12 THEN '12 PM'
    ELSE printf('%d PM', hour_24 - 12)
  END AS hour_label,
  hour_24,
  crash_count,
  harmful_crash_count
FROM hourly
ORDER BY hour_24;
