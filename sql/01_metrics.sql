-- Headline counts over every crash in the filter, not only the harmful ones.
--
-- This query used to restrict itself to injury and fatal crashes, which made
-- its crash_count disagree with the total shown in the page header, and made
-- its average injuries figure mean "per injury crash" while being read as
-- "per crash". Both numbers now say what they are.
SELECT
  COUNT(*)                                          AS crash_count,
  COUNT(*) FILTER (
    WHERE number_of_persons_injured > 0
       OR number_of_persons_killed  > 0)            AS harmful_crash_count,
  COALESCE(SUM(number_of_persons_injured), 0)       AS total_injuries,
  COALESCE(SUM(number_of_persons_killed), 0)        AS total_fatalities,
  COUNT(*) FILTER (
    WHERE (number_of_persons_injured > 0
        OR number_of_persons_killed  > 0)
      AND latitude IS NOT NULL)                     AS mappable_crash_count
FROM collisions_clean
WHERE crash_datetime >= $start_date
  AND crash_datetime <  $end_date
  AND list_contains($boroughs, COALESCE(borough_resolved, 'UNKNOWN'));
