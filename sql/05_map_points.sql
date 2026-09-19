-- Geolocated injury/fatality crashes for the heatmap.
--
-- Capped at $row_limit: a browser cannot usefully render hundreds of
-- thousands of points, and shipping them all would blow the app's memory.
-- Ordering by hash(collision_id) takes a uniform, deterministic subset, so
-- the map stays stable across reruns instead of shimmering on every filter
-- change the way ORDER BY random() would.
SELECT
  latitude,
  longitude,
  COALESCE(borough, 'UNKNOWN')                       AS borough,
  strftime(crash_datetime, '%Y-%m-%d %H:%M:%S')      AS crash_datetime_str,
  number_of_persons_injured,
  number_of_persons_killed
FROM collisions_clean
WHERE (number_of_persons_injured     > 0
    OR number_of_pedestrians_injured > 0
    OR number_of_persons_killed      > 0)
  AND latitude  IS NOT NULL
  AND longitude IS NOT NULL
  AND crash_datetime >= $start_date
  AND crash_datetime <  $end_date
  AND list_contains($boroughs, COALESCE(borough, 'UNKNOWN'))
ORDER BY hash(collision_id)
LIMIT $row_limit;
