-- Sidebar bounds: the selectable date range and borough list.
-- Cheap enough to run on every load; takes no parameters.
SELECT
  MIN(crash_datetime)                                      AS min_datetime,
  MAX(crash_datetime)                                      AS max_datetime,
  COUNT(*)                                                 AS total_crashes,
  list_sort(list(DISTINCT COALESCE(borough, 'UNKNOWN')))   AS boroughs
FROM collisions_clean;
