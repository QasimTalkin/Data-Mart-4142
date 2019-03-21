-- Drill Down, Roll up
-- -- Total number of fatalities in Ottawa during the four years
SELECT  count(CASE WHEN f.isfatal THEN 1 END) as fatal_accidents
FROM hours h, facts f
WHERE  h.year between '2014' and '2017'
	AND h.hour_key = f.hour_key
-- -- Total number of fatalities atalities in Ottawa during 2015
SELECT  count(CASE WHEN f.isfatal THEN 1 END) as fatal_accidents
FROM hours h, facts f
WHERE  h.year = '2015'
	AND h.hour_key = f.hour_key
-- -- Total number of fatalities fatalities during an ice stormin Ottawaduring 2014
SELECT  count(CASE WHEN f.isfatal THEN 1 END)  as fatal_accidents
FROM hours h, weather w, facts f
WHERE  h.year = '2014',
	AND w.condition = 'ice-storm'
	AND h.hour_key = f.hour_key
	AND w.weather_key = f.weather_key
-- -- Total number of fatalities fatalities during an ice stormin Ottawa during december 2014
SELECT  count(CASE WHEN f.isfatal THEN 1 END)  as fatal_accidents
FROM hours h, weather w, facts f
WHERE  h.year = '2014',
	AND h.month_of_year = 'December'
AND w.condition = 'ice-storm' -- this needs some change!!!!!!!~~!~~!!
	AND h.hour_key = f.hour_key
	AND w.weather_key = f.weather_key




-- Drill Down, Roll up, Slic and Dice
-- -- compare the number of accidents on Mondays, versus the number of accidents on Fridays.
SELECT h.day, count(is_fatal) as Total_accidents
FROM hours h, facts f
WHERE  (h.day = 'Monday' or  h.day = 'Friday')
	AND h.hour_key = f.hour_key
GROUP_BY h.day

-- -- compare the number of fatal accidents in Nepean, during 2017, with the number of fatal accidents in Orleans, during 2014
SELECT l.neighbourhood, count(CASE WHEN f.isfatal THEN 1 END) as Fatal_accidents
FROM hours h, locales l, facts f
WHERE l.neighbourhood in (
		SELECT neighbourhood
		FROM hours h, locales l,
		WHERE l.neighbourhood = 'Nepean'
			AND h.current_year = '2017'
			AND f.hour_key = h.hour_key
			AND f.local_key = l.Loca_key
		)
	AND l.neighbourhood in (
		SELECT neighbourhood
		FROM hours h, locales l,
		WHERE l.neighbourhood = 'Orleans'
			AND h.current_year = '2014'
			AND f.hour_key = h.hour_key
			AND f.local_key = l.Loca_key
		)
GROUP BY l.neighbourhood