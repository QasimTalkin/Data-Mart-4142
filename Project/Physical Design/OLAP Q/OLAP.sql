-- Q1 Drill Down, Roll up
--- Total number of fatalities in Ottawa during the four years
---------------TESTED----------------------
SELECT count(*) as fatal_accidents
FROM facts
WHERE is_fatal = true
--- Total number of fatalities atalities in Ottawa during 2015
---------------TESTED----------------------
SELECT count(*) as fatal_accidents_2015
FROM hours h, facts f
WHERE is_fatal = true 
	AND h.CURRENT_year = '2015'
	AND f.hour_key = h.hour_key
--- Total number of fatalities fatalities during an ice stormin Ottawaduring 2014
--
--
--- Total number of fatalities fatalities during an ice stormin Ottawa during december 2014
--
--
--Q2--
--Drill down, roll up, slice and dice: Contrast X in {accidents, fatal accidents, not fatal
---Compare the number of accidents on Mondays, versus the number of accidents on Fridays.
---------------TESTED----------------------
SELECT h.day_of_week, count(is_fatal) as Total_accidents
FROM hours h, facts f
WHERE  (h.day_of_week = 'Monday' or  h.day_of_week = 'Friday')
AND h.hour_key = f.hour_key
GROUP BY h.day_of_week
---Compare the number of fatal accidents on Mondays, versus the number of fatal accidents on Fridays.
--
--
---Contrast the number of fatal accidents in Nepean, during 2017, with the number of fatalities in Orleans, during 2014.
--
--
---Contrast the number of fatal accidents in Nepean on Mondays, with the number of fatalities in Nepean on Fridays.
--
--
---Contrast the number of fatal accidents in Nepean on Mondays between 05h00 and 08h00, with the number of fatalities in Gloucester during the same period of time.
--
--
---Contrast the number of accidents at intersections versus those that do not occur at intersections.
---------------TESTED----------------------
select 'Intersection', count(f.*) as accidents_at_intersection
from locales l, facts f
where l.locale_key = f.locale_key
and f.is_intersection = true
union
select 'Not intersection', count(f.*) as accidents_at_intersection
from locales l, facts f
where l.locale_key = f.locale_key
and f.is_intersection = false

---Cntrast the number of fatal accidents at intersections versus those that do not occur at intersections.
--
--
---Contrast the number of fatal accidents at intersections in Downtown Ottawa versus those that do not occur at intersections. 
--
--

--Q3--
--- Explore the interplay between weather conditions and accidents, focussing on traffic control, intersection, impact type and visibility.
--- Contrast the total number of accidents during summer, with the number of fatalities during fall.
-- Determine the interplay between road surface and the number of accidents.
---------------TESTED----------------------
select acc.road_surface, count(f.*) as num_accidents
from facts f, accidents acc
where f.accident_key = acc.accident_key
group by acc.road_surface
order by 1
--Determine the interplay between prolonged heat waves with high humidity levels and the number of accidents.
--
--
--Determine the interplay between strong rain showers and the number of accidents.
--
--
--Contrast the number of accidents in Nepean during summer, with the number of fatalities in Orleans during winter.
--
--
--Contrast the number of accidents in Nepean when it snows, with the number of fatalities in Nepean when it rains.
---------------TESTED----------------------
select case
	when w.weather like '%Rain%' then 'Rain'
	when w.weather like '%Snow%' then 'Snow'
	end as weather, count(f.*) as num_accidents
from weather w, facts f, locales l
where (w.weather like '%Rain%' or w.weather like '%Snow%')
and w.weather_key = f.weather_key
and f.locale_key = l.locale_key
and l.neighborhood = 'Nepean'
group by case
	when w.weather like '%Rain%' then 'Rain'
	when w.weather like '%Snow%' then 'Snow'
	end

--Determine the interplay between traffic control, impact types and frequencies of accidents.



--Q4--
--Locate “hot spots” for certain types of accidents, focussing on traffic control, intersection, impact type and visibility.
--- The intersections with the most accidents over the four years.
---------------TESTED----------------------
select l.street_name, l.INTERSECTION_1, count(CASE WHEN f.is_intersection THEN 1 END) as INTERSECTION
from locales l, facts f
where l.locale_key = f.locale_key
GROUP by l.street_name, l.INTERSECTION_1 
ORDER by INTERSECTION Desc
-- The neighborhoods with the most accidents during evening rush hour (from 4 to 7).
---------------TESTED----------------------
select l.neighborhood, count(f) as accidents
from locales l, hours h, facts f 
Where f.locale_key = l.locale_key 
	AND h.hour_start >= '04:00:00' and h.hour_start <='07:00:00'
	AND h.hour_key = f.hour_key
group by l.neighborhood
ORDER by accidents DESC
-- The intersections with the most accidents in fall, during dusk.
--
--
--The sections of highways with the most accidents when visibility is poor.
--
--
---

--Q5--
-- Calculate trends over the years.
--Determine the weekly trends in fatal accidents in Downtown Ottawa during the four years.
---------------TESTED----------------------
select t1.year, avg(t1.num_fatal) as avg_weekly_fatal
from (select extract('isoyear' from h.currentdate) as year,
		extract ('week' from h.currentdate) as week,
		count(f.*) as num_fatal
	from facts f, hours h, locales l
	where f.hour_key = h.hour_key
	and f.locale_key = l.locale_key
	and f.is_fatal = true
	and l.neighborhood = 'Downtown'
	group by h.currentdate) as t1
group by t1.year

--Determine the monthly trends in adverse weather conditions over the four years.
---------------TESTED----------------------
select t1.current_year, avg(t1.num_accidents) as avg_monthly_accidents
from (select h.current_year, h.month_of_year, count(f.*) as num_accidents
	from facts f, hours h, weather w
	where f.hour_key = h.hour_key
	  and f.weather_key = w.weather_key
	  and w.weather not like '%Clear%'
	  and w.weather not like '%Cloudy%'
	group by h.current_year, h.month_of_year) as t1
group by t1.current_year