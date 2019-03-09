
-- Disaster Dimension
CREATE TABLE hours(
    hour_key serial PRIMARY KEY,
    hour_start timetz,
    hour_end timetz,
    accident_date date,
    day_of_week int,
    month_of_year int,
    current_year int,
    is_weekend bool,
    is_holiday bool,
    holiday_name text
);



CREATE table location (
    location_key int PRIMARY KEY,
    street_name text,
    intersection_1 text,
    intersection_2 text,
    longitude float,
    latitude float,
    neighborhood text
);

CREATE TABLE events(
    event_key int PRIMARY KEY,
    event_name text,
    event_start timestamptz,
    event_end timestamptz
);

CREATE TABLE weather(
    weather_key int PRIMARY KEY,
    station_name text,
    longitude float,
    latitude float,
    temperature_c float,
    visibility text,
    wind_speed_kmh int,
    wind_chill int,
    wind_direction text,
    pressure int
);

CREATE TABLE accidents(
    accident_key int PRIMARY KEY,
    accident_time timetz,
    -- should these be numbers?
    environment text,
    road_surface text,
    traffic_control text,
    visibility text,
    impact_type text
);

CREATE TABLE facts(
    hour_key int REFERENCES hours(hour_key),
    location_key int REFERENCES location(location_key),
    event_key int REFERENCES events(event_key),
    weather_key int REFERENCES weather(weather_key),
    accident_key int REFERENCES accidents(accident_key),
    is_fatal bool,
    is_intersection bool
);