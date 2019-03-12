CREATE TABLE IF NOT EXISTS hours(
    hour_key serial PRIMARY KEY,
    hour_start timetz NOT NULL,
    hour_end timetz NOT NULL,
    current_date date NOT NULL,
    day_of_week char(3) NOT NULL,
    month_of_year char(3) NOT NULL,
    current_year int NOT NULL,
    is_weekend bool NOT NULL,
    is_holiday bool NOT NULL,
    holiday_name varchar(50)
);

CREATE TABLE IF NOT EXISTS locales(
    locale_key serial PRIMARY KEY,
    street_name varchar(100),
    intersection_1 varchar(100),
    intersection_2 varchar(100),
    longitude float8 NOT NULL,
    latitude float8 NOT NULL,
    neighborhood varchar(100)
);

CREATE TABLE IF NOT EXISTS events(
    event_key serial PRIMARY KEY,
    event_name varchar(150) NOT NULL,
    event_start timestamptz,
    event_end timestamptz
);

CREATE TABLE IF NOT EXISTS weather(
    weather_key serial PRIMARY KEY,
    station_name varchar(100) NOT NULL,
    longitude float8 NOT NULL,
    latitude float8 NOT NULL,
    temperature_c int,
    visibility varchar(50),
    wind_speed_kmh int,
    wind_chill int,
    wind_direction char(1),
    pressure int
);

CREATE TABLE IF NOT EXISTS accidents(
    accident_key serial PRIMARY KEY,
    accident_time timetz NOT NULL,
    -- should these be numbers?
    environment varchar(50),
    road_surface varchar(50),
    traffic_control varchar(50),
    visibility varchar(50),
    impact_type varchar(50)
);

CREATE TABLE IF NOT EXISTS facts(
    hour_key int REFERENCES hours(hour_key),
    locale_key int REFERENCES locales(locale_key),
    event_key int REFERENCES events(event_key),
    weather_key int REFERENCES weather(weather_key),
    accident_key int REFERENCES accidents(accident_key),
    is_fatal bool NOT NULL,
    is_intersection bool NOT NULL,
    PRIMARY KEY(hour_key, locale_key, event_key, weather_key, accident_key)
);