DROP TABLE IF EXISTS weather_data ;

CREATE TABLE weather_data (
    id      serial primary key ,
    city        varchar(100)  ,
    country_code    varchar(5) not null ,
    temperature     numeric(5,2) not null ,
    fees_like       numeric(5,2) not null ,
    humidity        integer not null ,
    pressure        integer not null ,
    wind_speed      numeric(6,2) not null ,
    weather_main    varchar(50) not null ,
    description     varchar(200) ,
    collected_at    timestamp not null ,
    loaded_at       timestamp not null ,
) ;

-- index
CREATE INDEX idx weather_city ON weather_data(city) ;
CREATE INDEX idx weather_collected ON weather_data(collected_at) ;


