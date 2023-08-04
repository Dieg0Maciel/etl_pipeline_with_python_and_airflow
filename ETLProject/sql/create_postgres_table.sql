CREATE TABLE IF NOT EXISTS hourly_forecast(
    request_time  VARCHAR(50),
    forecast_time  VARCHAR(25),
    description  VARCHAR(100),
    temperature  FLOAT(8),
    pressure  INT,
    humidity  INT,
    wind_speed  FLOAT(8)
) 
