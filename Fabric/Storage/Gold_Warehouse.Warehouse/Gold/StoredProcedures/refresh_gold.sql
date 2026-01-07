CREATE   PROCEDURE [Gold].[refresh_gold]
 AS
 BEGIN
    TRUNCATE TABLE Gold.DimDate;
    INSERT INTO Gold.DimDate (date, year, quarter, month, month_name, week_of_year, day_of_year, day, day_of_week, day_name, is_weekend)
    SELECT * FROM Silver_Lakehouse.dbo.DimDate;

    TRUNCATE TABLE Gold.DimFX;
    INSERT INTO Gold.DimFX (date, fx)
    SELECT * FROM Silver_Lakehouse.dbo.DimFX;
    
    TRUNCATE TABLE Gold.DimWeather;
    INSERT INTO Gold.DimWeather (year, month, avg_fahr, avg_celsius, sum_precipitation)
    SELECT year(date), month(date), avg_fahr, avg_celsius, sum_precipitation FROM Silver_Lakehouse.dbo.DimWeather;

    TRUNCATE TABLE Gold.DimGDP;
    INSERT INTO Gold.DimGDP (year, gdp)
    SELECT * FROM Silver_Lakehouse.dbo.DimGDP;

    TRUNCATE TABLE Gold.DimZone;
    INSERT INTO Gold.DimZone (zone_id, borough, zone, service_zone)
    SELECT * FROM Silver_Lakehouse.dbo.DimZone;

    TRUNCATE TABLE Gold.FactAirQualityHourly;
    INSERT INTO Gold.FactAirQualityHourly (zone_id, fx_id, gdp_id, weather_id, date, hour, pm25, o3)
    SELECT S1.zone_id, FX.fx_id, GDP.gdp_id, W.weather_id, S1.dt, S1.pu_hour, S1.value as pm25, S2.value as o3
    FROM Silver_Lakehouse.dbo.DimSensors S1
    FULL JOIN Silver_Lakehouse.dbo.DimSensors S2
    ON S1.zone_id = S2.zone_id
        AND S1.dt = S2.dt
        AND S1.pu_hour = S2.pu_hour
        AND S2.name = 'o3'
    LEFT JOIN Gold.DimFX FX
    ON S1.dt = FX.date
    LEFT JOIN Gold.DimGDP GDP
    ON year(S1.dt) = GDP.year
    LEFT JOIN Gold.DimWeather W
    ON year(S1.dt) = W.year AND month(S1.dt) = W.month 
    WHERE S1.name = 'pm25';

    TRUNCATE TABLE Gold.FactAirQualityDaily;
    INSERT INTO Gold.FactAirQualityDaily (zone_id, fx_id, gdp_id, weather_id, date, pm25, o3)
    SELECT zone_id, fx_id, gdp_id, weather_id, date, ROUND(SUM(pm25) / COUNT(pm25), 2), ROUND(SUM(o3) / COUNT(o3), 3)
    FROM Gold.FactAirQualityHourly
    GROUP BY zone_id, date, fx_id, gdp_id, weather_id;

    TRUNCATE TABLE Gold.FactTaxiHourly;
    INSERT INTO Gold.FactTaxiHourly (zone_id, fx_id, gdp_id, weather_id, date, hour, count_trips, sum_fares, sum_total)
    SELECT T.pu_loc_id, FX.fx_id, GDP.gdp_id, W.weather_id, T.pu_dt, T.pu_hour, COUNT(*), ROUND(SUM(T.fare), 2), ROUND(SUM(T.total), 2) 
    FROM (
        SELECT pu_loc_id, pu_dt, pu_hour, fare, total
        FROM Silver_Lakehouse.dbo.DimTrip
        UNION
        SELECT do_loc_id, pu_dt, pu_hour, fare, total
        FROM Silver_Lakehouse.dbo.DimTrip
        WHERE pu_loc_id != do_loc_id
    ) T
    LEFT JOIN Gold.DimFX FX
        ON T.pu_dt = FX.date
    LEFT JOIN Gold.DimGDP GDP
        ON year(T.pu_dt) = GDP.year
    LEFT JOIN Gold.DimWeather W
        ON year(T.pu_dt) = W.year AND month(T.pu_dt) = W.month 
    GROUP BY T.pu_loc_id, T.pu_dt, T.pu_hour, FX.fx_id, GDP.gdp_id, W.weather_id;

    TRUNCATE TABLE Gold.FactTaxiDaily;
    INSERT INTO Gold.FactTaxiDaily (zone_id, fx_id, gdp_id, weather_id, date, count_trips, sum_fares, sum_total)
    SELECT zone_id, fx_id, gdp_id, weather_id, date, SUM(count_trips), ROUND(SUM(sum_fares), 2), ROUND(SUM(sum_total), 2) 
    FROM Gold.FactTaxiHourly
    GROUP BY zone_id, date, fx_id, gdp_id, weather_id;
END;