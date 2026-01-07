CREATE TABLE [Gold].[FactAirQualityDaily] (

	[aq_daily_id] bigint IDENTITY NOT NULL, 
	[zone_id] int NULL, 
	[fx_id] bigint NULL, 
	[gdp_id] bigint NULL, 
	[weather_id] bigint NULL, 
	[date] date NULL, 
	[pm25] float NULL, 
	[o3] float NULL
);