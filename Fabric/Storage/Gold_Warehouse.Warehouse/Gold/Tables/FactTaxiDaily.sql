CREATE TABLE [Gold].[FactTaxiDaily] (

	[trips_daily_id] bigint IDENTITY NOT NULL, 
	[zone_id] int NULL, 
	[fx_id] bigint NULL, 
	[gdp_id] bigint NULL, 
	[weather_id] bigint NULL, 
	[date] date NULL, 
	[count_trips] bigint NULL, 
	[sum_fares] float NULL, 
	[sum_total] float NULL
);