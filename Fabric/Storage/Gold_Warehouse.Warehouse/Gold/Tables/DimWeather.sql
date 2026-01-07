CREATE TABLE [Gold].[DimWeather] (

	[weather_id] bigint IDENTITY NOT NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[avg_fahr] float NULL, 
	[avg_celsius] float NULL, 
	[sum_precipitation] float NULL
);