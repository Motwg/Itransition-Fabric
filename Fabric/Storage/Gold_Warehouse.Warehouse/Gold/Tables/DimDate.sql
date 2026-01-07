CREATE TABLE [Gold].[DimDate] (

	[date] date NOT NULL, 
	[year] int NULL, 
	[quarter] int NULL, 
	[month] int NULL, 
	[month_name] varchar(20) NULL, 
	[week_of_year] int NULL, 
	[day_of_year] int NULL, 
	[day] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(20) NULL, 
	[is_weekend] bit NULL
);