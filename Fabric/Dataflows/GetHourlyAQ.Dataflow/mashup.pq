[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "2c02544d-0315-499e-9895-af59e611b26b"]}[Data]{[lakehouseId = "7fca3f40-0732-49d4-93d1-55cbe9734873"]}[Data];
shared LastDate = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "2c02544d-0315-499e-9895-af59e611b26b"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "7fca3f40-0732-49d4-93d1-55cbe9734873"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Sensors", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 2", each [sensor_id] = p_sensor_id),
  Last = DateTimeZone.From(List.Max(#"Filtered rows"[from_utc], DateTimeZone.ToUtc(datetime_from)))
in
  Last;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Sensors_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "name", DestinationColumnName = "name"], [SourceColumnName = "units", DestinationColumnName = "units"], [SourceColumnName = "from_utc", DestinationColumnName = "from_utc"], [SourceColumnName = "zone", DestinationColumnName = "zone"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared Sensors = let
  TargetURL = Text.Combine({"https://api.openaq.org/v3/sensors/", Number.ToText(p_sensor_id), "/hours?limit=1000&datetime_from=", DateTimeZone.ToText(LastDate + #duration(0, 1, 0, 0), [Format=datetime_format, Culture="en-US"])}),
  Source = Json.Document(Web.Contents(TargetURL,
  [
     Headers = [
      #"X-API-Key" = aq_api_key, 
      #"Content-Type" = "application/json"
     ]
  ]
  )),
  Navigation = Source[results],
  #"Converted to table" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table", "Column1", {"value", "parameter", "period", "coordinates", "summary", "coverage"}, {"value", "parameter", "period", "coordinates", "summary", "coverage"}),
  #"Expanded parameter" = Table.ExpandRecordColumn(#"Expanded Column1", "parameter", {"name", "units"}, {"name", "units"}),
  #"Expanded period" = Table.ExpandRecordColumn(#"Expanded parameter", "period", {"datetimeFrom", "datetimeTo"}, {"datetimeFrom", "datetimeTo"}),
  #"Expanded datetimeFrom" = Table.ExpandRecordColumn(#"Expanded period", "datetimeFrom", {"utc", "local"}, {"datetimeFrom.utc", "datetimeFrom.local"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded datetimeFrom", {"coordinates", "summary", "coverage", "datetimeFrom.local", "datetimeTo"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"datetimeFrom.utc", "from_utc"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"value", type number}, {"name", type text}, {"units", type text}, {"from_utc", type datetime}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "sensor_id", each p_sensor_id), {{"sensor_id", Int64.Type}}),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Added custom", "zone", each p_zone), {{"zone", Int64.Type}}),
  #"Reordered columns" = Table.ReorderColumns(#"Added custom 1", {"sensor_id", "value", "name", "units", "from_utc"})
in
  #"Reordered columns";
shared datetime_from = #datetimezone(2020, 9, 1, 0, 0, 0, 0, 0) meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type datetimezone, DefaultValue = null];
shared datetime_format = "yyyy-MM-dd'T'HH:mm:ss'Z'" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared p_sensor_id = 673 meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type number, DefaultValue = null];
shared aq_api_key = "292bc89f8037d40e2a50ec8a210410a9da92e93494c6f6a9b270d005bc0a2b57" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared p_zone = 41 meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type number, List = {41, 37, 132, 147, 187}, DefaultValue = null];
shared Sensors_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "2c02544d-0315-499e-9895-af59e611b26b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "7fca3f40-0732-49d4-93d1-55cbe9734873"]}[Data],
  TableNavigation = Navigation_2{[Id = "Sensors", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
