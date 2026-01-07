[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "2c02544d-0315-499e-9895-af59e611b26b"]}[Data]{[lakehouseId = "7fca3f40-0732-49d4-93d1-55cbe9734873"]}[Data];
[BindToDefaultDestination = true]
shared GDP = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json")),
  Navigation = Source{1},
  #"Converted to table" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1 1" = Table.ExpandRecordColumn(#"Converted to table", "Column1", {"date", "value"}, {"date", "value"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded Column1 1", {{"date", Int64.Type}, {"value", Int64.Type}})
in
  #"Changed column type";
