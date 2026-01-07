[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "2c02544d-0315-499e-9895-af59e611b26b"]}[Data]{[lakehouseId = "7fca3f40-0732-49d4-93d1-55cbe9734873"]}[Data];
[BindToDefaultDestination = true]
shared ECB = let
  Source = Csv.Document(Web.Contents("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"), [Delimiter = ",", Columns = 32, Encoding = 65001]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"KEY", type text}, {"FREQ", type text}, {"CURRENCY", type text}, {"CURRENCY_DENOM", type text}, {"EXR_TYPE", type text}, {"EXR_SUFFIX", type text}, {"TIME_PERIOD", type date}, {"OBS_VALUE", type number}, {"OBS_STATUS", type text}, {"OBS_CONF", type text}, {"OBS_PRE_BREAK", type text}, {"OBS_COM", type text}, {"TIME_FORMAT", type duration}, {"BREAKS", type text}, {"COLLECTION", type text}, {"COMPILING_ORG", type text}, {"DISS_ORG", type text}, {"DOM_SER_IDS", type text}, {"PUBL_ECB", type text}, {"PUBL_MU", type text}, {"PUBL_PUBLIC", type text}, {"UNIT_INDEX_BASE", type text}, {"COMPILATION", type text}, {"COVERAGE", type text}, {"DECIMALS", Int64.Type}, {"NAT_TITLE", type text}, {"SOURCE_AGENCY", type text}, {"SOURCE_PUB", type text}, {"TITLE", type text}, {"TITLE_COMPL", type text}, {"UNIT", type text}, {"UNIT_MULT", Int64.Type}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"FREQ", "CURRENCY", "CURRENCY_DENOM", "KEY", "EXR_TYPE", "EXR_SUFFIX", "TIME_FORMAT", "BREAKS", "OBS_COM", "OBS_PRE_BREAK", "OBS_CONF", "OBS_STATUS", "COLLECTION", "COMPILING_ORG", "DISS_ORG", "DOM_SER_IDS", "PUBL_ECB", "PUBL_MU", "PUBL_PUBLIC", "UNIT_INDEX_BASE", "COMPILATION", "COVERAGE", "DECIMALS", "NAT_TITLE", "SOURCE_AGENCY", "SOURCE_PUB", "TITLE", "TITLE_COMPL", "UNIT", "UNIT_MULT"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"TIME_PERIOD", "date"}, {"OBS_VALUE", "ecb"}})
in
  #"Renamed columns";
