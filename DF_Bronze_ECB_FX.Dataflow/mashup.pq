[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "."]}[Data]{[lakehouseName = "lakehouse"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ECB_FX_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ECB_FX = let
  Source = Csv.Document(Web.Contents("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"), [Delimiter = ",", Columns = 32, Encoding = 65001]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Sorted rows" = Table.Sort(#"Promoted headers", {{"TIME_PERIOD", Order.Descending}}),
  #"Selected columns" = Table.SelectColumns(#"Sorted rows", {"TIME_PERIOD", "OBS_VALUE"}),
  #"Changed column type with locale" = Table.TransformColumnTypes(#"Selected columns", {{"OBS_VALUE", type number}, {"TIME_PERIOD", type date}}, "en-US")
in
  #"Changed column type with locale";
shared ECB_FX_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[lakehouseName = "lakehouse"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "ECB_FX", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
