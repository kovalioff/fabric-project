SELECT t.DateID, z.Borough, z.Zone, t.TotalTrips, t.TotalAmount,
    t.TotalAmountEUR, aq.PM25, aq.NO2, aq.TemperatureC,
    aq.StationCount, aq.MinCoveragePercent, g.Value AS USAnnualGDP
FROM gold.FactTaxiDaily t
INNER JOIN gold.DimZone z ON t.ZoneID = z.ZoneID
INNER JOIN gold.DimDate d ON t.DateID = d.DateID
LEFT JOIN gold.FactAirQualityDaily aq ON t.DateID = aq.DateID AND t.ZoneID = aq.ZoneID
LEFT JOIN gold.DimGDP g ON d.Year = g.year
ORDER BY t.DateID, z.Zone;
