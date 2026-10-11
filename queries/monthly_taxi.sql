SELECT d.Year, d.Month, z.Borough,
    SUM(t.TotalTrips) AS TotalTrips,
    SUM(t.TotalAmount) AS TotalAmountUSD,
    SUM(t.TotalAmountEUR) AS TotalAmountEUR,
    ROUND(SUM(t.TotalAmount) / SUM(t.TotalTrips), 2) AS AvgRevenuePerTripUSD
FROM gold.FactTaxiDaily t
INNER JOIN gold.DimDate d ON t.DateID = d.DateID
INNER JOIN gold.DimZone z ON t.ZoneID = z.ZoneID
GROUP BY d.Year, d.Month, z.Borough
ORDER BY d.Year, d.Month, z.Borough;
