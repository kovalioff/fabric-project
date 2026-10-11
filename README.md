# NYC taxi and air quality

A Microsoft Fabric project combining yellow taxi trips, OpenAQ measurements, ECB exchange rates and annual US GDP.

## Data flow

`PL_Master_Orchestrator` runs Bronze, Silver and Gold in order. Gold finishes with `NB_Gold_Checks`; failed checks fail the pipeline.

- Bronze downloads missing monthly taxi Parquet files, OpenAQ daily measurements and the ECB and World Bank datasets.
- Silver filters invalid trips, normalizes air quality units, maps stations to TLC polygons and fills exchange rates forward.
- Gold creates daily taxi and air quality facts plus date, zone, exchange rate and GDP dimensions.

Both facts use one row per date and taxi zone. Air quality is available only for zones containing an OpenAQ station. Missing measurements remain NULL.

## Setup

1. Connect this repository to a Fabric workspace with a schema-enabled Lakehouse named `lakehouse` and a Spark runtime with Python 3.11 or newer.
2. Attach that Lakehouse to every notebook. When importing into another workspace, update the notebook Lakehouse bindings.
3. Configure the Lakehouse and Web connections for both Dataflows and the Fabric connection used by the Master pipeline.
4. Get an API key from [OpenAQ](https://explore.openaq.org/register). For a manual run, pass it as `OpenAQApiKey`. Do not commit it or store it as a pipeline default.
5. For scheduled runs, store the key in Azure Key Vault. Set the Master pipeline default `KeyVaultUrl` to the vault URL and `OpenAQSecretName` to the secret name. The identity running the notebook needs permission to read that secret.
6. Run the Master pipeline manually once and inspect `gold.DataQualitySummary` before enabling its daily schedule. Do not run multiple Masters against the same Lakehouse at the same time.

| Parameter | Default | Meaning |
| --- | --- | --- |
| StartDate | 2024-01-01 | First day included in both facts |
| EndDate | empty | Last day of the month two months before the run |
| OpenAQApiKey | empty | API key for a manual run |
| KeyVaultUrl | empty | Vault URL for unattended runs |
| OpenAQSecretName | openaq-api-key | Secret containing the API key |

Dates use YYYY-MM-DD. The supported range starts in 2015. TLC normally publishes with a two-month delay; a missing monthly file fails the run and is retried on the next run. To backfill, change StartDate. Existing taxi files are reused; remove a specific file from `Files/bronze_taxi` to reload a corrected source month.

## Processing rules

Taxi trips need a positive distance and total amount, a nonnegative fare, valid zone IDs and a dropoff after pickup. Distances above 500 miles and pickup dates outside the source month are excluded. Passenger count is kept as reported. Trips are not deduplicated because TLC does not provide a unique trip ID.

OpenAQ uses stationary sensors within 25 km of central NYC and then matches them to TLC polygons. Daily dates use the local timestamp from OpenAQ. Records with quality flags, less than 75% coverage or unsupported units are excluded. Each run replaces the selected OpenAQ period after checking that measurements were returned; it does not delete the existing tables first.

Pollutant concentrations in Gold use ug/m3. Gas values in ppm and ppb are converted at 25 C and 1 atm using a molar volume of 24.45 L/mol. NOx is accepted only in mass concentration units. Temperature is Celsius, humidity is percent and PM0.3 count is particles/cm3. Zone values are arithmetic means of the available sensor daily averages. StationCount and SensorCount show how many stations and sensors contributed across all parameters; they can differ by pollutant.

ECB values are USD per EUR. Missing days use the last earlier published rate; SourceRateDate records that date. EUR amounts use the reciprocal rate. US GDP is annual nominal GDP in current USD, not NYC GDP. Do not sum the repeated annual GDP value in the daily query or use it to claim a causal relationship with taxi traffic.

## Analysis

Run the SQL files in `queries` against the Lakehouse SQL analytics endpoint. `daily_taxi_air_quality.sql` joins the daily facts and annual GDP; `monthly_taxi.sql` summarizes revenue and trips by borough. They can also be used as the starting point for a Power BI report.

The exported Warehouse and `taxi-zone` Copy Job are not used by the Master pipeline. The pipeline reads and writes the Lakehouse; the zone notebook downloads the CSV lookup directly.

## Checks

Gold checks nonempty tables, unique and non-null keys, dimension references, exchange rate coverage, matching dates and zones between the facts, and equality of Silver and Gold trip counts and revenue. It writes row counts and the check timestamp to `gold.DataQualitySummary` only after all checks pass.

Local tests cover the Spark transformations, month boundaries, API parsing and geographic source format. Run `python -m pip install -r requirements-dev.txt`, then `python -m pytest`. Java 17 is required for the Spark tests. A full run still needs the configured Fabric workspace and an OpenAQ key.

## Sources

- [NYC TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [OpenAQ API](https://docs.openaq.org/)
- [ECB reference rates](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html)
- [World Bank GDP indicator](https://data.worldbank.org/indicator/NY.GDP.MKTP.CD)
- [EPA gas concentration conversion](https://www.epa.gov/sites/production/files/2014-08/documents/072000-appendix-urban-air-toxics-report-congress.pdf)
