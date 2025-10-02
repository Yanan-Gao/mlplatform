# Frequency ETL Pipeline (Scala)

This repository contains a modular, EMR-friendly implementation of the frequency features ETL. It mirrors the original Databricks notebooks but is organized as standalone Spark jobs that can be submitted as EMR Steps. Each job has its own assembly JAR and a small, explicit set of `-D` configuration parameters.

The pipeline produces the following datasets under S3 (partitioned by date `year=YYYY/month=MM/day=DD`):

- Daily Aggregation: `.../env=<env>/features/data/frequency/daily_agg/v1/<subset>/year=...` and Click-bot UIIDs: `.../env=<env>/features/data/frequency/click_bot_uiids/v1/click_bot_uiids/year=...`
- Seven-day Aggregations: `.../env=<env>/features/data/frequency/seven_day_agg/v1/<subset>/{uiid_campaign|per_uiid}/year=...`
- Short-window Features: `.../env=<env>/features/data/frequency/short_window/v1/year=...`
- Recency Features: `.../env=<env>/features/data/frequency/recency/v1/year=...`
- Training Table: `.../env=<env>/features/data/frequency/training_table/v1/<subset>/year=...`

Note on environments: when `-Dttd.env=prodTest`, the jobs read from `prod` and write to `test` (prodTest mapping). For any other value (e.g. `dev`, `prod`), the job reads and writes within that same environment.

---

## Modules and Flow

1) FrequencyDailyAggregation (job.FrequencyDailyAggregation)
- Inputs: BidsImpressions (day partition), ClickTracker (date=YYYYMMDD/hour=*)
- Filters: ROI (AdGroup + CampaignROIGoal priority=1), optional isolated advertisers CSV
- Outputs:
  - Daily counts per (UIID, CampaignId, AdvertiserId) by date under `env=<env>/features/data/frequency/daily_agg/v1/<subset>`
  - Click-bot UIIDs under `env=<env>/features/data/frequency/click_bot_uiids/v1/click_bot_uiids`

2) SevenDayUiidAggregation (job.SevenDayUiidAggregation)
- Inputs: `env=<env>/features/data/frequency/daily_agg/v1/<subset>` for past N days (default 7)
- Outputs:
  - `uiid_campaign` join view and `per_uiid` view, plus optional aggregate set bands (e.g. offsets [1,2], [1,2,3,4], [1,2,3,4,5,6])

3) ShortWindowFrequency (job.ShortWindowFrequency)
- Inputs: BidsImpressions and ClickTracker for run date and previous day; optional UIID bot list and isolated advertisers
- Outputs: short windows (1/3/6/12h), same-day stats per impression under `env=<env>/features/data/frequency/short_window/v1`

4) RecencyFeatures (job.RecencyFeatures)
- Inputs: BidsImpressions and ClickTracker for `lookbackDays` + run date; optional UIID bot list and isolated advertisers
- Outputs: UIID and UIID+Campaign recency seconds per impression under `env=<env>/features/data/frequency/recency/v1`

5) TrainingTableAssembly (job.TrainingTableAssembly)
- Inputs: Short-window features, Seven-day aggregations (uiid_campaign + per_uiid), Recency features
- Outputs: Per-impression training table, including long-window approximations (e.g. 2d/4d/6d derived from aggregate sets) under `env=<env>/features/data/frequency/training_table/v1/<subset>`

---

## Assembly JARs (MR example)

The CI publishes the module JARs to the merge-request folder:

- s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/frequency-daily-aggregation.jar
- s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/short-window-frequency.jar
- s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/seven-day-uiid-aggregation.jar
- s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/recency-features.jar
- s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/training-table-assembly.jar

Use these JARs in the `spark-submit` EMR Steps below (or your prod path for release runs).

---

## EMR Step Templates

The examples below use conservative Spark settings and pass parameters via `--driver-java-options` (as `-Dkey=value`). Adjust executor sizes/partitions for your cluster.

### 1) FrequencyDailyAggregation

Class: `job.FrequencyDailyAggregation`

Parameters (keys):
- `ttd.env` (string): `dev`, `prodTest` (maps to read=prod, write=test), `prod`
- `date` (ISO yyyy-MM-dd)
- `partitions` (int), `roiGoalTypes` (comma CSV, e.g. `3,4`)
- `isolatedCsv` (S3 path to advertiser exclusion CSV)
- `koaEnvOverwrite` (string, default `prod`): override environment used to read KOA BidsImpressions
- Inputs/outputs (override as needed):
  - `bidsImpressionsRoot` (default from TTD geronimo `BidsImpressions.BIDSIMPRESSIONSS3`)
  - `clickTrackerRoot` (default `s3://ttd-datapipe-data/parquet/rtb_clicktracker_verticaload/v=1`)
  - `dailyAggOutputRoot` (default `.../frequency/daily_agg/v1`)
  - `clickBotUiidsRoot` (default `.../frequency/click_bot_uiids/v1`)

Example (single line):
```
spark-submit --deploy-mode cluster --class job.FrequencyDailyAggregation --executor-memory 202G --executor-cores 32 --conf "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC" --conf spark.driver.memory=110G --conf spark.driver.cores=15 --conf spark.sql.shuffle.partitions=5000 --conf spark.driver.maxResultSize=50G --conf spark.dynamicAllocation.enabled=true --conf spark.memory.fraction=0.7 --conf spark.memory.storageFraction=0.25 --conf spark.app.name=FrequencyDailyAggregation "--driver-java-options=-Dttd.env=dev -Ddate=2025-08-08 -Dpartitions=200 -DroiGoalTypes=3,4 -DisolatedCsv=s3://thetradedesk-mlplatform-us-east-1/env=prod/metadata/philo/advertiserFilter/advertiser_exclusion_list.csv -DkoaEnvOverwrite=prod -DbidsImpressionsRoot=s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1 -DclickTrackerRoot=s3://ttd-datapipe-data/parquet/rtb_clicktracker_verticaload/v=1 -DdailyAggOutputRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/daily_agg/v1 -DclickBotUiidsRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/click_bot_uiids/v1" s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/frequency-daily-aggregation.jar
```

### 2) SevenDayUiidAggregation

Class: `job.SevenDayUiidAggregation`

Parameters:
- `ttd.env`, `date`, `partitions`
- `subset` (e.g. `unrestricted`, `isolated`)
- `lookbackDays` (default 7)
- `aggregateSets` (JSON), e.g. `'[[1,2],[1,2,3,4],[1,2,3,4,5,6]]'`
- `dailyRoot` (input), `outputRoot` (output)

Example (single line):
```
spark-submit --deploy-mode cluster --class job.SevenDayUiidAggregation --executor-memory 40G --executor-cores 8 --conf spark.sql.shuffle.partitions=3000 --conf spark.driver.memory=20G --conf spark.driver.cores=4 --conf spark.dynamicAllocation.enabled=true "--driver-java-options=-Dttd.env=dev -Ddate=2025-08-08 -Dsubset=unrestricted -DlookbackDays=7 -Dpartitions=200 -DaggregateSets=[[1,2],[1,2,3,4],[1,2,3,4,5,6]] -DdailyRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/daily_agg/v1 -DoutputRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/seven_day_agg/v1" s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/seven-day-uiid-aggregation.jar
```

### 3) ShortWindowFrequency

Class: `job.ShortWindowFrequency`

Parameters:
- `ttd.env`, `date`, `partitions`
- `blackout` (default `-180`) – inclusive upper bound for rangeBetween
- `shortWindows` (CSV of seconds; default `3600,10800,21600,43200`)
- `roiGoalTypes` (CSV), `isolatedCsv` (S3)
- `koaEnvOverwrite` (string, default `prod`): override environment used to read KOA BidsImpressions
- `uiidBotFilteringRoot` (optional input from Daily job)
- Inputs/outputs:
  - `bidsImpressionsRoot`, `clickTrackerRoot`
  - `outputRoot` (`.../frequency/short_window/v1`)

Example (single line):
```
spark-submit --deploy-mode cluster --class job.ShortWindowFrequency --executor-memory 60G --executor-cores 12 --conf spark.sql.shuffle.partitions=4000 --conf spark.driver.memory=30G --conf spark.driver.cores=6 "--driver-java-options=-Dttd.env=dev -Ddate=2025-08-08 -Dpartitions=200 -Dblackout=-180 -DshortWindows=3600,10800,21600,43200 -DroiGoalTypes=3,4 -DisolatedCsv=s3://thetradedesk-mlplatform-us-east-1/env=prod/metadata/philo/advertiserFilter/advertiser_exclusion_list.csv -DkoaEnvOverwrite=prod -DuiidBotFilteringRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/click_bot_uiids/v1 -DbidsImpressionsRoot=s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1 -DclickTrackerRoot=s3://ttd-datapipe-data/parquet/rtb_clicktracker_verticaload/v=1 -DoutputRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/short_window/v1" s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/short-window-frequency.jar
```

### 4) RecencyFeatures

Class: `job.RecencyFeatures`

Parameters:
- `ttd.env`, `date`, `partitions`
- `lookbackDays` (default 3), `roiGoalTypes` (CSV), `isolatedCsv` (S3)
- `uiidBotFilteringRoot` (optional input from Daily job)
- `koaEnvOverwrite` (string, default `prod`): override environment used to read KOA BidsImpressions
- Inputs/outputs: `bidsImpressionsRoot`, `clickTrackerRoot`, `outputRoot`

Example (single line):
```
spark-submit --deploy-mode cluster --class job.RecencyFeatures --executor-memory 40G --executor-cores 8 --conf spark.sql.shuffle.partitions=3000 --conf spark.driver.memory=20G --conf spark.driver.cores=4 "--driver-java-options=-Dttd.env=dev -Ddate=2025-08-08 -DlookbackDays=3 -Dpartitions=200 -DroiGoalTypes=3,4 -DisolatedCsv=s3://thetradedesk-mlplatform-us-east-1/env=prod/metadata/philo/advertiserFilter/advertiser_exclusion_list.csv -DkoaEnvOverwrite=prod -DuiidBotFilteringRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/click_bot_uiids/v1 -DbidsImpressionsRoot=s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1 -DclickTrackerRoot=s3://ttd-datapipe-data/parquet/rtb_clicktracker_verticaload/v=1 -DoutputRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/recency/v1" s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/recency-features.jar
```

### 5) TrainingTableAssembly

Class: `job.TrainingTableAssembly`

Parameters:
- `ttd.env`, `date`, `subset`, `partitions`
- `aggregateSets` (JSON) – e.g. `'[[1,2],[1,2,3,4],[1,2,3,4,5,6]]'` (fallbacks to defaults if omitted)
- Input roots: `shortWindowRoot`, `sevenDayRoot`, `recencyRoot`
- Output root: `outputRoot`

Example (single line):
```
spark-submit --deploy-mode cluster --class job.TrainingTableAssembly --executor-memory 60G --executor-cores 12 --conf spark.sql.shuffle.partitions=4000 --conf spark.driver.memory=30G --conf spark.driver.cores=6 "--driver-java-options=-Dttd.env=dev -Ddate=2025-08-08 -Dsubset=unrestricted -Dpartitions=200 -DaggregateSets=[[1,2],[1,2,3,4],[1,2,3,4,5,6]] -DshortWindowRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/short_window/v1 -DsevenDayRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/seven_day_agg/v1 -DrecencyRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/recency/v1 -DoutputRoot=s3://thetradedesk-mlplatform-us-east-1/env=dev/features/data/frequency/training_table/v1" s3://thetradedesk-mlplatform-us-east-1/libs/frequency/jars/mergerequests/ped-DATPERF-7085-frequency-features/training-table-assembly.jar
```

---

## Operational Notes

- Aggregate sets: input must be valid JSON (array of int arrays). The code validates and deduplicates positive integers.
- Subsets: `unrestricted` is everything not in the isolated advertiser CSV; `isolated` is the intersection.
- prodTest mapping: `ttd.env=prodTest` reads from `prod` roots and writes to `dev` roots (this keeps dev writes isolated).
- Partitions: the examples pass large shuffle partitions to match historical cluster sizes; tune as needed per workload.
- Click-bot filtering: produced by Daily job and optionally used by Recency and Short-window jobs.

If you need a consolidated step runner or any job-specific defaults changed, let me know and I can extend this README with sample Bash wrappers.
