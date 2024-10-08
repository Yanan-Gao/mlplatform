# Kongming Scala ETL

## Test with DS sync
- Set up ds-sync as per the instructions on https://gitlab.adsrvr.org/thetradedesk/teams/aifun/mlops/ds-sync
- Login to SAML and select `"ttd-production (003576902480) / ttd_developer_elevated"`. For IDP account configuration, see https://atlassian.thetradedesk.com/confluence/display/EN/AWS+STS+Token+Generation
  > saml2aws login --session-duration=28800
- Set the repo up for ds-sync.
  > ttd-ds init
- Update `.meta/cluster.json` and `.meta/sync.json` as required. See `project/cluster.json` for a working config file. 
- Start the cluster. DS sync should sync the scala source files and compile the project remotely.
  > ttd-ds cluster start
- Run an ETL job.
  > spark-submit --deploy-mode cluster --class job.PositiveLabelGenerator --executor-memory 100G --executor-cores 16 --conf "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC" --conf spark.driver.memory=110G --conf spark.driver.cores=15 --conf spark.sql.shuffle.partitions=1400 --conf spark.default.parallelism=1400 --conf spark.driver.maxResultSize=50G --conf spark.dynamicAllocation.enabled=true --conf spark.memory.fraction=0.7 --conf spark.memory.storageFraction=0.25 --conf spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED --conf spark.sql.autoBroadcastJoinThreshold=-1 "--driver-java-options=-Dlog4j2.formatMsgNoLookups=true -Dspark.sql.objectHashAggregate.sortBased.fallbackThreshold=4096 -Ddate=2023-07-26 -DjobExperimentName=research -Dttd.DailyBidsImpressionsDataset.experimentName=research -Dttd.AdGroupPolicyDataset.experimentName=research -Dttd.DailyBidRequestDataset.experimentName=research -Dttd.DailyConversionDataset.experimentName=research  -Dttd.DailyPositiveBidRequestDataset.experimentName=research -Dttd.DailyPositiveCountSummaryDataset.experimentName=research -Dttd.DailyPositiveCountSummaryDataset.isInChain=true -Dttd.env=prodTest" kongming/target/scala-2.12/kongming.jar

## To generate production jar
To generate kongming_production.jar, we need to push a tag named like ```kongming_production_.*```, to trigger job kongming:deploy_production. There are two ways to do so:
- From command line: Run src\main\bash\update_production_tag.sh to tag kongming_production to the latest commit.
- From gitlab UI: 
  - step 1: go to **Code>Tags** page, Url: https://gitlab.adsrvr.org/thetradedesk/teams/aifun/mlplatform/-/tags
  - step 2: click the **New Tag** on the uppper right
  - step 3: In the **New Tag** page: put ```kongming_production_<date>``` (e.g. kongming_production_20240929235900) as **Tag name**; leave Message **empty** as default (**Important!!!**)
  - step 4: click the **Create tag** button at the bottom

