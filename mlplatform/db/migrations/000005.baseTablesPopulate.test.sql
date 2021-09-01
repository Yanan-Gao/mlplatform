-- In the future we probably want to break these up further
-- into PopulateFeature.sql, PopulateFeatureVersion.sql

INSERT INTO featurestore.Feature (FeatureName, FeatureDescription)
VALUES ('testFeature1','testing the db table')

INSERT INTO featurestore.FeatureVersion (FeatureId, FeatureVersion,CodePath,DocumentationPath,DataLocation,SchemaString)
VALUES (1,1,'s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/prod/plutus.jar','http://tradedesk/confluence/1','s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod','col1:string,col2:int')

INSERT INTO featurestore.FeatureIdLookup
(InternalFeatureId, CreatedDateUtc)
VALUES (1, getutcdate())

INSERT INTO featurestore.FeatureTags
(FeatureId, TagName)
VALUES (1,'myTagName1')

INSERT INTO featurestore.FeatureDatasets
(FeatureId, FeatureVersion, FeatureInputStart, FeatureInputEnd)
VALUES (1,1,'2021-08-06','2021-08-11')