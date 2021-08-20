----
-- Runs after `premission.sql` to create base tables
----
use mlplatform
go

IF (NOT EXISTS (select *
                 from INFORMATION_SCHEMA.TABLES
                 where TABLE_SCHEMA = 'featurestore'
                 and  TABLE_NAME = 'Feature'))
begin
    create table dbo.Feature(
        FeatureId BIGINT IDENTITY(1,1),
        FeatureName varchar(64) unique not null,
        FeatureDescription varchar(128) not null,
        FeatureTaxonomy varchar(128),
        CreatedDateUtc datetime default getutcdate() not null,
        LastModifiedDateUtc datetime,

        constraint pk_Feature_FeatureId primary key (FeatureId)
    )
end


-- INSERT INTO Feature (FeatureName, FeatureDescription)
-- VALUES ('testFeature1','testing the db table')

IF (NOT EXISTS (select *
                 from INFORMATION_SCHEMA.TABLES
                 where TABLE_SCHEMA = 'featurestore'
                 and  TABLE_NAME = 'FeatureVersion'))
begin
   create table dbo.FeatureVersion(
       FeatureId BIGINT not null,
       FeatureVersion INT not null,
       CodePath varchar(128) not null
       DocumentationPath varchar(128) not null,
       DataLocation varchar(128) not null,
       SchemaString varchar(max) not null,
       CreatedDateUtc datetime default getutcdate() not null,
       LastModifiedDateUtc datetime,

       constraint pk_FeatureId_FeatureVersion primary key (FeatureId, FeatureVersion),
       constraint fk_FeatureVersion_FeatureId foreign key (FeatureId) references Feature(FeatureId)
    )
end

-- INSERT INTO FeatureVersion (FeatureId, FeatureVersion,CodePath,DocumentationPath,DataLocation,SchemaString)
-- VALUES (1,1,'s3://thetradedesk-mlplatform-us-east-1/libs/plutus/jars/prod/plutus.jar','https://ttdcorp-my.sharepoint.com/:w:/r/personal/michael_davy_thetradedesk_com/_layouts/15/guestaccess.aspx?e=6HJxdR&share=Edmad7vPi31FsGkrDLzT0J8Bvmkrk4udwLyPQibq_N-QfQ','s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod','col1:string,col2:int')

IF (NOT EXISTS (select *
                 from INFORMATION_SCHEMA.TABLES
                 where TABLE_SCHEMA = 'featurestore'
                 and  TABLE_NAME = 'FeatureIdLookup'))
begin
    create table dbo.FeatureIdLookup(
        InternalFeatureId BIGINT unique not null,
        PublicPrefix VARCHAR(3) not null constraint FeatureIdLookup_publicPrefix default ('fid'),
        PublicFeatureId BIGINT IDENTITY(1,1) not null,
        CreatedDateUtc datetime default getutcdate() not null,
        LastModifiedDateUtc datetime,

        constraint pk_PublicFeatureId_InternalFeatureId primary key (PublicFeatureId, InternalFeatureId),
        constraint fk_FeatureIdLookup_InternalFeatureId foreign key (InternalFeatureId) references dbo.Feature(FeatureId)
    )
end

-- INSERT INTO FeatureIdLookup
-- (InternalFeatureId, PublicFeatureId)
-- VALUES (1)

IF (NOT EXISTS (select *
                 from INFORMATION_SCHEMA.TABLES
                 where TABLE_SCHEMA = 'featurestore'
                 and  TABLE_NAME = 'FeatureTags'))
begin
    create table dbo.FeatureTags(
        FeatureId BIGINT not null,
        TagName varchar(128) not null,
        CreatedDateUtc datetime default getutcdate() not null,
        LastModifiedDateUtc datetime,

        constraint pk_FeatureId_TagName primary key (FeatureId, TagName),
        constraint fk_FeatureTags_FeatureId foreign key (FeatureId) references dbo.Feature(FeatureId),
        constraint uq_FeatureId_TagName unique clustered(TagName,FeatureId) --caveat: sorting perf for the clustered index on the tagname may be nonideal
    )
end

-- INSERT INTO FeatureTags
-- (FeatureId, TagName,TagValue)
-- VALUES (1,'myTagName1')

IF (NOT EXISTS (select *
                 from INFORMATION_SCHEMA.TABLES
                 where TABLE_SCHEMA = 'featurestore'
                 and  TABLE_NAME = 'FeatureDatasets'))
begin
    create table dbo.FeatureDatasets(  --aka TimeTravelTable etc
        FeatureId BIGINT not null,
        FeatureVersion INT not null,
        FeatureProductionDate datetime default getutcdate() not null, --FeatureProductionDate is the date which the data is produced for. This could be different than the date the record is written on (CreatedDateUtc), in the case of a failed job from previous day
        FeatureInputStart datetime,
        FeatureInputEnd datetime,
        CreatedDateUtc datetime default getutcdate() not null,
        LastModifiedDateUtc datetime,

        constraint pk_FeatureId_FeatureVersion_FeatureProductionDate_FeatureInputStart_FeatureInputEnd primary key (FeatureId, FeatureVersion, FeatureProductionDate, FeatureInputStart, FeatureInputEnd),
        constraint fk_FeatureDatasets_FeatureId foreign key (FeatureId) references dbo.Feature(FeatureId)
    )
end

-- INSERT INTO FeatureDatasets
-- (FeatureId, FeatureVersion, FeatureInputStart, FeatureInputEnd)
-- VALUES (1,1,'2021-08-06','2021-08-11')