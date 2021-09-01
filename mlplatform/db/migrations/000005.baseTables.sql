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
    create table featurestore.Feature(
        FeatureId BIGINT IDENTITY(1,1),
        FeatureName varchar(64) unique not null,
        FeatureDescription varchar(128) not null,
        FeatureTaxonomy varchar(128),
        CreatedDateUtc datetime default getutcdate() not null,
        LastModifiedDateUtc datetime,

        constraint pk_Feature_FeatureId primary key (FeatureId)
    )
end

IF (NOT EXISTS (select *
                from INFORMATION_SCHEMA.TABLES
                where TABLE_SCHEMA = 'featurestore'
                and  TABLE_NAME = 'FeatureVersion'))
begin
   create table featurestore.FeatureVersion(
       FeatureId BIGINT not null,
       FeatureVersion INT not null,
       CodePath varchar(1024) not null,
       DocumentationPath varchar(1024) not null,
       DataLocation varchar(max) not null,
       SchemaString varchar(max) not null,
       CreatedDateUtc datetime default getutcdate() not null,
       LastModifiedDateUtc datetime,

       constraint pk_FeatureId_FeatureVersion primary key (FeatureId, FeatureVersion),
       constraint fk_FeatureVersion_FeatureId foreign key (FeatureId) references featurestore.Feature(FeatureId)
    )
end

IF (NOT EXISTS (select *
                from INFORMATION_SCHEMA.TABLES
                where TABLE_SCHEMA = 'featurestore'
                and  TABLE_NAME = 'FeatureIdLookup'))
begin
    create table featurestore.FeatureIdLookup(
        InternalFeatureId BIGINT unique not null,
        PublicPrefix VARCHAR(3) not null constraint FeatureIdLookup_publicPrefix default ('fid'),
        PublicFeatureId BIGINT IDENTITY(1,1) not null,
        CreatedDateUtc datetime default getutcdate() not null,
        LastModifiedDateUtc datetime,

        constraint pk_PublicFeatureId_InternalFeatureId primary key (PublicFeatureId, InternalFeatureId),
        constraint fk_FeatureIdLookup_InternalFeatureId foreign key (InternalFeatureId) references featurestore.Feature(FeatureId)
    )
end

IF (NOT EXISTS (select *
                from INFORMATION_SCHEMA.TABLES
                where TABLE_SCHEMA = 'featurestore'
                and  TABLE_NAME = 'FeatureTag'))
begin
    create table featurestore.FeatureTag(
        FeatureId BIGINT not null,
        TagName varchar(128) not null,
        CreatedDateUtc datetime default getutcdate() not null,
        LastModifiedDateUtc datetime,

        constraint pk_FeatureId_TagName primary key (FeatureId, TagName),
        constraint fk_FeatureTag_FeatureId foreign key (FeatureId) references featurestore.Feature(FeatureId),
        constraint uq_FeatureId_TagName unique clustered(TagName,FeatureId) --caveat: sorting perf for the clustered index on the tagname may be nonideal
    )
end

IF (NOT EXISTS (select *
                from INFORMATION_SCHEMA.TABLES
                where TABLE_SCHEMA = 'featurestore'
                and  TABLE_NAME = 'FeatureDataset'))
begin
    create table featurestore.FeatureDataset(  --aka TimeTravelTable etc
        FeatureId BIGINT not null,
        FeatureVersion INT not null,
        FeatureProductionDate datetime default getutcdate() not null, --FeatureProductionDate is the date which the data is produced for. This could be different than the date the record is written on (CreatedDateUtc), in the case of a failed job from previous day
        FeatureInputStart datetime,
        FeatureInputEnd datetime,
        CreatedDateUtc datetime default getutcdate() not null,
        LastModifiedDateUtc datetime,

        constraint pk_FeatureId_FeatureVersion_FeatureProductionDate_FeatureInputStart_FeatureInputEnd primary key (FeatureId, FeatureVersion, FeatureProductionDate, FeatureInputStart, FeatureInputEnd),
        constraint fk_FeatureDataset_FeatureId foreign key (FeatureId) references featurestore.Feature(FeatureId)
    )
end
