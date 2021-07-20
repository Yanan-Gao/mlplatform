----
-- Setup database, schema Roles and Users
-- Should only run this once, but, added conditionals to allow re-runs for local testing
----

----
-- Create Database `mlplatform`
----
IF NOT EXISTS ( SELECT * FROM sys.databases WHERE name='mlplatform')
BEGIN
 CREATE DATABASE mlplatform
END
GO


USE mlplatform
GO

----
-- Create Schema `featurestore` under `mlplatform`
----
IF NOT EXISTS ( SELECT * FROM information_schema.schemata WHERE schema_name = 'featurestore' )
BEGIN
    EXEC ('CREATE SCHEMA featurestore') -- get around limitation that CREATE SCHEMA needs to be the only statement in a batch
END
GO

----
-- Create role `API`. This role is meant to be used by the Feature Store Service.
-- Access should map to API operations which for now are on tables only, i.e., SELECT, INSERT, UPDATE, DELETE
----
IF DATABASE_PRINCIPAL_ID('API') IS NULL
BEGIN
    CREATE ROLE API;
END
GO



----
-- Read only LOGIN `feature_store_ro`. Use this account for inspecting data in the DB.
----
IF NOT EXISTS(
    SELECT *
    FROM sys.syslogins
    WHERE name = 'feature_store_ro' )
BEGIN

    CREATE LOGIN feature_store_ro
    WITH PASSWORD = 'fs!RO!1234', DEFAULT_DATABASE = mlplatform

END
GO


----
-- Feature Store Service LOGIN `feature_store_service`. This is the login allocation to the Feature Store service
----
IF NOT EXISTS(
    SELECT *
    FROM sys.syslogins
    WHERE name = 'feature_store_service' )
BEGIN

    CREATE LOGIN feature_store_service
    WITH PASSWORD = 'fs!SRV!1234', DEFAULT_DATABASE = mlplatform

END
GO

----
-- User creation. 1-1 mapping to login names.
----


----
-- `feature_store_ro` user.
----
USE mlplatform
GO
IF NOT EXISTS ( SELECT * FROM sys.database_principals WHERE type = 'S' AND name = 'feature_store_ro')
BEGIN
    CREATE USER [feature_store_ro] FOR LOGIN [feature_store_ro] WITH DEFAULT_SCHEMA = [featurestore]
END

----
-- `feature_store_service` user.
----
USE mlplatform
GO
IF NOT EXISTS ( SELECT * FROM sys.database_principals WHERE type = 'S' AND name = 'feature_store_service')
BEGIN
    CREATE USER [feature_store_service] FOR LOGIN [feature_store_service] WITH DEFAULT_SCHEMA = [featurestore]
END
