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




----
-- Read only LOGIN `feature_store_ro`. Use this account for inspecting data in the DB.
----
IF NOT EXISTS(
    SELECT *
    FROM sys.syslogins
    WHERE name = 'feature_store_ro' )
BEGIN

    CREATE LOGIN feature_store_ro
    WITH PASSWORD = --- FILL ME IN FROM 1PASSWORD prod.mlplatform.featurestore.rouser
         ,DEFAULT_DATABASE = mlplatform

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
    WITH PASSWORD = --- FILL ME IN FROM 1PASSWORD prod.mlplatform.featurestore.featurestoreserviceuser
         , DEFAULT_DATABASE = mlplatform

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
