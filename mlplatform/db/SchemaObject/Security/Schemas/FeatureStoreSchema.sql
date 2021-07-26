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


