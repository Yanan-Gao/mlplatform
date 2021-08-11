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
-- Verify
----
-- SELECT s.name AS schema_name,
--        s.schema_id,
--        u.name AS schema_owner
-- FROM sys.schemas AS s
-- INNER JOIN sys.sysusers AS u
--         ON u.uid = s.principal_id
-- ORDER BY s.name
----
