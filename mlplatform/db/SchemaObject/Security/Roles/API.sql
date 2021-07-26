----
-- Create role `API`. This role is meant to be used by the Feature Store Service.
-- Access should map to API operations which for now are on tables only, i.e., SELECT, INSERT, UPDATE, DELETE
----
IF DATABASE_PRINCIPAL_ID('API') IS NULL
BEGIN
    CREATE ROLE API;
END
GO


