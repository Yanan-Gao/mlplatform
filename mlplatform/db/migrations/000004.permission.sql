----
-- Runs after `setup.sql` to set permissions for roles and users
----

----
-- API role for services that wish to connect.
----
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA :: featurestore TO API
ALTER ROLE API ADD MEMBER feature_store_service;


----
-- Verify 
----
-- SELECT name, createdate
-- FROM sysusers
-- WHERE issqlrole = 1
----

----
-- Read only logins/users.
----
ALTER ROLE db_datareader ADD MEMBER feature_store_ro;

----
-- Verify
----
-- SELECT DP1.name AS DatabaseRoleName,
--        isnull (DP2.name, 'No members') AS DatabaseUserName
--  FROM sys.database_role_members AS DRM
--  RIGHT OUTER JOIN sys.database_principals AS DP1
--    ON DRM.role_principal_id = DP1.principal_id
--  LEFT OUTER JOIN sys.database_principals AS DP2
--    ON DRM.member_principal_id = DP2.principal_id
-- WHERE DP1.type = 'R'
-- ORDER BY DP1.name;
----
