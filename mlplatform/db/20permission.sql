----
-- Runs after `setup.sql` to set permissions for roles and users
----

----
-- API role for services that wish to connect.
----
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA :: featurestore TO API
ALTER ROLE API ADD MEMBER feature_store_service;

----
-- Read only logins/users.
----
ALTER ROLE db_datareader ADD MEMBER feature_store_ro;
