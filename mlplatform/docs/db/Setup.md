
[[_TOC_]]

Feature Store (FS) uses a dockerized Microsoft SQL Server as its Database (DB) [^TTDDBAS].
This file documents our setup. SQL code is reviewed (by team and DBA experts) 
and stored in this repo under the [db folder](../../db)



# RDMBS information 

## Databases 

### `mlplatform`

Schemas 

1. `featurestore`

## Logins and Users

* Login : who can login to the *server*
* User : RDBMS entity that can access databases

We keep a 1-1 mapping between login names and usernames. 

1. `feature_store_ro` -- read only account 
1. `feature_store_service` -- REST service account 

The best practise here is to use the minimal set of permissions for each user/login/role that they need to get the job done.
As a minimum default

1. Read Only Account: For read only access 
1. Application specific: For each application that needs access to a DB within the RDBMS



# One time setup 

Run (you can copy paste into your DB IDE) the following files this order 

1. [10setup.sql](../../db/10setup.sql)
1. [20permissions.sql](../../db/20permission.sql)

## Roles 

* `API` role for Services requesting access. 

## Logins 

* `feature_store_ro` -- read only account for inspecting data 
* `feature_store_service` -- used by the Feature Store service

# Tools 

## Docker image for local testing 

Microsoft (MS) provides [docker images for MSSQL](https://hub.docker.com/_/microsoft-mssql-server).
Even though these docker images provided by MS are not *identical* to the docker image provided by TTD DBAs, 
they are useful as a local testing setup for MSSQL code.

## Azure Data Studio

1. [Download Azure Data Studio](https://docs.microsoft.com/en-us/sql/azure-data-studio/download-azure-data-studio?view=sql-server-ver15)
1. [Connect to DB Quickstart](https://docs.microsoft.com/en-us/sql/azure-data-studio/quickstart-sql-server?view=sql-server-ver15)

# References 

* [MS SQL Server Docs](https://docs.microsoft.com/en-us/sql/sql-server/?view=sql-server-ver15) 
* [MS SQL public docker images](https://hub.docker.com/_/microsoft-mssql-server) -- Good for playing around with SQL code and testing
* [#dev-linux-mssql-on-k8](https://thetradedesk.slack.com/archives/CFESY29C6) TTD Slack channel for dockerized MSSQL. 
Use this channel for getting help with the dockerized solution of MSSQL and ask for DBA feedback/reviews. 

## Examples from `adplatform`s code 

DBAs recommended we see these files (and any included files) as larger examples of how logins and permissions are used in `adplatform`

1. [Create Users](https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/474273f87da43dd3058e7ffadbc0092341ec107f/DB/SchedReporting/SchedReporting/Scripts/Post-Deployment/CreateUsersAndLogins.sql)
1. [Add Permissions](https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/474273f87da43dd3058e7ffadbc0092341ec107f/DB/Provisioning/ProvisioningDB/Scripts/Post-Deployment/Permissions.sql)



[^TTDDBAS]: [Database As A Service](https://atlassian.thetradedesk.com/confluence/x/GgudBw)