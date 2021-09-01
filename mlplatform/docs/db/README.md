
[[_TOC_]]

Feature Store (FS) uses a dockerized Microsoft SQL Server as its Database (DB) [^TTDDBAS].

# Architecture (Testing DB vs Prod DB)

# Testing DB vs Prod DB

We support 3 instances of the dockerized MSSQL setup,

1. one for the `local` environment
    * Spin up test database with test data
      * in repo root, run `ttd runr deploydatabases` or `ttd runr dev`
      * repo will be accessible via the following:
        * hostname = 127.0.0.1,1436
        * username = sa
        * password = MyC0m9l&xP@ssw0rd
    * To tear down db, run `ttd runr down`
2. one for the `test` environment
    * server `TestMLPlatformDb.adsrvr.org`
3. for the `prod` environment
    * server: `mlplatformdb.adsrvr.org`

The `test` instance should be a *super set* of the `prod` database--it might have extra schemas and tables used for testing.
This setup allows us to

1. test ideas in `testing` before going to `prod`
1. seed data from `prod` into `testing` for experimenting/testing/root cause with ease

# Contributions

All sql code *must* be reviewed by the team and optionally by a DBA if needed.
All sql code is under the [`db`[(../../db)] folder inside `mlplatform`.

1. Create a pull request with your changes and your plan for rolling this to `prod` 
1. Get it reviewed by the team ([#scrum-aifun](https://thetradedesk.slack.com/archives/C01RMJ10G79))
1. Arrange with the team for deployment 




[^TTDDBAS]: [Database As A Service](https://atlassian.thetradedesk.com/confluence/x/GgudBw)
