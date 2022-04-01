## Geronimo

Geronimo is the shared code and datasets for koa v4.

### Deployment:
Geronimo is published to nexus as snapshot on each merge request. Releases are deployed on merge to master

to reference gernomio from other projects add:

``credentials += Credentials(
"Sonatype Nexus Repository Manager", "nexus.adsrvr.org", System.getenv("NEXUS_MAVEN_READ_USER"), System.getenv("NEXUS_MAVEN_READ_PASS"))
resolvers += "TTDNexusSnapshots" at "https://nexus.adsrvr.org/repository/ttd-snapshot"
resolvers += "TTDNexusReleases" at "https://nexus.adsrvr.org/repository/ttd-release"
``

and

``"com.thetradedesk" %% "geronimo" % <version>``

to your build.sbt file