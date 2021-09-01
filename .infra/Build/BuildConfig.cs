using TTD.Common.Filesystem;
using TTD.DevInfra.Build.Common;
using TTD.DevInfra.Build.Definitions;

namespace Build
{
    class BuildConfig
    {
        public static BuildDefinition Definition = new BuildDefinition(
            buildGlobs: default,
            nugetPackageGlobs: default,
            unitTestsGlobs: default,
            e2eTestsGlobs: default,
            applications: default,
            components: new[]
            {
                new ComponentDefinition("mssql")
                    .WithEnvironmentDeployment("Development",
                        new KustomizeComponent((RelativePath) "mlplatform/k8s/feature-store/db/mssql/overlays/development",
                            portForwardType: "service", portForwardPort: "1436:1436",
                            waitSpec: "-l app=mssql --for=condition=ready --timeout 60s pod", waitRetryCount: 3))
            },
            databases: new []
            {
                new DbMigratorDefinition(
                    "MLPlatform", 
                    (RelativePath) "mlplatform/db/migrations"
                )            
                    // This database uses two optional idempotent script directories, see https://gitlab.adsrvr.org/thetradedesk/dbmigrator#defining-idempotent-changes-stored-procedures-and-functions
                    .WithIdempotentScriptDirectory((RelativePath)"mlplatform/db/stored-procedures")
                    .WithIdempotentScriptDirectory((RelativePath)"mlplatform/db/functions")
                    // Optional- define a regex that will be used to verify migration script file names. Defaults to ^[0-9]{6}\.  
                    //.WithMigrationScriptPrefixPattern("^[0-9]{3}\\.")
                    // Defines two deployment environments, using sample data for the development environment
                    // Test data (i.e. migration scripts defined with a `*.test.sql` file extension) allow development/ CI environments to be populated with a set of testing/ sample data
                    .WithEnvironmentDeployment("Development", new DbMigratorDeployment(useTestData: true))
                    .WithEnvironmentDeployment("Production", new DbMigratorDeployment(useTestData: false))
            }
        );
    }
}