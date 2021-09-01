using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Runr.Hosting;
using TTD.DevInfra.Build.Common;
using TTD.DevInfra.Build.Common.Tasks;

namespace Build
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            var runner = new GenericHostTaskRunner()
                .ConfigureDefaultHostEnvironment("Development")
                .ConfigureHost((host, _) => host
                    .ConfigureServices((context, services) =>
                    {
                        services.RegisterCommonBuildServices(BuildConfig.Definition, context.Configuration);
                        services.RegisterTaskNodes(Assembly.GetExecutingAssembly());
                    })
                    .ConfigureAppConfiguration((hostContext, configBuilder) => configBuilder
                        .AddJsonFile("appsettings.json")
                        .AddJsonFile($"appsettings.{hostContext.HostingEnvironment.EnvironmentName}.json", optional: false)
                        .AddEnvironmentVariables("RUNR_")
                    )
                )
                .ConfigureCommandLine(cmdBuilder => cmdBuilder
                    .AddCommonBuildOptions());

            return await runner.RunWithDefault<EnsureToolsPresent>(args);
        }

    }
}
