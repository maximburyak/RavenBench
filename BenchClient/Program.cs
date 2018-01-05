using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace BenchClient
{
    public class Program
    {
        public static void Main(string[] args)
        {
            BuildWebHost(args).Run();
        }

        public static IWebHost BuildWebHost(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = 200;
            ThreadPool.SetMinThreads(50, 50);

            var configuration = new ConfigurationBuilder()
           .SetBasePath(Directory.GetCurrentDirectory())
           .AddJsonFile("appsettings.json").Build();

            var listeningPort = configuration["port"];
            IWebHostBuilder webHostBuilder = WebHost.CreateDefaultBuilder(args);
            if (string.IsNullOrEmpty(listeningPort) == false)
                webHostBuilder = webHostBuilder.UseUrls($"http://*:{listeningPort}");
            return webHostBuilder.UseStartup<Startup>()
                .Build();
        }
    }
}
