using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using BenchTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.ETL;
using Sparrow;
using Sparrow.Json;

namespace Bench4._0
{
    class Program
    {

        static void Main(string[] args)
        {
            if (args.Length == 1 && args[0].Equals("local",StringComparison.InvariantCultureIgnoreCase))
            {
                new BenchTest().DoTest().Wait();
            }            
            else if (args.Length == 5)
            {
                new BenchTest(args[0], args[1], args[2], Int32.Parse(args[3]), long.Parse(args[4])).DoTest().Wait();
            }
            else
            {
                Console.WriteLine($@"Please use the bench tool as following:
1) Either use with the ""local"" parameter, like ""dotnet bench.4.0.dll local"", in this case, we will use 3 local servers with ports 8080, 8081, 8082 and doc count of 10_000. 
OR
2) Pass 4 parameters: dotnet bench.4.0.dll http://[host1]:port1 http://[host2]:port2 http://[host3]:port3 [documents amount] [cache size in MB]");
                
            }
        }


    }
}
