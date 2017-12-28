using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Protocols;
using Raven.Client.Documents;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace BenchClient
{
    public class Store
    {
        public static Lazy<DocumentStore> Node1Instance = new Lazy<DocumentStore>(()=>
        {
            return TestInstance.GenerateStore(TestInstance.Node1Url, "Bench");
        });
        public static Lazy<DocumentStore> Node2Instance = new Lazy<DocumentStore>(() =>
        {
            return TestInstance.GenerateStore(TestInstance.Node2Url, "Bench");
        });
        public static Lazy<DocumentStore> Node3Instance = new Lazy<DocumentStore>(() =>
        {
            return TestInstance.GenerateStore(TestInstance.Node3Url, "Bench");
        });
        public static BenchTests.BenchTest TestInstance;

        static Store()
        {
            var configuration = new ConfigurationBuilder()
           .SetBasePath(Directory.GetCurrentDirectory())
           .AddJsonFile("appsettings.json").Build();

            var node1Url = configuration["node1Url"];
            var node2Url = configuration["node2Url"];
            var node3Url = configuration["node3Url"];
            var documentsCount = configuration["documentsCoung"];
            var cacheSizeInMB = configuration["cacheSizeInMB"];


            TestInstance = new BenchTests.BenchTest(node1Url ?? "http://localhsot:8080", 
                node2Url ?? "http://localhsot:8081", 
                node3Url ?? "http://localhsot:8082", 
                documentsCount != null ? Int32.Parse(documentsCount) : 100_000, 
                cacheSizeInMB!= null? long.Parse(cacheSizeInMB):1000
                );           

        }
    }
}
