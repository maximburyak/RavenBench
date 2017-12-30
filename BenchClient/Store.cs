using BenchTests;
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
        private static DocumentStore m_node1Instance;
        public static DocumentStore Node1Instance
        {
            get
            {
                if (m_node1Instance == null)
                {
                    m_node1Instance = TestInstance?.GenerateStore(TestInstance.Node1Url, "Bench");
                }
                return m_node1Instance;
            }
            set
            {
                m_node1Instance = value;
            }
        }


        private static DocumentStore m_node2Instance;
        public static DocumentStore Node2Instance
        {
            get
            {
                if (m_node2Instance == null)
                {
                    m_node2Instance = TestInstance?.GenerateStore(TestInstance.Node2Url, "Bench");
                }
                return m_node2Instance;
            }
            set
            {
                m_node2Instance = value;
            }
        }

        private static DocumentStore m_node3Instance;
        public static DocumentStore Node3Instance
        {
            get
            {
                if (m_node3Instance == null)
                {
                    m_node1Instance = TestInstance.GenerateStore(TestInstance.Node3Url, "Bench");
                }
                return m_node3Instance;
            }
            set
            {
                m_node3Instance = value;
            }
        }


        public static BenchTest TestInstance;

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

        public static void ChangeConfig(string node1Url, string node2Url, string node3Url,int? documentsCount, long? cacheSizeInMB)
        {
             TestInstance = new BenchTests.BenchTest(node1Url ?? "http://localhsot:8080",
                node2Url ?? "http://localhsot:8081",
                node3Url ?? "http://localhsot:8082",
                documentsCount != null ? (int)(documentsCount) : TestInstance?.DocumentsCount ?? 100_000,
                cacheSizeInMB != null ? (int)cacheSizeInMB  : TestInstance?.DocumentsCount ?? 1000
                );
            Node1Instance = TestInstance.GenerateStore(TestInstance.Node1Url, "Bench");
            Node2Instance = TestInstance.GenerateStore(TestInstance.Node2Url, "Bench");
            Node3Instance = TestInstance.GenerateStore(TestInstance.Node3Url, "Bench");
        }
    }
}
