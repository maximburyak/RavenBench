using Raven.Client;
using Raven.Client.Documents;
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
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BenchTests
{
    public static class SubsExtentions
    {
        private class ActionObserver<T> : IObserver<T>
        {
            private readonly Action<T> _onNext;
            private readonly Action<Exception> _onError;
            private readonly Action _onCompleted;

            public ActionObserver(Action<T> onNext, Action<Exception> onError = null, Action onCompleted = null)
            {
                _onNext = onNext;
                _onError = onError;
                _onCompleted = onCompleted;
            }

            public void OnCompleted()
            {
                _onCompleted?.Invoke();
            }

            public void OnError(Exception error)
            {
                _onError?.Invoke(error);
            }

            public void OnNext(T value)
            {
                _onNext(value);
            }
        }

        public static IDisposable Subscribe<T>(this IObservable<T> self, Action<T> onNext, Action<Exception> onError)
        {
            return self.Subscribe(new ActionObserver<T>(onNext, onError));
        }
    }

    public class BenchResult
    {
        public long Duration;
        public long DocumentsCount;
        public long FailureCount;
        public long SuccessCount;
        public long AverageRequestDuration;
        public long MaxReauestDuration = 0;
        public long MinRequestDuration = long.MaxValue;
        public long MaxErrorDuration = 0;
        public long MinErrorDuration = long.MaxValue;
        public long Iterations = 0;
        public ConcurrentBag<Exception> Errors = new ConcurrentBag<Exception>();

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append($"Duration: {Duration}ms{Environment.NewLine}");
            sb.Append($"Rate: {Duration / 1000 / DocumentsCount}ops/sec{Environment.NewLine}");
            sb.Append($"Success count: {SuccessCount}{Environment.NewLine}");
            sb.Append($"FiluerCount count: {SuccessCount}{Environment.NewLine}");

            return sb.ToString();
            
        }
    }

    public class BenchResultCollector
    {       
        private Stopwatch _sp;

        BenchResult _result;
        public BenchResultCollector(bool startStopwatch = true)
        {
            _sp = new Stopwatch();
            if (startStopwatch)
            {
                _sp.Start();
            }
            
            _result = new BenchResult();
        }

        public void Start()
        {
            _sp.Start();
        }
        public BenchResult GetResult()
        {
            _sp.Stop();
            _result.Duration = _sp.ElapsedMilliseconds;
            return _result;
        }

        public void RegisterOperation(Func<long> operation)
        {
            var operationDuration = Stopwatch.StartNew();
            try
            {
                var docCount = operation();
                operationDuration.Stop();
                long elapsedMilliseconds = operationDuration.ElapsedMilliseconds;
                ProcessSuccess(docCount, elapsedMilliseconds);
            }
            catch (Exception ex)
            {

            }
        }

        private void ProcessSuccess(long docCount, long elapsedMilliseconds)
        {
            Interlocked.Increment(ref _result.SuccessCount);
            Interlocked.Add(ref _result.DocumentsCount, docCount);
            while (true)
            {
                var curMax = _result.MaxReauestDuration;

                if (curMax < elapsedMilliseconds)
                {
                    if (Interlocked.CompareExchange(ref _result.MaxReauestDuration, elapsedMilliseconds, curMax) == elapsedMilliseconds)
                        break;
                }
                else
                {
                    break;
                }
            }
            while (true)
            {
                var curMin = _result.MinRequestDuration;

                if (curMin > elapsedMilliseconds)
                {
                    if (Interlocked.CompareExchange(ref _result.MinRequestDuration, elapsedMilliseconds, curMin) == elapsedMilliseconds)
                        break;
                }
                else
                {
                    break;
                }
            }
        }

        private void ProcessFailure(Exception exception, long elapsedMilliseconds)
        {
            Interlocked.Increment(ref _result.FailureCount);
            _result.Errors.Add(exception);
            while (true)
            {
                var curMax = _result.MaxErrorDuration;

                if (curMax < elapsedMilliseconds)
                {
                    if (Interlocked.CompareExchange(ref _result.MaxErrorDuration, elapsedMilliseconds, curMax) == elapsedMilliseconds)
                        break;
                }
                else
                {
                    break;
                }
            }
            while (true)
            {
                var curMin = _result.MinErrorDuration;

                if (curMin > elapsedMilliseconds)
                {
                    if (Interlocked.CompareExchange(ref _result.MinErrorDuration, elapsedMilliseconds, curMin) == elapsedMilliseconds)
                        break;
                }
                else
                {
                    break;
                }
            }
        }

        public async Task RegisterOperationAsync(Func<Task<long>> operation)
        {
            try
            {
                var operationDuration = Stopwatch.StartNew();
                var docCount = await operation().ConfigureAwait(false);
                operationDuration.Stop();

                long elapsedMilliseconds = operationDuration.ElapsedMilliseconds;

                while (true)
                {
                    var curMax = _result.MaxReauestDuration;

                    if (curMax < elapsedMilliseconds)
                    {
                        if (Interlocked.CompareExchange(ref _result.MaxReauestDuration, elapsedMilliseconds, curMax) == elapsedMilliseconds)
                            break;
                    }
                    else
                    {
                        break;
                    }
                }
                while (true)
                {
                    var curMin = _result.MinRequestDuration;

                    if (curMin > elapsedMilliseconds)
                    {
                        if (Interlocked.CompareExchange(ref _result.MinRequestDuration, elapsedMilliseconds, curMin) == elapsedMilliseconds)
                            break;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                ProcessFailure(ex,)
            }
        }
    } 


    public class BenchTest
    {
        public int DocumentsCount = 1000;
        public string Node1Url = "http://localhost:8080";
        public string Node2Url = "http://localhost:8081";
        public string Node3Url = "http://localhost:8082";
        public long CacheSizeInMB = 5000;

        public BenchTest()
        {
        }

        public BenchTest(string nodeUrl1, string nodeUrl2, string nodeUrl3, int documentsCount, long cacheSizeInMB)
        {
            Node1Url = nodeUrl1;
            Node2Url = nodeUrl2;
            Node3Url = nodeUrl3;
            DocumentsCount = documentsCount;
            CacheSizeInMB = cacheSizeInMB;
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public DateTime BirthDate { get; set; }
            public long Balance { get; set; }
            public List<User> Children { get; set; }
            public string UniqueId { get; internal set; }
        }

        public class AddNodeToClusterCommand : RavenCommand
        {
            public string NodeToAddUrl;
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/cluster/node?url={NodeToAddUrl}&watcher=false";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put
                };
            }
        }

        public async Task SetupCluster(DocumentStore node1, DocumentStore node2, DocumentStore node3)
        {
            RequestExecutor requestExecutor = node1.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var getClusterTopologyCommand = new GetClusterTopologyCommand();
                await requestExecutor.ExecuteAsync(getClusterTopologyCommand, context);
                if (getClusterTopologyCommand.Result.Topology.AllNodes.Count == 3)
                    return;
                await requestExecutor.ExecuteAsync(new AddNodeToClusterCommand { NodeToAddUrl = node2.Urls[0] }, context);
                await requestExecutor.ExecuteAsync(new AddNodeToClusterCommand { NodeToAddUrl = node3.Urls[0] }, context);
            }
        }
        
        public async Task DoTest()
        {
            ServicePointManager.DefaultConnectionLimit = 200;
            ThreadPool.SetMinThreads(50, 50);

            using (var store1 = GenerateStore(Node1Url, "Bench"))
            {
                // Serial Stores
                await ResetDatabase(Node1Url, "Bench");
                await SerialStores(store1);
                await SerialStores(store1);

                // Parallel Stores
                await ResetDatabase(Node1Url, "Bench");
                await ParallelStores(store1, 1);
                await ParallelStores(store1, 1);

                // Parallel stores X2 parallelism
                await ResetDatabase(Node1Url, "Bench");
                await ParallelStores(store1, 2);
                await ParallelStores(store1, 2);

                // Serial Batches
                await ResetDatabase(Node1Url, "Bench");
                await SerialBatchStores(store1);
                await SerialBatchStores(store1);

                // Parallel Batches
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBatchStores(store1, 1);
                await ParallelBatchStores(store1, 1);


                // Parallel Batches x2 parallelism
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBatchStores(store1, 2);
                await ParallelBatchStores(store1, 2);

                // Bulk insert
                await ResetDatabase(Node1Url, "Bench");
                await BulkInsert(store1);
                await BulkInsert(store1);

                // Parallel bullk inserts
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await ParallelBulkInserts(store1, 1);

                // Parallel bulk insert x2 parallelism
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 2);
                await ParallelBulkInserts(store1, 2);

                // Load documents serially
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await LoadDocumentsSerially(store1);
                await ParallelBulkInserts(store1, 1);
                await LoadDocumentsSerially(store1);

                // Load documents parallely
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await LoadDocumentsParallelly(store1, 1);
                await ParallelBulkInserts(store1, 1);
                await LoadDocumentsParallelly(store1, 1);

                // Load documents parallely x2 parallelism
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await LoadDocumentsParallelly(store1, 2);
                await ParallelBulkInserts(store1, 1);
                await LoadDocumentsParallelly(store1, 2);

                // Simple map indexing
                await ResetDatabase(Node1Url, "Bench");
                await StoreSingleDoc(store1, "Bench");
                await SimpleMapIndexing(store1);
                await StoreSingleDoc(store1, "Bench");
                await SimpleMapIndexing(store1);

                // Simple map indexing single document no caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleMapIndexQueryWithSingleDocumentResults(store1, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleMapIndexQueryWithSingleDocumentResults(store1, true);

                // Simple map indexing single document with caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleMapIndexQueryWithSingleDocumentResults(store1, false);
                await ParallelBulkInserts(store1, 1);
                await SimpleMapIndexQueryWithSingleDocumentResults(store1, false);

                // Simple map indexing all results no caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleMapQueryAllResults(store1, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleMapQueryAllResults(store1, true);

                // Simple map indexing all results with caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleMapQueryAllResults(store1, false);
                await ParallelBulkInserts(store1, 1);
                await SimpleMapQueryAllResults(store1, false);

                // Simple map indexing all results streaming
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleMapIndexingStreamingAllResults(store1);
                await ParallelBulkInserts(store1, 1);
                await SimpleMapIndexingStreamingAllResults(store1);

                // Map reduce indexing
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await MapReduceIndexingAllResults(store1);
                await ParallelBulkInserts(store1, 1);
                await MapReduceIndexingAllResults(store1);

                // Map reduce 1to1 indexing no caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await MapReduce1To1IndexingAllResults(store1, true);
                await ParallelBulkInserts(store1, 1);
                await MapReduce1To1IndexingAllResults(store1, true);

                // Map reduce 1to1 indexing with caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await MapReduce1To1IndexingAllResults(store1, false);
                await ParallelBulkInserts(store1, 1);
                await MapReduce1To1IndexingAllResults(store1, false);

                // Simple map 100 queries no caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleMap100QueriesAllResults(store1, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleMap100QueriesAllResults(store1, true);

                // Simple map 100 queries with caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleMap100QueriesAllResults(store1, false);
                await ParallelBulkInserts(store1, 1);
                await SimpleMap100QueriesAllResults(store1, false);

                // Simple map 100 queries parallel no caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelMapQueriesAllResults(store1, 1, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelMapQueriesAllResults(store1, 1, true);

                // Simple map 100 queries parallel with caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelMapQueriesAllResults(store1, 1, false);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelMapQueriesAllResults(store1, 1, false);

                // Simple map 100 queres parallel x2 parallelism no caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelMapQueriesAllResults(store1, 2, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelMapQueriesAllResults(store1, 2, true);

                // Simple map 100 queres parallel x2 parallelism with caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelMapQueriesAllResults(store1, 2, false);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelMapQueriesAllResults(store1, 2, false);

                // Simple query with simple transformer no caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleQueryWithSimpleTransformer(store1, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleQueryWithSimpleTransformer(store1, true);

                // Simple query with simple transformer with caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleQueryWithSimpleTransformer(store1, false);
                await ParallelBulkInserts(store1, 1);
                await SimpleQueryWithSimpleTransformer(store1, false);

                // Simple 100 queries with simple transformer
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithSimpleTransformer(store1, 1);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithSimpleTransformer(store1, 1);

                // Simple 100 queries with simple transformer x2 parallelism 
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithSimpleTransformer(store1, 2);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithSimpleTransformer(store1, 2);

                // Simple 100 queries with simple transformer, no caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithSimpleTransformer(store1, 1, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithSimpleTransformer(store1, 1, true);

                // Simple 100 queries with simple transformer x2 parallelism 
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithSimpleTransformer(store1, 2, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithSimpleTransformer(store1, 2, true);

                // Simple query with complex transformer no caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleQueryWithComplexTransformer(store1, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleQueryWithComplexTransformer(store1, true);

                // Simple query with complex transformer with caching
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleQueryWithComplexTransformer(store1, false);
                await ParallelBulkInserts(store1, 1);
                await SimpleQueryWithComplexTransformer(store1, false);

                // Simple 100 queries with complex transformer
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithComplexTransformer(store1, 1);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithComplexTransformer(store1, 1);

                // Simple 100 queries with complex transformer x2 parallelism
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithComplexTransformer(store1, 2);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithComplexTransformer(store1, 2);

                // Simple 100 queries with complex transformer
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithComplexTransformer(store1, 1, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithComplexTransformer(store1, 1, true);

                // Simple 100 queries with complex transformer x2 parallelism
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithComplexTransformer(store1, 2, true);
                await ParallelBulkInserts(store1, 1);
                await SimpleFullParallelQueriesWithComplexTransformer(store1, 2, true);

                // Subscription Single item batches
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                SubscriptionsSingleItemBatch(store1);
                await ParallelBulkInserts(store1, 1);
                SubscriptionsSingleItemBatch(store1);

                // Subscriptions 1k items batch
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                Subscriptions1KItemsBatch(store1);
                await ParallelBulkInserts(store1, 1);
                Subscriptions1KItemsBatch(store1);

                // Subscriptions 20 in parallel 200 items batch
                await ResetDatabase(Node1Url, "Bench");
                await ParallelBulkInserts(store1, 1);
                Subscriptions20InParallel200ItemsBatch(store1);
                await ParallelBulkInserts(store1, 1);
                Subscriptions20InParallel200ItemsBatch(store1);

                // replication stuff
                using (var store2 = GenerateStore(Node2Url, string.Empty))
                using (var store3 = GenerateStore(Node3Url, string.Empty))
                {
                    // replication in cluster
                    await ResetDatabase(Node1Url, "Bench");
                    await SetupCluster(store1, store2, store3);
                    await AddDatabaseToNode("Bench", leader: store1, storeToAddTo: store2);
                    await WaitForReplication(store1);
                    await ParallelBatchStores(store1, 1, waitForReplication: false);
                    await WaitForReplication(store1);
                    await ParallelBatchStores(store1, 1, waitForReplication: false);
                    await WaitForReplication(store1);


                    // external replication
                    await ResetDatabase(Node1Url, "ExBench1", "A");
                    await ResetDatabase(Node1Url, "ExBench2", "B");
                    await SetupReplicationAsync(store1, store2, "ExBench1", "ExBench2");
                    var lastId = await StoreSingleDoc(store1, "ExBench1");
                    await WaitForExternalReplication(store1, store2, "ExBench1", "ExBench2", lastId, "External");
                    (_,lastId) = await ParallelBatchStores(store1, 1, "ExBench1", retreieveLastId: true);
                    await WaitForExternalReplication(store1, store2, "ExBench1", "ExBench2", lastId, "External");
                    (_,lastId) = await ParallelBatchStores(store1, 1, "ExBench1", retreieveLastId: true);
                    await WaitForExternalReplication(store1, store2, "ExBench1", "ExBench2", lastId, "External");

                    // ETL replication
                    await ResetDatabase(Node1Url, "ETLBench1", "A");
                    await ResetDatabase(Node1Url, "ETLBench2", "B");
                    await SetupETLRavenReplication(store1, store2, "ETLBench1", "ETLBench2");
                    lastId = await StoreSingleDoc(store1, "ExBench1");
                    await WaitForExternalReplication(store1, store2, "ETLBench1", "ETLBench2", lastId, "ETL");
                    (_,lastId ) = await ParallelBatchStores(store1, 1, "ETLBench1", retreieveLastId: true);
                    await WaitForExternalReplication(store1, store2, "ETLBench1", "ETLBench2", lastId, "ETL");
                    (_, lastId) = await ParallelBatchStores(store1, 1, "ETLBench1", retreieveLastId: true);
                    await WaitForExternalReplication(store1, store2, "ETLBench1", "ETLBench2", lastId, "ETL");
                }
            }

            using (var fileStream = new FileStream("output.csv", FileMode.Create))
            using (var streamWriter = new StreamWriter(fileStream))
            {
                foreach (var result in BenchResults)
                {
                    streamWriter.WriteLine($"\"{result.TestDescription}\",\"{result.Duration}\"");
                }
            }
        }

        private List<(string TestDescription, long Duration)> BenchResults = new List<(string TestDescription, long Duration)>();

        private void LogProgress(string testDescription, long duration)
        {
            Console.WriteLine($"{testDescription}; Duration: {duration}");
            BenchResults.Add((testDescription, duration));

        }

        public async Task<string> StoreSingleDoc(DocumentStore store, string documentDatabase)
        {
            using (var session = store.OpenAsyncSession(documentDatabase))
            {
                var user = GenerateUser(1, 30);
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
                return session.Advanced.GetDocumentId(user);
            }
        }

        public async Task AddDatabaseToNode(string databaseName, DocumentStore leader, DocumentStore storeToAddTo)
        {
            RequestExecutor requestExecuter = leader.GetRequestExecutor();
            if (requestExecuter.TopologyNodes.Count == 3)
                return;
            using (requestExecuter.ContextPool.AllocateOperationContext(out var context))
            {
                Dictionary<string, string> clusterTopology = null;

                while (clusterTopology == null || clusterTopology.Count < 3)
                {
                    GetClusterTopologyCommand getTopologyCommand = new GetClusterTopologyCommand();
                    await leader.GetRequestExecutor().ExecuteAsync(getTopologyCommand
                        , context);
                    clusterTopology = getTopologyCommand.Result.Topology.Members;
                }



                var nodeToAddTo = clusterTopology.Where(x => string.Equals(x.Value, storeToAddTo.Urls[0], StringComparison.InvariantCultureIgnoreCase)).First();
                await leader.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName, nodeToAddTo.Key));
            }
        }

        public async Task ResetDatabase(string url, string databaseName, string nodeTagToCreateIn = "A", X509Certificate2 cert = null)
        {
            using (var store = GenerateStore(url, databaseName, cert))
            {
                try
                {
                    var databaseNames = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, 1024));
                    if (databaseNames.Contains(databaseName))
                        await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, true, timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
                }
                catch { }

                while (true)
                {
                    var databaseNames = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, 1024));
                    if (databaseNames.Contains(databaseName) == false)
                        break;
                }

                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord
                {
                    DatabaseName = databaseName,
                    Topology = new DatabaseTopology
                    {
                        Members = new List<string> { nodeTagToCreateIn }
                    }

                }));

            }
        }

        public async Task RunInParallel(int multiplier, Func<int, Task> getAsyncAction, int? iterations = null)
        {
            var parallelismLevel = Environment.ProcessorCount * multiplier;
            
            if (iterations.HasValue== false)
            {
                iterations = parallelismLevel;
            }
            var tasks = Enumerable.Range(0, iterations.Value).Select(i =>
            {
                return getAsyncAction(i);
            });
            for (var i = 0; i < iterations;)
            {
                var itemsToTake = parallelismLevel;

                if (i + parallelismLevel >= iterations)
                {
                    itemsToTake = iterations.Value - i;
                }

                var tasksToRun = tasks.Skip(i).Take(itemsToTake).ToList();
                await Task.WhenAll(tasksToRun.ToArray());
                i += itemsToTake;
            }
        }

        public async Task<BenchResult> SerialStores(DocumentStore store,bool setIDs = true, int childWidth = 30)
        {
            var collector = new BenchResultCollector();            
            var sp = Stopwatch.StartNew();
            for (int i = 0; i < DocumentsCount; i++)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(GenerateUser(i, childWidth, setIDs));
                        await session.SaveChangesAsync();
                    }
                    return 1;
                });
            }            

            LogProgress($"Serial stores", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }

        public async Task<BenchResult> ParallelStores(DocumentStore store, int multiplier, bool setIDs = true, int childWidth = 30)
        {
            var collector = new BenchResultCollector();
                        
            var sp = Stopwatch.StartNew();

            await RunInParallel(multiplier,
                async i =>
                {
                    await collector.RegisterOperationAsync(async () =>
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(GenerateUser(i, childWidth, setIDs));
                            await session.SaveChangesAsync();
                        }
                        return 1;
                    });

                },
                DocumentsCount);

            
            LogProgress($"Parallel stores (x{multiplier})", sp.ElapsedMilliseconds);
            return collector.GetResult();

        }
        
        public async Task<BenchResult> SerialBatchStores(DocumentStore store, bool setIDs = true, int childWidth = 30, int batchSize = 100)
        {
            var collector = new BenchResultCollector();
            var sp = Stopwatch.StartNew();
            
            for (int i = 0; i < DocumentsCount/batchSize; i++)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        for (var j = 0; j < batchSize; j++)
                            await session.StoreAsync(GenerateUser(i * (DocumentsCount / batchSize) + j, 30, setIDs));
                        await session.SaveChangesAsync();
                    }
                    return batchSize;
                });
            }
            

            LogProgress($"Serial batches stores", sp.ElapsedMilliseconds);
            return collector.GetResult();

        }
        public async Task<(BenchResult,string)> ParallelBatchStores(DocumentStore store, int multiplier, string dbName = null, bool waitForReplication = false, bool retreieveLastId = false, bool setIDs = true, int childWidth = 30, int batchSize = 100)
        {
            var collector = new BenchResultCollector();
            var idsAndEtags = new ConcurrentBag<(string DocId, int Etag)>();

            var sp = Stopwatch.StartNew();

            await RunInParallel(multiplier, async i =>
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession(dbName))
                    {
                        User lastEntity = null;
                        for (var j = 0; j < batchSize; j++)
                        {
                            lastEntity = GenerateUser(i * (DocumentsCount / batchSize) + j, childWidth, setIDs);
                            await session.StoreAsync(lastEntity);
                        }

                        if (waitForReplication)
                            session.Advanced.WaitForReplicationAfterSaveChanges();

                        await session.SaveChangesAsync();

                        if (retreieveLastId)
                        {
                            var metadata = session.Advanced.GetMetadataFor(lastEntity);
                            var changeVector = metadata[Constants.Documents.Metadata.ChangeVector] as string;
                            var id = metadata[Constants.Documents.Metadata.Id] as string;
                            var etag = changeVector.Split(',')
                            .Select(y => y.Split(':')[1].Split('-')[0])
                            .OrderByDescending(Int32.Parse)
                            .Select(Int32.Parse)
                            .First();
                            idsAndEtags.Add((id, etag));
                        }
                    }
                    return batchSize;
                });
            }, DocumentsCount/ batchSize);
            

            LogProgress($"Parallel batches stores (x{multiplier}) {(waitForReplication ? (" and waited for replication") : (""))}", sp.ElapsedMilliseconds);

            if (retreieveLastId)
                return (collector.GetResult(), idsAndEtags.OrderByDescending(x => x.Etag).Select(x => x.DocId).First());
            else
                return (collector.GetResult(), string.Empty);


        }
        private User GenerateUser(int i, int childWidth, bool setId = true)
        {
            return new User
            {
                Id = setId?i.ToString():null,
                Name = (i % 10).ToString(),
                Age = i,
                Balance = i * 4,
                BirthDate = DateTime.Now,
                UniqueId = i.ToString(),
                Children = Enumerable.Range(1, childWidth).Select(x => new User
                {
                    Name = (i % 10).ToString(),
                    Age = i,
                    Balance = i * 2,
                    BirthDate = DateTime.Now,
                    UniqueId = $"{i}_{x}",
                }).ToList()
            };
        }

        public async Task<BenchResult> BulkInsert(DocumentStore store,bool setIDs = true, int childWidth = 30)
        {            
            var collector = new BenchResultCollector();
            var sp = Stopwatch.StartNew();
            
            using (var bi = store.BulkInsert())
            {
                for (var i = 0; i < DocumentsCount; i++)
                {
                    await collector.RegisterOperationAsync(async () =>
                    {
                        await bi.StoreAsync(GenerateUser(i, childWidth, setIDs));
                        return 1;
                    });
                }
            };
            
            LogProgress($"BulkInsert", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> ParallelBulkInserts(DocumentStore store, int multiplier, bool setIDs = true, int childWidth = 30)
        {
            var collector = new BenchResultCollector();
            var sp = Stopwatch.StartNew();
            
            await RunInParallel(multiplier,
                async i =>
                {
                    await collector.RegisterOperationAsync(async () =>
                    {
                        using (var session = store.BulkInsert())
                        {
                            for (var j = 0; j < DocumentsCount / Environment.ProcessorCount * multiplier; j++)
                                await session.StoreAsync(GenerateUser(i * (DocumentsCount / Environment.ProcessorCount * multiplier) + j, childWidth, setIDs));
                        }
                        return DocumentsCount / Environment.ProcessorCount * multiplier;
                    });
                }, Environment.ProcessorCount * multiplier);

            
            LogProgress($"Parallel bulk inserts (x{multiplier})", sp.ElapsedMilliseconds);

            return collector.GetResult();
        }

        public async Task<BenchResult> SingleDocPatchesSerial(DocumentStore store)
        {
            
            List<string> IDs = new List<string>();
            await GetDocumentIDs(store, IDs);

            var sp = Stopwatch.StartNew();
            var collector = new BenchResultCollector();


            for (var i = 0; i < DocumentsCount; i++)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        session.Advanced.Increment<User, int>(IDs[i], x => x.Age, 2);
                        await session.SaveChangesAsync();
                    }
                    return 1;
                });
            }
            

            LogProgress($"SingleDocBatchesOneByOne", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }

        public async Task<BenchResult> ParallelSingleDocPatches(DocumentStore store, int modifier)
        {
            BlockingCollection<string> IDsBC = new BlockingCollection<string>();
            List<string> IDs = new List<string>();
            await GetDocumentIDs(store, IDs);
            IDs.ForEach(IDsBC.Add);

            var collector = new BenchResultCollector();
            var sp = Stopwatch.StartNew();
            
            await RunInParallel(modifier,
                async i =>
                {
                    await collector.RegisterOperationAsync(async () =>
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            IDsBC.TryTake(out string id);
                            session.Advanced.Increment<User, int>(id, x => x.Age, 2);
                            await session.SaveChangesAsync();
                        }
                        return 1;
                    });
                }, IDsBC.Count);
            
            LogProgress($"Load {DocumentsCount} documents parallely x{modifier}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }

        public async Task<BenchResult> LoadDocumentsSerially(DocumentStore store)
        {            
            List<string> IDs = new List<string>();
            await GetDocumentIDs(store, IDs);
            var sp = Stopwatch.StartNew();

            var collector = new BenchResultCollector();
            
            foreach (var id in IDs)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                        await session.LoadAsync<User>(id);
                    return 1;
                });                
            }
            
            LogProgress($"Load {DocumentsCount} documents serially", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> LoadDocumentsParallelly(DocumentStore store, int modifier)
        {
            BlockingCollection<string> IDsBC = new BlockingCollection<string>();
            List<string> IDs = new List<string>();            
            await GetDocumentIDs(store, IDs);
            IDs.ForEach(IDsBC.Add);

            var collector = new BenchResultCollector();
            
            var sp = Stopwatch.StartNew();            

            await RunInParallel(modifier,
                async i =>
                {
                    await collector.RegisterOperationAsync(async () =>
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            IDsBC.TryTake(out string id);
                            await session.LoadAsync<User>(id);
                        }
                        return 1;
                    });
                
                }, IDsBC.Count);
            
            LogProgress($"Load {DocumentsCount} documents parallely x{modifier}", sp.ElapsedMilliseconds);
                        
            return collector.GetResult();
        }

        private static User ExtractUser(BlittableJsonReaderObject y)
        {
            User result = new User();
            result.Age = (int)(long)y["Age"];
            result.Balance = (int)(long)y["Balance"];
            result.BirthDate = DateTime.Parse(y["BirthDate"].ToString());            
            result.Name = y["Name"].ToString();
            result.UniqueId = y["UniqueId"].ToString();
            result.Children = new List<User>();
            var children = y["Children"] as BlittableJsonReaderArray;
            if (children != null)
            {
                foreach (BlittableJsonReaderObject child in children)
                {
                    result.Children.Add(ExtractUser(child));
                }
            }

            return result;
        }

        public async Task<BenchResult> LoadDocumentsParallellyIn100DocsBatches(DocumentStore store, int modifier)
        {
            BlockingCollection<List<string>> IDsBC = new BlockingCollection<List<string>>();
            List<string> IDs = new List<string>();
            await GetDocumentIDs(store, IDs);
            
            for (var i=0; i< DocumentsCount / 100; i++)
            {
                IDsBC.Add(IDs.Skip(i*100).Take(100).ToList());
            }

            var sp = Stopwatch.StartNew();

            var collector = new BenchResultCollector();
            
            await RunInParallel(modifier,
                async i =>
                {
                    await collector.RegisterOperationAsync(async () =>
                    {
                        List<string> ids;
                        using (var session = store.OpenAsyncSession())
                        {
                            IDsBC.TryTake(out ids);
                            await session.Advanced.LoadIntoStreamAsync(ids, new MemoryStream());
                        }
                        return ids.Count; 
                    });
                }, IDsBC.Count);

            LogProgress($"Load {DocumentsCount} documents in 100 docs batches  parallely x{modifier}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        
        public async Task<BenchResult> LoadDocumentsByIdIn100DocsBatches(DocumentStore store, int batchSize = 100)
        {
            List<string> IDs = new List<string>();
            await GetDocumentIDs(store, IDs);
            var sp = Stopwatch.StartNew();

            var collector = new BenchResultCollector();

            for (var i = 0; i < DocumentsCount / batchSize; i++)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.LoadAsync<User>(IDs.Skip(i * batchSize).Take(batchSize));
                    }
                    return batchSize;
                });
            }
            LogProgress($"Load {DocumentsCount} documents in 100 docs batches", sp.ElapsedMilliseconds);

            return collector.GetResult();
        }

        private async Task GetDocumentIDs(DocumentStore store, List<string> IDs)
        {
            using (var session = store.OpenAsyncSession())
            {
                var stream = await session.Advanced.StreamAsync(
                session.Query<User>()                    
                    .Take(DocumentsCount)                    
                    .Select(x=>x.Id)
                    .OfType<string>()
                    );
                while (await stream.MoveNextAsync())
                {
                    IDs.Add(stream.Current.Document);
                }
            }
        }

        public async Task<BenchResult> SimpleMapIndexing(DocumentStore store)
        {
            var collector = new BenchResultCollector();
            var sp = Stopwatch.StartNew();
            
            using (var session = store.OpenAsyncSession())
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    await session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromHours(10)))
                        .Where(x => x.Age < -1)
                        .Take(DocumentsCount)
                        .ToListAsync();
                    return DocumentsCount;
                });
            }
            
            LogProgress($"Simple map index, no results", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> SimpleMapIndexQueryWithSingleDocumentResults(DocumentStore store, bool noCaching)
        {
            var sp = Stopwatch.StartNew();
            var collector = new BenchResultCollector();
            
            for (var i = 0; i < DocumentsCount; i++)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.Query<User>()
                            .Customize(x =>
                            {
                                x.WaitForNonStaleResults();
                                if (noCaching)
                                    x.NoCaching();
                            })
                            .Where(x => x.Age == i)
                            .ToListAsync();
                    }
                    return 1;
                });
            }
            LogProgress($"Simple map index, {DocumentsCount} queries with unique single results {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> SimpleMapQueryAllResults(DocumentStore store, bool noCaching, int batchSize = 100)
        {
            var sp = Stopwatch.StartNew();
            var collector = new BenchResultCollector();            
            for (var i = 0; i < DocumentsCount / 100; i++)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var min = i * batchSize;
                        var max = i * batchSize + batchSize;

                        await session.Query<User>()
                            .Customize(x =>
                            {
                                x.WaitForNonStaleResults();
                                if (noCaching)
                                    x.NoCaching();
                            })
                            .Where(x => x.Age >= min && x.Age <= max)                            
                            .ToListAsync();
                    }
                    return batchSize;
                });
            }
            
            LogProgress($"Simple map index, all results {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }

        public async Task<BenchResult> SimpleMapIndexingStreamingAllResults(DocumentStore store)
        {
            var sp = Stopwatch.StartNew();
            
            using (var session = store.OpenAsyncSession())
            {
                await session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Age == 1)
                            .Take(1).ToListAsync();
            }

            var collector = new BenchResultCollector();

            await collector.RegisterOperationAsync(async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    var stream = await session.Advanced.StreamAsync<User>(
                    session.Query<User>()
                        .Where(x => x.Age != -1)
                        .Take(DocumentsCount)
                        );
                    while (await stream.MoveNextAsync()) ;
                    return DocumentsCount;
                }
            });

            LogProgress($"Simple map index streaming, all results", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> MapReduceIndexingAllResults(DocumentStore store)
        {
            var collector = new BenchResultCollector();
            var sp = Stopwatch.StartNew();
            using (var session = store.OpenAsyncSession())
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    var res = await session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .GroupBy(x => x.Name, x => 1, (keyName, g) => new
                        {
                            Name = keyName,
                            Amount = g.Count()
                        })
                        .Take(DocumentsCount)
                        .ToListAsync();
                    return DocumentsCount;
                });
            }
            LogProgress($"Map reduce indexing, all results", sp.ElapsedMilliseconds);

            return collector.GetResult();
        }
        public async Task<BenchResult> MapReduce1To1IndexingAllResults(DocumentStore store, bool noCaching, int batchSize = 100)
        {
            var collector = new BenchResultCollector();
            var sp = Stopwatch.StartNew();
            for (var i = 0; i < DocumentsCount / batchSize; i++)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var min = i * batchSize;
                        var max = i * batchSize + batchSize;
                    
                        await session.Query<User>()
                            .Customize(x =>
                            {
                                x.WaitForNonStaleResults();
                                if (noCaching)
                                    x.NoCaching();
                            })
                            .GroupBy(x => x.Age,
                                x => 1,
                                (keyName, g) => new
                                {
                                    Age = keyName,
                                    Amount = g.Count()
                                }
                            )
                            .Where(x => x.Age >= min && x.Age <= max)                            
                            .ToListAsync();
                        return batchSize;
                    }
                });
            }
            LogProgress($"Map reduce 1-1 indexing, all results, 100 docs batches {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> SimpleMap100QueriesAllResults(DocumentStore store, bool noCaching, int batchSize = 100)
        {
            var sp = Stopwatch.StartNew();
            var collector = new BenchResultCollector();
            for (int i = 0; i < 100; i++)
            {
                for (var j = 0; j < DocumentsCount / batchSize; j++)
                {
                    await collector.RegisterOperationAsync(async () =>
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            var min = j * batchSize;
                            var max = j * batchSize + batchSize;

                            await session.Query<User>()
                                .Customize(x =>
                                {
                                    x.WaitForNonStaleResults();
                                    if (noCaching)
                                        x.NoCaching();
                                })
                                .Where(x => x.Age >= min && x.Age <= max)
                                .ToListAsync();
                            return batchSize;
                        }
                    });
                }
            }
            LogProgress($"Simple 100 map index queries, 100 docs batches {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> SimpleFullParallelMapQueriesAllResults(DocumentStore store, int modifier, bool noCaching, int batchSize = 100)
        {
            var sp = Stopwatch.StartNew();

            var collector = new BenchResultCollector();
            await RunInParallel(modifier,
                async i =>
                {
                    for (var j = 0; j < DocumentsCount / batchSize; j++)
                    {
                        await collector.RegisterOperationAsync(async () =>
                        {
                            using (var session = store.OpenAsyncSession())
                            {
                                var min = j * batchSize;
                                var max = j * batchSize + batchSize;

                                await session.Query<User>()
                                .Customize(x =>
                                {
                                    x.WaitForNonStaleResults();
                                    if (noCaching)
                                        x.NoCaching();
                                })
                                .Where(x => x.Age >= min && x.Age <= max)
                                .ToListAsync();
                                return batchSize;
                            }
                        });
                    }
                });
            LogProgress($"Simple map index, 100 queries parallel (x{modifier})all results {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }

        public async Task<BenchResult> SimpleMapQueriesParallelAllResults(DocumentStore store, int modifier, bool noCaching, int batchSize = 100)
        {
            var sp = Stopwatch.StartNew();
            var collector = new BenchResultCollector();
            await RunInParallel(modifier,
                async i =>
                {
                    await collector.RegisterOperationAsync(async () =>
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            var min = i * batchSize;
                            var max = i * batchSize + batchSize;

                            await session.Query<User>()
                            .Customize(x =>
                            {
                                x.WaitForNonStaleResults();
                                if (noCaching)
                                    x.NoCaching();
                            })
                            .Where(x => x.Age >= min && x.Age <= max)                            
                            .ToListAsync();
                            return batchSize;
                        }
                    });

                }, DocumentsCount/batchSize);
            LogProgress($"Simple map index, 100 queries parallel (x{modifier})all results {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> SimpleQueryWithSimpleTransformer(DocumentStore store, bool noCaching, int batchSize = 100)
        {
            var sp = Stopwatch.StartNew();

            var collector = new BenchResultCollector();
            for (var i = 0; i < DocumentsCount / batchSize; i++)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var min = i * batchSize;
                        var max = i * batchSize + batchSize;

                        await session.Query<User>()
                             .Customize(x =>
                             {
                                 x.WaitForNonStaleResults();
                                 if (noCaching)
                                     x.NoCaching();
                             })
                            .Select(x => new
                            {
                                x.Name,
                                x.Age
                            })
                            .Where(x => x.Age >= min && x.Age <= max)                   
                            .ToListAsync();
                        return batchSize;
                    }
                });
            }
            LogProgress($"100 queries with simple transformer, 100 docs batches {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> SimpleFullParallelQueriesWithSimpleTransformer(DocumentStore store, int modifier, bool noCaching = false, int batchSize = 100)
        {
            var sp = Stopwatch.StartNew();
            var collector = new BenchResultCollector();
            await RunInParallel(modifier,
                async i =>
                {
                    for (var j = 0; j < DocumentsCount / batchSize; j++)
                    {
                        await collector.RegisterOperationAsync(async () =>
                        {
                            using (var session = store.OpenAsyncSession())
                            {
                                var min = j * batchSize;
                                var max = j * batchSize + batchSize;

                                await session.Query<User>()
                                    .Customize(x =>
                                    {
                                        x.WaitForNonStaleResults();
                                        if (noCaching)
                                            x.NoCaching();
                                    })
                                    .Where(x => x.Age >= min && x.Age <= max)
                                    .Select(x => new
                                    {
                                        x.Name,
                                        x.Age
                                    }).ToListAsync();
                            }
                            return batchSize;
                        });
                    }
                });

            LogProgress($"100 queries parallel with simple transformer (x{modifier}), 100 docs batches {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> SimpleQueryWithComplexTransformer(DocumentStore store, bool noCaching, int batchSize = 100)
        {
            var sp = Stopwatch.StartNew();

            var collector = new BenchResultCollector();
            for (var i = 0; i < DocumentsCount / batchSize; i++)
            {
                await collector.RegisterOperationAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var min = i * batchSize;
                        var max = i * batchSize + batchSize;
                        
                        await session.Query<User>()
                            .Customize(x =>
                            {
                                x.WaitForNonStaleResults();
                                if (noCaching)
                                    x.NoCaching();
                            })
                            .Where(x => x.Age >= min && x.Age <= max)
                            .Select(x => new
                            {
                                x.Name,
                                x.Age,
                                BalanceSum = x.Children.Sum(c => c.Balance + c.Age)
                            })                            
                            .ToListAsync();
                        return batchSize;
                    }
                });
            }
            LogProgress($"100 queries with simple transformer, 100 docs batches {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> SimpleFullParallelQueriesWithComplexTransformer(DocumentStore store, int modifier, bool noCaching = false, int batchSize = 100)
        {
            var sp = Stopwatch.StartNew();
            var collector = new BenchResultCollector();
            await RunInParallel(modifier,
                async i =>
                {
                    for (var j = 0; j < DocumentsCount / batchSize; j++)
                    {
                        await collector.RegisterOperationAsync(async () =>
                        {
                            using (var session = store.OpenAsyncSession())
                            {
                                var min = j * batchSize;
                                var max = j * batchSize + batchSize;

                                await session.Query<User>()
                                    .Customize(x =>
                                    {
                                        x.WaitForNonStaleResults();
                                        if (noCaching)
                                            x.NoCaching();
                                    })
                                    .Where(x => x.Age >= min && x.Age <= max)
                                    .Select(x => new
                                    {
                                        x.Name,
                                        x.Age,
                                        BalanceSum = x.Children.Sum(c => c.Balance + c.Age)
                                    })
                                    
                                    .ToListAsync();
                            }
                            return batchSize;
                        });
                    }
                });
            
            LogProgress($"100 queries parallel with complex transformer (x{modifier}), 100 docs batches {(noCaching ? "no caching" : "with caching")}", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public BenchResult SubscriptionsSingleItemBatch(DocumentStore store)
        {
            var sp = Stopwatch.StartNew();
            var subsName = store.Subscriptions.Create<User>();
            var collection = new BenchResultCollector();
            using (var subscriptionWorker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subsName)
            {
                MaxDocsPerBatch = 1
            }))
            {
                var countdownEvent = new CountdownEvent(DocumentsCount);
                subscriptionWorker.AfterAcknowledgment += async x =>
                {
                    await collection.RegisterOperationAsync(async () =>
                    {
                        countdownEvent.Signal(x.Items.Count);
                        return x.Items.Count;
                    });
                };
                
                var subsTask = subscriptionWorker.Run(x => { });
                collection.RegisterOperation(() =>
                {
                    Task.WaitAny(Task.Run(() => countdownEvent.Wait()), subsTask);
                    return 0;
                });
            }
            LogProgress($"Subscription, single doc batches", sp.ElapsedMilliseconds);
            return collection.GetResult();
        }
        public BenchResult Subscriptions1KItemsBatch(DocumentStore store)
        {
            var sp = Stopwatch.StartNew();
            var collection = new BenchResultCollector();
            var subsName = store.Subscriptions.Create<User>();
            using (var subscriptionWorker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subsName)
            {
                MaxDocsPerBatch = 1000
            }))
            {
                var countdownEvent = new CountdownEvent(DocumentsCount);
                subscriptionWorker.AfterAcknowledgment += async x =>
                {
                    await collection.RegisterOperationAsync(async () =>
                    {
                        countdownEvent.Signal(x.Items.Count);
                        return x.Items.Count;
                    });
                };
                collection.RegisterOperation(() =>
                {
                    Task.WaitAny(Task.Run(()=> countdownEvent.Wait()), subscriptionWorker.Run(x => { }));
                    return 0;
                });
            }
            LogProgress($"Subscription, 1K docs batches", sp.ElapsedMilliseconds);
            return collection.GetResult();
        }
        public async Task<BenchResult> SubscriptionsCheckLatency(DocumentStore store)
        {
            var collector = new BenchResultCollector(false);
            var subsName = store.Subscriptions.Create<User>();
            var sp = new Stopwatch();
            using (var subscriptionWorker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subsName)
            {
                MaxDocsPerBatch = 1000
            }
                ))
            {
                var amre = new AsyncManualResetEvent();
                subscriptionWorker.AfterAcknowledgment += async x =>
                {
                    amre.Set();
                };
                var subsTask = subscriptionWorker.Run(x => { });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(GenerateUser(1, 30));
                    await session.SaveChangesAsync();
                }
                await amre.WaitAsync();
                amre.Reset();
                collector.Start();
                sp.Start();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(GenerateUser(1, 30));
                    await session.SaveChangesAsync();
                }
                await collector.RegisterOperationAsync(async () =>
                {
                    await Task.WhenAny(amre.WaitAsync(), subsTask);
                    return 0;
                });                
            }
            LogProgress($"Subscription, check document latency", sp.ElapsedMilliseconds);
            return collector.GetResult();

        }
        public BenchResult Subscriptions20InParallel200ItemsBatch(DocumentStore store, int parallelism= 20, int batchSize = 200)
        {
            var sp = Stopwatch.StartNew();

            var collector = new BenchResultCollector();
            Parallel.For(0, parallelism,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = parallelism
                },
                i =>
                {
                    var subsName = store.Subscriptions.Create<User>();
                    using (var subscriptionWorker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subsName)
                    {
                        MaxDocsPerBatch = batchSize
                    }
                        ))
                    {
                        var countdownEvent = new CountdownEvent(DocumentsCount);
                        subscriptionWorker.AfterAcknowledgment += async x =>
                        {
                            await collector.RegisterOperationAsync(async () =>
                            {
                                countdownEvent.Signal(x.Items.Count);
                                return x.Items.Count;
                            });
                        };

                        collector.RegisterOperation(() =>
                        {
                            Task.WaitAny(subscriptionWorker.Run(x => { }), Task.Run(() => countdownEvent.Wait()));
                            return 0;
                        });
                        
                    }
                });
            LogProgress($"Subscription, 20 in parallel, 200 docs batches", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> WaitForReplication(DocumentStore store)
        {
            var sp = Stopwatch.StartNew();
            var collector = new BenchResultCollector();
            await collector.RegisterOperationAsync(async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(GenerateUser(DocumentsCount + 1, 30));
                    session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(30));
                    await session.SaveChangesAsync();
                }
                return DocumentsCount + 1;
            });

            LogProgress($"Documents replication", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }
        public async Task<BenchResult> WaitForExternalReplication(DocumentStore source, DocumentStore destination, string sourceDBName, string destinationDBName, string lastId, string replicationType)
        {
            var sp = Stopwatch.StartNew();
            var collector = new BenchResultCollector();
            await collector.RegisterOperationAsync(async () =>
            {
                using (var session = source.OpenAsyncSession(sourceDBName))
                {
                    await WaitForDocumentToReplicateAsync<User>(destination, lastId, destinationDBName);
                }
                return DocumentsCount;
            });
            LogProgress($"Documents {replicationType} replication", sp.ElapsedMilliseconds);
            return collector.GetResult();
        }

        public async Task WaitForDocument(DocumentStore store, string id, string externalDBName)
        {
            using (var session = store.OpenAsyncSession(externalDBName))
            {
                while (true)
                {
                    if (await session.LoadAsync<User>(id) != null)
                        break;
                }
            }
        }
        public async Task WaitForDocumentToReplicateAsync<T>(DocumentStore store, string id, string externalDBName)
            where T : class
        {
            var tcs = new TaskCompletionSource<bool>();

            using (var doc = store.Changes(externalDBName).ForDocument(id).Subscribe(x => tcs.SetResult(x.Id == id), x => { }))
            {
                Task docLoadTask = WaitForDocument(store, id, externalDBName);
                await Task.WhenAny(docLoadTask, tcs.Task);
            }
        }
        public async Task SetupReplicationAsync(DocumentStore fromStore, DocumentStore toStore, string fromDB, string toDB)
        {
            var databaseWatcher = new ExternalReplication(fromDB, $"ConnectionString-{toStore.Urls[0] + " (DB: " + toDB + ")"}");

            await fromStore.Maintenance.ForDatabase(fromDB).SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = databaseWatcher.ConnectionStringName,
                Database = toDB,
                TopologyDiscoveryUrls = toStore.Urls
            }));

            var op = new UpdateExternalReplicationOperation(databaseWatcher);
            await fromStore.Maintenance.ForDatabase(fromDB).SendAsync(op);
        }

        public async Task SetupETLRavenReplication(DocumentStore fromStore, DocumentStore toStore, string fromDB, string toDB)
        {
            var connectionStringName = $"RavenETLConnectionString-{toStore.Urls[0] + " (DB: " + toDB + ")"}";

            await fromStore.Maintenance.ForDatabase(fromDB).SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = connectionStringName,
                Database = toDB,
                TopologyDiscoveryUrls = toStore.Urls
            }));

            var etlConfiguration = new RavenEtlConfiguration()
            {
                Name = "RavenETLUsers",
                ConnectionStringName = connectionStringName,
                Transforms =
                    {
                        new Transformation()
                        {
                            Name = "loadAll",
                            Collections = {"Users"},
                            Script = "loadToUsers(this)"
                        }
                    }
            };

            await fromStore.Maintenance.ForDatabase(fromDB).SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfiguration));
        }


        public DocumentStore GenerateStore(string url, string databaseName, X509Certificate2 cert = null)
        {
            DocumentStore documentStore = new DocumentStore
            {
                Urls = new[] { url },
                Database = databaseName,
                Certificate = cert
            };
            documentStore.Conventions.MaxHttpCacheSize = new Size(this.CacheSizeInMB, SizeUnit.Megabytes);
            var temp = documentStore.Conventions.DeserializeEntityFromBlittable;
            documentStore.Conventions.DeserializeEntityFromBlittable = (x, y) =>
            {
                if (x == typeof(User))
                {
                    User result = ExtractUser(y);

                    return result;
                }
                else
                {
                    return temp(x, y);
                }
                
            };

            documentStore.Initialize();
            return documentStore;
        }
    }
}
