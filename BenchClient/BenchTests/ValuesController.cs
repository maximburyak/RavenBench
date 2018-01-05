using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace BenchClient.Controllers
{
    [Route("Operations")]
    public class ValuesController : Controller
    {
        [HttpGet("ResetDatabase")]
        public async Task ResetDatabase()
        {
            await Store.TestInstance.ResetDatabase(Store.Node1Instance.Urls[0], "bench");
            Store.Node1Instance = null;
        }

        [HttpGet("SerialStores")]
        public async Task GetSerialStores()
        {
            await Store.TestInstance.SerialStores(Store.Node1Instance);
        }

        [HttpGet("ParallelStores/{parallelism:int}")]
        public async Task GetParallelStores(int parallelism)
        {
            await Store.TestInstance.ParallelStores(Store.Node1Instance, parallelism);
        }

        [HttpGet("SerialBatchStores")]
        public async Task SerialBatchStores()
        {
            await Store.TestInstance.SerialBatchStores(Store.Node1Instance);
        }

        [HttpGet("ParallelBatchStores/{parallelism:int}")]
        public async Task ParallelBatchStores(int parallelism)
        {
            await Store.TestInstance.ParallelBatchStores(Store.Node1Instance, parallelism);
        }

        [HttpGet("BulkInsert")]
        public async Task BulkInsert()
        {
            await Store.TestInstance.BulkInsert(Store.Node1Instance);
        }

        [HttpGet("ParallelBulkInserts/{parallelism:int}")]
        public async Task ParallelBulkInserts(int parallelism)
        {
            await Store.TestInstance.ParallelBulkInserts(Store.Node1Instance, parallelism);
        }

        [HttpGet("SimpleMapIndexingAllResults/{noCaching:bool}")]
        public async Task BulkInsert(bool noCaching)
        {
            await Store.TestInstance.SimpleMapIndexingAllResults(Store.Node1Instance, noCaching);
        }

        [HttpGet("LoadDocumentsSerially")]
        public async Task LoadDocumentsSerially()
        {
            await Store.TestInstance.LoadDocumentsSerially(Store.Node1Instance);
        }

        [HttpGet("LoadDocumentsByIdIn100DocsBatches")]
        public async Task LoadDocumentsByIdIn100DocsBatches()
        {
            await Store.TestInstance.LoadDocumentsByIdIn100DocsBatches(Store.Node1Instance);
        }

        [HttpGet("LoadDocumentsParallelly/{parallelism:int}")]
        public async Task LoadDocumentsParallelly(int parallelism)
        {
            await Store.TestInstance.LoadDocumentsParallelly(Store.Node1Instance, parallelism);
        }

        [HttpGet("LoadDocumentsParallellyIn100DocsBatches/{parallelism:int}")]
        public async Task LoadDocumentsParallellyIn100DocsBatches(int parallelism)
        {
            await Store.TestInstance.LoadDocumentsParallellyIn100DocsBatches(Store.Node1Instance, parallelism);
        }

        [HttpGet("SimpleMap100Queries/{noCaching:bool}")]
        public async Task SimpleMap100Queries(bool noCaching)
        {
            await Store.TestInstance.SimpleMap100QueriesAllResults(Store.Node1Instance, noCaching);
        }

        [HttpGet("SimpleMap100Queries/{noCaching:bool}/{parallelism:int}")]
        public async Task SimpleMap100Queries(bool noCaching, int parallelism)
        {
            await Store.TestInstance.SimpleMap100QueriesParallelAllResults(Store.Node1Instance, parallelism, noCaching);
        }

        [HttpGet("SimpleMap100Queries/{noCaching:bool}")]
        public async Task SimpleQueryWithSimpleTransformer(bool noCaching)
        {
            await Store.TestInstance.SimpleQueryWithSimpleTransformer(Store.Node1Instance, noCaching);
        }

        [HttpGet("SimpleMap100Queries/{noCaching:bool}/{parallelism:int}")]
        public async Task SimpleParallel100QueriesWithSimpleTransformer(bool noCaching, int parallelism)
        {
            await Store.TestInstance.SimpleParallel100QueriesWithSimpleTransformer(Store.Node1Instance, parallelism, noCaching);
        }

        [HttpGet("SimpleQueryWithComplexTransformer/{noCaching:bool}")]
        public async Task SimpleParallel100QueriesWithSimpleTransformer(bool noCaching)
        {
            await Store.TestInstance.SimpleQueryWithComplexTransformer(Store.Node1Instance, noCaching);
        }

        [HttpGet("Simple100ParallelQueriesWithComplexTransformer/{noCaching:bool}/{parallelism:int}")]
        public async Task Simple100ParallelQueriesWithComplexTransformer(bool noCaching, int parallelism)
        {
            await Store.TestInstance.Simple100ParallelQueriesWithComplexTransformer(Store.Node1Instance, parallelism, noCaching);
        }

        [HttpGet("Simple100ParallelQueriesWithComplexTransformer")]
        public async Task SimpleMapIndexingStreamingAllResults()
        {
            await Store.TestInstance.SimpleMapIndexingStreamingAllResults(Store.Node1Instance);
        }

        [HttpGet("SubscriptionsCheckLatency")]
        public async Task SubscriptionsCheckLatency()
        {
            await Store.TestInstance.SubscriptionsCheckLatency(Store.Node1Instance);
        }

        [HttpGet("SubscriptionsSingleItemBatch")]
        public void SubscriptionsSingleItemBatch()
        {
            Store.TestInstance.SubscriptionsSingleItemBatch(Store.Node1Instance);
        }

        [HttpGet("Subscriptions1KItemsBatch")]
        public void Subscriptions1KItemsBatch()
        {
            Store.TestInstance.Subscriptions1KItemsBatch(Store.Node1Instance);
        }

        [HttpGet("Subscriptions20InParallel200ItemsBatch")]
        public void Subscriptions20InParallel200ItemsBatch()
        {
            Store.TestInstance.Subscriptions20InParallel200ItemsBatch(Store.Node1Instance);
        }




    }
}
