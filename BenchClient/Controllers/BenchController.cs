using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace BenchClient.Controllers
{    
    [Route("Operations")]
    public class BenchController : Controller
    {
        // GET api/values
        
        [HttpGet("ResetDatabase")]
        public async Task ResetDatabase()
        {
            await Store.TestInstance.ResetDatabase(Store.Node1Instance.Urls[0],"Bench", cert:Store.cert);
        }

        [HttpGet("SetDocCount/{docCount:int}")]
        public async Task SetrDocCOunt(int docCount)
        {
            Store.ChangeConfig(Store.Node1Instance.Urls[0], Store.Node2Instance.Urls[0], Store.Node3Instance.Urls[0],docCount, Store.TestInstance.CacheSizeInMB);
        }

        [HttpGet("SerialStores/{setIDs:bool}")]
        public async Task GetSerialStores(bool setIDs)
        {
            await Store.TestInstance.SerialStores(Store.Node1Instance, setIDs);
        }

        [HttpGet("ParallelStores/{parallelism:int}/{setIDs:bool}")]
        public async Task GetParallelStores(int parallelism, bool setIDs)
        {
            await Store.TestInstance.ParallelStores(Store.Node1Instance, parallelism, setIDs);
        }

        [HttpGet("SerialBatchStores/{setIDs:bool}")]
        public async Task SerialBatchStores(bool setIDs)
        {
            await Store.TestInstance.SerialBatchStores(Store.Node1Instance, setIDs);
        }
        
        [HttpGet("ParallelBatchStores/{parallelism:int}/{setIDs:bool}")]
        public async Task ParallelBatchStores(int parallelism, bool setIDs)
        {
            await Store.TestInstance.ParallelBatchStores(Store.Node1Instance, parallelism, setIDs:setIDs);
        }

        [HttpGet("BulkInsert/{setIDs:bool}")]
        public async Task BulkInsert(bool setIDs)
        {
            await Store.TestInstance.BulkInsert(Store.Node1Instance, setIDs);
        }

        [HttpGet("ParallelBulkInserts/{parallelism:int}/{setIDs:bool}")]
        public async Task ParallelBulkInserts(int parallelism, bool setIDs)
        {
            await Store.TestInstance.ParallelBulkInserts(Store.Node1Instance, parallelism, setIDs);
        }

        [HttpGet("SimpleMapIndexingAllResults/{noCaching:bool}")]
        public async Task SimpleMapIndexingAllResults(bool noCaching)
        {
            await Store.TestInstance.SimpleMapIndexingAllResults(Store.Node1Instance, noCaching);
        }

        [HttpGet("SingleDocPatchesSerial/{noCaching:bool}/{setIDs:bool}")]
        public async Task SingleDocPatchesSerial()
        {
            await Store.TestInstance.SingleDocPatchesSerial(Store.Node1Instance);
        }

        [HttpGet("SingleDocPatchesParallel/{parallelism:int}")]
        public async Task SingleDocPatchesParallel(int parallelism)
        {
            await Store.TestInstance.ParallelSingleDocPatches(Store.Node1Instance, parallelism);
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

        [HttpGet("SimpleMap100Queries/{noCaching:bool}")]
        public async Task SimpleMap100Queries(bool noCaching)
        {
            await Store.TestInstance.SimpleMap100QueriesAllResults(Store.Node1Instance,noCaching);
        }

        [HttpGet("SimpleMap100Queries/{noCaching:bool}/{parallelism:int}")]
        public async Task SimpleMap100Queries(bool noCaching, int parallelism)
        {
            await Store.TestInstance.SimpleMap100QueriesParallelAllResults(Store.Node1Instance, parallelism,noCaching);
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


      

        // GET api/values
        [Route("Stuff")]
        public IEnumerable<string> GetStuff()
        {
            return new string[] { "stuff1", "stuff2" };
        }       
    }
}
