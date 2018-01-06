using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace BenchClient.Controllers
{
    [Route("")]
    public class ValuesController : Controller
    {
        [HttpGet("ResetDatabase")]
        public async Task<string> ResetDatabase()
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.ResetDatabase(Store.Node1Instance.Urls[0], "bench");
            Store.Node1Instance = null;
            return $"total: {p.ElapsedMilliseconds}";
        }

        [HttpGet("SerialStores/{setIDs:bool}")]
        public async Task<string> SerialStores(bool setIDs)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SerialStores(Store.Node1Instance, setIDs);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("ParallelStores/{parallelism:int}/{setIDs:bool}")]
        public async Task<string> GetParallelStores(int parallelism, bool setIDs)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.ParallelStores(Store.Node1Instance, parallelism, setIDs);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SerialBatchStores/{setIDs:bool}")]
        public async Task<string> SerialBatchStores(bool setIDs)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SerialBatchStores(Store.Node1Instance, setIDs);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("ParallelBatchStores/{parallelism:int}/{setIDs:bool}")]
        public async Task<string> ParallelBatchStores(int parallelism, bool setIDs)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.ParallelBatchStores(Store.Node1Instance, parallelism, setIDs: setIDs);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("BulkInsert/{setIDs:bool}")]
        public async Task<string> BulkInsert(bool setIDs)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.BulkInsert(Store.Node1Instance, setIDs);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("ParallelBulkInserts/{parallelism:int}/{setIDs:bool}")]
        public async Task<string> ParallelBulkInserts(int parallelism, bool setIDs)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.ParallelBulkInserts(Store.Node1Instance, parallelism, setIDs);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("LoadDocumentsSerially")]
        public async Task<string> LoadDocumentsSerially()
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.LoadDocumentsSerially(Store.Node1Instance);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("LoadDocumentsByIdIn100DocsBatches")]
        public async Task<string> LoadDocumentsByIdIn100DocsBatches()
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.LoadDocumentsByIdIn100DocsBatches(Store.Node1Instance);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("LoadDocumentsParallelly/{parallelism:int}")]
        public async Task<string> LoadDocumentsParallelly(int parallelism)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.LoadDocumentsParallelly(Store.Node1Instance, parallelism);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("LoadDocumentsParallellyIn100DocsBatches/{parallelism:int}")]
        public async Task<string> LoadDocumentsParallellyIn100DocsBatches(int parallelism)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.LoadDocumentsParallellyIn100DocsBatches(Store.Node1Instance, parallelism);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("LoadDocumentsParallellyIn100DocsBatchesNoSerialization/{parallelism:int}")]
        public async Task<string> LoadDocumentsParallellyIn100DocsBatchesNoSerialization(int parallelism)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.LoadDocumentsParallellyIn100DocsBatches(Store.Node1Instance, parallelism);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SimpleMap100Queries/{noCaching:bool}")]
        public async Task<string> SimpleMap100Queries(bool noCaching)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SimpleMap100QueriesAllResults(Store.Node1Instance, noCaching);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SimpleMap100Queries/{noCaching:bool}/{parallelism:int}")]
        public async Task<string> SimpleMap100Queries(bool noCaching, int parallelism)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SimpleMap100QueriesParallelAllResults(Store.Node1Instance, parallelism, noCaching);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SimpleMap100Queries/{noCaching:bool}")]
        public async Task<string> SimpleQueryWithSimpleTransformer(bool noCaching)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SimpleQueryWithSimpleTransformer(Store.Node1Instance, noCaching);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SimpleParallel100QueriesWithSimpleTransformer/{noCaching:bool}/{parallelism:int}")]
        public async Task<string> SimpleParallel100QueriesWithSimpleTransformer(bool noCaching, int parallelism)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SimpleParallel100QueriesWithSimpleTransformer(Store.Node1Instance, parallelism, noCaching);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SimpleQueryWithComplexTransformer/{noCaching:bool}")]
        public async Task<string> SimpleParallel100QueriesWithSimpleTransformer(bool noCaching)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SimpleQueryWithComplexTransformer(Store.Node1Instance, noCaching);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("Simple100ParallelQueriesWithComplexTransformer/{noCaching:bool}/{parallelism:int}")]
        public async Task<string> Simple100ParallelQueriesWithComplexTransformer(bool noCaching, int parallelism)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.Simple100ParallelQueriesWithComplexTransformer(Store.Node1Instance, parallelism, noCaching);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SimpleMapIndexingStreamingAllResults")]
        public async Task<string> SimpleMapIndexingStreamingAllResults()
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SimpleMapIndexingStreamingAllResults(Store.Node1Instance);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SubscriptionsCheckLatency")]
        public async Task<string> SubscriptionsCheckLatency()
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SubscriptionsCheckLatency(Store.Node1Instance);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SubscriptionsSingleItemBatch")]
        public string SubscriptionsSingleItemBatch()
        {
            var p = Stopwatch.StartNew();
            Store.TestInstance.SubscriptionsSingleItemBatch(Store.Node1Instance);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("Subscriptions1KItemsBatch")]
        public string Subscriptions1KItemsBatch()
        {
            var p = Stopwatch.StartNew();
            Store.TestInstance.Subscriptions1KItemsBatch(Store.Node1Instance);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("Subscriptions20InParallel200ItemsBatch")]
        public string Subscriptions20InParallel200ItemsBatch()
        {
            var p = Stopwatch.StartNew();
            Store.TestInstance.Subscriptions20InParallel200ItemsBatch(Store.Node1Instance);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }       

        [HttpGet("SetDocCount/{docCount:int}")]
        public async Task<string> SetDocCount(int docCount)
        {
            var p = Stopwatch.StartNew();
            Store.ChangeConfig(Store.Node1Instance.Urls[0], Store.Node2Instance.Urls[0], Store.Node3Instance.Urls[0], docCount, Store.TestInstance.CacheSizeInMB);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }
        

        [HttpGet("SimpleMapIndexingAllResults/{noCaching:bool}")]
        public async Task<string> SimpleMapIndexingAllResults(bool noCaching)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SimpleMapIndexingAllResults(Store.Node1Instance, noCaching);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SingleDocPatchesSerial/{noCaching:bool}/{setIDs:bool}")]
        public async Task<string> SingleDocPatchesSerial()
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.SingleDocPatchesSerial(Store.Node1Instance);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet("SingleDocPatchesParallel/{parallelism:int}")]
        public async Task<string> SingleDocPatchesParallel(int parallelism)
        {
            var p = Stopwatch.StartNew();
            await Store.TestInstance.ParallelSingleDocPatches(Store.Node1Instance, parallelism);
            return $"total: {p.ElapsedMilliseconds}; rate: { (int)(Store.TestInstance.DocumentsCount /p.Elapsed.TotalSeconds ) }";
        }

        [HttpGet]        
        public ContentResult Index()
        {
            var response = new HttpResponseMessage(System.Net.HttpStatusCode.OK);
            var cvType = typeof(ValuesController);
            var allHttpGetAttributes = cvType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .SelectMany(x=>x.CustomAttributes)                
                .Where(x => x.AttributeType == typeof(HttpGetAttribute)).ToArray();

            var baseUrl = Request.Host.Value;
            var indexSB = new StringBuilder();
            indexSB.Append(@"<html><head><title>Raven bench client</title>
<script lang=""javascript"">
window.onload = function(){

}
var counter = 0;
function GoToPath(controller, path){
    var path = '/'+ path;

    var siblings = controller.parentNode.parentNode.children[0].children[1].children;
    
    if (!!siblings && siblings.length > 0){
        for (var i=0; i< siblings.length; i++){
            var curSibling = siblings[i];                

            if (!curSibling.tagName || curSibling.tagName.toLowerCase() !== 'input')
                continue;
            if (curSibling.type == 'number'){
                path+= '/' + siblings[i].value;
            }
            else if (curSibling.type == 'checkbox'){
                path+= '/' + siblings[i].checked;
            }            
		}  
	}		
    controller.parentNode.childNodes[1].textContent = 'waiting...';
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function(){
        if (this.readyState > 2 && this.status == 200) {
            controller.parentNode.childNodes[1].textContent = this.responseText;            
        }
    };
    xhttp.open('GET', path, true);
    xhttp.send();
    
    //window.open(path, '_newtab'+counter);
    counter++;
}

</script>   
<style>

.item {
    border:black 1px inset;
    margin-left:25%;
    margin-right:25%;
}
.title {
    text-decoration: underline;
    font-weight:bold;
    margin-bottom:5px;
}
.action{
width:60%;
display: inline-block;
}
.goto{
width:40%;
display: inline-block;
}


</style>

</head><body>");

            foreach (var httpGetAttribute in allHttpGetAttributes)
            {
                if (httpGetAttribute.ConstructorArguments.Count == 0)
                    continue;

                indexSB.Append(@"<div class=""item"">");
                indexSB.Append(@"<div class=""action"">");
                
                var httpGetParam = httpGetAttribute.ConstructorArguments[0];
                var path = httpGetParam.Value.ToString();
                var parts = path.Split('/');

                indexSB.Append($@"<span class=""title"">{parts[0]}:</span>");
                indexSB.Append("<div>");
                if (parts.Length > 1)                
                {
                    var urlWithoutParams = $"{baseUrl}/{parts[0]}";
                    var paramsCount = parts.Length - 1;
                    
                    foreach (var paramPart in parts.Skip(1))
                    {
                        var woBracets = paramPart.Substring(1, paramPart.Length - 2);
                        var paramAndName = woBracets.Split(":");

                        indexSB.Append($"   <label>{paramAndName[0]}</label>");
                        switch (paramAndName[1])
                        {
                            case "int":
                                indexSB.Append("<input type=\"number\" value=\"1\"/>");
                                break;
                            case "bool":
                                indexSB.Append("<input type=\"checkbox\"/>");
                                break;
                        }                        
                    }
                }
                indexSB.Append(@"</div>");
                indexSB.Append(@"</div>");
                indexSB.Append(@"<div class=""goto"">");
                indexSB.Append($"<input type=\"button\"onclick=\"GoToPath(this,'{parts[0]}')\" value='GO!'>");
                indexSB.Append($"<span></span>");
                indexSB.Append("</div>");
                indexSB.Append("</div>");
                indexSB.Append("</div>");

            }

            indexSB.Append("</body></html>");

            return new ContentResult
            {
                ContentType = "text/html",
                StatusCode = (int)HttpStatusCode.OK,
                Content = indexSB.ToString()
            };            
        }



    }
}
